# Reliable Event Stream

The event service methods in `event.go` (`CreatePullPointSubscription`,
`PullMessages`, `RenewSubscription`, `Unsubscribe`) map one SOAP call each.
Calling them directly leaves every reliability problem to the caller: a
subscription that expires between pulls silently stops delivering, a dropped
connection needs a manual re-subscribe, and a camera that redelivers
notifications produces duplicate events.

`EventStream` (`event_stream.go`) wraps those primitives in one self-managing
loop. It is built for recorders that must not miss a door-camera motion event:
the stream stays subscribed on its own and emits normalized, de-duplicated
events until the caller stops it.

## Quick start

```go
stream := client.NewEventStream(onvif.EventStreamConfig{}) // zero config = production defaults

go func() {
    if err := stream.Run(ctx); !errors.Is(err, onvif.ErrEventStreamClosed) {
        log.Printf("event stream ended: %v", err)
    }
}()

for event := range stream.Events {
    recorder.Record(event.Topic, event.Time, event.Source, event.Data)
}

stream.Close() // best-effort remote Unsubscribe, then releases the goroutine
```

`Run` blocks; `Events` is a buffered channel; `Close` is idempotent and safe
to call concurrently, from another goroutine, or before `Run` has started.

## Normalized events

Each `StreamEvent` is the wire `NotificationMessage` flattened for direct
consumption:

| Field              | Meaning                                                        |
| ------------------ | -------------------------------------------------------------- |
| `ID`               | Stable SHA-256 identity over topic, time, producer and items.  |
| `Sequence`         | 1-based, gap-free across the stream's lifetime.                |
| `Topic`            | ONVIF topic, e.g. `tns1:VideoSource/MotionAlarm`.              |
| `Time`             | Device-assigned `UtcTime` (zero when the camera omitted it).   |
| `LocalTime`        | Local receive time; always set, order on this if clocks drift. |
| `ProducerAddress`  | Lens/device that produced the event (`ProducerReference`).     |
| `PropertyOperation`| `Initialized` / `Changed` / `Deleted` / `""`.                   |
| `Source`, `Key`, `Data` | The `tt:SimpleItem` lists, verbatim.                     |

## Failure policies

The failure modes below each have an explicit policy rather than relying on
caller behavior.

### Subscription expiry and renewal

A timer fires `RenewLeadTime` **before** the reported termination time,
independently of any in-flight long-poll. Renewal and pulling never overlap on
the wire: when the timer fires during a pull, the bounded pull is allowed to
finish (canceling its SOAP request would tear down an HTTP connection some
cameras treat as the subscription itself) and its messages are still
delivered. The stream renews before the camera-side expiry timer can fire, so
the event flow does not silently stop.

### Transient disconnect and re-subscription

A failed pull or a refused renewal drops the subscription reference and
re-creates it from scratch after exponential backoff
(`ReconnectBackoff` doubling up to `MaxReconnectBackoff`). The replay cache
survives the reconnect, so notifications the camera redelivers during the gap
are suppressed. After `ResubscribeAttempts` consecutive failures `Run` returns
`ErrSubscriptionLost`; the wrapped cause remains available via `errors.As`.

### Duplicate notifications

ONVIF pull points do not assign message IDs, so identity is derived from the
notification content: topic, producer, property operation, device `UtcTime`
and every source/key/data item. The same notification redelivered after a
reconnect hashes to the same `ID` and is suppressed; a bounded, time-windowed
cache (`DedupWindow`, `DedupMaxKeys`) bounds memory and evicts oldest-first.
Events without a device `UtcTime` still de-duplicate on content because the
local receive time is deliberately excluded from the identity.

### Camera clock skew

Every create/renew/pull response carrying a `CurrentTime` updates
`EventStreamStats.ClockSkew` (device time minus local time). The renewal
deadline is computed from the device-reported termination time on the local
clock, so a skewed camera clock does not shift when renewal happens.
`StreamEvent.Time` stays the device timestamp; `StreamEvent.LocalTime` is the
local receive time for ordering when the camera clock is untrustworthy.

### Authentication expiry

Credential rejections (HTTP 401/403 or an authentication SOAP fault, matched
by status and reason text) are terminal: retrying identical requests cannot
help until credentials are rotated via `Client.SetCredentials`. `Run` returns
immediately with `ErrSubscriptionLost` wrapping the auth error instead of
burning the reconnect budget.

### Slow consumer

`PullMessages` must keep being called or the camera's pull-point buffer fills
and the subscription can expire, so the pulling goroutine **never blocks on a
slow consumer**. When the buffered `Events` channel (`QueueSize`) is full the
newest event is dropped, `EventStreamStats.Dropped` is incremented and
`OnEventDropped` (if configured) is invoked. `Sequence` remains gap-free among
delivered events because a dropped event never consumes a sequence number.

### Cancellation and shutdown

Both parent-context cancellation and `Close` return `ErrEventStreamClosed`
from `Run`. On exit the stream best-effort calls `Unsubscribe` on a detached
context (the caller's context may already be done) so the camera frees the
pull-point slot, then the single Run goroutine exits. A stream can be `Run`
again after a clean shutdown; `Close` remains effective across restarts.

## Configuration

`EventStreamConfig` zero values resolve to the production defaults in
`event_stream.go`. The invariants `withDefaults` enforces:

- `RenewLeadTime` below the subscription lifetime (clamped to half), so
  renewal cannot fire on every iteration;
- `PullTimeout + 2 * RenewLeadTime <= SubscriptionTermination`, so one
  long-poll fits inside one lifetime with renewal and round-trip slack;
- positive `MessageLimit`, bounded `QueueSize`, and capped reconnect backoff.

## Observability

`Stats()` returns a snapshot: `Delivered`, `Duplicates`, `Dropped`,
`PullErrors`, `Reconnects`, `Renewals`, `RenewFailures` and `ClockSkew`. The
virtual camera's `EventStats()` mirrors the server-side operation counts,
which the tests in `event_stream_test.go` use to assert renewal, reconnect and
unsubscribe behavior without timing assumptions.

## Virtual camera support

The virtual camera (`server` package) gained the producer side needed to test
all of this:

- `Server.PostEvent(EventInput)` enqueues a notification and unblocks a
  long-poll immediately;
- `Server.PostEventRepeat()` redelivers the last notification byte-identically;
- `Server.EnableLongPolling()` switches `PullMessages` from immediate empty
  responses to blocking long-polls;
- `Server.SetEventTestHooks(EventTestHooks)` injects auth rejection, renew
  rejection, pull faults, silent subscription expiry and a clock offset;
- `Server.WaitForPullWaiters()` and `Server.EventStats()` make tests
  deterministic without sleeps.
