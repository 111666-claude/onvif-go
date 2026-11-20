package onvif_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/0x524a/onvif-go"
	"github.com/0x524a/onvif-go/server"
)

// motionEvent builds the door-camera motion-alarm notification the whole
// reliable-stream feature is framed around.
func motionEvent(state string) server.EventInput {
	return server.EventInput{
		Topic:             "tns1:VideoSource/MotionAlarm",
		ProducerAddress:   "urn:uuid:door-camera-1",
		PropertyOperation: "Changed",
		Source: []server.SimpleItem{
			{Name: "VideoSourceToken", Value: "VideoSource_1"},
		},
		Data: []server.SimpleItem{
			{Name: "State", Value: state},
		},
	}
}

// startEventServer boots an event-enabled virtual camera on an ephemeral
// port and returns the running server, its event-service endpoint and
// credentials. Cleanup is registered on t.
func startEventServer(t *testing.T, hooks server.EventTestHooks) (*server.Server, string, string, string) {
	t.Helper()

	config := server.DefaultConfig()
	config.Host = "127.0.0.1"
	config.Port = 0
	config.Output = discardWriter{}
	config.SupportEvents = true
	config.Username = "admin"
	config.Password = "password"
	ready := make(chan struct{})
	config.Ready = ready

	srv, err := server.New(config)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetEventTestHooks(hooks)
	srv.EnableLongPolling()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop after cancel")
		}
	})

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("event server never became ready")
	case err := <-errCh:
		t.Fatalf("server exited before ready: %v", err)
	}

	endpoint := "http://" + srv.Addr() + config.BasePath + "/events_service"

	return srv, endpoint, config.Username, config.Password
}

// discardWriter is io.Discard without importing io in every helper.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// fastStreamConfig shrinks every timing constant so failure-mode tests run
// in milliseconds instead of minutes, while keeping the production defaults
// exercised by the defaults test separate. The invariant
// PullTimeout + 2*RenewLeadTime < SubscriptionTermination (enforced by
// withDefaults) is preserved here explicitly.
func fastStreamConfig() onvif.EventStreamConfig {
	return onvif.EventStreamConfig{
		Filter:                  "",
		SubscriptionTermination: 2 * time.Second,
		RenewLeadTime:           500 * time.Millisecond,
		PullTimeout:             500 * time.Millisecond,
		MessageLimit:            20,
		QueueSize:               16,
		DedupWindow:             30 * time.Second,
		DedupMaxKeys:            128,
		ReconnectBackoff:        50 * time.Millisecond,
		MaxReconnectBackoff:     250 * time.Millisecond,
		ResubscribeAttempts:     4,
		CloseTimeout:            3 * time.Second,
	}
}

// runStream starts stream.Run in a goroutine and returns a channel carrying
// its terminal error.
func runStream(t *testing.T, ctx context.Context, stream *onvif.EventStream) <-chan error {
	t.Helper()

	errCh := make(chan error, 1)
	go func() { errCh <- stream.Run(ctx) }()

	return errCh
}

// waitEvent reads one event from ch or fails the test.
func waitEvent(t *testing.T, ch <-chan onvif.StreamEvent, timeout time.Duration, what string) onvif.StreamEvent {
	t.Helper()

	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)

		return onvif.StreamEvent{}
	}
}

// TestEventStreamDeliversNormalizedEvents is the happy path: the virtual
// camera produces motion events and the stream hands back normalized events
// carrying source, time and topic.
func TestEventStreamDeliversNormalizedEvents(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := runStream(t, ctx, stream)

	// Wait until the pull loop is actually waiting, then produce two events.
	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("PullMessages never entered its long-poll")
	}

	if err := srv.PostEvent(motionEvent("true")); err != nil {
		t.Fatalf("PostEvent: %v", err)
	}
	if err := srv.PostEvent(motionEvent("false")); err != nil {
		t.Fatalf("PostEvent: %v", err)
	}

	first := waitEvent(t, stream.Events, 3*time.Second, "first motion event")
	second := waitEvent(t, stream.Events, 3*time.Second, "second motion event")

	if first.Topic != "tns1:VideoSource/MotionAlarm" {
		t.Errorf("Topic = %q", first.Topic)
	}
	if first.ProducerAddress != "urn:uuid:door-camera-1" {
		t.Errorf("ProducerAddress = %q", first.ProducerAddress)
	}
	if first.Time.IsZero() {
		t.Error("device Time should be populated")
	}
	if first.LocalTime.IsZero() {
		t.Error("LocalTime should always be populated")
	}
	if len(first.Source) != 1 || first.Source[0].Name != "VideoSourceToken" {
		t.Errorf("Source not normalized: %+v", first.Source)
	}
	if len(first.Data) != 1 || first.Data[0].Value != "true" {
		t.Errorf("Data not normalized: %+v", first.Data)
	}
	if first.ID == "" || len(first.ID) != 64 {
		t.Errorf("ID should be a hex SHA-256 digest, got %q", first.ID)
	}
	if first.Sequence != 1 {
		t.Errorf("first Sequence = %d, want 1", first.Sequence)
	}
	if second.Sequence != 2 {
		t.Errorf("second Sequence = %d, want 2 (gap-free)", second.Sequence)
	}
	if second.ID == first.ID {
		t.Error("distinct state changes must have distinct IDs")
	}

	// Orderly shutdown: Close unsubscribes remotely and Run returns
	// ErrEventStreamClosed.
	stream.Close()
	select {
	case err := <-runErr:
		if !errors.Is(err, onvif.ErrEventStreamClosed) {
			t.Fatalf("Run after Close returned %v, want ErrEventStreamClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Close")
	}

	stats := srv.EventStats()
	if stats.Unsubscribes != 1 {
		t.Errorf("expected exactly one remote Unsubscribe, got %d", stats.Unsubscribes)
	}
	if stats.SubscriptionsCreated < 1 {
		t.Error("expected at least one subscription creation")
	}
}

// TestEventStreamRenewsBeforeExpiry proves the subscription does not silently
// stop: with a 2s lifetime the stream must renew while events keep flowing.
func TestEventStreamRenewsBeforeExpiry(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	cfg := fastStreamConfig()
	stream := client.NewEventStream(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := runStream(t, ctx, stream)

	// Let at least two renewal cycles elapse (2s subscription, 0.75s lead).
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			if got := srv.EventStats().Renews; got < 2 {
				t.Fatalf("expected >=2 renewals over the window, got %d", got)
			}
		case <-time.After(200 * time.Millisecond):
			if srv.EventStats().Renews >= 2 {
				goto renewed
			}
		}
		if srv.EventStats().Renews >= 2 {
			break
		}
	}
renewed:

	// Events still flow after renewal: the subscription never went away.
	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no active long-poll after renewals")
	}
	if err := srv.PostEvent(motionEvent("true")); err != nil {
		t.Fatalf("PostEvent after renewal: %v", err)
	}
	ev := waitEvent(t, stream.Events, 3*time.Second, "post-renewal event")
	if ev.Topic == "" {
		t.Fatal("received empty event after renewal")
	}

	stream.Close()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}

	if srv.EventStats().SubscriptionsExpired != 0 {
		t.Error("stream should renew before the server-side expiry timer fires")
	}
}

// TestEventStreamDeduplicatesRedelivery posts a byte-identical notification
// twice (the camera redelivering after a reconnect). Only one event may be
// emitted, and the duplicate must show up in stats.
func TestEventStreamDeduplicatesRedelivery(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := runStream(t, ctx, stream)

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll waiting")
	}

	if err := srv.PostEvent(motionEvent("true")); err != nil {
		t.Fatalf("PostEvent: %v", err)
	}
	first := waitEvent(t, stream.Events, 3*time.Second, "original event")

	// Redelivery: identical topic/items/UtcTime, as a real camera would
	// resend after a dropped connection.
	if err := srv.PostEventRepeat(); err != nil {
		t.Fatalf("PostEventRepeat: %v", err)
	}

	// Then a genuinely new event must still get through and prove the stream
	// did not stall.
	if err := srv.PostEvent(motionEvent("false")); err != nil {
		t.Fatalf("PostEvent: %v", err)
	}
	second := waitEvent(t, stream.Events, 3*time.Second, "event after redelivery")

	if second.ID == first.ID {
		t.Error("the new state change was suppressed as a duplicate")
	}
	if second.Sequence != first.Sequence+1 {
		t.Errorf("sequence gap after suppressed duplicate: %d -> %d",
			first.Sequence, second.Sequence)
	}

	stats := stream.Stats()
	if stats.Duplicates < 1 {
		t.Errorf("expected Duplicates >= 1, got %+v", stats)
	}
	if stats.Delivered != 2 {
		t.Errorf("expected exactly 2 delivered events, got %d", stats.Delivered)
	}

	stream.Close()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// TestEventStreamReconnectsAfterPullFault forces a PullMessages fault: the
// stream must re-create the subscription and keep delivering, with the
// replay cache intact so a redelivered event is suppressed.
func TestEventStreamReconnectsAfterPullFault(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := runStream(t, ctx, stream)

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll waiting")
	}
	if err := srv.PostEvent(motionEvent("true")); err != nil {
		t.Fatalf("PostEvent: %v", err)
	}
	first := waitEvent(t, stream.Events, 3*time.Second, "event before fault")

	// Inject one pull fault. The stream should tear down and re-create the
	// pull point, then resume.
	srv.SetEventTestHooks(server.EventTestHooks{RejectPull: true})

	statsOK := make(chan struct{})
	go func() {
		for {
			if stream.Stats().Reconnects >= 1 {
				close(statsOK)

				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	select {
	case <-statsOK:
	case <-time.After(5 * time.Second):
		t.Fatalf("stream did not reconnect; stats=%+v", stream.Stats())
	}

	srv.SetEventTestHooks(server.EventTestHooks{})

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll after reconnect")
	}
	if err := srv.PostEvent(motionEvent("false")); err != nil {
		t.Fatalf("PostEvent after reconnect: %v", err)
	}
	second := waitEvent(t, stream.Events, 3*time.Second, "event after reconnect")

	if second.Sequence != first.Sequence+1 {
		t.Errorf("sequence not continuous across reconnect: %d -> %d",
			first.Sequence, second.Sequence)
	}

	stream.Close()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}

	if created := srv.EventStats().SubscriptionsCreated; created < 2 {
		t.Errorf("expected a fresh subscription after the fault, got %d creates", created)
	}
}

// TestEventStreamReconnectsAfterSilentExpiry simulates the exact failure the
// requirement names: a subscription that expires between pulls. The next
// pull faults, the stream re-subscribes and continuity is preserved.
func TestEventStreamReconnectsAfterSilentExpiry(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := runStream(t, ctx, stream)

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll waiting")
	}
	if err := srv.PostEvent(motionEvent("true")); err != nil {
		t.Fatalf("PostEvent: %v", err)
	}
	first := waitEvent(t, stream.Events, 3*time.Second, "pre-expiry event")

	srv.SetEventTestHooks(server.EventTestHooks{ExpireOnNextPull: true})

	// Wait for the reconnection counters to move.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && stream.Stats().Reconnects == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if stream.Stats().Reconnects == 0 {
		t.Fatalf("stream did not reconnect after silent expiry; stats=%+v", stream.Stats())
	}

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll after expiry recovery")
	}
	if err := srv.PostEvent(motionEvent("false")); err != nil {
		t.Fatalf("PostEvent after recovery: %v", err)
	}
	second := waitEvent(t, stream.Events, 3*time.Second, "post-recovery event")

	if second.Sequence != first.Sequence+1 {
		t.Errorf("sequence gap across expiry recovery: %d -> %d",
			first.Sequence, second.Sequence)
	}

	stream.Close()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// TestEventStreamRenewRejectedTriggersResubscribe makes Renew fail and
// asserts the stream re-creates the subscription rather than quietly
// stopping.
func TestEventStreamRenewRejectedTriggersResubscribe(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{RejectRenew: true})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	cfg := fastStreamConfig()
	// Renewal window arrives quickly so the test does not linger.
	cfg.SubscriptionTermination = time.Second
	cfg.RenewLeadTime = 400 * time.Millisecond
	stream := client.NewEventStream(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := runStream(t, ctx, stream)

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if stream.Stats().RenewFailures >= 1 && stream.Stats().Reconnects >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats := stream.Stats()
	if stats.RenewFailures == 0 {
		t.Errorf("expected a rejected renewal, stats=%+v", stats)
	}
	if stats.Reconnects == 0 {
		t.Errorf("expected a reconnect after rejected renewal, stats=%+v", stats)
	}

	// Recover: stop rejecting, and events should flow again.
	srv.SetEventTestHooks(server.EventTestHooks{})

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll after renew rejection recovery")
	}
	if err := srv.PostEvent(motionEvent("true")); err != nil {
		t.Fatalf("PostEvent after recovery: %v", err)
	}
	ev := waitEvent(t, stream.Events, 3*time.Second, "post-recovery event")
	if ev.Topic == "" {
		t.Fatal("empty event after recovery")
	}

	stream.Close()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// TestEventStreamAuthFailureIsTerminal verifies an authentication rejection
// stops the stream with ErrSubscriptionLost instead of retrying forever.
func TestEventStreamAuthFailureIsTerminal(t *testing.T) {
	_, endpoint, user, pass := startEventServer(t, server.EventTestHooks{RejectAuth: true})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = stream.Run(ctx)
	if !errors.Is(err, onvif.ErrSubscriptionLost) {
		t.Fatalf("Run = %v, want ErrSubscriptionLost", err)
	}
	if !strings.Contains(err.Error(), "authentication") {
		t.Errorf("error should mention authentication, got %v", err)
	}
}

// TestEventStreamSlowConsumerDropsAndReports fills the output buffer with a
// non-reading consumer and asserts the stream keeps pulling (subscription
// stays alive), drops are counted, and the callback fires. After draining,
// events flow again with a gap-free sequence.
func TestEventStreamSlowConsumerDropsAndReports(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	cfg := fastStreamConfig()
	cfg.QueueSize = 2 // Tiny buffer to force drops quickly.
	var dropped []onvif.StreamEvent
	var mu sync.Mutex
	cfg.OnEventDropped = func(ev onvif.StreamEvent, dropErr error) {
		mu.Lock()
		defer mu.Unlock()
		if !errors.Is(dropErr, onvif.ErrEventDropped) {
			t.Errorf("drop callback error = %v, want ErrEventDropped", dropErr)
		}
		dropped = append(dropped, ev)
	}

	stream := client.NewEventStream(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := runStream(t, ctx, stream)

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll waiting")
	}

	// Flood distinct events (different State values => distinct IDs).
	for i := range 20 {
		if err := srv.PostEvent(motionEvent(fmt.Sprintf("state-%d", i))); err != nil {
			t.Fatalf("PostEvent %d: %v", i, err)
		}
	}

	// Wait for drops to be recorded.
	dropDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(dropDeadline) && stream.Stats().Dropped == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if stream.Stats().Dropped == 0 {
		t.Fatalf("expected drops with a stalled consumer, stats=%+v", stream.Stats())
	}

	mu.Lock()
	if len(dropped) == 0 {
		mu.Unlock()
		t.Fatal("OnEventDropped never fired")
	}
	mu.Unlock()

	// Drain whatever made the buffer; the subscription must still be alive.
	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("pull loop died under backpressure")
	}

	// Drain the buffered events, then confirm fresh events still arrive and
	// sequences are gap-free among delivered events.
drain:
	for {
		select {
		case <-stream.Events:
		case <-time.After(300 * time.Millisecond):
			break drain
		}
	}

	if err := srv.PostEvent(motionEvent("recovered")); err != nil {
		t.Fatalf("PostEvent after drain: %v", err)
	}
	ev := waitEvent(t, stream.Events, 3*time.Second, "post-drain event")
	if ev.Sequence == 0 {
		t.Error("delivered event must carry a sequence")
	}

	stream.Close()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// TestEventStreamClockSkewIsMeasured offsets the virtual camera's clock and
// asserts the stream reports a matching skew estimate while still delivering
// events with both timestamps populated.
func TestEventStreamClockSkewIsMeasured(t *testing.T) {
	const skew = 2 * time.Minute
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{ClockOffset: skew})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := runStream(t, ctx, stream)

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll waiting")
	}
	if err := srv.PostEvent(motionEvent("true")); err != nil {
		t.Fatalf("PostEvent: %v", err)
	}
	ev := waitEvent(t, stream.Events, 3*time.Second, "skewed-clock event")

	if ev.Time.IsZero() || ev.LocalTime.IsZero() {
		t.Fatal("both timestamps must be populated")
	}
	// The device timestamp should be roughly skew ahead of the local
	// receive timestamp (allow slack for scheduling and rounding).
	delta := ev.Time.Sub(ev.LocalTime)
	if delta < skew-30*time.Second || delta > skew+30*time.Second {
		t.Errorf("device/local time delta = %v, want ~%v", delta, skew)
	}

	measured := stream.Stats().ClockSkew
	if measured < skew-30*time.Second || measured > skew+30*time.Second {
		t.Errorf("reported ClockSkew = %v, want ~%v", measured, skew)
	}

	stream.Close()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// TestEventStreamParentContextCancellation verifies canceling the caller's
// context stops Run cleanly and releases the goroutine, with a remote
// unsubscribe.
func TestEventStreamParentContextCancellation(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- stream.Run(ctx) }()

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll waiting")
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, onvif.ErrEventStreamClosed) {
			t.Fatalf("Run after ctx cancel = %v, want ErrEventStreamClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	if srv.EventStats().Unsubscribes != 1 {
		t.Errorf("expected remote unsubscribe on shutdown, got %d",
			srv.EventStats().Unsubscribes)
	}

	// Close must remain idempotent after context-driven shutdown.
	stream.Close()
	stream.Close()
}

// TestEventStreamCloseBeforeRun is safe: closing a stream that never started
// must not panic or block.
func TestEventStreamCloseBeforeRun(t *testing.T) {
	_, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())
	stream.Close()
	stream.Close()
}

// TestEventStreamRunRejectsConcurrentRun ensures two Run calls cannot share
// one stream.
func TestEventStreamRunRejectsConcurrentRun(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := runStream(t, ctx, stream)

	if !srv.WaitForPullWaiters(1, 3*time.Second) {
		t.Fatal("no long-poll waiting")
	}

	if err := stream.Run(ctx); !errors.Is(err, onvif.ErrEventStreamRunning) {
		t.Fatalf("second Run = %v, want ErrEventStreamRunning", err)
	}

	stream.Close()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// TestEventStreamRunCanRestart verifies a stream can be Run again after a
// clean shutdown and that Close keeps working across restarts - the stop
// handle must be re-armed per Run rather than consumed once.
func TestEventStreamRunCanRestart(t *testing.T) {
	srv, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	stream := client.NewEventStream(fastStreamConfig())

	for round := range 2 {
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- stream.Run(ctx) }()

		if !srv.WaitForPullWaiters(1, 3*time.Second) {
			cancel()
			t.Fatalf("round %d: no long-poll waiting", round)
		}

		// Close (not just cancel) must stop this Run and unsubscribe.
		stream.Close()
		select {
		case err := <-errCh:
			if !errors.Is(err, onvif.ErrEventStreamClosed) {
				t.Fatalf("round %d: Run = %v, want ErrEventStreamClosed", round, err)
			}
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("round %d: Run did not stop on Close", round)
		}
		cancel()
	}

	if got := srv.EventStats().Unsubscribes; got < 2 {
		t.Errorf("expected an unsubscribe per run, got %d", got)
	}
}

// TestEventStreamDefaults verifies the zero-value config yields the documented
// production defaults and clamps an unsafe renew lead time.
func TestEventStreamDefaults(t *testing.T) {
	_, endpoint, user, pass := startEventServer(t, server.EventTestHooks{})

	client, err := onvif.NewClient(endpoint, onvif.WithCredentials(user, pass))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// A zero config must not panic and must construct a usable stream.
	stream := client.NewEventStream(onvif.EventStreamConfig{})
	if cap(stream.Events) == 0 {
		t.Error("default Events channel should be buffered")
	}

	// Lead-time clamp: lead >= lifetime would renew on every loop.
	cfg := onvif.EventStreamConfig{
		SubscriptionTermination: time.Second,
		RenewLeadTime:           time.Hour,
	}
	_ = client.NewEventStream(cfg)
}
