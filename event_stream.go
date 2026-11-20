package onvif

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strings"
	"sync"
	"time"
)

// Reliable event-stream errors.
var (
	// ErrEventStreamClosed is returned by Run after Close (or parent-context
	// cancellation) has stopped the stream. It is the one non-nil terminal
	// error that does not indicate a device problem.
	ErrEventStreamClosed = errors.New("event stream closed")

	// ErrEventStreamRunning is returned by Run when the stream already has a
	// Run goroutine active.
	ErrEventStreamRunning = errors.New("event stream is already running")

	// ErrEventDropped is reported through EventStreamConfig.OnEventDropped
	// when the output channel cannot keep up and a notification had to be
	// discarded to protect the pulling goroutine and the camera's pull-point
	// buffer.
	ErrEventDropped = errors.New("event stream consumer is too slow")

	// ErrSubscriptionLost is returned after the device repeatedly refused to
	// create or renew a pull-point subscription, so the stream can no longer
	// make progress. Wrapped transient errors remain available via errors.As/Is.
	ErrSubscriptionLost = errors.New("event subscription lost")
)

// Reliable event-stream timing defaults. Values are exported implicitly
// through EventStreamConfig zero-value handling; the constants document the
// numbers and keep tests and documentation in one place.
const (
	defaultStreamSubscriptionTermination = 5 * time.Minute
	defaultStreamReconnectBackoff        = 2 * time.Second
	defaultStreamMaxReconnectBackoff     = 30 * time.Second
	defaultStreamRenewLeadTime           = 30 * time.Second
	defaultStreamPullTimeout             = 30 * time.Second
	defaultStreamMessageLimit            = 100
	defaultStreamQueueSize               = 256
	defaultStreamDedupWindow             = 10 * time.Minute
	defaultStreamDedupMaxKeys            = 4096
	defaultStreamResubscribeAttempts     = 3
	defaultStreamCloseTimeout            = 10 * time.Second

	// minPullTimeout floors an effective pull timeout shortened by the
	// renewal-margin cap, so the wire value never becomes PT0S.
	minPullTimeout = 100 * time.Millisecond
)

// StreamEvent is a normalized ONVIF notification: the wire-level
// NotificationMessage flattened so a recorder never has to reach into nested
// structs, with stable identity and ordering fields filled in by the stream.
type StreamEvent struct {
	// ID is the stable identity of the notification, used for de-duplication.
	// It is derived from topic, source/key/data items, producer address and
	// the device-assigned UtcTime: the same notification redelivered after a
	// reconnect hashes to the same ID, while two distinct state changes do
	// not. IDs are hex SHA-256 digests, safe to use as map keys or log tokens.
	ID string

	// Sequence is 1-based and gap-free for the lifetime of the stream: it
	// increments once per delivered event, after de-duplication, so a recorder
	// can assert continuity by checking Sequence == previous+1. Reconnects,
	// renewals and dropped duplicates never create gaps.
	Sequence uint64

	// Topic is the ONVIF topic expression, e.g.
	// "tns1:VideoSource/MotionAlarm".
	Topic string

	// Time is the event time the device assigned (tt:Message/@UtcTime). It is
	// zero when the device omitted the attribute; LocalTime then carries the
	// only available timestamp.
	Time time.Time

	// LocalTime is when the stream received and normalized the event, using
	// the local clock. It is always set and is the field to order on when a
	// device clock is unreliable.
	LocalTime time.Time

	// ProducerAddress is the wsnt:ProducerReference/Address, identifying the
	// device (or lens, on multi-lens cameras) that produced the event. Empty
	// when the device omitted it.
	ProducerAddress string

	// PropertyOperation is the tt:Message/@PropertyOperation value
	// ("Initialized", "Changed", "Deleted", "" when absent).
	PropertyOperation string

	// Source, Key and Data carry the tt:SimpleItem lists verbatim.
	Source []SimpleItem
	Key    []SimpleItem
	Data   []SimpleItem
}

// EventStreamConfig configures an EventStream. The zero value is valid: every
// field falls back to a documented default.
type EventStreamConfig struct {
	// Filter is an optional wsnt:TopicExpression passed to
	// CreatePullPointSubscription. Empty means no filter.
	Filter string

	// SubscriptionTermination is the lifetime requested for each
	// subscription and renewal. Must be positive; zero uses the default.
	SubscriptionTermination time.Duration

	// RenewLeadTime is how long before the reported termination time a
	// renewal is attempted. It must stay well below SubscriptionTermination;
	// zero uses the default.
	RenewLeadTime time.Duration

	// PullTimeout is the long-poll timeout sent in PullMessages. A blocking
	// long poll is what keeps the subscription alive on cameras that expire
	// idle subscriptions, and bounds how long cancellation waits. Zero uses
	// the default.
	PullTimeout time.Duration

	// MessageLimit is the PullMessages MessageLimit (maximum notifications
	// returned per call). Must be positive; zero uses the default.
	MessageLimit int

	// QueueSize bounds the buffered output channel. When the consumer cannot
	// keep up and the buffer is full, the newest event is dropped and, if set,
	// OnEventDropped is invoked - pulling must never block behind a slow
	// consumer, because a stalled pull loop lets the subscription expire.
	// Zero uses the default; negative values are treated as zero (unbuffered,
	// drop-on-first-slow-read).
	QueueSize int

	// DedupWindow is how long a notification ID stays in the replay cache.
	// Redelivery after a reconnect typically arrives within seconds; the
	// default tolerates long outages. Zero uses the default.
	DedupWindow time.Duration

	// DedupMaxKeys bounds the replay cache so an event burst cannot grow it
	// without limit. When the cache is full its oldest entry is evicted.
	// Zero uses the default.
	DedupMaxKeys int

	// ReconnectBackoff is the initial wait after a failed pull/create/renew
	// before retrying. It doubles on each consecutive failure up to
	// MaxReconnectBackoff and resets after a successful PullMessages.
	// Zero uses the default.
	ReconnectBackoff time.Duration

	// MaxReconnectBackoff caps the exponential reconnect backoff. Zero uses
	// the default.
	MaxReconnectBackoff time.Duration

	// ResubscribeAttempts is how many consecutive CreatePullPointSubscription
	// (or renewal) failures are tolerated before Run gives up with
	// ErrSubscriptionLost. Zero uses the default.
	ResubscribeAttempts int

	// CloseTimeout bounds how long Close waits for the pulling goroutine to
	// exit after Unsubscribe. Zero uses the default.
	CloseTimeout time.Duration

	// OnEventDropped, when non-nil, is called with the event that could not be
	// enqueued and ErrEventDropped. It runs on the pulling goroutine, so it
	// must be cheap and must not block on the Events channel.
	OnEventDropped func(event StreamEvent, err error)
}

// EventStreamStats is a point-in-time snapshot of stream counters, returned
// by EventStream.Stats. All counters are safe to read while the stream runs.
type EventStreamStats struct {
	// Delivered is the number of events sent on the Events channel.
	Delivered uint64
	// Duplicates is the number of notifications suppressed because their ID
	// was already in the replay cache.
	Duplicates uint64
	// Dropped is the number of events discarded due to a full output buffer.
	Dropped uint64
	// PullErrors is the number of PullMessages calls that failed.
	PullErrors uint64
	// Reconnects is the number of times a subscription had to be re-created
	// after a failure or expiry.
	Reconnects uint64
	// Renewals is the number of successful Renew calls.
	Renewals uint64
	// RenewFailures is the number of Renew calls that failed.
	RenewFailures uint64
	// ClockSkew is device-time minus local-time estimated from the most recent
	// Create/Renew/Pull response that carried both clocks. Zero until at least
	// one such response has been seen.
	ClockSkew time.Duration
}

// EventStream maintains a reliable, self-renewing ONVIF pull-point
// subscription and emits normalized, de-duplicated events.
//
// Lifecycle:
//
//	stream := client.NewEventStream(cfg)
//	go func() {
//	    if err := stream.Run(ctx); err != nil { ... }
//	}()
//	for ev := range stream.Events { ... } // or select on it
//	stream.Close() // idempotent; unsubscribes remotely and stops Run
//
// A single Run goroutine performs every SOAP call, so renewal, pulling and
// re-subscription never race each other on the wire. Close and a canceled
// parent context are the only concurrent inputs, both handled through one
// internal cancel.
type EventStream struct {
	client *Client
	cfg    EventStreamConfig

	// Events carries normalized notifications. It is never closed by the
	// stream: Run's return marks the end, and closing a channel a consumer
	// might still select on would risk a panic on the sender side. Buffering
	// is fixed at construction from QueueSize.
	Events <-chan StreamEvent
	out    chan StreamEvent

	// runMu makes Run single-use at a time and guards running/stop; closeMu
	// serializes Close calls.
	runMu   sync.Mutex
	running bool
	closeMu sync.Mutex
	stop    context.CancelFunc

	statsMu sync.RWMutex
	stats   EventStreamStats

	// sequence counts delivered, de-duplicated events and is the source of
	// StreamEvent.Sequence. Only the Run goroutine writes it.
	sequence uint64

	// dedup is the bounded replay cache; only the Run goroutine touches it,
	// so it needs no lock of its own.
	dedup *dedupCache
}

// NewEventStream creates an EventStream with the given configuration. The
// zero-value config yields production-safe defaults. The stream does nothing
// until Run is called.
func (c *Client) NewEventStream(config EventStreamConfig) *EventStream {
	config = config.withDefaults()

	ch := make(chan StreamEvent, config.QueueSize)

	return &EventStream{
		client: c,
		cfg:    config,
		Events: ch,
		out:    ch,
		dedup:  newDedupCache(config.DedupWindow, config.DedupMaxKeys),
	}
}

// withDefaults fills every unset/non-positive field with its default and
// normalizes values that would otherwise break invariants.
func (cfg EventStreamConfig) withDefaults() EventStreamConfig {
	if cfg.SubscriptionTermination <= 0 {
		cfg.SubscriptionTermination = defaultStreamSubscriptionTermination
	}

	if cfg.RenewLeadTime <= 0 {
		cfg.RenewLeadTime = defaultStreamRenewLeadTime
	}

	// A lead time at or beyond the subscription lifetime would schedule a
	// renewal immediately on every loop iteration. Clamp it to half the
	// lifetime, which still leaves a full renewal round-trip budget.
	if cfg.RenewLeadTime >= cfg.SubscriptionTermination {
		cfg.RenewLeadTime = cfg.SubscriptionTermination / 2
	}

	if cfg.PullTimeout <= 0 {
		cfg.PullTimeout = defaultStreamPullTimeout
	}

	// Keep PullTimeout shorter than the subscription lifetime: the renewal
	// timer fires independently and is not blocked by an in-flight pull, but a
	// pull longer than the whole lifetime would be pointless and could still
	// straddle an expiry on cameras that cancel requests on termination. The
	// cap reserves one RenewLeadTime for the renewal itself and one for
	// round-trip slack.
	maxPull := cfg.SubscriptionTermination - 2*cfg.RenewLeadTime
	if maxPull < minPullTimeout {
		// The lifetime is too short for the requested lead; shrink the lead
		// to a quarter of the lifetime so at least one pull still fits. This
		// preserves the invariant PullTimeout > 0 and keeps renewal ahead of
		// the camera-side expiry.
		cfg.RenewLeadTime = cfg.SubscriptionTermination / 4
		maxPull = cfg.SubscriptionTermination - 2*cfg.RenewLeadTime
	}
	if cfg.PullTimeout > maxPull {
		cfg.PullTimeout = maxPull
	}

	if cfg.MessageLimit <= 0 {
		cfg.MessageLimit = defaultStreamMessageLimit
	}

	if cfg.QueueSize < 0 {
		cfg.QueueSize = 0
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = defaultStreamQueueSize
	}

	if cfg.DedupWindow <= 0 {
		cfg.DedupWindow = defaultStreamDedupWindow
	}

	if cfg.DedupMaxKeys <= 0 {
		cfg.DedupMaxKeys = defaultStreamDedupMaxKeys
	}

	if cfg.ReconnectBackoff <= 0 {
		cfg.ReconnectBackoff = defaultStreamReconnectBackoff
	}

	if cfg.MaxReconnectBackoff <= 0 {
		cfg.MaxReconnectBackoff = defaultStreamMaxReconnectBackoff
	}

	if cfg.ReconnectBackoff > cfg.MaxReconnectBackoff {
		cfg.ReconnectBackoff = cfg.MaxReconnectBackoff
	}

	if cfg.ResubscribeAttempts <= 0 {
		cfg.ResubscribeAttempts = defaultStreamResubscribeAttempts
	}

	if cfg.CloseTimeout <= 0 {
		cfg.CloseTimeout = defaultStreamCloseTimeout
	}

	return cfg
}

// Run starts the stream and blocks until the parent context is canceled,
// Close is called, or the device permanently refuses the subscription
// (ErrSubscriptionLost). It returns ErrEventStreamClosed for an orderly
// shutdown.
//
// Run is single-use per stream instance: calling it while a previous Run is
// active returns ErrEventStreamRunning. After a clean return it may be called
// again, which starts a fresh subscription and continues the Sequence counter
// and replay cache (callers that want a clean sequence should build a new
// stream).
func (s *EventStream) Run(ctx context.Context) error {
	s.runMu.Lock()
	if s.running {
		s.runMu.Unlock()

		return ErrEventStreamRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.stop = cancel
	s.running = true
	s.runMu.Unlock()

	defer func() {
		s.runMu.Lock()
		s.running = false
		s.stop = nil
		s.runMu.Unlock()
		cancel()
	}()

	err := s.loop(runCtx)
	if errors.Is(err, context.Canceled) {
		// Cancellation comes from either the parent context or Close; both are
		// an orderly stop from the caller's point of view.
		return ErrEventStreamClosed
	}

	return err
}

// Close stops the stream: it cancels the running loop, best-effort
// unsubscribes the remote pull-point subscription, and waits for the Run
// goroutine to release. It is safe to call concurrently and repeatedly, and
// before Run has started. Close never returns the stream's terminal error;
// callers learn that from Run's return value.
func (s *EventStream) Close() {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	s.runMu.Lock()
	stop := s.stop
	s.runMu.Unlock()

	// Context cancel funcs are idempotent, so calling this on every Close is
	// safe; re-reading it each time (rather than once via sync.Once) is what
	// makes Close work again after a second Run has installed a fresh cancel.
	if stop != nil {
		stop()
	}

	// Best-effort remote unsubscribe on a detached context: the caller's
	// context may already be done, and the server still needs to free the
	// pull-point slot. The run loop owns the active subscription reference,
	// so the unsubscribe itself happens there on the way out; this wait only
	// bounds the goroutine's exit.
	s.runMu.Lock()
	running := s.running
	s.runMu.Unlock()

	if running {
		deadline := time.NewTimer(s.cfg.CloseTimeout)
		defer deadline.Stop()

		poll := time.NewTicker(10 * time.Millisecond)
		defer poll.Stop()

		for {
			s.runMu.Lock()
			still := s.running
			s.runMu.Unlock()

			if !still {
				return
			}

			select {
			case <-poll.C:
			case <-deadline.C:
				return
			}
		}
	}
}

// Stats returns a snapshot of the stream counters.
func (s *EventStream) Stats() EventStreamStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	return s.stats
}

// loop is the single pulling goroutine. It owns the subscription lifecycle,
// the replay cache and all counter mutations.
//
// Renewal is driven by an absolute-deadline timer rather than by comparing
// clocks between pulls: the timer fires RenewLeadTime before the reported
// termination time regardless of how long a long-poll is blocking, and a
// select between the pull result and the timer channel decides who acts
// first. The in-flight pull is never canceled when renewal wins - canceling
// its SOAP request would tear down the HTTP connection some cameras treat as
// the subscription itself; the bounded long-poll is instead drained after
// the renewal, so the two never overlap on the wire.
func (s *EventStream) loop(ctx context.Context) error {
	var (
		subRef           string
		terminationTime  time.Time
		consecutiveFails int
		backoff          = s.cfg.ReconnectBackoff
	)

	// unsubscribeOnExit best-effort revokes the remote subscription when the
	// loop leaves for any reason. A detached context is deliberate: on
	// parent-context cancellation the request would otherwise be skipped, and
	// the camera would keep the pull-point slot until its termination time.
	defer func() {
		if subRef == "" {
			return
		}

		unsubCtx, cancel := context.WithTimeout(context.Background(), s.cfg.CloseTimeout)
		defer cancel()

		_ = s.client.Unsubscribe(unsubCtx, subRef)
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// (Re)create the subscription when there is none.
		if subRef == "" {
			ref, term, err := s.createSubscription(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				if isAuthError(err) {
					// Credentials are rejected: retrying the same request
					// cannot help until the caller rotates credentials via
					// Client.SetCredentials. Surface immediately rather than
					// burning the resubscribe budget.
					return fmt.Errorf("%w: authentication rejected: %v", ErrSubscriptionLost, err)
				}

				consecutiveFails++
				if consecutiveFails >= s.cfg.ResubscribeAttempts {
					return fmt.Errorf("%w: create failed after %d attempts: %w",
						ErrSubscriptionLost, consecutiveFails, err)
				}

				if !waitOrCancel(ctx, backoff) {
					return ctx.Err()
				}

				backoff = nextBackoff(backoff, s.cfg.MaxReconnectBackoff)

				continue
			}

			subRef = ref
			terminationTime = term
			consecutiveFails = 0
			backoff = s.cfg.ReconnectBackoff
		}

		// renewTimer fires RenewLeadTime before the device-side termination,
		// independently of pull timing. A zero termination time (device did
		// not report one) falls back to renewing every lifetime minus lead.
		renewAt := s.renewDeadline(terminationTime)
		renewTimer := time.NewTimer(time.Until(renewAt))

		// The pull runs on its own goroutine but is NOT canceled when the
		// renewal timer wins the select: a canceled SOAP request tears down
		// the HTTP connection, and some cameras treat that as a lost
		// subscription. The long-poll is already bounded by PullTimeout, so
		// letting it finish naturally costs at most PullTimeout while the
		// renewal proceeds sequentially after.
		pullDone := make(chan pullResult, 1)

		go func() {
			messages, deviceNow, term, err := s.pullDetailed(ctx, subRef, s.cfg.PullTimeout)
			pullDone <- pullResult{messages, deviceNow, term, err}
		}()

		select {
		case <-ctx.Done():
			renewTimer.Stop()

			return ctx.Err()

		case <-renewTimer.C:
			// Renewal deadline reached. Renew first (the in-flight pull is
			// read-only and cannot conflict), then drain its result so the
			// goroutine does not leak. Any messages the pull returned are
			// still delivered after de-duplication.
			term, err := s.renew(ctx, subRef)
			if err != nil {
				renewTimer.Stop()
				<-pullDone

				if ctx.Err() != nil {
					return ctx.Err()
				}

				s.bumpStat(func(st *EventStreamStats) { st.RenewFailures++ })

				if isAuthError(err) {
					return fmt.Errorf("%w: authentication rejected on renew: %v", ErrSubscriptionLost, err)
				}

				// A refused renewal means the pull point is no longer
				// trustworthy. Drop it and re-create from scratch; the replay
				// cache survives so redelivery during the gap is de-duplicated.
				subRef = ""
				terminationTime = time.Time{}

				consecutiveFails++
				s.bumpStat(func(st *EventStreamStats) { st.Reconnects++ })

				if consecutiveFails >= s.cfg.ResubscribeAttempts {
					return fmt.Errorf("%w: renew failed after %d attempts: %w",
						ErrSubscriptionLost, consecutiveFails, err)
				}

				if !waitOrCancel(ctx, backoff) {
					return ctx.Err()
				}

				backoff = nextBackoff(backoff, s.cfg.MaxReconnectBackoff)

				continue
			}

			terminationTime = term
			s.bumpStat(func(st *EventStreamStats) { st.Renewals++ })

			// Wait for the in-flight pull to settle before the next iteration
			// starts another, keeping exactly one SOAP call at a time.
			pullRes := <-pullDone
			if pullRes.err == nil {
				// Only fold in the clock estimate: the pull raced this
				// renewal, so its TerminationTime predates the Renew response
				// and must not overwrite the fresh deadline above.
				if !pullRes.deviceNow.IsZero() {
					s.bumpStat(func(st *EventStreamStats) {
						st.ClockSkew = pullRes.deviceNow.Sub(time.Now())
					})
				}
				for i := range pullRes.messages {
					s.deliver(ctx, pullRes.messages[i])
				}
			}

		case pullRes := <-pullDone:
			renewTimer.Stop()

			if pullRes.err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				s.bumpStat(func(st *EventStreamStats) { st.PullErrors++ })

				if isAuthError(pullRes.err) {
					return fmt.Errorf("%w: authentication rejected on pull: %v", ErrSubscriptionLost, pullRes.err)
				}

				// Transient failure: re-create the pull point after backoff.
				// Keep the replay cache; the camera may redeliver.
				subRef = ""
				terminationTime = time.Time{}

				consecutiveFails++
				s.bumpStat(func(st *EventStreamStats) { st.Reconnects++ })

				if consecutiveFails >= s.cfg.ResubscribeAttempts {
					return fmt.Errorf("%w: pull failed after %d attempts: %w",
						ErrSubscriptionLost, consecutiveFails, pullRes.err)
				}

				if !waitOrCancel(ctx, backoff) {
					return ctx.Err()
				}

				backoff = nextBackoff(backoff, s.cfg.MaxReconnectBackoff)

				continue
			}

			// A successful pull resets the failure budget and backoff.
			consecutiveFails = 0
			backoff = s.cfg.ReconnectBackoff

			s.applyPullResult(pullRes, &terminationTime)

			for i := range pullRes.messages {
				s.deliver(ctx, pullRes.messages[i])
			}
		}
	}
}

// pullResult bundles one PullMessages round trip's outputs for the channel
// that decouples the pull goroutine from the renewal timer.
type pullResult struct {
	messages  []NotificationMessage
	deviceNow time.Time
	term      time.Time
	err       error
}

// renewDeadline computes the absolute local time at which the renewal timer
// must fire: RenewLeadTime before the reported termination, or a fallback
// based on the configured lifetime when the device reported none.
func (s *EventStream) renewDeadline(terminationTime time.Time) time.Time {
	if !terminationTime.IsZero() {
		return terminationTime.Add(-s.cfg.RenewLeadTime)
	}

	return time.Now().Add(s.cfg.SubscriptionTermination - s.cfg.RenewLeadTime)
}

// applyPullResult folds the device-reported clock and termination time from
// a successful pull into loop state.
func (s *EventStream) applyPullResult(res pullResult, terminationTime *time.Time) {
	if !res.deviceNow.IsZero() {
		s.bumpStat(func(st *EventStreamStats) { st.ClockSkew = res.deviceNow.Sub(time.Now()) })
	}

	if !res.term.IsZero() {
		*terminationTime = res.term
	}
}

// createSubscription creates a pull-point subscription and translates the
// wire-level PullPointSubscription into the fields the loop needs.
func (s *EventStream) createSubscription(ctx context.Context) (string, time.Time, error) {
	term := s.cfg.SubscriptionTermination

	sub, err := s.client.CreatePullPointSubscription(ctx, s.cfg.Filter, &term, "")
	if err != nil {
		return "", time.Time{}, err
	}

	if sub.SubscriptionReference == "" {
		return "", time.Time{}, fmt.Errorf("%w: device returned an empty SubscriptionReference", ErrInvalidResponse)
	}

	s.rememberClock(sub.CurrentTime)

	return sub.SubscriptionReference, sub.TerminationTime, nil
}

// renew extends the subscription and returns the device-reported new
// termination time.
func (s *EventStream) renew(ctx context.Context, subRef string) (time.Time, error) {
	currentTime, terminationTime, err := s.client.RenewSubscription(ctx, subRef, s.cfg.SubscriptionTermination)
	if err != nil {
		return time.Time{}, err
	}

	s.rememberClock(currentTime)

	return terminationTime, nil
}

// pullDetailed is PullMessages plus the device-reported CurrentTime and
// TerminationTime, which the public PullMessages API discards. Both are
// needed for clock-skew estimation and renewal scheduling. timeout is the
// effective long-poll budget, already capped by the caller so it cannot
// straddle a renewal deadline.
func (s *EventStream) pullDetailed(ctx context.Context, subRef string, timeout time.Duration) ([]NotificationMessage, time.Time, time.Time, error) {
	// PullMessages rejects non-positive timeouts; a floor also keeps an
	// aggressively shortened poll well-formed against cameras that reject
	// sub-second durations.
	if timeout < minPullTimeout {
		timeout = minPullTimeout
	}

	return s.client.pullMessagesDetailed(ctx, subRef, timeout, s.cfg.MessageLimit)
}

// rememberClock updates the clock-skew estimate from a device-reported time,
// ignoring zero values.
func (s *EventStream) rememberClock(deviceNow time.Time) {
	if deviceNow.IsZero() {
		return
	}

	s.bumpStat(func(st *EventStreamStats) { st.ClockSkew = deviceNow.Sub(time.Now()) })
}

// deliver normalizes one notification, applies replay de-duplication and
// enqueues it without ever blocking the pulling goroutine.
func (s *EventStream) deliver(ctx context.Context, msg NotificationMessage) {
	event := s.normalize(msg)

	if !s.dedup.seen(event.ID) {
		s.bumpStat(func(st *EventStreamStats) { st.Duplicates++ })

		return
	}

	// Non-blocking send. A full buffer means a slow consumer; dropping the
	// newest event keeps pulling and therefore keeps the subscription alive.
	// Sequence numbers are reserved only for events that actually enter the
	// buffer, so the delivered stream is gap-free even under backpressure.
	select {
	case s.out <- s.withNextSequence(event):
		s.bumpStat(func(st *EventStreamStats) { st.Delivered++ })
	case <-ctx.Done():
		return
	default:
		s.bumpStat(func(st *EventStreamStats) { st.Dropped++ })

		if s.cfg.OnEventDropped != nil {
			s.cfg.OnEventDropped(event, ErrEventDropped)
		}
	}
}

// withNextSequence stamps event with the next gap-free sequence number. Only
// the Run goroutine calls it, so the read-modify-write needs no lock.
func (s *EventStream) withNextSequence(event StreamEvent) StreamEvent {
	s.sequence++
	event.Sequence = s.sequence

	return event
}

// normalize flattens a wire NotificationMessage into a StreamEvent and
// computes its stable identity.
func (s *EventStream) normalize(msg NotificationMessage) StreamEvent {
	event := StreamEvent{
		Topic:             msg.Topic,
		Time:              msg.Message.UtcTime,
		LocalTime:         time.Now().UTC(),
		ProducerAddress:   msg.ProducerAddress,
		PropertyOperation: msg.Message.PropertyOperation,
		Source:            msg.Message.Source,
		Key:               msg.Message.Key,
		Data:              msg.Message.Data,
	}
	event.ID = computeEventID(event)

	return event
}

// bumpStat runs fn under the stats lock.
func (s *EventStream) bumpStat(fn func(*EventStreamStats)) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	fn(&s.stats)
}

// waitOrCancel sleeps for d unless ctx ends first, reporting whether the wait
// ran to completion.
func waitOrCancel(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// nextBackoff doubles d, capped at max.
func nextBackoff(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}

	return d
}

// isAuthError classifies an error as a credential problem the stream cannot
// repair by retrying. ONVIF devices express rejection either as an HTTP 401/
// 403 or as a SOAP Sender fault whose reason mentions authentication or
// authorization; both the internal soap sentinels and the reason text are
// matched so the check works across transports.
func isAuthError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrAuthenticationFailed) {
		return true
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "status 401") || strings.Contains(msg, "status 403") {
		return true
	}

	return strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "unauthorized") ||
		strings.Contains(msg, "invalid username or password")
}

// computeEventID builds the stable identity of a normalized event. Every
// identity-bearing field is fed through an FNV-style write to a SHA-256
// hash; item ordering is part of the identity, matching how the device emits
// SimpleItem lists deterministically.
func computeEventID(event StreamEvent) string {
	h := sha256.New()

	writeHashString(h, event.Topic)
	writeHashString(h, event.ProducerAddress)
	writeHashString(h, event.PropertyOperation)

	if !event.Time.IsZero() {
		writeHashString(h, event.Time.UTC().Format(time.RFC3339Nano))
	} else {
		// A device that omits UtcTime gives no device-side identity anchor;
		// fall back to nothing (not LocalTime, which would make every redelivery
		// unique and defeat de-duplication). The content fields still
		// distinguish event kinds.
		writeHashString(h, "no-utctime")
	}

	writeItems(h, event.Source)
	writeItems(h, event.Key)
	writeItems(h, event.Data)

	return hex.EncodeToString(h.Sum(nil))
}

// writeHashString writes a length-prefixed string so fields cannot be
// confused across boundaries ("ab"+"c" vs "a"+"bc").
func writeHashString(h hash.Hash, s string) {
	var lenBuf [8]byte
	v := uint64(len(s))
	for i := 7; i >= 0; i-- {
		lenBuf[i] = byte(v)
		v >>= 8
	}
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(s))
}

// writeItems writes a SimpleItem list into the identity hash.
func writeItems(h hash.Hash, items []SimpleItem) {
	for _, item := range items {
		writeHashString(h, item.Name)
		writeHashString(h, item.Value)
	}
}

// dedupCache is a bounded, time-windowed replay cache keyed by event ID. It
// records when each ID was first seen and evicts entries once they age out of
// the window or when the size cap forces oldest-first eviction. Only the
// stream's single Run goroutine accesses it.
type dedupCache struct {
	window time.Duration
	max    int
	// entries records the local first-seen time of each event ID still
	// inside the window.
	entries map[string]time.Time
	// order keeps insertion order for oldest-first eviction. A slice used as
	// a FIFO is enough: IDs are added once and removed from the front.
	order []string
}

// newDedupCache constructs a replay cache with the given window and size cap.
func newDedupCache(window time.Duration, max int) *dedupCache {
	return &dedupCache{
		window:  window,
		max:     max,
		entries: make(map[string]time.Time),
	}
}

// seen reports whether id is new. A new ID is recorded and returns true; an
// ID already present and still inside the window returns false. Expired
// entries are purged lazily. Expiry is anchored to the local receive time
// rather than the device-assigned event time, which may jump when a camera's
// clock is adjusted.
func (d *dedupCache) seen(id string) bool {
	now := time.Now()
	d.purge(now)

	if _, ok := d.entries[id]; ok {
		return false
	}

	d.entries[id] = now
	d.order = append(d.order, id)

	if len(d.order) > d.max {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.entries, oldest)
	}

	return true
}

// purge drops entries older than the window, in insertion order, stopping at
// the first still-fresh entry.
func (d *dedupCache) purge(now time.Time) {
	cutoff := now.Add(-d.window)

	for len(d.order) > 0 {
		oldestID := d.order[0]
		seenAt, ok := d.entries[oldestID]
		if !ok {
			// Defensive: order and map should never diverge, but do not let a
			// stale slice entry pin the loop.
			d.order = d.order[1:]

			continue
		}

		if seenAt.After(cutoff) {
			break
		}

		d.order = d.order[1:]
		delete(d.entries, oldestID)
	}
}
