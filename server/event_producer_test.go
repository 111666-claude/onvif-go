package server

import (
	"errors"
	"testing"
	"time"
)

// newEventServer builds a Server with an active pull-point subscription and
// long polling enabled, the common precondition for producer tests.
func newEventServer(t *testing.T) *Server {
	t.Helper()

	srv, err := New(createTestConfig())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	srv.EnableLongPolling()

	if _, err := srv.HandleCreatePullPointSubscription(
		CreatePullPointSubscriptionRequest{InitialTerminationTime: testDuration1Min},
	); err != nil {
		t.Fatalf("HandleCreatePullPointSubscription() error = %v", err)
	}

	return srv
}

func sampleEventInput() EventInput {
	return EventInput{
		Topic:             "tns1:VideoSource/MotionAlarm",
		ProducerAddress:   "urn:uuid:door-camera-1",
		PropertyOperation: "Changed",
		Source:            []SimpleItem{{Name: "VideoSourceToken", Value: "VideoSource_1"}},
		Data:              []SimpleItem{{Name: "State", Value: "true"}},
	}
}

// TestPostEventReachesNextPull covers the producer-to-consumer path: a
// PullMessages call blocked in its long poll returns as soon as PostEvent
// fires, carrying the full normalized notification.
func TestPostEventReachesNextPull(t *testing.T) {
	srv := newEventServer(t)

	type pullResult struct {
		resp interface{}
		err  error
	}
	resultCh := make(chan pullResult, 1)

	go func() {
		resp, err := srv.HandlePullMessages(PullMessagesRequest{
			Timeout:      testDuration5Sec,
			MessageLimit: 10,
		})
		resultCh <- pullResult{resp, err}
	}()

	if !srv.WaitForPullWaiters(1, 2*time.Second) {
		t.Fatal("PullMessages never entered its long-poll")
	}

	if err := srv.PostEvent(sampleEventInput()); err != nil {
		t.Fatalf("PostEvent() error = %v", err)
	}

	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("HandlePullMessages() error = %v", res.err)
		}

		resp, ok := res.resp.(*PullMessagesResponse)
		if !ok {
			t.Fatalf("response type = %T, want *PullMessagesResponse", res.resp)
		}
		if len(resp.NotificationMessages) != 1 {
			t.Fatalf("got %d notifications, want 1", len(resp.NotificationMessages))
		}

		msg := resp.NotificationMessages[0]
		if msg.Topic.Value != "tns1:VideoSource/MotionAlarm" {
			t.Errorf("Topic = %q", msg.Topic.Value)
		}
		if msg.ProducerReference == nil || msg.ProducerReference.Address != "urn:uuid:door-camera-1" {
			t.Errorf("ProducerReference not normalized: %+v", msg.ProducerReference)
		}
		if msg.Message.Inner.PropertyOperation != "Changed" {
			t.Errorf("PropertyOperation = %q", msg.Message.Inner.PropertyOperation)
		}
		if msg.Message.Inner.UtcTime == "" {
			t.Error("UtcTime should be stamped")
		}
		if msg.Message.Inner.Source == nil || len(msg.Message.Inner.Source.SimpleItems) != 1 {
			t.Errorf("Source items missing: %+v", msg.Message.Inner.Source)
		}
		if msg.Message.Inner.Data == nil || msg.Message.Inner.Data.SimpleItems[0].Value != "true" {
			t.Errorf("Data items missing: %+v", msg.Message.Inner.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long-poll did not return after PostEvent")
	}

	stats := srv.EventStats()
	if stats.NotificationsQueued != 1 || stats.NotificationsSent != 1 {
		t.Errorf("stats = %+v, want one queued and one sent", stats)
	}
}

// TestPostEventWithoutSubscription reports ErrSubscriptionNotFound rather
// than dropping the event silently.
func TestPostEventWithoutSubscription(t *testing.T) {
	srv, err := New(createTestConfig())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	err = srv.PostEvent(sampleEventInput())
	if !errors.Is(err, ErrSubscriptionNotFound) {
		t.Errorf("PostEvent() error = %v, want ErrSubscriptionNotFound", err)
	}
}

// TestPostEventRejectsEmptyTopic guards the input validation.
func TestPostEventRejectsEmptyTopic(t *testing.T) {
	srv := newEventServer(t)

	input := sampleEventInput()
	input.Topic = ""

	err := srv.PostEvent(input)
	if !errors.Is(err, ErrInvalidParameter) {
		t.Errorf("PostEvent() error = %v, want ErrInvalidParameter", err)
	}
}

// TestPostEventRespectsMessageLimit proves PullMessages only hands out the
// requested number of notifications and keeps the rest queued.
func TestPostEventRespectsMessageLimit(t *testing.T) {
	srv := newEventServer(t)

	for range 3 {
		if err := srv.PostEvent(sampleEventInput()); err != nil {
			t.Fatalf("PostEvent() error = %v", err)
		}
	}

	resp, err := srv.HandlePullMessages(PullMessagesRequest{
		Timeout:      testDuration1Sec,
		MessageLimit: 2,
	})
	if err != nil {
		t.Fatalf("HandlePullMessages() error = %v", err)
	}
	first := resp.(*PullMessagesResponse)
	if len(first.NotificationMessages) != 2 {
		t.Fatalf("first pull returned %d, want 2", len(first.NotificationMessages))
	}

	resp, err = srv.HandlePullMessages(PullMessagesRequest{
		Timeout:      testDuration1Sec,
		MessageLimit: 10,
	})
	if err != nil {
		t.Fatalf("HandlePullMessages() error = %v", err)
	}
	second := resp.(*PullMessagesResponse)
	if len(second.NotificationMessages) != 1 {
		t.Fatalf("second pull returned %d, want the remaining 1", len(second.NotificationMessages))
	}
}

// TestPostEventRepeatRedeliversLastEvent covers the reconnect-redelivery
// simulation: an already-consumed notification can be re-sent byte-identically.
func TestPostEventRepeatRedeliversLastEvent(t *testing.T) {
	srv := newEventServer(t)

	if err := srv.PostEvent(sampleEventInput()); err != nil {
		t.Fatalf("PostEvent() error = %v", err)
	}

	first, err := srv.HandlePullMessages(PullMessagesRequest{
		Timeout:      testDuration1Sec,
		MessageLimit: 10,
	})
	if err != nil {
		t.Fatalf("HandlePullMessages() error = %v", err)
	}
	firstResp := first.(*PullMessagesResponse)
	firstMsg := firstResp.NotificationMessages[0]

	// The queue is now empty; repeat must use lastDelivered, not fail.
	if err := srv.PostEventRepeat(); err != nil {
		t.Fatalf("PostEventRepeat() error = %v", err)
	}

	second, err := srv.HandlePullMessages(PullMessagesRequest{
		Timeout:      testDuration1Sec,
		MessageLimit: 10,
	})
	if err != nil {
		t.Fatalf("HandlePullMessages() error = %v", err)
	}
	secondResp := second.(*PullMessagesResponse)
	if len(secondResp.NotificationMessages) != 1 {
		t.Fatalf("redelivery returned %d messages, want 1", len(secondResp.NotificationMessages))
	}

	secondMsg := secondResp.NotificationMessages[0]
	if secondMsg.Topic.Value != firstMsg.Topic.Value ||
		secondMsg.Message.Inner.UtcTime != firstMsg.Message.Inner.UtcTime ||
		secondMsg.Message.Inner.Data.SimpleItems[0].Value != firstMsg.Message.Inner.Data.SimpleItems[0].Value {
		t.Error("redelivered notification is not byte-identical to the original")
	}
}

// TestPostEventRepeatWithoutHistory fails cleanly when nothing can be repeated.
func TestPostEventRepeatWithoutHistory(t *testing.T) {
	srv := newEventServer(t)

	if err := srv.PostEventRepeat(); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Errorf("PostEventRepeat() error = %v, want ErrSubscriptionNotFound", err)
	}
}

// TestLongPollingDefaultsOff confirms the original immediate-return contract
// survives for callers that never enable long polling (this is why the old
// handler-level tests never block).
func TestLongPollingDefaultsOff(t *testing.T) {
	srv, err := New(createTestConfig())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if _, err := srv.HandleCreatePullPointSubscription(
		CreatePullPointSubscriptionRequest{InitialTerminationTime: testDuration1Min},
	); err != nil {
		t.Fatalf("HandleCreatePullPointSubscription() error = %v", err)
	}

	start := time.Now()
	resp, err := srv.HandlePullMessages(PullMessagesRequest{
		Timeout:      testDuration5Sec,
		MessageLimit: 10,
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("HandlePullMessages() error = %v", err)
	}
	if len(resp.(*PullMessagesResponse).NotificationMessages) != 0 {
		t.Error("expected an empty batch")
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("long polling should be off by default; pull blocked for %v", elapsed)
	}
}

// TestEventHooksRejectPullAndRenew verify the fault-injection hooks the
// reliable-stream tests rely on, at the handler layer.
func TestEventHooksRejectPullAndRenew(t *testing.T) {
	srv := newEventServer(t)
	srv.SetEventTestHooks(EventTestHooks{RejectPull: true, RejectRenew: true})

	if _, err := srv.HandlePullMessages(PullMessagesRequest{
		Timeout:      testDuration1Sec,
		MessageLimit: 10,
	}); !errors.Is(err, ErrFaultInjected) {
		t.Errorf("HandlePullMessages() error = %v, want ErrFaultInjected", err)
	}

	if _, err := srv.HandleRenew(RenewRequest{TerminationTime: testDuration1Min}); !errors.Is(err, ErrFaultInjected) {
		t.Errorf("HandleRenew() error = %v, want ErrFaultInjected", err)
	}

	stats := srv.EventStats()
	if stats.PullsRejected != 1 || stats.RenewsRejected != 1 {
		t.Errorf("stats = %+v, want one rejected pull and one rejected renew", stats)
	}
}

// TestEventHookExpireOnNextPull simulates silent subscription expiry: the
// next pull faults once and the subscription is gone afterwards.
func TestEventHookExpireOnNextPull(t *testing.T) {
	srv := newEventServer(t)
	srv.SetEventTestHooks(EventTestHooks{ExpireOnNextPull: true})

	if _, err := srv.HandlePullMessages(PullMessagesRequest{
		Timeout:      testDuration1Sec,
		MessageLimit: 10,
	}); !errors.Is(err, ErrFaultInjected) {
		t.Errorf("HandlePullMessages() error = %v, want ErrFaultInjected", err)
	}

	// The hook self-resets, but the subscription stays cleared.
	if _, err := srv.HandlePullMessages(PullMessagesRequest{
		Timeout:      testDuration1Sec,
		MessageLimit: 10,
	}); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Errorf("post-expiry pull error = %v, want ErrSubscriptionNotFound", err)
	}
}

// TestEventHookClockOffsetShiftsReportedTimes verifies the virtual camera's
// clock offset flows into the timestamps clients use for skew estimation.
func TestEventHookClockOffsetShiftsReportedTimes(t *testing.T) {
	srv := newEventServer(t)
	const offset = 3 * time.Minute
	srv.SetEventTestHooks(EventTestHooks{ClockOffset: offset})

	before := time.Now().Add(offset)

	resp, err := srv.HandleRenew(RenewRequest{TerminationTime: testDuration1Min})
	if err != nil {
		t.Fatalf("HandleRenew() error = %v", err)
	}
	renew := resp.(*RenewResponse)

	current, err := time.Parse(time.RFC3339Nano, renew.CurrentTime)
	if err != nil {
		t.Fatalf("CurrentTime is not RFC3339: %v", err)
	}
	if delta := current.Sub(before); delta < -time.Second || delta > time.Second {
		t.Errorf("CurrentTime offset = %v, want ~%v", delta, offset)
	}
}

// TestEventQueueEviction ensures a flooded queue cannot grow without bound:
// the oldest entry is evicted and the advisory error is returned.
func TestEventQueueEviction(t *testing.T) {
	srv := newEventServer(t)

	// Fill beyond the bound. Each event is distinct so all are retained until
	// the cap forces eviction.
	for i := range eventQueueBound + 5 {
		input := sampleEventInput()
		input.Data = []SimpleItem{{Name: "State", Value: string(rune('a' + i%26))}}
		err := srv.PostEvent(input)
		if i < eventQueueBound {
			if err != nil {
				t.Fatalf("PostEvent(%d) unexpected error: %v", i, err)
			}
		} else if !errors.Is(err, ErrEventQueueFull) {
			t.Fatalf("PostEvent(%d) error = %v, want ErrEventQueueFull", i, err)
		}
	}

	srv.subscriptionMu.RLock()
	queued := len(srv.subscription.queue)
	srv.subscriptionMu.RUnlock()

	if queued != eventQueueBound {
		t.Errorf("queue length = %d, want capped at %d", queued, eventQueueBound)
	}
}
