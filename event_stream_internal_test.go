package onvif

import (
	"errors"
	"testing"
	"time"
)

// TestDedupCacheRejectsRepeats covers the core replay-suppression contract:
// the first sighting is new, every sighting inside the window is a duplicate,
// and after the window elapses the ID is new again.
func TestDedupCacheRejectsRepeats(t *testing.T) {
	cache := newDedupCache(50*time.Millisecond, 8)

	if !cache.seen("id-1") {
		t.Error("first sighting must be new")
	}
	if cache.seen("id-1") {
		t.Error("second sighting inside the window must be a duplicate")
	}
	if !cache.seen("id-2") {
		t.Error("a different ID must be new")
	}

	// Wait beyond the window; lazy purge on the next call must drop the entry.
	time.Sleep(70 * time.Millisecond)
	if !cache.seen("id-1") {
		t.Error("ID should be new again after the dedup window elapsed")
	}
}

// TestDedupCacheEvictsOldestAtCap verifies the size bound cannot be exceeded
// and that eviction is oldest-first.
func TestDedupCacheEvictsOldestAtCap(t *testing.T) {
	cache := newDedupCache(time.Minute, 3)

	cache.seen("a")
	cache.seen("b")
	cache.seen("c")
	cache.seen("d") // forces eviction of "a"

	if len(cache.entries) != 3 {
		t.Fatalf("cache size = %d, want capped at 3", len(cache.entries))
	}
	if _, ok := cache.entries["a"]; ok {
		t.Error("oldest entry \"a\" should have been evicted")
	}
	if !cache.seen("a") {
		t.Error("after eviction \"a\" should be accepted as new")
	}
}

// TestComputeEventIDIsStableAndDistinct proves identity semantics: the same
// notification hashes identically across redelivery, while any identity-
// bearing change (topic, time, source, data, producer) yields a new ID.
func TestComputeEventIDIsStableAndDistinct(t *testing.T) {
	base := StreamEvent{
		Topic:             "tns1:VideoSource/MotionAlarm",
		Time:              time.Date(2026, 11, 20, 4, 0, 0, 0, time.UTC),
		ProducerAddress:   "urn:uuid:door-camera-1",
		PropertyOperation: "Changed",
		Source:            []SimpleItem{{Name: "VideoSourceToken", Value: "VideoSource_1"}},
		Data:              []SimpleItem{{Name: "State", Value: "true"}},
	}

	id1 := computeEventID(base)
	id2 := computeEventID(base)
	if id1 != id2 || len(id1) != 64 {
		t.Fatalf("identical events must map to the same hex SHA-256 ID; got %q and %q", id1, id2)
	}

	variants := []StreamEvent{base}
	variants = append(variants, base)
	variants[1].Time = base.Time.Add(time.Second)

	changedData := base
	changedData.Data = []SimpleItem{{Name: "State", Value: "false"}}
	variants = append(variants, changedData)

	changedTopic := base
	changedTopic.Topic = "tns1:VideoSource/Other"
	variants = append(variants, changedTopic)

	changedSource := base
	changedSource.Source = []SimpleItem{{Name: "VideoSourceToken", Value: "VideoSource_2"}}
	variants = append(variants, changedSource)

	changedProducer := base
	changedProducer.ProducerAddress = "urn:uuid:other-camera"
	variants = append(variants, changedProducer)

	seen := map[string]bool{}
	for i, v := range variants {
		id := computeEventID(v)
		if seen[id] {
			t.Errorf("variant %d collapsed onto an existing ID %q", i, id)
		}
		seen[id] = true
	}
}

// TestComputeEventIDNoUtcTimeStillDeduplicates covers devices that omit
// UtcTime: redelivery must still collide because LocalTime is deliberately
// excluded from the identity (it differs on every receipt).
func TestComputeEventIDNoUtcTimeStillDeduplicates(t *testing.T) {
	event := StreamEvent{
		Topic:     "tns1:VideoSource/MotionAlarm",
		LocalTime: time.Now(),
		Data:      []SimpleItem{{Name: "State", Value: "true"}},
	}

	redelivered := event
	redelivered.LocalTime = event.LocalTime.Add(500 * time.Millisecond)

	if computeEventID(event) != computeEventID(redelivered) {
		t.Error("events without UtcTime must still de-duplicate on content")
	}
}

// TestEventStreamConfigDefaults checks the production defaults and the
// invariants withDefaults is responsible for.
func TestEventStreamConfigDefaults(t *testing.T) {
	cfg := EventStreamConfig{}.withDefaults()

	if cfg.SubscriptionTermination != defaultStreamSubscriptionTermination {
		t.Errorf("SubscriptionTermination = %v", cfg.SubscriptionTermination)
	}
	if cfg.RenewLeadTime != defaultStreamRenewLeadTime {
		t.Errorf("RenewLeadTime = %v", cfg.RenewLeadTime)
	}
	if cfg.PullTimeout <= 0 || cfg.QueueSize <= 0 || cfg.MessageLimit <= 0 {
		t.Errorf("positive defaults expected, got %+v", cfg)
	}
	if cfg.PullTimeout >= cfg.SubscriptionTermination {
		t.Error("PullTimeout must fit inside the subscription lifetime")
	}
}

// TestEventStreamConfigClampsLeadTime covers an unsafe lead time at or beyond
// the subscription lifetime, which would otherwise make renewal fire
// immediately on every iteration.
func TestEventStreamConfigClampsLeadTime(t *testing.T) {
	cfg := EventStreamConfig{
		SubscriptionTermination: time.Minute,
		RenewLeadTime:           time.Hour,
	}.withDefaults()

	if cfg.RenewLeadTime >= time.Minute {
		t.Errorf("RenewLeadTime = %v must be clamped below the lifetime", cfg.RenewLeadTime)
	}
}

// TestIsAuthErrorClassification ensures the stream can tell a credential
// problem (terminal) from a transient transport fault (retryable).
func TestIsAuthErrorClassification(t *testing.T) {
	authCases := []error{
		ErrAuthenticationFailed,
		wrapErr(ErrHTTPRequestFailed, "request failed with status 401: bad creds"),
		wrapErr(ErrHTTPRequestFailed, "request failed with status 403"),
		errors.New("SOAP fault: [Sender] Authentication failed: Invalid username or password"),
	}
	for _, err := range authCases {
		if !isAuthError(err) {
			t.Errorf("isAuthError(%v) = false, want true", err)
		}
	}

	transient := []error{
		wrapErr(ErrHTTPRequestFailed, "request failed with status 500"),
		errors.New("SOAP fault: [Receiver] Action not supported"),
		wrapErr(ErrConnectionFailed, "dial tcp: connection refused"),
	}
	for _, err := range transient {
		if isAuthError(err) {
			t.Errorf("isAuthError(%v) = true, want false", err)
		}
	}
}

// wrapErr is a tiny helper so the test does not import fmt solely for
// Errorf-style wrapping.
func wrapErr(base error, msg string) error {
	return &wrappedError{base: base, msg: msg}
}

type wrappedError struct {
	base error
	msg  string
}

func (e *wrappedError) Error() string { return e.msg }
func (e *wrappedError) Unwrap() error { return e.base }
