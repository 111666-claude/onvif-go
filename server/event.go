package server

import (
	"encoding/xml"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// defaultSubscriptionDuration is used when a CreatePullPointSubscription or
// Renew request omits its termination-time field.
const defaultSubscriptionDuration = 10 * time.Minute

// eventQueueBound caps how many unconsumed notifications one subscription
// buffers. When PostEvent exceeds it the oldest notification is evicted, so a
// stopped PullMessages consumer cannot grow the queue without limit.
const eventQueueBound = 1024

// defaultPullTimeout is the long-poll wait used when a PullMessages request
// carries no parseable Timeout. It mirrors the 1-second floor real ONVIF
// devices apply and keeps test runs short.
const defaultPullTimeout = time.Second

// maxPullTimeout bounds how long one PullMessages call blocks, regardless of
// the requested timeout, so a misbehaving client cannot hold a handler
// goroutine for an arbitrary duration.
const maxPullTimeout = 5 * time.Minute

// Event service SOAP message types.
//
// The pull-point implementation covers CreatePullPointSubscription,
// PullMessages (with a real notification producer and long polling), Renew
// and Unsubscribe - enough to exercise this repo's own reliable event stream
// end-to-end. Seek, SetEventSynchronizationPoint, GetServiceCapabilities,
// GetEventProperties, and the EventBroker family are out of scope.

// CreatePullPointSubscriptionRequest represents a CreatePullPointSubscription request.
type CreatePullPointSubscriptionRequest struct {
	XMLName                xml.Name `xml:"CreatePullPointSubscription"`
	InitialTerminationTime string   `xml:"InitialTerminationTime"`
}

// CreatePullPointSubscriptionResponse represents a CreatePullPointSubscription response.
type CreatePullPointSubscriptionResponse struct {
	XMLName               xml.Name              `xml:"http://www.onvif.org/ver10/events/wsdl CreatePullPointSubscriptionResponse"`
	SubscriptionReference SubscriptionReference `xml:"SubscriptionReference"`
	CurrentTime           string                `xml:"CurrentTime"`
	TerminationTime       string                `xml:"TerminationTime"`
}

// SubscriptionReference represents a WS-Eventing subscription reference.
type SubscriptionReference struct {
	Address string `xml:"Address"`
}

// PullMessagesRequest represents a PullMessages request.
type PullMessagesRequest struct {
	XMLName      xml.Name `xml:"PullMessages"`
	Timeout      string   `xml:"Timeout"`
	MessageLimit int      `xml:"MessageLimit"`
}

// PullMessagesResponse represents a PullMessages response. Notification
// messages are appended by the handler rather than fixed in the struct, so an
// empty pull marshals a response without any NotificationMessage elements -
// matching what real cameras return for an idle pull point.
type PullMessagesResponse struct {
	XMLName              xml.Name                 `xml:"http://www.onvif.org/ver10/events/wsdl PullMessagesResponse"`
	CurrentTime          string                   `xml:"CurrentTime"`
	TerminationTime      string                   `xml:"TerminationTime"`
	NotificationMessages []NotificationMessageXML `xml:"NotificationMessage"`
}

// NotificationMessageXML is one wsnt:NotificationMessage on the wire. The
// nested Message element is required: the client parser expects
// wsnt:Message wrapping tt:Message (see event.go's PullMessagesResponse).
type NotificationMessageXML struct {
	XMLName           xml.Name          `xml:"http://docs.oasis-open.org/wsn/b-2 NotificationMessage"`
	Topic             TopicXML          `xml:"Topic"`
	ProducerReference *ReferenceXML     `xml:"ProducerReference,omitempty"`
	Message           MessageWrapperXML `xml:"Message"`
}

// TopicXML carries the concrete-set topic expression.
type TopicXML struct {
	XMLName xml.Name `xml:"http://docs.oasis-open.org/wsn/b-2 Topic"`
	Dialect string   `xml:"Dialect,attr,omitempty"`
	Value   string   `xml:",chardata"`
}

// ReferenceXML is a wsa:EndpointReference-style address wrapper.
type ReferenceXML struct {
	XMLName xml.Name `xml:"http://docs.oasis-open.org/wsn/b-2 ProducerReference"`
	Address string   `xml:"http://www.w3.org/2005/08/addressing Address"`
}

// MessageWrapperXML is the outer wsnt:Message; it contains the ONVIF
// tt:Message carrying PropertyOperation, UtcTime and the item lists.
type MessageWrapperXML struct {
	XMLName xml.Name       `xml:"http://docs.oasis-open.org/wsn/b-2 Message"`
	Inner   MessageBodyXML `xml:"http://www.onvif.org/ver10/schema Message"`
}

// MessageBodyXML is the tt:Message element.
type MessageBodyXML struct {
	PropertyOperation string            `xml:"PropertyOperation,attr,omitempty"`
	UtcTime           string            `xml:"UtcTime,attr,omitempty"`
	Source            *ItemContainerXML `xml:"Source,omitempty"`
	Key               *ItemContainerXML `xml:"Key,omitempty"`
	Data              *ItemContainerXML `xml:"Data,omitempty"`
}

// ItemContainerXML groups tt:SimpleItem elements.
type ItemContainerXML struct {
	SimpleItems []SimpleItemXML `xml:"SimpleItem"`
}

// SimpleItemXML is one tt:SimpleItem.
type SimpleItemXML struct {
	XMLName xml.Name `xml:"http://www.onvif.org/ver10/schema SimpleItem"`
	Name    string   `xml:"Name,attr"`
	Value   string   `xml:"Value,attr"`
}

// RenewRequest represents a Renew request.
type RenewRequest struct {
	XMLName         xml.Name `xml:"Renew"`
	TerminationTime string   `xml:"TerminationTime"`
}

// RenewResponse represents a Renew response.
type RenewResponse struct {
	XMLName         xml.Name `xml:"http://docs.oasis-open.org/wsn/b-2 RenewResponse"`
	CurrentTime     string   `xml:"CurrentTime"`
	TerminationTime string   `xml:"TerminationTime"`
}

// UnsubscribeResponse represents an Unsubscribe response.
type UnsubscribeResponse struct {
	XMLName xml.Name `xml:"http://docs.oasis-open.org/wsn/b-2 UnsubscribeResponse"`
}

// EventInput is one notification a caller asks the virtual camera to produce.
// Time is optional: zero means "stamp it with the server's (possibly offset)
// clock now", which is what motion-alarm producers do.
type EventInput struct {
	Topic             string
	ProducerAddress   string
	PropertyOperation string
	Time              time.Time
	Source            []SimpleItem
	Key               []SimpleItem
	Data              []SimpleItem
}

// Event service handlers

// now applies the test clock offset to time.Now.
func (s *Server) now() time.Time {
	s.subscriptionMu.RLock()
	offset := s.eventHooks.ClockOffset
	s.subscriptionMu.RUnlock()

	return time.Now().Add(offset)
}

// nowLocked is now() for callers that already hold subscriptionMu. Go's
// RWMutex is not reentrant, so taking the read lock here while a writer
// holds it would self-deadlock; this reads the offset directly instead.
func (s *Server) nowLocked() time.Time {
	return time.Now().Add(s.eventHooks.ClockOffset)
}

// HandleCreatePullPointSubscription handles CreatePullPointSubscription requests.
// It replaces any existing subscription with a new one - this simulator
// supports exactly one active pull-point subscription at a time (see
// eventSubscription's doc comment for why).
func (s *Server) HandleCreatePullPointSubscription(body interface{}) (interface{}, error) {
	var req CreatePullPointSubscriptionRequest
	if err := unmarshalBody(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	duration := defaultSubscriptionDuration
	if req.InitialTerminationTime != "" {
		d, err := parseISODuration(req.InitialTerminationTime)
		if err != nil {
			return nil, fmt.Errorf("invalid InitialTerminationTime: %w", err)
		}
		duration = d
	}

	now := s.now()
	sub := &eventSubscription{
		terminationTime: now.Add(duration),
		wake:            make(chan struct{}),
	}

	s.subscriptionMu.Lock()
	if s.subscription != nil && s.subscription.expiryTimer != nil {
		s.subscription.expiryTimer.Stop()
	}
	s.subscription = sub
	s.eventStats.SubscriptionsCreated++
	s.scheduleExpiryLocked(sub, duration)
	s.subscriptionMu.Unlock()

	return &CreatePullPointSubscriptionResponse{
		SubscriptionReference: SubscriptionReference{Address: s.eventsServiceURL()},
		CurrentTime:           now.UTC().Format(time.RFC3339Nano),
		TerminationTime:       sub.terminationTime.UTC().Format(time.RFC3339Nano),
	}, nil
}

// HandlePullMessages handles PullMessages requests with long polling: when no
// notification is queued it blocks up to Timeout, returning as soon as
// PostEvent produces one.
func (s *Server) HandlePullMessages(body interface{}) (interface{}, error) {
	var req PullMessagesRequest
	if err := unmarshalBody(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	timeout := defaultPullTimeout
	if req.Timeout != "" {
		if d, err := parseISODuration(req.Timeout); err == nil && d > 0 {
			timeout = d
		}
	}
	if timeout > maxPullTimeout {
		timeout = maxPullTimeout
	}

	messageLimit := req.MessageLimit
	if messageLimit <= 0 {
		messageLimit = 1
	}

	s.subscriptionMu.Lock()

	if s.eventHooks.ExpireOnNextPull {
		s.eventHooks.ExpireOnNextPull = false
		s.clearSubscriptionLocked()
		s.subscriptionMu.Unlock()

		return nil, fmt.Errorf("%w: subscription expired before PullMessages", ErrFaultInjected)
	}

	if s.eventHooks.RejectPull {
		s.eventStats.PullsRejected++
		s.subscriptionMu.Unlock()

		return nil, fmt.Errorf("%w: PullMessages rejected by test hook", ErrFaultInjected)
	}

	sub := s.subscription
	if sub == nil {
		s.subscriptionMu.Unlock()

		return nil, ErrSubscriptionNotFound
	}

	s.eventStats.Pulls++

	// Long polling is opt-in: handler-level unit tests and any caller that
	// expects an immediate empty response keep the original contract, while
	// the reliable event stream enables blocking so events are delivered the
	// moment PostEvent produces them.
	longPoll := s.longPollEvents

	// Wait under the lock for a notification or the timeout. PullMessages is
	// the only mutating reader of the queue, and PostEvent takes the same
	// lock, so no notification can be missed between the check and the wait.
	deadline := time.Now().Add(timeout)

	for longPoll && len(sub.queue) == 0 && time.Now().Before(deadline) {
		wake := sub.wake
		sub.pullWaiters++
		s.subscriptionMu.Unlock()

		remaining := time.Until(deadline)
		timer := time.NewTimer(remaining)
		select {
		case <-wake:
		case <-timer.C:
		}
		timer.Stop()

		s.subscriptionMu.Lock()

		sub = s.subscription
		if sub == nil {
			// The subscription expired or was replaced during the wait.
			s.subscriptionMu.Unlock()

			return nil, ErrSubscriptionNotFound
		}
		sub.pullWaiters--
	}

	now := s.nowLocked()

	var batch []queuedNotification
	if len(sub.queue) > 0 {
		n := len(sub.queue)
		if n > messageLimit {
			n = messageLimit
		}
		batch = sub.queue[:n]
		sub.queue = sub.queue[n:]
		last := batch[len(batch)-1]
		sub.lastDelivered = &last
		s.eventStats.NotificationsSent += len(batch)
	}

	term := sub.terminationTime
	s.subscriptionMu.Unlock()

	resp := &PullMessagesResponse{
		CurrentTime:          now.UTC().Format(time.RFC3339Nano),
		TerminationTime:      term.UTC().Format(time.RFC3339Nano),
		NotificationMessages: make([]NotificationMessageXML, 0, len(batch)),
	}

	for _, n := range batch {
		resp.NotificationMessages = append(resp.NotificationMessages, toNotificationXML(n))
	}

	return resp, nil
}

// HandleRenew handles Renew requests, extending the active subscription's
// termination time.
func (s *Server) HandleRenew(body interface{}) (interface{}, error) {
	var req RenewRequest
	if err := unmarshalBody(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	duration := defaultSubscriptionDuration
	if req.TerminationTime != "" {
		d, err := parseISODuration(req.TerminationTime)
		if err != nil {
			return nil, fmt.Errorf("invalid TerminationTime: %w", err)
		}
		duration = d
	}

	s.subscriptionMu.Lock()
	defer s.subscriptionMu.Unlock()

	if s.eventHooks.RejectRenew {
		s.eventStats.RenewsRejected++

		return nil, fmt.Errorf("%w: Renew rejected by test hook", ErrFaultInjected)
	}

	sub := s.subscription
	if sub == nil {
		return nil, ErrSubscriptionNotFound
	}

	now := s.nowLocked()
	sub.terminationTime = now.Add(duration)
	s.eventStats.Renews++
	s.scheduleExpiryLocked(sub, duration)

	return &RenewResponse{
		CurrentTime:     now.UTC().Format(time.RFC3339Nano),
		TerminationTime: sub.terminationTime.UTC().Format(time.RFC3339Nano),
	}, nil
}

// HandleUnsubscribe handles Unsubscribe requests, immediately terminating
// the active subscription. Unsubscribe carries no request fields, so there
// is nothing to decode from body.
func (s *Server) HandleUnsubscribe(_ interface{}) (interface{}, error) {
	s.subscriptionMu.Lock()
	defer s.subscriptionMu.Unlock()

	if s.subscription == nil {
		return nil, ErrSubscriptionNotFound
	}
	s.eventStats.Unsubscribes++
	s.clearSubscriptionLocked()

	return &UnsubscribeResponse{}, nil
}

// clearSubscriptionLocked stops the expiry timer and drops the active
// subscription. Callers must hold subscriptionMu.
func (s *Server) clearSubscriptionLocked() {
	if s.subscription == nil {
		return
	}
	if s.subscription.expiryTimer != nil {
		s.subscription.expiryTimer.Stop()
	}
	// Unblock any long-poll so it re-checks under the lock and reports the
	// subscription as gone rather than sleeping out its timeout.
	if s.subscription.wake != nil {
		close(s.subscription.wake)
	}
	s.subscription = nil
}

// scheduleExpiryLocked stops any expiry timer already pending for sub and
// schedules a new one that clears s.subscription after delay - but only if
// s.subscription is still sub by the time the timer fires. That
// pointer-identity check matters because CreatePullPointSubscription
// replaces the whole *eventSubscription rather than mutating one in place:
// without it, a very-late-firing timer from a since-replaced subscription
// could nil out a brand-new one. Must be called with s.subscriptionMu held.
func (s *Server) scheduleExpiryLocked(sub *eventSubscription, delay time.Duration) {
	if sub.expiryTimer != nil {
		sub.expiryTimer.Stop()
	}
	sub.expiryTimer = time.AfterFunc(delay, func() {
		s.subscriptionMu.Lock()
		defer s.subscriptionMu.Unlock()
		if s.subscription == sub {
			s.eventStats.SubscriptionsExpired++
			s.clearSubscriptionLocked()
		}
	})
}

// PostEvent enqueues a notification for delivery to the active pull-point
// subscription and immediately unblocks a PullMessages long-poll. It returns
// ErrSubscriptionNotFound when no subscription is active, and the advisory
// ErrEventQueueFull (wrapped, alongside a nil error is not possible - the
// event is still accepted) when an old entry had to be evicted.
func (s *Server) PostEvent(input EventInput) error {
	if input.Topic == "" {
		return fmt.Errorf("%w: event topic cannot be empty", ErrInvalidParameter)
	}

	eventTime := input.Time
	if eventTime.IsZero() {
		eventTime = s.now()
	}

	n := queuedNotification{
		topic:     input.Topic,
		producer:  input.ProducerAddress,
		operation: input.PropertyOperation,
		utcTime:   eventTime,
		source:    cloneItems(input.Source),
		key:       cloneItems(input.Key),
		data:      cloneItems(input.Data),
	}

	s.subscriptionMu.Lock()
	defer s.subscriptionMu.Unlock()

	sub := s.subscription
	if sub == nil {
		return ErrSubscriptionNotFound
	}

	var evicted error
	if len(sub.queue) >= eventQueueBound {
		sub.queue = sub.queue[1:]
		evicted = fmt.Errorf("%w: oldest notification evicted", ErrEventQueueFull)
	}

	sub.queue = append(sub.queue, n)
	s.eventStats.NotificationsQueued++

	if sub.wake != nil {
		close(sub.wake)
		sub.wake = make(chan struct{})
	}

	return evicted
}

// PostEventRepeat re-enqueues a byte-identical copy of the most recently
// delivered notification, simulating a camera redelivering after a
// reconnect. It falls back to the last queued notification when nothing has
// been pulled yet. It returns ErrSubscriptionNotFound when there is no
// notification to repeat or no active subscription.
func (s *Server) PostEventRepeat() error {
	s.subscriptionMu.Lock()
	defer s.subscriptionMu.Unlock()

	sub := s.subscription
	if sub == nil {
		return ErrSubscriptionNotFound
	}

	var repeat queuedNotification

	switch {
	case sub.lastDelivered != nil:
		repeat = *sub.lastDelivered
	case len(sub.queue) > 0:
		repeat = sub.queue[len(sub.queue)-1]
	default:
		return ErrSubscriptionNotFound
	}

	repeat.redeliver = true

	var evicted error
	if len(sub.queue) >= eventQueueBound {
		sub.queue = sub.queue[1:]
		evicted = fmt.Errorf("%w: oldest notification evicted", ErrEventQueueFull)
	}

	sub.queue = append(sub.queue, repeat)
	s.eventStats.NotificationsQueued++

	if sub.wake != nil {
		close(sub.wake)
		sub.wake = make(chan struct{})
	}

	return evicted
}

// SetEventTestHooks replaces the event-service test hooks. Passing the zero
// EventTestHooks restores production behavior. Intended for tests.
func (s *Server) SetEventTestHooks(hooks EventTestHooks) {
	s.subscriptionMu.Lock()
	defer s.subscriptionMu.Unlock()
	s.eventHooks = hooks
}

// EnableLongPolling makes idle PullMessages calls block until a notification
// is posted or the request timeout elapses, instead of returning an empty
// batch immediately. The reliable event stream relies on this to receive
// door-camera motion events without a polling delay. Safe to call before any
// subscription exists; the setting persists across re-subscriptions.
func (s *Server) EnableLongPolling() {
	s.subscriptionMu.Lock()
	defer s.subscriptionMu.Unlock()
	s.longPollEvents = true
}

// EventStats returns a copy of the event-service counters.
func (s *Server) EventStats() EventServiceStats {
	s.subscriptionMu.RLock()
	defer s.subscriptionMu.RUnlock()

	return s.eventStats
}

// WaitForPullWaiters blocks until at least count PullMessages calls are
// blocked in their long-poll, or the timeout elapses. It lets a test enqueue
// an event only after a pull is genuinely waiting, eliminating sleep-based
// timing. Intended for tests.
func (s *Server) WaitForPullWaiters(count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		s.subscriptionMu.RLock()
		waiters := 0
		if s.subscription != nil {
			waiters = s.subscription.pullWaiters
		}
		s.subscriptionMu.RUnlock()

		if waiters >= count {
			return true
		}

		time.Sleep(2 * time.Millisecond)
	}

	return false
}

// cloneItems copies a SimpleItem slice so later caller mutation cannot alter
// a queued notification.
func cloneItems(items []SimpleItem) []SimpleItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]SimpleItem, len(items))
	copy(out, items)

	return out
}

// toNotificationXML maps a queued notification to its wire representation.
func toNotificationXML(n queuedNotification) NotificationMessageXML {
	msg := NotificationMessageXML{
		Topic: TopicXML{Value: n.topic},
		Message: MessageWrapperXML{
			Inner: MessageBodyXML{
				PropertyOperation: n.operation,
				UtcTime:           n.utcTime.UTC().Format(time.RFC3339Nano),
			},
		},
	}

	if n.producer != "" {
		msg.ProducerReference = &ReferenceXML{Address: n.producer}
	}

	if len(n.source) > 0 {
		msg.Message.Inner.Source = &ItemContainerXML{SimpleItems: toSimpleItemXML(n.source)}
	}
	if len(n.key) > 0 {
		msg.Message.Inner.Key = &ItemContainerXML{SimpleItems: toSimpleItemXML(n.key)}
	}
	if len(n.data) > 0 {
		msg.Message.Inner.Data = &ItemContainerXML{SimpleItems: toSimpleItemXML(n.data)}
	}

	return msg
}

// toSimpleItemXML maps the server SimpleItem type to the wire type.
func toSimpleItemXML(items []SimpleItem) []SimpleItemXML {
	out := make([]SimpleItemXML, len(items))
	for i, item := range items {
		out[i] = SimpleItemXML{Name: item.Name, Value: item.Value}
	}

	return out
}

// eventsServiceURL builds the URL this server's events service is reachable
// at, using the same host-normalization and base-URL construction as
// HandleGetCapabilities (device.go).
func (s *Server) eventsServiceURL() string {
	return s.advertisedBaseURL() + "/events_service"
}

// isoDurationPattern matches the limited ISO-8601 duration grammar this
// repo's own client ever produces (event.go's formatDuration): "PT<n>S",
// "PT<n>M", or "PT<n>M<n>S". No other producer of these values exists to
// test against, so a general ISO-8601 duration parser is out of scope.
var isoDurationPattern = regexp.MustCompile(`^PT(?:(\d+)M)?(?:(\d+)S)?$`)

// parseISODuration parses an ISO-8601 duration string in the grammar
// described by isoDurationPattern.
func parseISODuration(s string) (time.Duration, error) {
	m := isoDurationPattern.FindStringSubmatch(s)
	if m == nil || (m[1] == "" && m[2] == "") {
		return 0, fmt.Errorf("%w: %s", ErrInvalidDuration, s)
	}

	var d time.Duration

	if m[1] != "" {
		minutes, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, fmt.Errorf("%w: %s", ErrInvalidDuration, s)
		}
		d += time.Duration(minutes) * time.Minute
	}

	if m[2] != "" {
		seconds, err := strconv.Atoi(m[2])
		if err != nil {
			return 0, fmt.Errorf("%w: %s", ErrInvalidDuration, s)
		}
		d += time.Duration(seconds) * time.Second
	}

	return d, nil
}
