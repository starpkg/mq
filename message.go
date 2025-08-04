package mq

import (
	"fmt"
	"time"

	"github.com/1set/starlet/dataconv"
	"go.starlark.net/starlark"
)

// MessageResult represents a message received from or sent to a queue
type MessageResult struct {
	// Message identification
	MessageID string `json:"message_id"`
	Body      string `json:"body"`

	// Message metadata and properties
	Properties map[string]interface{} `json:"properties"`

	// Message grouping and correlation
	SessionID     string `json:"session_id"`
	CorrelationID string `json:"correlation_id"`
	ReplyTo       string `json:"reply_to"`

	// Timing information
	EnqueueTime   time.Time  `json:"enqueue_time"`
	ScheduledTime *time.Time `json:"scheduled_time,omitempty"`
	LockExpiresAt *time.Time `json:"lock_expires_at,omitempty"`

	// Delivery information
	DeliveryCount int `json:"delivery_count"`
	TimeToLive    int `json:"time_to_live"` // TTL in seconds

	// Service-specific information (internal use)
	ReceiptHandle   string      `json:"receipt_handle"` // Service-specific handle for acknowledgment
	OriginalMessage interface{} `json:"-"`              // Original service-specific message object (not serialized)

	// Operation result
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// ToStarlark converts MessageResult to a Starlark dict
func (m *MessageResult) ToStarlark() (starlark.Value, error) {
	result := make(map[string]interface{})

	result["message_id"] = m.MessageID
	result["body"] = m.Body
	result["properties"] = m.Properties
	result["session_id"] = m.SessionID
	result["correlation_id"] = m.CorrelationID
	result["reply_to"] = m.ReplyTo
	result["enqueue_time"] = m.EnqueueTime.Format(time.RFC3339)

	if m.ScheduledTime != nil {
		result["scheduled_time"] = m.ScheduledTime.Format(time.RFC3339)
	} else {
		result["scheduled_time"] = nil
	}

	if m.LockExpiresAt != nil {
		result["lock_expires_at"] = m.LockExpiresAt.Format(time.RFC3339)
	} else {
		result["lock_expires_at"] = nil
	}

	result["delivery_count"] = m.DeliveryCount
	result["time_to_live"] = m.TimeToLive
	result["receipt_handle"] = m.ReceiptHandle
	result["success"] = m.Success
	result["error"] = m.Error

	return dataconv.Marshal(result)
}

// FromStarlark populates MessageResult from a Starlark value
func (m *MessageResult) FromStarlark(val starlark.Value) error {
	dict, ok := val.(*starlark.Dict)
	if !ok {
		return fmt.Errorf("expected dict, got %T", val)
	}

	data, err := convertStarlarkDictToInterface(dict)
	if err != nil {
		return err
	}

	if messageID, ok := data["message_id"].(string); ok {
		m.MessageID = messageID
	}
	if body, ok := data["body"].(string); ok {
		m.Body = body
	}
	if properties, ok := data["properties"].(map[string]interface{}); ok {
		m.Properties = properties
	}
	if sessionID, ok := data["session_id"].(string); ok {
		m.SessionID = sessionID
	}
	if correlationID, ok := data["correlation_id"].(string); ok {
		m.CorrelationID = correlationID
	}
	if replyTo, ok := data["reply_to"].(string); ok {
		m.ReplyTo = replyTo
	}
	if deliveryCount, ok := data["delivery_count"].(int); ok {
		m.DeliveryCount = deliveryCount
	}
	if timeToLive, ok := data["time_to_live"].(int); ok {
		m.TimeToLive = timeToLive
	}
	if receiptHandle, ok := data["receipt_handle"].(string); ok {
		m.ReceiptHandle = receiptHandle
	}
	if success, ok := data["success"].(bool); ok {
		m.Success = success
	}
	if errMsg, ok := data["error"].(string); ok {
		m.Error = errMsg
	}

	// Parse time fields
	if enqueueTimeStr, ok := data["enqueue_time"].(string); ok {
		if t, err := time.Parse(time.RFC3339, enqueueTimeStr); err == nil {
			m.EnqueueTime = t
		}
	}
	if scheduledTimeStr, ok := data["scheduled_time"].(string); ok && scheduledTimeStr != "" {
		if t, err := time.Parse(time.RFC3339, scheduledTimeStr); err == nil {
			m.ScheduledTime = &t
		}
	}
	if lockExpiresStr, ok := data["lock_expires_at"].(string); ok && lockExpiresStr != "" {
		if t, err := time.Parse(time.RFC3339, lockExpiresStr); err == nil {
			m.LockExpiresAt = &t
		}
	}

	return nil
}

// Copy creates a copy of the MessageResult
func (m *MessageResult) Copy() *MessageResult {
	result := &MessageResult{
		MessageID:     m.MessageID,
		Body:          m.Body,
		SessionID:     m.SessionID,
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		EnqueueTime:   m.EnqueueTime,
		DeliveryCount: m.DeliveryCount,
		TimeToLive:    m.TimeToLive,
		ReceiptHandle: m.ReceiptHandle,
		Success:       m.Success,
		Error:         m.Error,
	}

	// Copy properties map
	if m.Properties != nil {
		result.Properties = make(map[string]interface{})
		for k, v := range m.Properties {
			result.Properties[k] = v
		}
	}

	// Copy time pointers
	if m.ScheduledTime != nil {
		scheduled := *m.ScheduledTime
		result.ScheduledTime = &scheduled
	}
	if m.LockExpiresAt != nil {
		lockExpires := *m.LockExpiresAt
		result.LockExpiresAt = &lockExpires
	}

	return result
}

// IsExpired checks if the message lock has expired
func (m *MessageResult) IsExpired() bool {
	if m.LockExpiresAt == nil {
		return false
	}
	return time.Now().After(*m.LockExpiresAt)
}

// TimeUntilExpiry returns the duration until the message lock expires
func (m *MessageResult) TimeUntilExpiry() time.Duration {
	if m.LockExpiresAt == nil {
		return 0
	}
	return time.Until(*m.LockExpiresAt)
}

// IsScheduled checks if the message is scheduled for future delivery
func (m *MessageResult) IsScheduled() bool {
	if m.ScheduledTime == nil {
		return false
	}
	return m.ScheduledTime.After(time.Now())
}

// TimeUntilScheduled returns the duration until the message becomes available
func (m *MessageResult) TimeUntilScheduled() time.Duration {
	if m.ScheduledTime == nil {
		return 0
	}
	return time.Until(*m.ScheduledTime)
}

// NewMessageResult creates a new MessageResult with basic information
func NewMessageResult(messageID, body string) *MessageResult {
	return &MessageResult{
		MessageID:   messageID,
		Body:        body,
		Properties:  make(map[string]interface{}),
		EnqueueTime: time.Now(),
		Success:     true,
	}
}

// NewErrorMessageResult creates a MessageResult representing an error
func NewErrorMessageResult(messageID, errorMsg string) *MessageResult {
	return &MessageResult{
		MessageID: messageID,
		Success:   false,
		Error:     errorMsg,
	}
}

// messageResultSliceToStarlark converts a slice of MessageResult to Starlark list
func messageResultSliceToStarlark(messages []*MessageResult) (starlark.Value, error) {
	values := make([]starlark.Value, len(messages))
	for i, msg := range messages {
		val, err := msg.ToStarlark()
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	return starlark.NewList(values), nil
}

// boolSliceToStarlark converts a slice of bool to Starlark list
func boolSliceToStarlark(bools []bool) starlark.Value {
	values := make([]starlark.Value, len(bools))
	for i, b := range bools {
		values[i] = starlark.Bool(b)
	}
	return starlark.NewList(values)
}
