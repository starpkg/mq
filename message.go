package mq

import (
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
