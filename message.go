package mq

import (
	"time"

	"github.com/1set/starlet/dataconv"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// MessageResult represents a unified message structure across different services
type MessageResult struct {
	// Core message data
	MessageID  string                 `json:"message_id"`
	Body       string                 `json:"body"`
	Properties map[string]interface{} `json:"properties,omitempty"`

	// Unified attributes
	SessionID       string `json:"session_id,omitempty"`       // Unified grouping/session
	CorrelationID   string `json:"correlation_id,omitempty"`   // Request correlation
	ReplyTo         string `json:"reply_to,omitempty"`         // Response destination
	DeduplicationID string `json:"deduplication_id,omitempty"` // For duplicate detection

	// Timing information
	EnqueueTime   time.Time `json:"enqueue_time,omitempty"`    // When message was queued
	ScheduledTime time.Time `json:"scheduled_time,omitempty"`  // When message becomes available
	LockExpiresAt time.Time `json:"lock_expires_at,omitempty"` // When lock expires
	TimeToLive    int       `json:"time_to_live,omitempty"`    // Message TTL in seconds

	// Processing information
	DeliveryCount int `json:"delivery_count,omitempty"` // Number of delivery attempts

	// Internal service-specific data
	ReceiptHandle string `json:"receipt_handle,omitempty"` // Service-specific handle for operations

	// Result status
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// Struct returns a Starlark struct representation of the MessageResult
func (m *MessageResult) Struct() *starlarkstruct.Struct {
	data := map[string]starlark.Value{
		"message_id":       starlark.String(m.MessageID),
		"body":             starlark.String(m.Body),
		"success":          starlark.Bool(m.Success),
		"session_id":       starlark.String(m.SessionID),
		"correlation_id":   starlark.String(m.CorrelationID),
		"reply_to":         starlark.String(m.ReplyTo),
		"deduplication_id": starlark.String(m.DeduplicationID),
		"delivery_count":   starlark.MakeInt(m.DeliveryCount),
		"time_to_live":     starlark.MakeInt(m.TimeToLive),
		"receipt_handle":   starlark.String(m.ReceiptHandle),
	}

	// Convert properties to Starlark value
	if m.Properties != nil {
		if props, err := dataconv.Marshal(m.Properties); err == nil {
			data["properties"] = props
		} else {
			data["properties"] = starlark.NewDict(0)
		}
	} else {
		data["properties"] = starlark.NewDict(0)
	}

	// Convert time fields to strings
	if !m.EnqueueTime.IsZero() {
		data["enqueue_time"] = starlark.String(m.EnqueueTime.Format(time.RFC3339))
	} else {
		data["enqueue_time"] = starlark.None
	}

	if !m.ScheduledTime.IsZero() {
		data["scheduled_time"] = starlark.String(m.ScheduledTime.Format(time.RFC3339))
	} else {
		data["scheduled_time"] = starlark.None
	}

	if !m.LockExpiresAt.IsZero() {
		data["lock_expires_at"] = starlark.String(m.LockExpiresAt.Format(time.RFC3339))
	} else {
		data["lock_expires_at"] = starlark.None
	}

	// Add error field
	if m.Error != "" {
		data["error"] = starlark.String(m.Error)
	} else {
		data["error"] = starlark.None
	}

	return starlarkstruct.FromStringDict(starlarkstruct.Default, data)
}

// Queue represents a unified queue structure
type Queue struct {
	// Basic information
	Name        string `json:"name"`
	ServiceType string `json:"service_type"`
	URL         string `json:"url,omitempty"`

	// Queue status
	MessageCount int `json:"message_count,omitempty"`

	// Unified configuration
	LockDuration     int  `json:"lock_duration"`      // Unified lock/visibility timeout in seconds
	RetentionPeriod  int  `json:"retention_period"`   // Message retention in seconds
	MaxDeliveryCount int  `json:"max_delivery_count"` // Maximum delivery attempts
	EnableSessions   bool `json:"enable_sessions"`    // Unified ordered processing
	MaxQueueSize     int  `json:"max_queue_size"`     // Queue size in bytes (-1 = unlimited)

	// Dead letter queue configuration
	DeadLetterConfig *DeadLetterConfig `json:"dead_letter_config,omitempty"`

	// Duplicate detection
	DuplicateDetection *DuplicateDetectionConfig `json:"duplicate_detection,omitempty"`

	// Timestamps
	CreatedTime  time.Time `json:"created_time,omitempty"`
	ModifiedTime time.Time `json:"modified_time,omitempty"`
}

// DeadLetterConfig represents unified dead letter queue configuration
type DeadLetterConfig struct {
	Enabled          bool   `json:"enabled"`
	MaxDeliveryCount int    `json:"max_delivery_count"`
	QueueName        string `json:"queue_name,omitempty"`
}

// DuplicateDetectionConfig represents unified duplicate detection configuration
type DuplicateDetectionConfig struct {
	Enabled       bool `json:"enabled"`
	WindowSeconds int  `json:"window_seconds"`
}

// Struct returns a Starlark struct representation of the Queue
func (q *Queue) Struct() *starlarkstruct.Struct {
	data := map[string]starlark.Value{
		"name":               starlark.String(q.Name),
		"service_type":       starlark.String(q.ServiceType),
		"url":                starlark.String(q.URL),
		"message_count":      starlark.MakeInt(q.MessageCount),
		"lock_duration":      starlark.MakeInt(q.LockDuration),
		"retention_period":   starlark.MakeInt(q.RetentionPeriod),
		"max_delivery_count": starlark.MakeInt(q.MaxDeliveryCount),
		"enable_sessions":    starlark.Bool(q.EnableSessions),
		"max_queue_size":     starlark.MakeInt(q.MaxQueueSize),
	}

	// Add dead letter configuration
	if q.DeadLetterConfig != nil {
		dlqData := map[string]starlark.Value{
			"enabled":            starlark.Bool(q.DeadLetterConfig.Enabled),
			"max_delivery_count": starlark.MakeInt(q.DeadLetterConfig.MaxDeliveryCount),
			"queue_name":         starlark.String(q.DeadLetterConfig.QueueName),
		}
		data["dead_letter_config"] = starlarkstruct.FromStringDict(starlarkstruct.Default, dlqData)
	} else {
		data["dead_letter_config"] = starlark.None
	}

	// Add duplicate detection configuration
	if q.DuplicateDetection != nil {
		ddData := map[string]starlark.Value{
			"enabled":        starlark.Bool(q.DuplicateDetection.Enabled),
			"window_seconds": starlark.MakeInt(q.DuplicateDetection.WindowSeconds),
		}
		data["duplicate_detection"] = starlarkstruct.FromStringDict(starlarkstruct.Default, ddData)
	} else {
		data["duplicate_detection"] = starlark.None
	}

	// Convert timestamps
	if !q.CreatedTime.IsZero() {
		data["created_time"] = starlark.String(q.CreatedTime.Format(time.RFC3339))
	} else {
		data["created_time"] = starlark.None
	}

	if !q.ModifiedTime.IsZero() {
		data["modified_time"] = starlark.String(q.ModifiedTime.Format(time.RFC3339))
	} else {
		data["modified_time"] = starlark.None
	}

	return starlarkstruct.FromStringDict(starlarkstruct.Default, data)
}

// Topic represents a topic structure (Azure Service Bus only)
type Topic struct {
	Name              string `json:"name"`
	ServiceType       string `json:"service_type"`
	SubscriptionCount int    `json:"subscription_count,omitempty"`
	MessageCount      int    `json:"message_count,omitempty"`
	SizeInBytes       int64  `json:"size_in_bytes,omitempty"`
	MaxSize           int64  `json:"max_size,omitempty"`
	RetentionPeriod   int    `json:"retention_period,omitempty"`

	// Duplicate detection
	DuplicateDetection *DuplicateDetectionConfig `json:"duplicate_detection,omitempty"`

	// Timestamps
	CreatedTime  time.Time `json:"created_time,omitempty"`
	ModifiedTime time.Time `json:"modified_time,omitempty"`
}

// Struct returns a Starlark struct representation of the Topic
func (t *Topic) Struct() *starlarkstruct.Struct {
	data := map[string]starlark.Value{
		"name":               starlark.String(t.Name),
		"service_type":       starlark.String(t.ServiceType),
		"subscription_count": starlark.MakeInt(t.SubscriptionCount),
		"message_count":      starlark.MakeInt(t.MessageCount),
		"size_in_bytes":      starlark.MakeInt64(t.SizeInBytes),
		"max_size":           starlark.MakeInt64(t.MaxSize),
		"retention_period":   starlark.MakeInt(t.RetentionPeriod),
	}

	// Add duplicate detection configuration
	if t.DuplicateDetection != nil {
		ddData := map[string]starlark.Value{
			"enabled":        starlark.Bool(t.DuplicateDetection.Enabled),
			"window_seconds": starlark.MakeInt(t.DuplicateDetection.WindowSeconds),
		}
		data["duplicate_detection"] = starlarkstruct.FromStringDict(starlarkstruct.Default, ddData)
	} else {
		data["duplicate_detection"] = starlark.None
	}

	// Convert timestamps
	if !t.CreatedTime.IsZero() {
		data["created_time"] = starlark.String(t.CreatedTime.Format(time.RFC3339))
	} else {
		data["created_time"] = starlark.None
	}

	if !t.ModifiedTime.IsZero() {
		data["modified_time"] = starlark.String(t.ModifiedTime.Format(time.RFC3339))
	} else {
		data["modified_time"] = starlark.None
	}

	return starlarkstruct.FromStringDict(starlarkstruct.Default, data)
}

// Subscription represents a subscription structure (Azure Service Bus only)
type Subscription struct {
	Name                   string               `json:"name"`
	TopicName              string               `json:"topic_name"`
	MessageCount           int                  `json:"message_count,omitempty"`
	DeadLetterMessageCount int                  `json:"dead_letter_message_count,omitempty"`
	LockDuration           int                  `json:"lock_duration,omitempty"`
	MaxDeliveryCount       int                  `json:"max_delivery_count,omitempty"`
	Filters                []SubscriptionFilter `json:"filters,omitempty"`
	CreatedTime            time.Time            `json:"created_time,omitempty"`
}

// SubscriptionFilter represents a subscription filter rule
type SubscriptionFilter struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
	Type       string `json:"type"` // sql_filter, correlation_filter, etc.
}

// Struct returns a Starlark struct representation of the Subscription
func (s *Subscription) Struct() *starlarkstruct.Struct {
	data := map[string]starlark.Value{
		"name":                      starlark.String(s.Name),
		"topic_name":                starlark.String(s.TopicName),
		"message_count":             starlark.MakeInt(s.MessageCount),
		"dead_letter_message_count": starlark.MakeInt(s.DeadLetterMessageCount),
		"lock_duration":             starlark.MakeInt(s.LockDuration),
		"max_delivery_count":        starlark.MakeInt(s.MaxDeliveryCount),
	}

	// Convert filters
	if len(s.Filters) > 0 {
		filters := make([]starlark.Value, len(s.Filters))
		for i, filter := range s.Filters {
			filterData := map[string]starlark.Value{
				"name":       starlark.String(filter.Name),
				"expression": starlark.String(filter.Expression),
				"type":       starlark.String(filter.Type),
			}
			filters[i] = starlarkstruct.FromStringDict(starlarkstruct.Default, filterData)
		}
		data["filters"] = starlark.NewList(filters)
	} else {
		data["filters"] = starlark.NewList(nil)
	}

	// Convert timestamp
	if !s.CreatedTime.IsZero() {
		data["created_time"] = starlark.String(s.CreatedTime.Format(time.RFC3339))
	} else {
		data["created_time"] = starlark.None
	}

	return starlarkstruct.FromStringDict(starlarkstruct.Default, data)
}

// ClientInfo represents information about a connected client
type ClientInfo struct {
	ServiceType       string                 `json:"service_type"`
	Region            string                 `json:"region,omitempty"`
	Namespace         string                 `json:"namespace,omitempty"`
	Connected         bool                   `json:"connected"`
	SupportedFeatures []string               `json:"supported_features"`
	Capabilities      map[string]interface{} `json:"capabilities,omitempty"`
}

// Struct returns a Starlark struct representation of the ClientInfo
func (c *ClientInfo) Struct() *starlarkstruct.Struct {
	data := map[string]starlark.Value{
		"service_type": starlark.String(c.ServiceType),
		"region":       starlark.String(c.Region),
		"namespace":    starlark.String(c.Namespace),
		"connected":    starlark.Bool(c.Connected),
	}

	// Convert supported features
	features := make([]starlark.Value, len(c.SupportedFeatures))
	for i, feature := range c.SupportedFeatures {
		features[i] = starlark.String(feature)
	}
	data["supported_features"] = starlark.NewList(features)

	// Convert capabilities
	if c.Capabilities != nil {
		if caps, err := dataconv.Marshal(c.Capabilities); err == nil {
			data["capabilities"] = caps
		} else {
			data["capabilities"] = starlark.NewDict(0)
		}
	} else {
		data["capabilities"] = starlark.NewDict(0)
	}

	return starlarkstruct.FromStringDict(starlarkstruct.Default, data)
}
