package mq

import (
	"fmt"
	"time"

	"github.com/1set/starlet/dataconv"
	"go.starlark.net/starlark"
)

// Queue represents a message queue with unified properties across services
type Queue struct {
	// Basic queue information
	Name        string `json:"name"`
	ServiceType string `json:"service_type"` // Which service backs this queue
	URL         string `json:"url"`          // Service-specific queue URL/identifier

	// Queue statistics
	MessageCount int `json:"message_count"`

	// Queue configuration (unified across services)
	LockDuration    int `json:"lock_duration"`    // Message lock duration in seconds
	RetentionPeriod int `json:"retention_period"` // Message retention in seconds

	// Dead letter queue configuration
	MaxDeliveryCount int               `json:"max_delivery_count"`
	DeadLetterConfig *DeadLetterConfig `json:"dead_letter_config"`

	// Message ordering and deduplication
	EnableSessions     bool                `json:"enable_sessions"`     // Ordered processing support
	DuplicateDetection *DuplicateDetection `json:"duplicate_detection"` // Deduplication configuration

	// Queue limits
	MaxQueueSize int64 `json:"max_queue_size"` // Queue size in bytes (-1 for unlimited)

	// Timestamps
	CreatedTime  time.Time `json:"created_time"`
	ModifiedTime time.Time `json:"modified_time"`
}

// DuplicateDetection represents duplicate detection configuration
type DuplicateDetection struct {
	Enabled       bool `json:"enabled"`
	WindowSeconds int  `json:"window_seconds"` // Deduplication window in seconds
}

// ToStarlark converts Queue to a Starlark dict
func (q *Queue) ToStarlark() (starlark.Value, error) {
	result := make(map[string]interface{})

	result["name"] = q.Name
	result["service_type"] = q.ServiceType
	result["url"] = q.URL
	result["message_count"] = q.MessageCount
	result["lock_duration"] = q.LockDuration
	result["retention_period"] = q.RetentionPeriod
	result["max_delivery_count"] = q.MaxDeliveryCount
	result["enable_sessions"] = q.EnableSessions
	result["max_queue_size"] = q.MaxQueueSize
	result["created_time"] = q.CreatedTime.Format(time.RFC3339)
	result["modified_time"] = q.ModifiedTime.Format(time.RFC3339)

	// Handle dead letter config
	if q.DeadLetterConfig != nil {
		dlqConfig := map[string]interface{}{
			"enabled":            q.DeadLetterConfig.Enabled,
			"queue_name":         q.DeadLetterConfig.QueueName,
			"max_delivery_count": q.DeadLetterConfig.MaxDeliveryCount,
		}
		result["dead_letter_config"] = dlqConfig
	} else {
		result["dead_letter_config"] = nil
	}

	// Handle duplicate detection
	if q.DuplicateDetection != nil {
		dupDetection := map[string]interface{}{
			"enabled":        q.DuplicateDetection.Enabled,
			"window_seconds": q.DuplicateDetection.WindowSeconds,
		}
		result["duplicate_detection"] = dupDetection
	} else {
		result["duplicate_detection"] = nil
	}

	return dataconv.Marshal(result)
}

// FromStarlark populates Queue from a Starlark value
func (q *Queue) FromStarlark(val starlark.Value) error {
	dict, ok := val.(*starlark.Dict)
	if !ok {
		return fmt.Errorf("expected dict, got %T", val)
	}

	data, err := convertStarlarkDictToInterface(dict)
	if err != nil {
		return err
	}

	if name, ok := data["name"].(string); ok {
		q.Name = name
	}
	if serviceType, ok := data["service_type"].(string); ok {
		q.ServiceType = serviceType
	}
	if url, ok := data["url"].(string); ok {
		q.URL = url
	}
	if messageCount, ok := data["message_count"].(int); ok {
		q.MessageCount = messageCount
	}
	if lockDuration, ok := data["lock_duration"].(int); ok {
		q.LockDuration = lockDuration
	}
	if retentionPeriod, ok := data["retention_period"].(int); ok {
		q.RetentionPeriod = retentionPeriod
	}
	if maxDeliveryCount, ok := data["max_delivery_count"].(int); ok {
		q.MaxDeliveryCount = maxDeliveryCount
	}
	if enableSessions, ok := data["enable_sessions"].(bool); ok {
		q.EnableSessions = enableSessions
	}
	if maxQueueSize, ok := data["max_queue_size"].(int64); ok {
		q.MaxQueueSize = maxQueueSize
	}

	// Parse time fields
	if createdTimeStr, ok := data["created_time"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdTimeStr); err == nil {
			q.CreatedTime = t
		}
	}
	if modifiedTimeStr, ok := data["modified_time"].(string); ok {
		if t, err := time.Parse(time.RFC3339, modifiedTimeStr); err == nil {
			q.ModifiedTime = t
		}
	}

	// Parse dead letter config
	if dlqData, ok := data["dead_letter_config"].(map[string]interface{}); ok {
		q.DeadLetterConfig = &DeadLetterConfig{}
		if enabled, ok := dlqData["enabled"].(bool); ok {
			q.DeadLetterConfig.Enabled = enabled
		}
		if queueName, ok := dlqData["queue_name"].(string); ok {
			q.DeadLetterConfig.QueueName = queueName
		}
		if maxDelCount, ok := dlqData["max_delivery_count"].(int); ok {
			q.DeadLetterConfig.MaxDeliveryCount = maxDelCount
		}
	}

	// Parse duplicate detection
	if dupData, ok := data["duplicate_detection"].(map[string]interface{}); ok {
		q.DuplicateDetection = &DuplicateDetection{}
		if enabled, ok := dupData["enabled"].(bool); ok {
			q.DuplicateDetection.Enabled = enabled
		}
		if windowSecs, ok := dupData["window_seconds"].(int); ok {
			q.DuplicateDetection.WindowSeconds = windowSecs
		}
	}

	return nil
}

// Copy creates a copy of the Queue
func (q *Queue) Copy() *Queue {
	result := &Queue{
		Name:             q.Name,
		ServiceType:      q.ServiceType,
		URL:              q.URL,
		MessageCount:     q.MessageCount,
		LockDuration:     q.LockDuration,
		RetentionPeriod:  q.RetentionPeriod,
		MaxDeliveryCount: q.MaxDeliveryCount,
		EnableSessions:   q.EnableSessions,
		MaxQueueSize:     q.MaxQueueSize,
		CreatedTime:      q.CreatedTime,
		ModifiedTime:     q.ModifiedTime,
	}

	// Copy dead letter config
	if q.DeadLetterConfig != nil {
		result.DeadLetterConfig = &DeadLetterConfig{
			Enabled:          q.DeadLetterConfig.Enabled,
			QueueName:        q.DeadLetterConfig.QueueName,
			MaxDeliveryCount: q.DeadLetterConfig.MaxDeliveryCount,
		}
	}

	// Copy duplicate detection
	if q.DuplicateDetection != nil {
		result.DuplicateDetection = &DuplicateDetection{
			Enabled:       q.DuplicateDetection.Enabled,
			WindowSeconds: q.DuplicateDetection.WindowSeconds,
		}
	}

	return result
}

// IsDeadLetterEnabled checks if dead letter queue is enabled
func (q *Queue) IsDeadLetterEnabled() bool {
	return q.DeadLetterConfig != nil && q.DeadLetterConfig.Enabled
}

// IsDuplicateDetectionEnabled checks if duplicate detection is enabled
func (q *Queue) IsDuplicateDetectionEnabled() bool {
	return q.DuplicateDetection != nil && q.DuplicateDetection.Enabled
}

// GetLockDuration returns the lock duration as a time.Duration
func (q *Queue) GetLockDuration() time.Duration {
	return time.Duration(q.LockDuration) * time.Second
}

// GetRetentionPeriod returns the retention period as a time.Duration
func (q *Queue) GetRetentionPeriod() time.Duration {
	return time.Duration(q.RetentionPeriod) * time.Second
}

// NewQueue creates a new Queue with default values
func NewQueue(name, serviceType string) *Queue {
	now := time.Now()
	return &Queue{
		Name:             name,
		ServiceType:      serviceType,
		MessageCount:     0,
		LockDuration:     30,      // 30 seconds default
		RetentionPeriod:  1209600, // 14 days default
		MaxDeliveryCount: 10,      // 10 attempts default
		EnableSessions:   false,
		MaxQueueSize:     -1, // Unlimited
		CreatedTime:      now,
		ModifiedTime:     now,
	}
}

// queueSliceToStarlark converts a slice of Queue to Starlark list
func queueSliceToStarlark(queues []*Queue) (starlark.Value, error) {
	values := make([]starlark.Value, len(queues))
	for i, queue := range queues {
		val, err := queue.ToStarlark()
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	return starlark.NewList(values), nil
}
