package mq

import (
	"fmt"
	"time"
)

// Configuration key constants
const (
	configKeyServiceType         = "service_type"
	configKeyConnectionString    = "connection_string"
	configKeyTimeout             = "timeout"
	configKeyMaxRetries          = "max_retries"
	configKeyAWSRegion           = "aws_region"
	configKeyAWSAccessKey        = "aws_access_key"
	configKeyAWSSecretKey        = "aws_secret_key"
	configKeyAWSSessionToken     = "aws_session_token"
	configKeyDefaultLockDuration = "default_lock_duration"
	configKeyDefaultBatchSize    = "default_batch_size"
)

// Service type constants
const (
	ServiceTypeAWSSQS          = "aws_sqs"
	ServiceTypeAzureServiceBus = "azure_servicebus"
	ServiceTypeAuto            = "auto"
)

// ClientConfig contains configuration for a message queue client
type ClientConfig struct {
	// Service configuration
	ServiceType      string // Service type (aws_sqs, azure_servicebus, auto)
	ConnectionString string // Azure Service Bus connection string

	// AWS specific configuration
	AWSRegion       string // AWS region
	AWSAccessKey    string // AWS access key ID
	AWSSecretKey    string // AWS secret access key
	AWSSessionToken string // AWS session token

	// Connection and performance settings
	Timeout    int // Connection timeout in seconds
	MaxRetries int // Maximum retry attempts

	// Default operation settings
	DefaultLockDuration int // Default message lock duration in seconds
	DefaultBatchSize    int // Default batch size for operations
}

// GetTimeout returns the timeout as a time.Duration
func (c *ClientConfig) GetTimeout() time.Duration {
	return time.Duration(c.Timeout) * time.Second
}

// GetDefaultLockDuration returns the default lock duration as a time.Duration
func (c *ClientConfig) GetDefaultLockDuration() time.Duration {
	return time.Duration(c.DefaultLockDuration) * time.Second
}

// Validate validates the configuration
func (c *ClientConfig) Validate() error {
	if c.ServiceType == "" {
		return fmt.Errorf("service_type is required")
	}

	switch c.ServiceType {
	case "aws_sqs":
		if c.AWSRegion == "" {
			return fmt.Errorf("aws_region is required for AWS SQS")
		}
	case "azure_servicebus":
		if c.ConnectionString == "" {
			return fmt.Errorf("connection_string is required for Azure Service Bus")
		}
	default:
		return fmt.Errorf("unsupported service type: %s", c.ServiceType)
	}

	if c.Timeout < 0 {
		return fmt.Errorf("timeout must be non-negative")
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be non-negative")
	}

	if c.DefaultLockDuration <= 0 {
		return fmt.Errorf("default_lock_duration must be positive")
	}

	if c.DefaultBatchSize <= 0 {
		return fmt.Errorf("default_batch_size must be positive")
	}

	return nil
}

// Copy creates a copy of the configuration
func (c *ClientConfig) Copy() *ClientConfig {
	return &ClientConfig{
		ServiceType:         c.ServiceType,
		ConnectionString:    c.ConnectionString,
		AWSRegion:           c.AWSRegion,
		AWSAccessKey:        c.AWSAccessKey,
		AWSSecretKey:        c.AWSSecretKey,
		AWSSessionToken:     c.AWSSessionToken,
		Timeout:             c.Timeout,
		MaxRetries:          c.MaxRetries,
		DefaultLockDuration: c.DefaultLockDuration,
		DefaultBatchSize:    c.DefaultBatchSize,
	}
}

// QueueOptions contains options for creating or configuring a queue
type QueueOptions struct {
	// Lock duration for messages (unified visibility timeout/lock duration)
	LockDuration int // Message lock duration in seconds

	// Message retention settings
	RetentionPeriod int // Message retention period in seconds

	// Dead letter queue configuration
	MaxDeliveryCount int               // Maximum delivery attempts before moving to DLQ
	DeadLetterConfig *DeadLetterConfig // Dead letter queue configuration

	// Message ordering and deduplication
	EnableSessions      bool // Enable sessions for message ordering (Azure) or FIFO (AWS)
	DuplicateDetection  bool // Enable duplicate message detection
	DuplicateWindowSecs int  // Duplicate detection window in seconds

	// Queue size limits
	MaxQueueSize int64 // Maximum queue size in bytes (-1 for unlimited)
}

// DeadLetterConfig contains dead letter queue configuration
type DeadLetterConfig struct {
	Enabled          bool   // Whether DLQ is enabled
	QueueName        string // Dead letter queue name
	MaxDeliveryCount int    // Maximum delivery count before moving to DLQ
}

// MessageOptions contains options for sending messages
type MessageOptions struct {
	// Message properties and metadata
	Properties map[string]interface{} // Message properties/attributes

	// Message scheduling
	ScheduledTime *time.Time // When message should become available for processing

	// Message grouping and correlation
	SessionID     string // Session ID for ordered processing
	CorrelationID string // Correlation ID for request tracking
	ReplyTo       string // Response destination queue

	// Message lifecycle
	TimeToLive int    // Message TTL in seconds
	MessageID  string // Message ID for deduplication
}

// ReceiveOptions contains options for receiving messages
type ReceiveOptions struct {
	// Receive behavior
	MaxCount int  // Maximum number of messages to receive
	WaitTime int  // Long polling wait time in seconds
	PeekOnly bool // Peek messages without receiving them

	// Message lock settings
	LockDuration *int // Override default lock duration
}

// BatchMessage represents a message for batch sending
type BatchMessage struct {
	Body          string                 // Message body
	Properties    map[string]interface{} // Message properties
	SessionID     string                 // Session ID
	CorrelationID string                 // Correlation ID
	ReplyTo       string                 // Reply to queue
	TimeToLive    int                    // TTL in seconds
	MessageID     string                 // Message ID
	ScheduledTime *time.Time             // Scheduled delivery time
}
