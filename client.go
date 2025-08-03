package mq

import (
	"context"
	"fmt"
)

// Client defines the unified interface for message queue operations
type Client interface {
	// Queue management
	CreateQueue(ctx context.Context, name string, options map[string]interface{}) (*Queue, error)
	DeleteQueue(ctx context.Context, name string) error
	ListQueues(ctx context.Context, prefix string) ([]*Queue, error)
	GetQueue(ctx context.Context, name string) (*Queue, error)
	QueueExists(ctx context.Context, name string) (bool, error)
	PurgeQueue(ctx context.Context, name string) error
	GetQueueInfo(ctx context.Context, name string) (*Queue, error)

	// Basic message operations
	SendMessage(ctx context.Context, queueName, body string, options map[string]interface{}) (*MessageResult, error)
	ReceiveMessages(ctx context.Context, queueName string, maxCount int, options map[string]interface{}) ([]*MessageResult, error)
	DeleteMessage(ctx context.Context, queueName, messageID string) error
	DeleteMessages(ctx context.Context, queueName string, messageIDs []string) ([]bool, error)

	// Message lock management (unified visibility/lock concept)
	ExtendMessageLock(ctx context.Context, queueName, messageID string, lockDuration int) error
	ReleaseMessageLock(ctx context.Context, queueName, messageID string) error

	// Batch operations (auto-adapts to service limits)
	SendMessagesBatch(ctx context.Context, queueName string, messages []map[string]interface{}) ([]*MessageResult, error)

	// Message inspection (available when supported)
	PeekMessages(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error)

	// Message scheduling (unified approach)
	SendScheduledMessage(ctx context.Context, queueName, body string, scheduledTime string, options map[string]interface{}) (*MessageResult, error)
	CancelScheduledMessage(ctx context.Context, queueName, messageID string) error

	// Dead letter queue operations (unified interface)
	GetDeadLetterMessages(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error)
	ReprocessDeadLetterMessage(ctx context.Context, queueName, messageID string) error
	PurgeDeadLetterQueue(ctx context.Context, queueName string) error

	// Topic management (Azure Service Bus only, graceful failure on AWS SQS)
	CreateTopic(ctx context.Context, name string, options map[string]interface{}) (*Topic, error)
	DeleteTopic(ctx context.Context, name string) error
	ListTopics(ctx context.Context, prefix string) ([]*Topic, error)
	TopicExists(ctx context.Context, name string) (bool, error)

	// Subscription management
	CreateSubscription(ctx context.Context, topicName, subscriptionName string, options map[string]interface{}) (*Subscription, error)
	DeleteSubscription(ctx context.Context, topicName, subscriptionName string) error
	ListSubscriptions(ctx context.Context, topicName string) ([]*Subscription, error)

	// Publish/Subscribe operations
	PublishMessage(ctx context.Context, topicName, body string, options map[string]interface{}) (*MessageResult, error)
	SubscribeMessages(ctx context.Context, topicName, subscriptionName string, maxCount int, options map[string]interface{}) ([]*MessageResult, error)

	// Message filtering (Azure Service Bus only)
	AddSubscriptionFilter(ctx context.Context, topicName, subscriptionName, ruleName, filterExpression string) error
	RemoveSubscriptionFilter(ctx context.Context, topicName, subscriptionName, ruleName string) error
	ListSubscriptionFilters(ctx context.Context, topicName, subscriptionName string) ([]SubscriptionFilter, error)

	// Connection management
	Close() error
	HealthCheck(ctx context.Context) error
	GetClientInfo() *ClientInfo
	CheckFeatureSupport(feature string) bool
}

// ServiceType constants
const (
	ServiceTypeAWSSQS          = "aws_sqs"
	ServiceTypeAzureServiceBus = "azure_servicebus"
	ServiceTypeAuto            = "auto"
)

// Feature constants for feature support checking
const (
	FeatureTopics             = "topics"
	FeatureSubscriptions      = "subscriptions"
	FeaturePeekMessages       = "peek_messages"
	FeatureCancelScheduled    = "cancel_scheduled_message"
	FeatureCorrelationID      = "correlation_id"
	FeatureReplyTo            = "reply_to"
	FeatureMessageSessions    = "message_sessions"
	FeatureFIFOQueues         = "fifo_queues"
	FeatureBatchOperations    = "batch_operations"
	FeatureDuplicateDetection = "duplicate_detection"
	FeatureDeadLetterQueue    = "dead_letter_queue"
	FeatureScheduledMessages  = "scheduled_messages"
)

// ConnectionConfig represents unified connection configuration
type ConnectionConfig struct {
	// Service selection
	ServiceType string `json:"service_type"`

	// Common configuration
	Timeout    int `json:"timeout"`     // Connection timeout in seconds
	MaxRetries int `json:"max_retries"` // Maximum retry attempts

	// AWS SQS specific
	AWSRegion       string `json:"aws_region,omitempty"`
	AWSAccessKey    string `json:"aws_access_key,omitempty"`
	AWSSecretKey    string `json:"aws_secret_key,omitempty"`
	AWSSessionToken string `json:"aws_session_token,omitempty"`

	// Azure Service Bus specific
	ConnectionString string `json:"connection_string,omitempty"`
	AzureNamespace   string `json:"azure_namespace,omitempty"`
	AzureSharedKey   string `json:"azure_shared_key,omitempty"`
	AzureKeyName     string `json:"azure_key_name,omitempty"`
}

// ClientFactory creates clients based on configuration
type ClientFactory struct{}

// NewClientFactory creates a new ClientFactory
func NewClientFactory() *ClientFactory {
	return &ClientFactory{}
}

// CreateClient creates a new client based on the configuration
func (f *ClientFactory) CreateClient(config *ConnectionConfig) (Client, error) {
	switch config.ServiceType {
	case ServiceTypeAWSSQS:
		return NewAWSSQSClient(config)
	case ServiceTypeAzureServiceBus:
		return NewAzureServiceBusClient(config)
	case ServiceTypeAuto:
		// Auto-detect based on available configuration
		if config.ConnectionString != "" || config.AzureNamespace != "" {
			return NewAzureServiceBusClient(config)
		} else if config.AWSRegion != "" || config.AWSAccessKey != "" {
			return NewAWSSQSClient(config)
		} else {
			return nil, NewMQError("invalid_configuration", "Cannot auto-detect service type: no valid configuration found")
		}
	default:
		return nil, NewMQError("unsupported_service", fmt.Sprintf("Unsupported service type: %s", config.ServiceType))
	}
}

// GetSupportedServices returns the list of supported service types
func (f *ClientFactory) GetSupportedServices() []string {
	return []string{ServiceTypeAWSSQS, ServiceTypeAzureServiceBus}
}
