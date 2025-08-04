package mq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// AzureServiceBusClient implements the Client interface for Azure Service Bus
type AzureServiceBusClient struct {
	config           *ClientConfig
	connectionString string
	namespace        string
	client           *azservicebus.Client
	senders          map[string]*azservicebus.Sender
	receivers        map[string]*azservicebus.Receiver
	sendMu           sync.RWMutex // Protects senders map and sending operations
	receiveMu        sync.RWMutex // Protects receivers map and receiving operations
}

// NewAzureServiceBusClient creates a new Azure Service Bus client
func NewAzureServiceBusClient(ctx context.Context, config *ClientConfig) (Client, error) {
	if config.ServiceType != "azure_servicebus" {
		return nil, fmt.Errorf("invalid service type for Azure Service Bus client: %s", config.ServiceType)
	}

	if config.ConnectionString == "" {
		return nil, fmt.Errorf("connection_string is required for Azure Service Bus")
	}

	// Create Service Bus client
	serviceBusClient, err := azservicebus.NewClientFromConnectionString(config.ConnectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Service Bus client: %w", err)
	}

	client := &AzureServiceBusClient{
		config:           config.Copy(),
		connectionString: config.ConnectionString,
		namespace:        extractNamespaceFromConnectionString(config.ConnectionString),
		client:           serviceBusClient,
		senders:          make(map[string]*azservicebus.Sender),
		receivers:        make(map[string]*azservicebus.Receiver),
	}

	return client, nil
}

// GetClientInfo returns information about the client
func (c *AzureServiceBusClient) GetClientInfo() map[string]interface{} {
	return map[string]interface{}{
		"service_type": "azure_servicebus",
		"namespace":    c.namespace,
		"timeout":      c.config.Timeout,
		"max_retries":  c.config.MaxRetries,
	}
}

// CreateQueue creates a new Service Bus queue
func (c *AzureServiceBusClient) CreateQueue(ctx context.Context, name string, options QueueOptions) (*Queue, error) {
	if err := validateQueueName(name); err != nil {
		return nil, NewMQError(ErrorTypeValidation, "azure_servicebus", "create_queue", "invalid queue name", err)
	}

	// Note: Queue creation requires admin permissions and separate admin client
	// For now, we'll return a queue structure assuming the queue exists or can be created
	// In practice, you would use the Azure Service Bus admin SDK to create queues

	// Return the queue information
	queue := NewQueue(name, "azure_servicebus")
	queue.URL = fmt.Sprintf("https://%s.servicebus.windows.net/%s", c.namespace, name)
	queue.LockDuration = coalesceInt(options.LockDuration, c.config.DefaultLockDuration)
	queue.RetentionPeriod = coalesceInt(options.RetentionPeriod, 1209600) // 14 days
	queue.MaxDeliveryCount = coalesceInt(options.MaxDeliveryCount, 10)
	queue.EnableSessions = options.EnableSessions
	queue.MaxQueueSize = options.MaxQueueSize

	// Azure Service Bus has built-in dead letter queue support
	if options.DeadLetterConfig != nil && options.DeadLetterConfig.Enabled {
		queue.DeadLetterConfig = &DeadLetterConfig{
			Enabled:          true,
			QueueName:        name + "/$deadletterqueue", // Built-in DLQ path
			MaxDeliveryCount: options.MaxDeliveryCount,
		}
	}

	if options.DuplicateDetection {
		queue.DuplicateDetection = &DuplicateDetection{
			Enabled:       true,
			WindowSeconds: coalesceInt(options.DuplicateWindowSecs, 300),
		}
	}

	return queue, nil
}

// DeleteQueue deletes a Service Bus queue
func (c *AzureServiceBusClient) DeleteQueue(ctx context.Context, name string) error {
	// Close any existing senders/receivers for this queue
	c.sendMu.Lock()
	if sender, exists := c.senders[name]; exists {
		sender.Close(ctx)
		delete(c.senders, name)
	}
	c.sendMu.Unlock()

	c.receiveMu.Lock()
	if receiver, exists := c.receivers[name]; exists {
		receiver.Close(ctx)
		delete(c.receivers, name)
	}
	c.receiveMu.Unlock()

	// TODO: Implement actual queue deletion when Azure SDK is available
	// For now, just clean up local resources
	// _, err := c.adminClient.DeleteQueue(ctx, name, nil)
	// if err != nil {
	//     var respErr *azcore.ResponseError
	//     if ok := errors.As(err, &respErr); ok && respErr.StatusCode == 404 {
	//         return nil
	//     }
	//     return NewMQError(ErrorTypeService, "azure_servicebus", "delete_queue", "failed to delete queue", err)
	// }

	return nil
}

// ListQueues lists Service Bus queues
func (c *AzureServiceBusClient) ListQueues(ctx context.Context, prefix string) ([]*Queue, error) {
	// TODO: Implement actual queue listing when Azure SDK is available
	// For now, return empty list
	return []*Queue{}, nil
}

// GetQueue gets information about a specific queue
func (c *AzureServiceBusClient) GetQueue(ctx context.Context, name string) (*Queue, error) {
	if name == "" {
		return nil, NewMQError(ErrorTypeNotFound, "azure_servicebus", "get_queue", "queue not found", nil)
	}

	// TODO: Implement actual queue retrieval when Azure SDK is available
	// For now, return a mock queue
	queue := NewQueue(name, "azure_servicebus")
	queue.URL = fmt.Sprintf("https://%s.servicebus.windows.net/%s", c.namespace, name)
	queue.LockDuration = c.config.DefaultLockDuration
	queue.RetentionPeriod = 1209600 // 14 days
	queue.MaxDeliveryCount = 10

	return queue, nil
}

// Exists checks if a queue exists
func (c *AzureServiceBusClient) Exists(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, nil
	}

	// TODO: Implement actual existence check when Azure SDK is available
	// For now, assume queue exists if name is not empty
	return name != "", nil
}

// Purge purges all messages from a queue
func (c *AzureServiceBusClient) Purge(ctx context.Context, name string) error {
	// TODO: Implement actual Azure Service Bus queue purging
	// Azure doesn't have a direct purge API, so this would involve:
	// 1. Receiving all messages in batches
	// 2. Completing them to remove from queue

	return nil
}

// GetInfo gets detailed queue information
func (c *AzureServiceBusClient) GetInfo(ctx context.Context, name string) (*Queue, error) {
	// For Azure Service Bus, this includes runtime properties
	return c.GetQueue(ctx, name)
}

// Send sends a message to a queue
func (c *AzureServiceBusClient) Send(ctx context.Context, queueName, body string, options MessageOptions) (*MessageResult, error) {
	if err := validateMessageBody(body); err != nil {
		return nil, NewMQError(ErrorTypeValidation, "azure_servicebus", "send", "invalid message body", err)
	}

	// TODO: Implement actual message sending when Azure SDK is available
	// For now, return a mock result
	messageID := options.MessageID
	if messageID == "" {
		messageID = generateMessageID()
	}

	// Build result
	result := NewMessageResult(messageID, body)
	result.Properties = normalizeProperties(options.Properties)
	result.SessionID = options.SessionID
	result.CorrelationID = options.CorrelationID
	result.ReplyTo = options.ReplyTo
	result.TimeToLive = options.TimeToLive

	if options.ScheduledTime != nil {
		result.ScheduledTime = options.ScheduledTime
	}

	return result, nil
}

// Receive receives messages from a queue
func (c *AzureServiceBusClient) Receive(ctx context.Context, queueName string, options ReceiveOptions) ([]*MessageResult, error) {
	// TODO: Implement actual message receiving when Azure SDK is available
	// For now, return empty list
	return []*MessageResult{}, nil
}

// Delete deletes messages from a queue (completes them in Service Bus terms)
func (c *AzureServiceBusClient) Delete(ctx context.Context, queueName string, messageIDs []string) ([]bool, error) {
	if len(messageIDs) == 0 {
		return []bool{}, nil
	}

	// TODO: Implement actual Azure Service Bus message completion
	// This would involve:
	// 1. Using message locks/settlement tokens
	// 2. Calling CompleteMessage for each message
	// 3. Handling batch operations (max 100 for Service Bus)

	// For now, return all successful
	results := make([]bool, len(messageIDs))
	for i := range results {
		results[i] = true
	}
	return results, nil
}

// Lock renews the lock on a message
func (c *AzureServiceBusClient) Lock(ctx context.Context, queueName, messageID string, duration int) error {
	// TODO: Implement actual Azure Service Bus lock renewal
	// This would involve calling RenewMessageLock API

	return nil
}

// Unlock abandons a message (releases the lock)
func (c *AzureServiceBusClient) Unlock(ctx context.Context, queueName, messageID string) error {
	// TODO: Implement actual Azure Service Bus message abandonment
	// This would involve calling AbandonMessage API

	return nil
}

// BatchSend sends multiple messages in batches
func (c *AzureServiceBusClient) BatchSend(ctx context.Context, queueName string, messages []BatchMessage) ([]*MessageResult, error) {
	if len(messages) == 0 {
		return []*MessageResult{}, nil
	}

	// Split into Service Bus batch size limits (max 100)
	batchSize := adaptBatchSize("azure_servicebus", 100)
	batches := splitBatchMessages(messages, batchSize)

	var allResults []*MessageResult

	for _, batch := range batches {
		batchResults, err := c.sendBatch(ctx, queueName, batch)
		if err != nil {
			return allResults, err
		}
		allResults = append(allResults, batchResults...)
	}

	return allResults, nil
}

// sendBatch sends a single batch of messages
func (c *AzureServiceBusClient) sendBatch(ctx context.Context, queueName string, messages []BatchMessage) ([]*MessageResult, error) {
	// TODO: Implement actual Azure Service Bus batch sending
	// This would involve calling SendMessageBatch API

	// For now, return mock results
	results := make([]*MessageResult, len(messages))
	for i, msg := range messages {
		results[i] = NewMessageResult(generateMessageID(), msg.Body)
		results[i].Properties = normalizeProperties(msg.Properties)
		results[i].SessionID = msg.SessionID
		results[i].CorrelationID = msg.CorrelationID
		results[i].ReplyTo = msg.ReplyTo
	}

	return results, nil
}

// Schedule schedules a message for future delivery
func (c *AzureServiceBusClient) Schedule(ctx context.Context, queueName, body string, scheduledTime time.Time, options MessageOptions) (*MessageResult, error) {
	// Azure Service Bus supports scheduling with ScheduledEnqueueTime
	msgOptions := options
	msgOptions.ScheduledTime = &scheduledTime

	return c.Send(ctx, queueName, body, msgOptions)
}

// Cancel cancels a scheduled message
func (c *AzureServiceBusClient) Cancel(ctx context.Context, queueName, messageID string) error {
	// TODO: Implement actual Azure Service Bus message cancellation
	// This would involve calling CancelScheduledMessage API

	return nil
}

// Peek peeks at messages without receiving them
func (c *AzureServiceBusClient) Peek(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error) {
	// TODO: Implement actual Azure Service Bus message peeking
	// This would involve:
	// 1. Creating Service Bus receiver
	// 2. Using PeekMessages API
	// 3. Converting to unified MessageResult

	// For now, return empty list
	return []*MessageResult{}, nil
}

// DeadLetterReceive receives messages from the dead letter queue
func (c *AzureServiceBusClient) DeadLetterReceive(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error) {
	// For Azure Service Bus, dead letter queue is a subqueue
	dlqPath := queueName + "/$deadletterqueue"
	options := ReceiveOptions{
		MaxCount: maxCount,
	}
	return c.Receive(ctx, dlqPath, options)
}

// DeadLetterRequeue moves a message back from DLQ to main queue
func (c *AzureServiceBusClient) DeadLetterRequeue(ctx context.Context, queueName, messageID string) error {
	// TODO: Implement actual requeue logic
	// This would involve:
	// 1. Receiving the message from DLQ
	// 2. Sending it to the main queue
	// 3. Completing it from DLQ

	return nil
}

// DeadLetterPurge purges all messages from the dead letter queue
func (c *AzureServiceBusClient) DeadLetterPurge(ctx context.Context, queueName string) error {
	dlqPath := queueName + "/$deadletterqueue"
	return c.Purge(ctx, dlqPath)
}

// Close closes the client connection
func (c *AzureServiceBusClient) Close() error {
	var firstErr error

	// Close all senders
	c.sendMu.Lock()
	for queueName, sender := range c.senders {
		if err := sender.Close(context.Background()); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close sender for queue %s: %w", queueName, err)
		}
	}
	c.senders = make(map[string]*azservicebus.Sender)
	c.sendMu.Unlock()

	// Close all receivers
	c.receiveMu.Lock()
	for queueName, receiver := range c.receivers {
		if err := receiver.Close(context.Background()); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close receiver for queue %s: %w", queueName, err)
		}
	}
	c.receivers = make(map[string]*azservicebus.Receiver)
	c.receiveMu.Unlock()

	// Close the main client
	if err := c.client.Close(context.Background()); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to close Azure Service Bus client: %w", err)
	}

	return firstErr
}

// Helper functions for Azure Service Bus specific operations

// getSender gets or creates a sender for the specified queue
func (c *AzureServiceBusClient) getSender(ctx context.Context, queueName string) (*azservicebus.Sender, error) {
	c.sendMu.RLock()
	sender, exists := c.senders[queueName]
	c.sendMu.RUnlock()

	if exists {
		return sender, nil
	}

	// Create new sender
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	// Double-check after acquiring write lock
	if sender, exists := c.senders[queueName]; exists {
		return sender, nil
	}

	newSender, err := c.client.NewSender(queueName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create sender for queue %s: %w", queueName, err)
	}

	c.senders[queueName] = newSender
	return newSender, nil
}

// getReceiver gets or creates a receiver for the specified queue
func (c *AzureServiceBusClient) getReceiver(ctx context.Context, queueName string) (*azservicebus.Receiver, error) {
	c.receiveMu.RLock()
	receiver, exists := c.receivers[queueName]
	c.receiveMu.RUnlock()

	if exists {
		return receiver, nil
	}

	// Create new receiver
	c.receiveMu.Lock()
	defer c.receiveMu.Unlock()

	// Double-check after acquiring write lock
	if receiver, exists := c.receivers[queueName]; exists {
		return receiver, nil
	}

	// Use PeekLock mode by default for better reliability
	receiverOpts := &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	}

	newReceiver, err := c.client.NewReceiverForQueue(queueName, receiverOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create receiver for queue %s: %w", queueName, err)
	}

	c.receivers[queueName] = newReceiver
	return newReceiver, nil
}

// buildQueueProperties builds Azure Service Bus queue properties from unified options
func (c *AzureServiceBusClient) buildQueueProperties(options QueueOptions) map[string]interface{} {
	// TODO: Return actual Azure SDK CreateQueueOptions when SDK is available
	// For now, return a map representation of the properties
	props := make(map[string]interface{})

	if options.LockDuration > 0 {
		props["LockDuration"] = fmt.Sprintf("PT%dS", options.LockDuration)
	}

	if options.RetentionPeriod > 0 {
		props["DefaultMessageTimeToLive"] = fmt.Sprintf("PT%dS", options.RetentionPeriod)
	}

	if options.MaxDeliveryCount > 0 {
		props["MaxDeliveryCount"] = options.MaxDeliveryCount
	}

	if options.EnableSessions {
		props["RequiresSession"] = options.EnableSessions
	}

	if options.DuplicateDetection {
		props["RequiresDuplicateDetection"] = options.DuplicateDetection
		if options.DuplicateWindowSecs > 0 {
			props["DuplicateDetectionHistoryTimeWindow"] = fmt.Sprintf("PT%dS", options.DuplicateWindowSecs)
		}
	}

	if options.MaxQueueSize > 0 {
		props["MaxSizeInMegabytes"] = options.MaxQueueSize / (1024 * 1024)
	}

	if options.DeadLetterConfig != nil && options.DeadLetterConfig.Enabled {
		props["EnableDeadLetteringOnMessageExpiration"] = true
	}

	return props
}

// buildServiceBusMessage builds an Azure Service Bus message from unified options
func (c *AzureServiceBusClient) buildServiceBusMessage(body string, options MessageOptions) map[string]interface{} {
	// TODO: Return actual Azure SDK Message when SDK is available
	// For now, return a map representation of the message
	messageID := options.MessageID
	if messageID == "" {
		messageID = generateMessageID()
	}

	msg := map[string]interface{}{
		"Body":      body,
		"MessageID": messageID,
	}

	if options.Properties != nil {
		msg["ApplicationProperties"] = options.Properties
	}

	if options.SessionID != "" {
		msg["SessionID"] = options.SessionID
	}

	if options.CorrelationID != "" {
		msg["CorrelationID"] = options.CorrelationID
	}

	if options.ReplyTo != "" {
		msg["ReplyTo"] = options.ReplyTo
	}

	if options.TimeToLive > 0 {
		msg["TimeToLive"] = time.Duration(options.TimeToLive) * time.Second
	}

	if options.ScheduledTime != nil {
		msg["ScheduledEnqueueTime"] = options.ScheduledTime
	}

	return msg
}

// TODO: Implement convertQueuePropertiesToQueue and convertServiceBusMessageToResult when Azure SDK is available

// extractNamespaceFromConnectionString extracts the namespace from a connection string
func extractNamespaceFromConnectionString(connectionString string) string {
	// Parse connection string to extract namespace
	// Format: Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...
	parts := strings.Split(connectionString, ";")
	for _, part := range parts {
		if strings.HasPrefix(part, "Endpoint=sb://") {
			endpoint := strings.TrimPrefix(part, "Endpoint=sb://")
			if idx := strings.Index(endpoint, "."); idx > 0 {
				return endpoint[:idx]
			}
		}
	}
	return "unknown"
}

// convertToServiceBusProperties converts unified options to Service Bus queue properties
func convertToServiceBusProperties(options QueueOptions) map[string]interface{} {
	props := make(map[string]interface{})

	if options.LockDuration > 0 {
		props["LockDuration"] = fmt.Sprintf("PT%dS", options.LockDuration) // ISO 8601 duration
	}

	if options.RetentionPeriod > 0 {
		props["DefaultMessageTimeToLive"] = fmt.Sprintf("PT%dS", options.RetentionPeriod)
	}

	if options.MaxDeliveryCount > 0 {
		props["MaxDeliveryCount"] = options.MaxDeliveryCount
	}

	if options.EnableSessions {
		props["RequiresSession"] = true
	}

	if options.DuplicateDetection {
		props["RequiresDuplicateDetection"] = true
		if options.DuplicateWindowSecs > 0 {
			props["DuplicateDetectionHistoryTimeWindow"] = fmt.Sprintf("PT%dS", options.DuplicateWindowSecs)
		}
	}

	if options.MaxQueueSize > 0 {
		props["MaxSizeInMegabytes"] = options.MaxQueueSize / (1024 * 1024) // Convert bytes to MB
	}

	// Dead letter queue is built-in for Service Bus
	if options.DeadLetterConfig != nil && options.DeadLetterConfig.Enabled {
		props["EnableDeadLetteringOnMessageExpiration"] = true
	}

	return props
}

// convertToServiceBusMessage converts unified message options to Service Bus message properties
func convertToServiceBusMessage(body string, options MessageOptions) map[string]interface{} {
	msg := map[string]interface{}{
		"Body": body,
	}

	if options.Properties != nil {
		msg["ApplicationProperties"] = options.Properties
	}

	if options.SessionID != "" {
		msg["SessionId"] = options.SessionID
	}

	if options.CorrelationID != "" {
		msg["CorrelationId"] = options.CorrelationID
	}

	if options.ReplyTo != "" {
		msg["ReplyTo"] = options.ReplyTo
	}

	if options.MessageID != "" {
		msg["MessageId"] = options.MessageID
	}

	if options.TimeToLive > 0 {
		msg["TimeToLive"] = fmt.Sprintf("PT%dS", options.TimeToLive)
	}

	if options.ScheduledTime != nil {
		msg["ScheduledEnqueueTime"] = options.ScheduledTime.Format(time.RFC3339)
	}

	return msg
}

// convertFromServiceBusMessage converts Service Bus message to unified MessageResult
func convertFromServiceBusMessage(sbMessage map[string]interface{}) *MessageResult {
	result := &MessageResult{
		Success: true,
	}

	if body, ok := sbMessage["Body"].(string); ok {
		result.Body = body
	}

	if msgID, ok := sbMessage["MessageId"].(string); ok {
		result.MessageID = msgID
	}

	if props, ok := sbMessage["ApplicationProperties"].(map[string]interface{}); ok {
		result.Properties = props
	}

	if sessionID, ok := sbMessage["SessionId"].(string); ok {
		result.SessionID = sessionID
	}

	if correlationID, ok := sbMessage["CorrelationId"].(string); ok {
		result.CorrelationID = correlationID
	}

	if replyTo, ok := sbMessage["ReplyTo"].(string); ok {
		result.ReplyTo = replyTo
	}

	if deliveryCount, ok := sbMessage["DeliveryCount"].(int); ok {
		result.DeliveryCount = deliveryCount
	}

	if enqueueTime, ok := sbMessage["EnqueuedTime"].(time.Time); ok {
		result.EnqueueTime = enqueueTime
	}

	if scheduledTime, ok := sbMessage["ScheduledEnqueueTime"].(time.Time); ok && !scheduledTime.IsZero() {
		result.ScheduledTime = &scheduledTime
	}

	if lockToken, ok := sbMessage["LockToken"].(string); ok {
		result.ReceiptHandle = lockToken
	}

	if expiresAt, ok := sbMessage["LockedUntil"].(time.Time); ok && !expiresAt.IsZero() {
		result.LockExpiresAt = &expiresAt
	}

	return result
}
