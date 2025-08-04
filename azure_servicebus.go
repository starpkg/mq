package mq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

// AzureServiceBusClient implements the Client interface for Azure Service Bus
type AzureServiceBusClient struct {
	config           *ClientConfig
	connectionString string
	namespace        string
	client           *azservicebus.Client
	adminClient      *admin.Client
	senders          map[string]*azservicebus.Sender
	receivers        map[string]*azservicebus.Receiver
	sendMu           sync.RWMutex // Protects senders map and sending operations
	receiveMu        sync.RWMutex // Protects receivers map and receiving operations
}

// NewAzureServiceBusClient creates a new Azure Service Bus client
func NewAzureServiceBusClient(ctx context.Context, config *ClientConfig) (Client, error) {
	if config.ServiceType != ServiceTypeAzureServiceBus {
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

	// Create admin client for queue management operations
	adminClient, err := admin.NewClientFromConnectionString(config.ConnectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Service Bus admin client: %w", err)
	}

	client := &AzureServiceBusClient{
		config:           config.Copy(),
		connectionString: config.ConnectionString,
		namespace:        extractNamespaceFromConnectionString(config.ConnectionString),
		client:           serviceBusClient,
		adminClient:      adminClient,
		senders:          make(map[string]*azservicebus.Sender),
		receivers:        make(map[string]*azservicebus.Receiver),
	}

	return client, nil
}

// GetClientInfo returns information about the client
func (c *AzureServiceBusClient) GetClientInfo() map[string]interface{} {
	return map[string]interface{}{
		"service_type": ServiceTypeAzureServiceBus,
		"namespace":    c.namespace,
		"timeout":      c.config.Timeout,
		"max_retries":  c.config.MaxRetries,
	}
}

// CreateQueue creates a new Service Bus queue using admin client
func (c *AzureServiceBusClient) CreateQueue(ctx context.Context, name string, options QueueOptions) (*Queue, error) {
	if err := validateQueueName(name); err != nil {
		return nil, NewMQError(ErrorTypeValidation, ServiceTypeAzureServiceBus, "create_queue", "invalid queue name", err)
	}

	// Check if queue already exists
	queueResponse, err := c.adminClient.GetQueue(ctx, name, nil)
	if err == nil && queueResponse != nil {
		// Queue exists, return the existing queue information
		return c.buildQueueFromProperties(name, queueResponse.QueueProperties), nil
	}

	// Create queue options for admin client
	queueOptions := &admin.CreateQueueOptions{
		Properties: &admin.QueueProperties{
			MaxDeliveryCount:           toInt32Ptr(int32(coalesceInt(options.MaxDeliveryCount, 10))),
			LockDuration:               toStringPtr(formatDuration(options.LockDuration, c.config.DefaultLockDuration)),
			DefaultMessageTimeToLive:   toStringPtr(formatDuration(options.RetentionPeriod, 1209600)), // 14 days default
			RequiresSession:            &options.EnableSessions,
			RequiresDuplicateDetection: &options.DuplicateDetection,
			EnablePartitioning:         getBoolPtr(false),
		},
	}

	// Set duplicate detection window if enabled
	if options.DuplicateDetection {
		queueOptions.Properties.DuplicateDetectionHistoryTimeWindow = toStringPtr(formatDuration(options.DuplicateWindowSecs, 300))
	}

	// Set max queue size if specified
	if options.MaxQueueSize > 0 {
		queueOptions.Properties.MaxSizeInMegabytes = toInt32Ptr(int32(options.MaxQueueSize / (1024 * 1024)))
	}

	// Create the queue
	createdResponse, err := c.adminClient.CreateQueue(ctx, name, queueOptions)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "create_queue", "failed to create queue", err)
	}

	return c.buildQueueFromProperties(name, createdResponse.QueueProperties), nil
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

	// Delete the queue using admin client
	_, err := c.adminClient.DeleteQueue(ctx, name, nil)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "delete_queue", "failed to delete queue", err)
	}

	return nil
}

// ListQueues lists Service Bus queues using admin client
func (c *AzureServiceBusClient) ListQueues(ctx context.Context, prefix string) ([]*Queue, error) {
	var queues []*Queue

	// Get queue properties from admin client
	pager := c.adminClient.NewListQueuesPager(nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "list_queues", "failed to list queues", err)
		}

		for _, queueItem := range page.Queues {
			// Filter by prefix if specified
			if prefix != "" && !strings.HasPrefix(queueItem.QueueName, prefix) {
				continue
			}

			queue := c.buildQueueFromProperties(queueItem.QueueName, queueItem.QueueProperties)
			queues = append(queues, queue)
		}
	}

	return queues, nil
}

// GetQueue gets information about a specific queue
func (c *AzureServiceBusClient) GetQueue(ctx context.Context, name string) (*Queue, error) {
	if name == "" {
		return nil, NewMQError(ErrorTypeNotFound, ServiceTypeAzureServiceBus, "get_queue", "queue not found", nil)
	}

	// Get queue properties from admin client
	queueResponse, err := c.adminClient.GetQueue(ctx, name, nil)
	if err != nil {
		return nil, NewMQError(ErrorTypeNotFound, ServiceTypeAzureServiceBus, "get_queue", "queue not found", err)
	}

	return c.buildQueueFromProperties(name, queueResponse.QueueProperties), nil
}

// Exists checks if a queue exists
func (c *AzureServiceBusClient) Exists(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, nil
	}

	// Check queue existence using admin client
	_, err := c.adminClient.GetQueue(ctx, name, nil)
	if err != nil {
		return false, nil // Queue doesn't exist or access denied
	}
	return true, nil
}

// Purge purges all messages from a queue
func (c *AzureServiceBusClient) Purge(ctx context.Context, name string) error {
	// Azure Service Bus doesn't have a direct purge API, so we need to:
	// 1. Receive all messages in batches
	// 2. Complete them to remove from queue

	// Get receiver for this queue
	receiver, err := c.getReceiver(ctx, name)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "purge", "failed to get receiver", err)
	}

	// Receive and complete messages in batches until queue is empty
	for {
		messages, err := receiver.ReceiveMessages(ctx, 32, &azservicebus.ReceiveMessagesOptions{
			TimeAfterFirstMessage: 1 * time.Second, // Short timeout to avoid hanging
		})
		if err != nil {
			return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "purge", "failed to receive messages", err)
		}

		// If no messages received, queue is empty
		if len(messages) == 0 {
			break
		}

		// Complete all received messages
		for _, msg := range messages {
			err = receiver.CompleteMessage(ctx, msg, nil)
			if err != nil {
				// Log the error but continue purging other messages
				// In a production environment, you might want to handle this differently
				continue
			}
		}
	}

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
		return nil, NewMQError(ErrorTypeValidation, ServiceTypeAzureServiceBus, "send", "invalid message body", err)
	}

	// Get or create a sender for this queue
	sender, err := c.getSender(ctx, queueName)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "send", "failed to get sender", err)
	}

	// Generate message ID if not provided
	messageID := options.MessageID
	if messageID == "" {
		messageID = generateMessageID()
	}

	// Build the Azure Service Bus message
	message := &azservicebus.Message{
		Body:      []byte(body),
		MessageID: &messageID,
	}

	// Set optional properties
	if options.Properties != nil {
		message.ApplicationProperties = normalizeProperties(options.Properties)
	}

	if options.SessionID != "" {
		message.SessionID = &options.SessionID
	}

	if options.CorrelationID != "" {
		message.CorrelationID = &options.CorrelationID
	}

	if options.ReplyTo != "" {
		message.ReplyTo = &options.ReplyTo
	}

	if options.TimeToLive > 0 {
		ttl := time.Duration(options.TimeToLive) * time.Second
		message.TimeToLive = &ttl
	}

	if options.ScheduledTime != nil {
		message.ScheduledEnqueueTime = options.ScheduledTime
	}

	// Send the message
	err = sender.SendMessage(ctx, message, nil)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "send", "failed to send message", err)
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
	// Get or create a receiver for this queue
	receiver, err := c.getReceiver(ctx, queueName)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "receive", "failed to get receiver", err)
	}

	// Determine the number of messages to receive
	maxCount := options.MaxCount
	if maxCount <= 0 {
		maxCount = 1
	}
	if maxCount > 32 { // Azure Service Bus limit
		maxCount = 32
	}

	// Set receive options
	receiveOpts := &azservicebus.ReceiveMessagesOptions{}
	if options.WaitTime > 0 {
		receiveOpts.TimeAfterFirstMessage = time.Duration(options.WaitTime) * time.Second
	}

	// Receive messages
	messages, err := receiver.ReceiveMessages(ctx, int(maxCount), receiveOpts)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "receive", "failed to receive messages", err)
	}

	// Convert to unified message results
	results := make([]*MessageResult, len(messages))
	for i, msg := range messages {
		result, err := c.convertFromAzureMessage(msg)
		if err != nil {
			return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "receive", "failed to convert message", err)
		}
		results[i] = result
	}

	return results, nil
}

// Delete deletes messages from a queue (completes them in Service Bus terms)
func (c *AzureServiceBusClient) Delete(ctx context.Context, queueName string, messageIDs []string) ([]bool, error) {
	if len(messageIDs) == 0 {
		return []bool{}, nil
	}

	results := make([]bool, len(messageIDs))

	// Note: For Azure Service Bus, we need the original ReceivedMessage objects to complete them
	// This implementation assumes messages are stored with their receipt handles when received
	// In practice, this would require a message cache or different API design

	// Note: For Azure Service Bus, we need the original ReceivedMessage objects to complete them
	// Since this API only provides messageIDs, we'll indicate this is not fully supported
	// In practice, message completion should be done immediately after processing using the
	// CompleteMessage method with the original ReceivedMessage
	for i := range messageIDs {
		results[i] = false // Cannot complete without original ReceivedMessage
	}

	return results, nil
}

// CompleteMessage completes a message using the original Azure ReceivedMessage
func (c *AzureServiceBusClient) CompleteMessage(ctx context.Context, queueName string, msgResult *MessageResult) error {
	// Get receiver for this queue
	receiver, err := c.getReceiver(ctx, queueName)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "complete_message", "failed to get receiver", err)
	}

	// Extract the original Azure message
	originalMsg, ok := msgResult.OriginalMessage.(*azservicebus.ReceivedMessage)
	if !ok {
		return NewMQError(ErrorTypeValidation, ServiceTypeAzureServiceBus, "complete_message", "invalid original message type", nil)
	}

	// Complete the message
	err = receiver.CompleteMessage(ctx, originalMsg, nil)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "complete_message", "failed to complete message", err)
	}

	return nil
}

// AbandonMessage abandons a message using the original Azure ReceivedMessage
func (c *AzureServiceBusClient) AbandonMessage(ctx context.Context, queueName string, msgResult *MessageResult) error {
	// Get receiver for this queue
	receiver, err := c.getReceiver(ctx, queueName)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "abandon_message", "failed to get receiver", err)
	}

	// Extract the original Azure message
	originalMsg, ok := msgResult.OriginalMessage.(*azservicebus.ReceivedMessage)
	if !ok {
		return NewMQError(ErrorTypeValidation, ServiceTypeAzureServiceBus, "abandon_message", "invalid original message type", nil)
	}

	// Abandon the message
	err = receiver.AbandonMessage(ctx, originalMsg, nil)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "abandon_message", "failed to abandon message", err)
	}

	return nil
}

// Lock renews the lock on a message
func (c *AzureServiceBusClient) Lock(ctx context.Context, queueName, messageID string, duration int) error {
	// Get receiver for this queue
	receiver, err := c.getReceiver(ctx, queueName)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "lock", "failed to get receiver", err)
	}

	// Note: Azure Service Bus lock renewal requires the original ReceivedMessage object
	// This method cannot be implemented with just messageID
	// Lock renewal should be done using RenewMessageLock with the original ReceivedMessage
	_ = receiver
	return NewMQError(ErrorTypeUnsupported, ServiceTypeAzureServiceBus, "lock", "lock renewal requires original message object", nil)
}

// Unlock abandons a message (releases the lock)
func (c *AzureServiceBusClient) Unlock(ctx context.Context, queueName, messageID string) error {
	// Get receiver for this queue
	receiver, err := c.getReceiver(ctx, queueName)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "unlock", "failed to get receiver", err)
	}

	// Note: Azure Service Bus message abandonment requires the original ReceivedMessage object
	// This method cannot be implemented with just messageID
	// Message abandonment should be done using AbandonMessage with the original ReceivedMessage
	_ = receiver
	return NewMQError(ErrorTypeUnsupported, ServiceTypeAzureServiceBus, "unlock", "message abandonment requires original message object", nil)
}

// RenewMessageLock renews the lock on a message using the original Azure ReceivedMessage
func (c *AzureServiceBusClient) RenewMessageLock(ctx context.Context, queueName string, msgResult *MessageResult) error {
	// Get receiver for this queue
	receiver, err := c.getReceiver(ctx, queueName)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "renew_lock", "failed to get receiver", err)
	}

	// Extract the original Azure message
	originalMsg, ok := msgResult.OriginalMessage.(*azservicebus.ReceivedMessage)
	if !ok {
		return NewMQError(ErrorTypeValidation, ServiceTypeAzureServiceBus, "renew_lock", "invalid original message type", nil)
	}

	// Renew the message lock
	err = receiver.RenewMessageLock(ctx, originalMsg, nil)
	if err != nil {
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "renew_lock", "failed to renew message lock", err)
	}

	return nil
}

// BatchSend sends multiple messages in batches
func (c *AzureServiceBusClient) BatchSend(ctx context.Context, queueName string, messages []BatchMessage) ([]*MessageResult, error) {
	if len(messages) == 0 {
		return []*MessageResult{}, nil
	}

	// Get or create a sender for this queue
	sender, err := c.getSender(ctx, queueName)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "batch_send", "failed to get sender", err)
	}

	// Azure Service Bus supports batch sending with message batches
	// We'll create a message batch and add messages to it
	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "batch_send", "failed to create message batch", err)
	}

	var allResults []*MessageResult
	var currentBatchResults []*MessageResult

	for _, batchMsg := range messages {
		// Generate message ID if not provided
		messageID := batchMsg.MessageID
		if messageID == "" {
			messageID = generateMessageID()
		}

		// Build the Azure Service Bus message
		message := &azservicebus.Message{
			Body:      []byte(batchMsg.Body),
			MessageID: &messageID,
		}

		// Set optional properties
		if batchMsg.Properties != nil {
			message.ApplicationProperties = normalizeProperties(batchMsg.Properties)
		}

		if batchMsg.SessionID != "" {
			message.SessionID = &batchMsg.SessionID
		}

		if batchMsg.CorrelationID != "" {
			message.CorrelationID = &batchMsg.CorrelationID
		}

		if batchMsg.ReplyTo != "" {
			message.ReplyTo = &batchMsg.ReplyTo
		}

		// Try to add the message to the current batch
		err = batch.AddMessage(message, nil)
		if err != nil {
			// If the batch is full, send it and create a new one
			if len(currentBatchResults) > 0 {
				sendErr := sender.SendMessageBatch(ctx, batch, nil)
				if sendErr != nil {
					return allResults, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "batch_send", "failed to send batch", sendErr)
				}
				allResults = append(allResults, currentBatchResults...)
				currentBatchResults = nil
			}

			// Create a new batch and try adding the message again
			batch, err = sender.NewMessageBatch(ctx, nil)
			if err != nil {
				return allResults, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "batch_send", "failed to create new message batch", err)
			}

			err = batch.AddMessage(message, nil)
			if err != nil {
				return allResults, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "batch_send", "message too large for batch", err)
			}
		}

		// Create result for this message
		result := NewMessageResult(messageID, batchMsg.Body)
		result.Properties = normalizeProperties(batchMsg.Properties)
		result.SessionID = batchMsg.SessionID
		result.CorrelationID = batchMsg.CorrelationID
		result.ReplyTo = batchMsg.ReplyTo
		currentBatchResults = append(currentBatchResults, result)
	}

	// Send the final batch if it has messages
	if len(currentBatchResults) > 0 {
		err = sender.SendMessageBatch(ctx, batch, nil)
		if err != nil {
			return allResults, NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, "batch_send", "failed to send final batch", err)
		}
		allResults = append(allResults, currentBatchResults...)
	}

	return allResults, nil
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

	// Note: Admin client doesn't have a Close method

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

// convertFromAzureMessage converts Azure Service Bus message to unified MessageResult
func (c *AzureServiceBusClient) convertFromAzureMessage(msg *azservicebus.ReceivedMessage) (*MessageResult, error) {
	result := &MessageResult{
		Success:   true,
		Body:      string(msg.Body),
		MessageID: msg.MessageID,
	}

	// Copy properties
	if msg.ApplicationProperties != nil {
		result.Properties = msg.ApplicationProperties
	}

	// Copy optional fields
	if msg.SessionID != nil {
		result.SessionID = *msg.SessionID
	}

	if msg.CorrelationID != nil {
		result.CorrelationID = *msg.CorrelationID
	}

	if msg.ReplyTo != nil {
		result.ReplyTo = *msg.ReplyTo
	}

	// Set delivery count
	result.DeliveryCount = int(msg.DeliveryCount)

	// Set enqueue time
	if msg.EnqueuedTime != nil {
		result.EnqueueTime = *msg.EnqueuedTime
	}

	// Set scheduled time
	if msg.ScheduledEnqueueTime != nil && !msg.ScheduledEnqueueTime.IsZero() {
		result.ScheduledTime = msg.ScheduledEnqueueTime
	}

	// Set lock token as receipt handle
	if len(msg.LockToken) > 0 {
		result.ReceiptHandle = string(msg.LockToken[:])
	}

	// Set lock expiry time
	if msg.LockedUntil != nil && !msg.LockedUntil.IsZero() {
		result.LockExpiresAt = msg.LockedUntil
	}

	// Store the original message for completion/abandonment
	result.OriginalMessage = msg

	return result, nil
}

// convertFromServiceBusMessage converts Service Bus message to unified MessageResult (legacy)
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

// buildQueueFromProperties creates a unified Queue object from Azure Service Bus queue properties
func (c *AzureServiceBusClient) buildQueueFromProperties(name string, props admin.QueueProperties) *Queue {
	queue := NewQueue(name, ServiceTypeAzureServiceBus)

	// Get the Service Bus endpoint from the client (no hard-coded URLs)
	queue.URL = fmt.Sprintf("https://%s/%s", c.namespace, name)

	// Map Azure properties to unified queue structure
	if props.LockDuration != nil {
		queue.LockDuration = parseDuration(*props.LockDuration, c.config.DefaultLockDuration)
	} else {
		queue.LockDuration = c.config.DefaultLockDuration
	}

	if props.DefaultMessageTimeToLive != nil {
		queue.RetentionPeriod = parseDuration(*props.DefaultMessageTimeToLive, 1209600)
	} else {
		queue.RetentionPeriod = 1209600 // 14 days
	}

	if props.MaxDeliveryCount != nil {
		queue.MaxDeliveryCount = int(*props.MaxDeliveryCount)
	} else {
		queue.MaxDeliveryCount = 10
	}

	if props.RequiresSession != nil {
		queue.EnableSessions = *props.RequiresSession
	}

	if props.MaxSizeInMegabytes != nil {
		queue.MaxQueueSize = int64(*props.MaxSizeInMegabytes) * 1024 * 1024 // Convert MB to bytes
	}

	// Azure Service Bus has built-in dead letter queue support
	queue.DeadLetterConfig = &DeadLetterConfig{
		Enabled:          true,
		QueueName:        name + "/$deadletterqueue", // Built-in DLQ path
		MaxDeliveryCount: queue.MaxDeliveryCount,
	}

	if props.RequiresDuplicateDetection != nil && *props.RequiresDuplicateDetection {
		windowSecs := 300 // 5 minutes default
		if props.DuplicateDetectionHistoryTimeWindow != nil {
			windowSecs = parseDuration(*props.DuplicateDetectionHistoryTimeWindow, 300)
		}
		queue.DuplicateDetection = &DuplicateDetection{
			Enabled:       true,
			WindowSeconds: windowSecs,
		}
	}

	queue.CreatedTime = time.Now()
	queue.ModifiedTime = time.Now()

	return queue
}

// Helper functions for Azure admin API type conversions
func getBoolPtr(value bool) *bool {
	return &value
}

func toInt32Ptr(value int32) *int32 {
	return &value
}

func toStringPtr(value string) *string {
	return &value
}

// formatDuration converts seconds to ISO 8601 duration format for Azure Service Bus
func formatDuration(seconds int, fallbackSeconds int) string {
	if seconds > 0 {
		return fmt.Sprintf("PT%dS", seconds)
	}
	return fmt.Sprintf("PT%dS", fallbackSeconds)
}

// parseDuration converts ISO 8601 duration string to seconds
func parseDuration(durationStr string, fallbackSeconds int) int {
	if durationStr == "" {
		return fallbackSeconds
	}

	// Parse simple PT<number>S format
	if strings.HasPrefix(durationStr, "PT") && strings.HasSuffix(durationStr, "S") {
		numStr := strings.TrimSuffix(strings.TrimPrefix(durationStr, "PT"), "S")
		if seconds, err := strconv.Atoi(numStr); err == nil {
			return seconds
		}
	}

	return fallbackSeconds
}
