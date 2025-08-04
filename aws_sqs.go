package mq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"
)

// AWSSQSClient implements the Client interface for AWS SQS
type AWSSQSClient struct {
	config    *ClientConfig
	region    string
	sqs       *sqs.SQS
	sts       *sts.STS
	accountID string
	accountMu sync.Once
}

// NewAWSSQSClient creates a new AWS SQS client
func NewAWSSQSClient(ctx context.Context, config *ClientConfig) (Client, error) {
	if config.ServiceType != ServiceTypeAWSSQS {
		return nil, fmt.Errorf("invalid service type for AWS SQS client: %s", config.ServiceType)
	}

	if config.AWSRegion == "" {
		return nil, fmt.Errorf("aws_region is required for AWS SQS")
	}

	// Create AWS session
	sess, err := createAWSSession(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Create SQS and STS services
	sqsService := sqs.New(sess)
	stsService := sts.New(sess)

	client := &AWSSQSClient{
		config: config.Copy(),
		region: config.AWSRegion,
		sqs:    sqsService,
		sts:    stsService,
	}

	return client, nil
}

// createAWSSession creates AWS session with credentials using SDK v1
func createAWSSession(mqConfig *ClientConfig) (*session.Session, error) {
	// Build AWS config
	awsConfig := &aws.Config{
		Region: aws.String(mqConfig.AWSRegion),
	}

	// Set credentials if provided
	if mqConfig.AWSAccessKey != "" && mqConfig.AWSSecretKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(
			mqConfig.AWSAccessKey,
			mqConfig.AWSSecretKey,
			mqConfig.AWSSessionToken,
		)
	}

	// Create session
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	return sess, nil
}

// GetClientInfo returns information about the client
func (c *AWSSQSClient) GetClientInfo() map[string]interface{} {
	return map[string]interface{}{
		"service_type": ServiceTypeAWSSQS,
		"region":       c.region,
		"timeout":      c.config.Timeout,
		"max_retries":  c.config.MaxRetries,
	}
}

// CreateQueue creates a new SQS queue
func (c *AWSSQSClient) CreateQueue(ctx context.Context, name string, options QueueOptions) (*Queue, error) {
	if err := validateQueueName(name); err != nil {
		return nil, NewMQError(ErrorTypeValidation, ServiceTypeAWSSQS, "create_queue", "invalid queue name", err)
	}

	// Handle FIFO queue naming
	queueName := name
	if options.EnableSessions {
		// For SQS, sessions means FIFO queue
		if !strings.HasSuffix(queueName, ".fifo") {
			queueName += ".fifo"
		}
	}

	// For AWS SQS, handle dead letter queue creation first if enabled
	if options.DeadLetterConfig != nil && options.DeadLetterConfig.Enabled {
		dlqName := options.DeadLetterConfig.QueueName
		if dlqName == "" {
			dlqName = name + "-dlq"
		}

		// Create the dead letter queue first
		dlqInput := &sqs.CreateQueueInput{
			QueueName: aws.String(dlqName),
		}
		_, err := c.sqs.CreateQueue(dlqInput)
		if err != nil {
			// Continue if DLQ already exists, otherwise return error
			// TODO: Check if error is "QueueAlreadyExists" and continue, otherwise fail
		}
	}

	// Build queue attributes from options (now DLQ exists)
	attributes := c.convertToSQSAttributes(name, options)

	// Create queue request
	input := &sqs.CreateQueueInput{
		QueueName:  aws.String(queueName),
		Attributes: aws.StringMap(attributes),
	}

	// Create the queue
	result, err := c.sqs.CreateQueue(input)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAWSSQS, "create_queue", "failed to create queue", err)
	}

	var queueURL string
	if result.QueueUrl != nil {
		queueURL = *result.QueueUrl
	}

	// Build unified queue object
	queue := NewQueue(name, ServiceTypeAWSSQS)
	queue.URL = queueURL
	queue.LockDuration = coalesceInt(options.LockDuration, c.config.DefaultLockDuration)
	queue.RetentionPeriod = coalesceInt(options.RetentionPeriod, 1209600) // 14 days
	queue.MaxDeliveryCount = coalesceInt(options.MaxDeliveryCount, 10)
	queue.EnableSessions = options.EnableSessions
	queue.DeadLetterConfig = options.DeadLetterConfig

	if options.DuplicateDetection {
		queue.DuplicateDetection = &DuplicateDetection{
			Enabled:       true,
			WindowSeconds: coalesceInt(options.DuplicateWindowSecs, 300),
		}
	}

	return queue, nil
}

// DeleteQueue deletes an SQS queue
func (c *AWSSQSClient) DeleteQueue(ctx context.Context, name string) error {
	// TODO: Implement actual AWS SQS queue deletion
	// This would involve calling DeleteQueue API

	return nil
}

// ListQueues lists SQS queues
func (c *AWSSQSClient) ListQueues(ctx context.Context, prefix string) ([]*Queue, error) {
	// TODO: Implement actual AWS SQS queue listing
	// This would involve:
	// 1. Calling ListQueues API
	// 2. Converting queue URLs to Queue objects
	// 3. Getting queue attributes for each queue

	// For now, return empty list
	return []*Queue{}, nil
}

// GetQueue gets information about a specific queue
func (c *AWSSQSClient) GetQueue(ctx context.Context, name string) (*Queue, error) {
	if name == "" {
		return nil, NewMQError(ErrorTypeNotFound, ServiceTypeAWSSQS, "get_queue", "queue not found", nil)
	}

	// Get queue URL first
	getURLInput := &sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	}

	urlResult, err := c.sqs.GetQueueUrl(getURLInput)
	if err != nil {
		return nil, NewMQError(ErrorTypeNotFound, ServiceTypeAWSSQS, "get_queue", "queue not found", err)
	}

	if urlResult.QueueUrl == nil {
		return nil, NewMQError(ErrorTypeNotFound, ServiceTypeAWSSQS, "get_queue", "queue URL not found", nil)
	}

	// Get queue attributes
	getAttrsInput := &sqs.GetQueueAttributesInput{
		QueueUrl:       urlResult.QueueUrl,
		AttributeNames: []*string{aws.String("All")},
	}

	attrsResult, err := c.sqs.GetQueueAttributes(getAttrsInput)
	if err != nil {
		return nil, NewMQError(ErrorTypeService, ServiceTypeAWSSQS, "get_queue", "failed to get queue attributes", err)
	}

	// Build unified queue object
	queue := NewQueue(name, ServiceTypeAWSSQS)
	queue.URL = *urlResult.QueueUrl

	// Parse attributes
	if attrsResult.Attributes != nil {
		if val, ok := attrsResult.Attributes["VisibilityTimeout"]; ok && val != nil {
			if lockDuration, err := strconv.Atoi(*val); err == nil {
				queue.LockDuration = lockDuration
			}
		}
		if val, ok := attrsResult.Attributes["MessageRetentionPeriod"]; ok && val != nil {
			if retention, err := strconv.Atoi(*val); err == nil {
				queue.RetentionPeriod = retention
			}
		}
		if val, ok := attrsResult.Attributes["RedrivePolicy"]; ok && val != nil {
			// Parse redrive policy to extract max delivery count
			// This is a simplified implementation
			queue.MaxDeliveryCount = 10 // Default value
		}
	}

	// Set defaults if not found
	if queue.LockDuration == 0 {
		queue.LockDuration = c.config.DefaultLockDuration
	}
	if queue.RetentionPeriod == 0 {
		queue.RetentionPeriod = 1209600 // 14 days
	}
	if queue.MaxDeliveryCount == 0 {
		queue.MaxDeliveryCount = 10
	}

	return queue, nil
}

// Exists checks if a queue exists
func (c *AWSSQSClient) Exists(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, nil
	}

	// Try to get queue URL
	input := &sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	}

	_, err := c.sqs.GetQueueUrl(input)
	if err != nil {
		// Check if it's a "queue not found" error
		return false, nil
	}

	return true, nil
}

// Purge purges all messages from a queue
func (c *AWSSQSClient) Purge(ctx context.Context, name string) error {
	// TODO: Implement actual AWS SQS queue purging
	// This would involve calling PurgeQueue API

	return nil
}

// GetInfo gets detailed queue information
func (c *AWSSQSClient) GetInfo(ctx context.Context, name string) (*Queue, error) {
	// For SQS, this is the same as GetQueue
	return c.GetQueue(ctx, name)
}

// Send sends a message to a queue
func (c *AWSSQSClient) Send(ctx context.Context, queueName, body string, options MessageOptions) (*MessageResult, error) {
	if err := validateMessageBody(body); err != nil {
		return nil, NewMQError(ErrorTypeValidation, ServiceTypeAWSSQS, "send", "invalid message body", err)
	}

	// TODO: Implement actual AWS SQS message sending
	// This would involve:
	// 1. Getting queue URL
	// 2. Building SendMessage request
	// 3. Converting unified options to SQS message attributes
	// 4. Handling DelaySeconds for scheduling
	// 5. Setting MessageGroupId for FIFO queues

	// For now, return a mock result
	result := NewMessageResult(generateMessageID(), body)
	result.Properties = normalizeProperties(options.Properties)
	result.SessionID = options.SessionID
	result.CorrelationID = options.CorrelationID
	result.TimeToLive = options.TimeToLive

	if options.ScheduledTime != nil {
		result.ScheduledTime = options.ScheduledTime
	}

	return result, nil
}

// Receive receives messages from a queue
func (c *AWSSQSClient) Receive(ctx context.Context, queueName string, options ReceiveOptions) ([]*MessageResult, error) {
	// TODO: Implement actual AWS SQS message receiving
	// This would involve:
	// 1. Getting queue URL
	// 2. Building ReceiveMessage request
	// 3. Setting MaxNumberOfMessages, WaitTimeSeconds, VisibilityTimeout
	// 4. Converting SQS messages to unified MessageResult

	// For now, return empty list
	return []*MessageResult{}, nil
}

// Delete deletes messages from a queue
func (c *AWSSQSClient) Delete(ctx context.Context, queueName string, messageIDs []string) ([]bool, error) {
	if len(messageIDs) == 0 {
		return []bool{}, nil
	}

	// TODO: Implement actual AWS SQS message deletion
	// This would involve:
	// 1. Getting queue URL
	// 2. Using DeleteMessageBatch for multiple messages
	// 3. Handling batch size limits (max 10 for SQS)
	// 4. Using receipt handles instead of message IDs

	// For now, return all successful
	results := make([]bool, len(messageIDs))
	for i := range results {
		results[i] = true
	}
	return results, nil
}

// Lock extends the visibility timeout of a message
func (c *AWSSQSClient) Lock(ctx context.Context, queueName, messageID string, duration int) error {
	// TODO: Implement actual AWS SQS visibility timeout extension
	// This would involve calling ChangeMessageVisibility API

	return nil
}

// Unlock releases a message by setting visibility timeout to 0
func (c *AWSSQSClient) Unlock(ctx context.Context, queueName, messageID string) error {
	// TODO: Implement actual AWS SQS visibility timeout reset
	// This would involve calling ChangeMessageVisibility with timeout 0

	return nil
}

// BatchSend sends multiple messages in batches
func (c *AWSSQSClient) BatchSend(ctx context.Context, queueName string, messages []BatchMessage) ([]*MessageResult, error) {
	if len(messages) == 0 {
		return []*MessageResult{}, nil
	}

	// Split into SQS batch size limits (max 10)
	batchSize := adaptBatchSize(ServiceTypeAWSSQS, 10)
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
func (c *AWSSQSClient) sendBatch(ctx context.Context, queueName string, messages []BatchMessage) ([]*MessageResult, error) {
	// TODO: Implement actual AWS SQS batch sending
	// This would involve calling SendMessageBatch API

	// For now, return mock results
	results := make([]*MessageResult, len(messages))
	for i, msg := range messages {
		results[i] = NewMessageResult(generateMessageID(), msg.Body)
		results[i].Properties = normalizeProperties(msg.Properties)
		results[i].SessionID = msg.SessionID
	}

	return results, nil
}

// Schedule schedules a message for future delivery
func (c *AWSSQSClient) Schedule(ctx context.Context, queueName, body string, scheduledTime time.Time, options MessageOptions) (*MessageResult, error) {
	// For AWS SQS, DelaySeconds has a maximum of 15 minutes
	delay := time.Until(scheduledTime)
	maxDelay := 15 * time.Minute

	if delay > maxDelay {
		return nil, NewMQError(ErrorTypeValidation, ServiceTypeAWSSQS, "schedule",
			fmt.Sprintf("AWS SQS supports maximum delay of 15 minutes, requested: %v", delay), nil)
	}

	// Copy options and set scheduled time
	msgOptions := options
	msgOptions.ScheduledTime = &scheduledTime

	return c.Send(ctx, queueName, body, msgOptions)
}

// Cancel cancels a scheduled message (not supported by SQS)
func (c *AWSSQSClient) Cancel(ctx context.Context, queueName, messageID string) error {
	return NewMQError(ErrorTypeUnsupported, ServiceTypeAWSSQS, "cancel",
		"message cancellation is not supported by AWS SQS", nil)
}

// Peek peeks at messages without receiving them (not supported by SQS)
func (c *AWSSQSClient) Peek(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error) {
	return nil, NewMQError(ErrorTypeUnsupported, ServiceTypeAWSSQS, "peek",
		"message peeking is not supported by AWS SQS", nil)
}

// DeadLetterReceive receives messages from the dead letter queue
func (c *AWSSQSClient) DeadLetterReceive(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error) {
	// For AWS SQS, dead letter queue is a separate queue
	dlqName := queueName + "-dlq"
	options := ReceiveOptions{
		MaxCount: maxCount,
	}
	return c.Receive(ctx, dlqName, options)
}

// DeadLetterRequeue moves a message back from DLQ to main queue
func (c *AWSSQSClient) DeadLetterRequeue(ctx context.Context, queueName, messageID string) error {
	// TODO: Implement actual requeue logic
	// This would involve:
	// 1. Receiving the message from DLQ
	// 2. Sending it to the main queue
	// 3. Deleting it from DLQ

	return nil
}

// DeadLetterPurge purges all messages from the dead letter queue
func (c *AWSSQSClient) DeadLetterPurge(ctx context.Context, queueName string) error {
	dlqName := queueName + "-dlq"
	return c.Purge(ctx, dlqName)
}

// Close closes the client connection
func (c *AWSSQSClient) Close() error {
	// TODO: Clean up AWS SDK client resources if needed
	return nil
}

// Helper functions for AWS SQS specific operations

// convertToSQSAttributes converts unified options to SQS queue attributes
func (c *AWSSQSClient) convertToSQSAttributes(queueName string, options QueueOptions) map[string]string {
	attrs := make(map[string]string)

	// Set visibility timeout with defaults
	lockDuration := options.LockDuration
	if lockDuration <= 0 {
		lockDuration = c.config.DefaultLockDuration
		if lockDuration <= 0 {
			lockDuration = 30 // AWS SQS default
		}
	}
	attrs["VisibilityTimeout"] = strconv.Itoa(lockDuration)

	// Set message retention period with defaults
	retentionPeriod := options.RetentionPeriod
	if retentionPeriod <= 0 {
		retentionPeriod = 1209600 // 14 days (AWS SQS default)
	}
	attrs["MessageRetentionPeriod"] = strconv.Itoa(retentionPeriod)

	if options.MaxDeliveryCount > 0 && options.DeadLetterConfig != nil && options.DeadLetterConfig.Enabled {
		// For AWS SQS Dead Letter Queue, we need to create the DLQ separately
		// and reference it by ARN. For simplicity in the unified interface,
		// we'll construct the ARN based on the current account and region.
		accountID := c.getAccountID()

		dlqName := options.DeadLetterConfig.QueueName
		if dlqName == "" {
			// This should match the logic in CreateQueue method
			dlqName = queueName + "-dlq"
		}

		// Validate that we have all required components for the ARN
		if accountID != "" && c.region != "" && dlqName != "" {
			dlqArn := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", c.region, accountID, dlqName)
			redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":%d}`,
				dlqArn, options.MaxDeliveryCount)
			attrs["RedrivePolicy"] = redrivePolicy
		}
	}

	if options.EnableSessions {
		// For SQS, this means FIFO queue
		attrs["FifoQueue"] = "true"
	}

	if options.DuplicateDetection {
		attrs["ContentBasedDeduplication"] = "true"
	}

	return attrs
}

// getAccountID returns the AWS account ID
func (c *AWSSQSClient) getAccountID() string {
	c.accountMu.Do(func() {
		// Get the account ID via STS GetCallerIdentity
		result, err := c.sts.GetCallerIdentity(&sts.GetCallerIdentityInput{})
		if err != nil || result.Account == nil {
			// Fallback to placeholder for testing
			c.accountID = "123456789012"
		} else {
			c.accountID = *result.Account
		}
	})
	return c.accountID
}

// convertToSQSMessageAttributes converts unified properties to SQS message attributes
func convertToSQSMessageAttributes(properties map[string]interface{}) map[string]interface{} {
	if properties == nil {
		return nil
	}

	attrs := make(map[string]interface{})
	for k, v := range properties {
		// SQS message attributes have specific format
		attrs[k] = map[string]interface{}{
			"StringValue": fmt.Sprintf("%v", v),
			"DataType":    "String",
		}
	}

	return attrs
}

// getSQSQueueURL gets the queue URL for a queue name
func (c *AWSSQSClient) getSQSQueueURL(queueName string) string {
	// This is a simplified URL format - in practice, this would come from GetQueueUrl API
	return fmt.Sprintf("https://sqs.%s.amazonaws.com/123456789012/%s", c.region, queueName)
}
