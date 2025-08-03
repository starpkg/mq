package mq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// AWSSQSClient implements the Client interface for AWS SQS
type AWSSQSClient struct {
	client     *sqs.Client
	region     string
	timeout    time.Duration
	maxRetries int
}

// NewAWSSQSClient creates a new AWS SQS client
func NewAWSSQSClient(config *ConnectionConfig) (*AWSSQSClient, error) {
	ctx := context.Background()

	var cfg aws.Config
	var err error

	// Configure AWS credentials and region
	if config.AWSAccessKey != "" && config.AWSSecretKey != "" {
		// Use explicit credentials
		cfg, err = awsConfig.LoadDefaultConfig(ctx,
			awsConfig.WithRegion(config.AWSRegion),
			awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				config.AWSAccessKey,
				config.AWSSecretKey,
				config.AWSSessionToken,
			)),
		)
	} else {
		// Use default credentials (IAM role, environment, etc.)
		cfg, err = awsConfig.LoadDefaultConfig(ctx,
			awsConfig.WithRegion(config.AWSRegion),
		)
	}

	if err != nil {
		return nil, NormalizeError(err, ServiceTypeAWSSQS, "connect")
	}

	// Create SQS client
	sqsClient := sqs.NewFromConfig(cfg)

	timeout := time.Duration(config.Timeout) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	maxRetries := config.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	return &AWSSQSClient{
		client:     sqsClient,
		region:     config.AWSRegion,
		timeout:    timeout,
		maxRetries: maxRetries,
	}, nil
}

// CreateQueue creates a new SQS queue
func (c *AWSSQSClient) CreateQueue(ctx context.Context, name string, options map[string]interface{}) (*Queue, error) {
	input := &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: make(map[string]string),
	}

	// Process unified options and convert to SQS attributes
	if lockDuration, ok := options["lock_duration"].(int); ok {
		input.Attributes["VisibilityTimeoutSeconds"] = strconv.Itoa(lockDuration)
	}

	if retentionPeriod, ok := options["retention_period"].(int); ok {
		input.Attributes["MessageRetentionPeriod"] = strconv.Itoa(retentionPeriod)
	}

	if maxDeliveryCount, ok := options["max_delivery_count"].(int); ok && maxDeliveryCount > 0 {
		// Dead letter queue configuration
		if dlqConfig, ok := options["dead_letter_config"].(map[string]interface{}); ok {
			if enabled, ok := dlqConfig["enabled"].(bool); ok && enabled {
				if dlqName, ok := dlqConfig["queue_name"].(string); ok {
					// Create DLQ first if it doesn't exist
					dlqURL, err := c.getOrCreateQueue(ctx, dlqName)
					if err != nil {
						return nil, err
					}

					// Configure redrive policy
					redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:%s:::%s","maxReceiveCount":%d}`,
						c.region, dlqName, maxDeliveryCount)
					input.Attributes["RedrivePolicy"] = redrivePolicy

					// Get DLQ ARN
					dlqArn, err := c.getQueueARN(ctx, dlqURL)
					if err != nil {
						return nil, err
					}
					input.Attributes["RedrivePolicy"] = fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":%d}`,
						dlqArn, maxDeliveryCount)
				}
			}
		}
	}

	if enableSessions, ok := options["enable_sessions"].(bool); ok && enableSessions {
		// FIFO queue
		if !strings.HasSuffix(name, ".fifo") {
			return nil, NewMQError("invalid_parameters", "FIFO queues must have names ending with .fifo").
				WithServiceType(ServiceTypeAWSSQS).
				WithOperation("create_queue")
		}
		input.Attributes["FifoQueue"] = "true"
	}

	if duplicateDetection, ok := options["duplicate_detection"].(map[string]interface{}); ok {
		if enabled, ok := duplicateDetection["enabled"].(bool); ok && enabled {
			input.Attributes["ContentBasedDeduplication"] = "true"
			if window, ok := duplicateDetection["window_seconds"].(int); ok {
				input.Attributes["DeduplicationScope"] = "queue"
				// Note: AWS SQS has a fixed 5-minute deduplication interval for FIFO queues
				_ = window // We can't actually set this in SQS, so we ignore it
			}
		}
	}

	result, err := c.client.CreateQueue(ctx, input)
	if err != nil {
		return nil, NormalizeError(err, ServiceTypeAWSSQS, "create_queue")
	}

	// Get queue information
	queueInfo, err := c.getQueueInfoByURL(ctx, *result.QueueUrl)
	if err != nil {
		return nil, err
	}

	return queueInfo, nil
}

// DeleteQueue deletes an SQS queue
func (c *AWSSQSClient) DeleteQueue(ctx context.Context, name string) error {
	queueURL, err := c.getQueueURL(ctx, name)
	if err != nil {
		return err
	}

	_, err = c.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	})

	return NormalizeError(err, ServiceTypeAWSSQS, "delete_queue")
}

// ListQueues lists SQS queues
func (c *AWSSQSClient) ListQueues(ctx context.Context, prefix string) ([]*Queue, error) {
	input := &sqs.ListQueuesInput{}
	if prefix != "" {
		input.QueueNamePrefix = aws.String(prefix)
	}

	result, err := c.client.ListQueues(ctx, input)
	if err != nil {
		return nil, NormalizeError(err, ServiceTypeAWSSQS, "list_queues")
	}

	queues := make([]*Queue, 0, len(result.QueueUrls))
	for _, queueURL := range result.QueueUrls {
		queueInfo, err := c.getQueueInfoByURL(ctx, queueURL)
		if err != nil {
			// Log error but continue with other queues
			continue
		}
		queues = append(queues, queueInfo)
	}

	return queues, nil
}

// GetQueue gets information about a specific queue
func (c *AWSSQSClient) GetQueue(ctx context.Context, name string) (*Queue, error) {
	return c.GetQueueInfo(ctx, name)
}

// QueueExists checks if a queue exists
func (c *AWSSQSClient) QueueExists(ctx context.Context, name string) (bool, error) {
	_, err := c.getQueueURL(ctx, name)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// PurgeQueue purges all messages from a queue
func (c *AWSSQSClient) PurgeQueue(ctx context.Context, name string) error {
	queueURL, err := c.getQueueURL(ctx, name)
	if err != nil {
		return err
	}

	_, err = c.client.PurgeQueue(ctx, &sqs.PurgeQueueInput{
		QueueUrl: aws.String(queueURL),
	})

	return NormalizeError(err, ServiceTypeAWSSQS, "purge_queue")
}

// GetQueueInfo gets detailed information about a queue
func (c *AWSSQSClient) GetQueueInfo(ctx context.Context, name string) (*Queue, error) {
	queueURL, err := c.getQueueURL(ctx, name)
	if err != nil {
		return nil, err
	}

	return c.getQueueInfoByURL(ctx, queueURL)
}

// SendMessage sends a message to a queue
func (c *AWSSQSClient) SendMessage(ctx context.Context, queueName, body string, options map[string]interface{}) (*MessageResult, error) {
	queueURL, err := c.getQueueURL(ctx, queueName)
	if err != nil {
		return nil, err
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(body),
	}

	// Process options
	if properties, ok := options["properties"].(map[string]interface{}); ok {
		input.MessageAttributes = make(map[string]types.MessageAttributeValue)
		for key, value := range properties {
			strValue := fmt.Sprintf("%v", value)
			input.MessageAttributes[key] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(strValue),
			}
		}
	}

	if sessionID, ok := options["session_id"].(string); ok && sessionID != "" {
		input.MessageGroupId = aws.String(sessionID)
	}

	if deduplicationID, ok := options["deduplication_id"].(string); ok && deduplicationID != "" {
		input.MessageDeduplicationId = aws.String(deduplicationID)
	}

	if scheduledTime, ok := options["scheduled_time"].(string); ok && scheduledTime != "" {
		// Convert to delay seconds (AWS SQS limitation: max 15 minutes)
		if delaySeconds, err := c.parseScheduledTime(scheduledTime); err == nil {
			if delaySeconds <= 900 { // 15 minutes
				input.DelaySeconds = int32(delaySeconds)
			} else {
				return nil, NewMQError("invalid_parameters", "AWS SQS delay cannot exceed 15 minutes").
					WithServiceType(ServiceTypeAWSSQS).
					WithOperation("send_message")
			}
		}
	}

	result, err := c.client.SendMessage(ctx, input)
	if err != nil {
		return &MessageResult{Success: false, Error: err.Error()}, NormalizeError(err, ServiceTypeAWSSQS, "send_message")
	}

	return &MessageResult{
		MessageID: *result.MessageId,
		Body:      body,
		Success:   true,
	}, nil
}

// ReceiveMessages receives messages from a queue
func (c *AWSSQSClient) ReceiveMessages(ctx context.Context, queueName string, maxCount int, options map[string]interface{}) ([]*MessageResult, error) {
	queueURL, err := c.getQueueURL(ctx, queueName)
	if err != nil {
		return nil, err
	}

	// AWS SQS limit is 10 messages per request
	if maxCount > 10 {
		maxCount = 10
	}

	input := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   int32(maxCount),
		MessageAttributeNames: []string{"All"},
	}

	// Process options
	if waitTime, ok := options["wait_time"].(int); ok && waitTime > 0 {
		// AWS SQS supports up to 20 seconds
		if waitTime > 20 {
			waitTime = 20
		}
		input.WaitTimeSeconds = int32(waitTime)
	}

	if lockDuration, ok := options["lock_duration"].(int); ok && lockDuration > 0 {
		input.VisibilityTimeout = int32(lockDuration)
	}

	result, err := c.client.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, NormalizeError(err, ServiceTypeAWSSQS, "receive_messages")
	}

	messages := make([]*MessageResult, len(result.Messages))
	for i, msg := range result.Messages {
		messages[i] = c.convertSQSMessage(msg)
	}

	return messages, nil
}

// DeleteMessage deletes a message from a queue
func (c *AWSSQSClient) DeleteMessage(ctx context.Context, queueName, messageID string) error {
	queueURL, err := c.getQueueURL(ctx, queueName)
	if err != nil {
		return err
	}

	// For SQS, we need the receipt handle, which should be stored in the message
	// This is a limitation of the unified interface - we'll need to pass the receipt handle as messageID
	_, err = c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(messageID), // messageID is actually the receipt handle
	})

	return NormalizeError(err, ServiceTypeAWSSQS, "delete_message")
}

// Helper methods

func (c *AWSSQSClient) getQueueURL(ctx context.Context, queueName string) (string, error) {
	result, err := c.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", NormalizeError(err, ServiceTypeAWSSQS, "get_queue_url")
	}
	return *result.QueueUrl, nil
}

func (c *AWSSQSClient) getQueueInfoByURL(ctx context.Context, queueURL string) (*Queue, error) {
	// Extract queue name from URL
	parts := strings.Split(queueURL, "/")
	queueName := parts[len(parts)-1]

	input := &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	}

	result, err := c.client.GetQueueAttributes(ctx, input)
	if err != nil {
		return nil, NormalizeError(err, ServiceTypeAWSSQS, "get_queue_attributes")
	}

	queue := &Queue{
		Name:        queueName,
		ServiceType: ServiceTypeAWSSQS,
		URL:         queueURL,
	}

	// Parse attributes
	if val, ok := result.Attributes["ApproximateNumberOfMessages"]; ok {
		if count, err := strconv.Atoi(val); err == nil {
			queue.MessageCount = count
		}
	}

	if val, ok := result.Attributes["VisibilityTimeoutSeconds"]; ok {
		if timeout, err := strconv.Atoi(val); err == nil {
			queue.LockDuration = timeout
		}
	}

	if val, ok := result.Attributes["MessageRetentionPeriod"]; ok {
		if retention, err := strconv.Atoi(val); err == nil {
			queue.RetentionPeriod = retention
		}
	}

	if val, ok := result.Attributes["RedrivePolicy"]; ok && val != "" {
		// Parse redrive policy JSON to extract max receive count
		queue.DeadLetterConfig = &DeadLetterConfig{
			Enabled: true,
		}
		// TODO: Parse JSON to get exact values
	}

	if val, ok := result.Attributes["FifoQueue"]; ok && val == "true" {
		queue.EnableSessions = true
	}

	if val, ok := result.Attributes["ContentBasedDeduplication"]; ok && val == "true" {
		queue.DuplicateDetection = &DuplicateDetectionConfig{
			Enabled:       true,
			WindowSeconds: 300, // AWS SQS fixed value
		}
	}

	return queue, nil
}

func (c *AWSSQSClient) getOrCreateQueue(ctx context.Context, queueName string) (string, error) {
	// Try to get existing queue
	queueURL, err := c.getQueueURL(ctx, queueName)
	if err == nil {
		return queueURL, nil
	}

	// Create the queue if it doesn't exist
	result, err := c.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", NormalizeError(err, ServiceTypeAWSSQS, "create_queue")
	}

	return *result.QueueUrl, nil
}

func (c *AWSSQSClient) getQueueARN(ctx context.Context, queueURL string) (string, error) {
	result, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", NormalizeError(err, ServiceTypeAWSSQS, "get_queue_arn")
	}

	if arn, ok := result.Attributes["QueueArn"]; ok {
		return arn, nil
	}

	return "", NewMQError("missing_attribute", "Queue ARN not found").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("get_queue_arn")
}

func (c *AWSSQSClient) convertSQSMessage(msg types.Message) *MessageResult {
	result := &MessageResult{
		MessageID:     *msg.MessageId,
		Body:          *msg.Body,
		ReceiptHandle: *msg.ReceiptHandle,
		Success:       true,
		Properties:    make(map[string]interface{}),
	}

	// Convert message attributes to properties
	for key, attr := range msg.MessageAttributes {
		if attr.StringValue != nil {
			result.Properties[key] = *attr.StringValue
		}
	}

	// Parse other SQS-specific attributes
	for key, value := range msg.Attributes {
		switch key {
		case "ApproximateReceiveCount":
			if count, err := strconv.Atoi(value); err == nil {
				result.DeliveryCount = count
			}
		case "ApproximateFirstReceiveTimestamp":
			if timestamp, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.EnqueueTime = time.Unix(timestamp/1000, 0)
			}
		}
	}

	return result
}

func (c *AWSSQSClient) parseScheduledTime(scheduledTime string) (int, error) {
	// Parse ISO 8601 timestamp and convert to delay seconds from now
	targetTime, err := time.Parse(time.RFC3339, scheduledTime)
	if err != nil {
		return 0, fmt.Errorf("invalid scheduled time format: %v", err)
	}

	delaySeconds := int(targetTime.Sub(time.Now()).Seconds())
	if delaySeconds < 0 {
		delaySeconds = 0
	}

	return delaySeconds, nil
}

// Implement remaining Client interface methods...

func (c *AWSSQSClient) DeleteMessages(ctx context.Context, queueName string, messageIDs []string) ([]bool, error) {
	// AWS SQS supports batch delete up to 10 messages
	results := make([]bool, len(messageIDs))

	for i := 0; i < len(messageIDs); i += 10 {
		end := i + 10
		if end > len(messageIDs) {
			end = len(messageIDs)
		}

		batchResults, err := c.deleteBatch(ctx, queueName, messageIDs[i:end])
		if err != nil {
			// Mark all messages in this batch as failed
			for j := i; j < end; j++ {
				results[j] = false
			}
			continue
		}

		// Copy batch results
		copy(results[i:], batchResults)
	}

	return results, nil
}

func (c *AWSSQSClient) deleteBatch(ctx context.Context, queueName string, receiptHandles []string) ([]bool, error) {
	queueURL, err := c.getQueueURL(ctx, queueName)
	if err != nil {
		results := make([]bool, len(receiptHandles))
		return results, err
	}

	entries := make([]types.DeleteMessageBatchRequestEntry, len(receiptHandles))
	for i, handle := range receiptHandles {
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprintf("msg_%d", i)),
			ReceiptHandle: aws.String(handle),
		}
	}

	result, err := c.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	})

	results := make([]bool, len(receiptHandles))
	if err != nil {
		return results, NormalizeError(err, ServiceTypeAWSSQS, "delete_messages")
	}

	// Mark successful deletes
	successfulIds := make(map[string]bool)
	for _, success := range result.Successful {
		successfulIds[*success.Id] = true
	}

	for i := range results {
		id := fmt.Sprintf("msg_%d", i)
		results[i] = successfulIds[id]
	}

	return results, nil
}

// Implement stub methods for unsupported features (with graceful failures)

func (c *AWSSQSClient) ExtendMessageLock(ctx context.Context, queueName, messageID string, lockDuration int) error {
	// This maps to ChangeMessageVisibility in SQS
	queueURL, err := c.getQueueURL(ctx, queueName)
	if err != nil {
		return err
	}

	_, err = c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueURL),
		ReceiptHandle:     aws.String(messageID), // messageID is receipt handle
		VisibilityTimeout: int32(lockDuration),
	})

	return NormalizeError(err, ServiceTypeAWSSQS, "extend_message_lock")
}

func (c *AWSSQSClient) ReleaseMessageLock(ctx context.Context, queueName, messageID string) error {
	// Set visibility timeout to 0 to immediately release the message
	return c.ExtendMessageLock(ctx, queueName, messageID, 0)
}

func (c *AWSSQSClient) SendMessagesBatch(ctx context.Context, queueName string, messages []map[string]interface{}) ([]*MessageResult, error) {
	// AWS SQS supports up to 10 messages per batch
	results := make([]*MessageResult, 0, len(messages))

	for i := 0; i < len(messages); i += 10 {
		end := i + 10
		if end > len(messages) {
			end = len(messages)
		}

		batchResults, err := c.sendBatch(ctx, queueName, messages[i:end])
		if err != nil {
			// Create error results for this batch
			for j := i; j < end; j++ {
				results = append(results, &MessageResult{
					Success: false,
					Error:   err.Error(),
				})
			}
			continue
		}

		results = append(results, batchResults...)
	}

	return results, nil
}

func (c *AWSSQSClient) sendBatch(ctx context.Context, queueName string, messages []map[string]interface{}) ([]*MessageResult, error) {
	queueURL, err := c.getQueueURL(ctx, queueName)
	if err != nil {
		return nil, err
	}

	entries := make([]types.SendMessageBatchRequestEntry, len(messages))
	for i, msg := range messages {
		body, _ := msg["body"].(string)
		entry := types.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("msg_%d", i)),
			MessageBody: aws.String(body),
		}

		// Handle message attributes
		if properties, ok := msg["properties"].(map[string]interface{}); ok {
			entry.MessageAttributes = make(map[string]types.MessageAttributeValue)
			for key, value := range properties {
				strValue := fmt.Sprintf("%v", value)
				entry.MessageAttributes[key] = types.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(strValue),
				}
			}
		}

		entries[i] = entry
	}

	result, err := c.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	})

	if err != nil {
		return nil, NormalizeError(err, ServiceTypeAWSSQS, "send_messages_batch")
	}

	results := make([]*MessageResult, len(messages))

	// Map successful results
	successfulResults := make(map[string]*MessageResult)
	for _, success := range result.Successful {
		successfulResults[*success.Id] = &MessageResult{
			MessageID: *success.MessageId,
			Body:      messages[0]["body"].(string), // TODO: Get correct body
			Success:   true,
		}
	}

	// Map failed results
	failedResults := make(map[string]string)
	for _, failed := range result.Failed {
		failedResults[*failed.Id] = *failed.Message
	}

	// Build final results array
	for i := range results {
		id := fmt.Sprintf("msg_%d", i)
		if successResult, ok := successfulResults[id]; ok {
			results[i] = successResult
		} else if errorMsg, ok := failedResults[id]; ok {
			results[i] = &MessageResult{
				Success: false,
				Error:   errorMsg,
			}
		} else {
			results[i] = &MessageResult{
				Success: false,
				Error:   "Unknown error",
			}
		}
	}

	return results, nil
}

// Unsupported operations (return appropriate errors)

func (c *AWSSQSClient) PeekMessages(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error) {
	return nil, NewMQError("feature_not_supported", "Peek messages is not supported by AWS SQS. Use receive_messages instead.").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("peek_messages")
}

func (c *AWSSQSClient) SendScheduledMessage(ctx context.Context, queueName, body string, scheduledTime string, options map[string]interface{}) (*MessageResult, error) {
	// Convert scheduled time to delay seconds
	delaySeconds, err := c.parseScheduledTime(scheduledTime)
	if err != nil {
		return nil, NewMQError("invalid_parameters", fmt.Sprintf("Invalid scheduled time: %v", err)).
			WithServiceType(ServiceTypeAWSSQS).
			WithOperation("send_scheduled_message")
	}

	if delaySeconds > 900 {
		return nil, NewMQError("feature_limitation", "AWS SQS only supports delays up to 15 minutes. Use CloudWatch Events for longer delays.").
			WithServiceType(ServiceTypeAWSSQS).
			WithOperation("send_scheduled_message")
	}

	// Add delay to options
	if options == nil {
		options = make(map[string]interface{})
	}
	options["scheduled_time"] = scheduledTime

	return c.SendMessage(ctx, queueName, body, options)
}

func (c *AWSSQSClient) CancelScheduledMessage(ctx context.Context, queueName, messageID string) error {
	return NewMQError("feature_not_supported", "Cancel scheduled message is not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("cancel_scheduled_message")
}

func (c *AWSSQSClient) GetDeadLetterMessages(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error) {
	// For SQS, dead letter messages are in a separate DLQ
	dlqName := queueName + "-dlq" // Convention
	return c.ReceiveMessages(ctx, dlqName, maxCount, nil)
}

func (c *AWSSQSClient) ReprocessDeadLetterMessage(ctx context.Context, queueName, messageID string) error {
	return NewMQError("feature_not_supported", "Reprocess dead letter message requires manual implementation in AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("reprocess_dead_letter_message")
}

func (c *AWSSQSClient) PurgeDeadLetterQueue(ctx context.Context, queueName string) error {
	dlqName := queueName + "-dlq"
	return c.PurgeQueue(ctx, dlqName)
}

// Topic operations (not supported by SQS)

func (c *AWSSQSClient) CreateTopic(ctx context.Context, name string, options map[string]interface{}) (*Topic, error) {
	return nil, NewMQError("feature_not_supported", "Topics are not supported by AWS SQS. Use Amazon SNS + SQS for pub/sub patterns.").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("create_topic")
}

func (c *AWSSQSClient) DeleteTopic(ctx context.Context, name string) error {
	return NewMQError("feature_not_supported", "Topics are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("delete_topic")
}

func (c *AWSSQSClient) ListTopics(ctx context.Context, prefix string) ([]*Topic, error) {
	return nil, NewMQError("feature_not_supported", "Topics are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("list_topics")
}

func (c *AWSSQSClient) TopicExists(ctx context.Context, name string) (bool, error) {
	return false, NewMQError("feature_not_supported", "Topics are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("topic_exists")
}

func (c *AWSSQSClient) CreateSubscription(ctx context.Context, topicName, subscriptionName string, options map[string]interface{}) (*Subscription, error) {
	return nil, NewMQError("feature_not_supported", "Subscriptions are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("create_subscription")
}

func (c *AWSSQSClient) DeleteSubscription(ctx context.Context, topicName, subscriptionName string) error {
	return NewMQError("feature_not_supported", "Subscriptions are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("delete_subscription")
}

func (c *AWSSQSClient) ListSubscriptions(ctx context.Context, topicName string) ([]*Subscription, error) {
	return nil, NewMQError("feature_not_supported", "Subscriptions are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("list_subscriptions")
}

func (c *AWSSQSClient) PublishMessage(ctx context.Context, topicName, body string, options map[string]interface{}) (*MessageResult, error) {
	return nil, NewMQError("feature_not_supported", "Publishing to topics is not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("publish_message")
}

func (c *AWSSQSClient) SubscribeMessages(ctx context.Context, topicName, subscriptionName string, maxCount int, options map[string]interface{}) ([]*MessageResult, error) {
	return nil, NewMQError("feature_not_supported", "Subscribing to topics is not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("subscribe_messages")
}

func (c *AWSSQSClient) AddSubscriptionFilter(ctx context.Context, topicName, subscriptionName, ruleName, filterExpression string) error {
	return NewMQError("feature_not_supported", "Subscription filters are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("add_subscription_filter")
}

func (c *AWSSQSClient) RemoveSubscriptionFilter(ctx context.Context, topicName, subscriptionName, ruleName string) error {
	return NewMQError("feature_not_supported", "Subscription filters are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("remove_subscription_filter")
}

func (c *AWSSQSClient) ListSubscriptionFilters(ctx context.Context, topicName, subscriptionName string) ([]SubscriptionFilter, error) {
	return nil, NewMQError("feature_not_supported", "Subscription filters are not supported by AWS SQS").
		WithServiceType(ServiceTypeAWSSQS).
		WithOperation("list_subscription_filters")
}

// Connection management

func (c *AWSSQSClient) Close() error {
	// AWS SDK handles connection pooling automatically
	return nil
}

func (c *AWSSQSClient) HealthCheck(ctx context.Context) error {
	// Simple health check by listing queues
	_, err := c.client.ListQueues(ctx, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(1),
	})
	return NormalizeError(err, ServiceTypeAWSSQS, "health_check")
}

func (c *AWSSQSClient) GetClientInfo() *ClientInfo {
	return &ClientInfo{
		ServiceType: ServiceTypeAWSSQS,
		Region:      c.region,
		Connected:   true,
		SupportedFeatures: []string{
			FeatureBatchOperations,
			FeatureDeadLetterQueue,
			FeatureScheduledMessages,
			FeatureFIFOQueues,
			FeatureDuplicateDetection,
		},
		Capabilities: map[string]interface{}{
			"max_batch_size":                 10,
			"max_message_size_bytes":         262144, // 256KB
			"max_visibility_timeout":         43200,  // 12 hours
			"max_delay_seconds":              900,    // 15 minutes
			"supports_long_polling":          true,
			"supports_fifo":                  true,
			"supports_content_deduplication": true,
		},
	}
}

func (c *AWSSQSClient) CheckFeatureSupport(feature string) bool {
	supportedFeatures := []string{
		FeatureBatchOperations,
		FeatureDeadLetterQueue,
		FeatureScheduledMessages,
		FeatureFIFOQueues,
		FeatureDuplicateDetection,
	}

	for _, supported := range supportedFeatures {
		if feature == supported {
			return true
		}
	}
	return false
}
