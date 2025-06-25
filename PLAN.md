# 📬 MQ (Message Queue) - Starlark Module Plan

## Overview

The **MQ** module provides a unified, simple, and intuitive interface for interacting with message queue services from Starlark scripts. It supports both **AWS SQS** and **Azure Service Bus** (Queues and Topics), abstracting the differences between these services while still allowing access to service-specific features when needed.

### Emoji & Description

📬 **Unified message queue operations for AWS SQS and Azure Service Bus**

## Key Features

### Core Operations

- **Send Messages**: Send messages to queues or topics with optional attributes and metadata
- **Receive Messages**: Receive messages from queues or subscriptions with configurable batch sizes
- **Delete Messages**: Delete messages after processing to prevent reprocessing
- **Peek Messages**: Peek at messages without removing them from the queue
- **Message Attributes**: Support for custom message attributes and metadata
- **Batch Operations**: Efficient batch sending and receiving of messages

### Queue Management

- **Create Queues/Topics**: Create new queues or topics with customizable properties
- **Delete Queues/Topics**: Remove queues or topics when no longer needed
- **List Queues/Topics**: Enumerate available queues and topics
- **Queue Properties**: Get and set queue properties (visibility timeout, message retention, etc.)

### Advanced Features

- **Dead Letter Queues**: Configure and manage dead letter queues for failed messages
- **Scheduled Messages**: Schedule messages for future delivery
- **Message Filtering**: Filter messages based on attributes (Azure Service Bus Topics)
- **Message Sessions**: Support for message sessions and ordering (Azure Service Bus)
- **Visibility Timeout**: Manage message visibility timeout for processing windows

### Error Handling & Reliability

- **Retry Logic**: Configurable retry policies for failed operations
- **Error Handling**: Comprehensive error handling with meaningful error messages
- **Connection Management**: Automatic connection pooling and reconnection
- **Timeout Configuration**: Configurable timeouts for operations

## Supported Services

### 1. AWS SQS (Simple Queue Service)

- **Standard Queues**: High throughput, at-least-once delivery
- **FIFO Queues**: Exactly-once processing, message ordering
- **Message Attributes**: Custom metadata with messages
- **Dead Letter Queues**: Automatic handling of failed messages
- **Visibility Timeout**: Configurable message processing windows

### 2. Azure Service Bus Queues

- **Basic Queues**: Reliable message queuing with FIFO ordering
- **Message Sessions**: Grouped message processing
- **Scheduled Messages**: Delay message delivery
- **Dead Letter Queues**: Automatic dead letter handling
- **Message Metadata**: Custom properties and user properties

### 3. Azure Service Bus Topics/Subscriptions

- **Publish/Subscribe**: One-to-many message distribution
- **Topic Filters**: SQL-like filters for message routing
- **Subscription Rules**: Route messages based on properties
- **Multiple Subscriptions**: Multiple consumers per topic

## Configuration Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `provider` | string | Service provider ("aws_sqs", "azure_bus") | "" |
| `connection_string` | string | Azure Service Bus connection string | "" |
| `aws_region` | string | AWS region for SQS | "us-east-1" |
| `aws_access_key_id` | string | AWS access key ID | "" |
| `aws_secret_access_key` | string | AWS secret access key | "" |
| `aws_session_token` | string | AWS session token (optional) | "" |
| `default_timeout` | int | Default timeout in seconds | 30 |
| `max_retries` | int | Maximum retry attempts | 3 |
| `visibility_timeout` | int | Default visibility timeout in seconds | 30 |
| `wait_time_seconds` | int | Long polling wait time | 0 |
| `max_messages` | int | Maximum messages per receive operation | 10 |

## Starlark API Design

### Core Functions

#### `send(queue_name, message, **kwargs)`

Send a message to a queue or topic.

**Parameters:**

- `queue_name` (string, required): Name of the queue or topic
- `message` (string, required): Message content
- `attributes` (dict, optional): Message attributes/properties
- `delay_seconds` (int, optional): Delay before message becomes available
- `message_group_id` (string, optional): Message group ID for FIFO queues
- `message_deduplication_id` (string, optional): Deduplication ID for FIFO queues
- `scheduled_time` (time, optional): Schedule message for future delivery (Azure)
- `session_id` (string, optional): Session ID for Azure Service Bus sessions
- `content_type` (string, optional): Content type of the message
- `correlation_id` (string, optional): Correlation ID for message tracking
- `reply_to` (string, optional): Reply-to queue/topic name
- `time_to_live` (int, optional): Message time-to-live in seconds

**Returns:** Message ID or batch results

**Example:**

```python
load("mq", "send")

# Send a simple message
message_id = send(
    queue_name="my-queue",
    message="Hello, World!",
    attributes={"priority": "high", "type": "notification"}
)

# Send a scheduled message (Azure Service Bus)
scheduled_id = send(
    queue_name="my-topic",
    message="Scheduled notification",
    scheduled_time=time.now().add(hours=1),
    attributes={"category": "reminder"}
)

# Send to FIFO queue (AWS SQS)
fifo_id = send(
    queue_name="my-queue.fifo",
    message="Ordered message",
    message_group_id="group1",
    message_deduplication_id="unique-123"
)
```

#### `receive(queue_name, **kwargs)`

Receive messages from a queue or subscription.

**Parameters:**

- `queue_name` (string, required): Name of the queue or subscription
- `max_messages` (int, optional): Maximum number of messages to receive
- `wait_time_seconds` (int, optional): Long polling wait time
- `visibility_timeout` (int, optional): Visibility timeout for received messages
- `peek_only` (bool, optional): Peek without removing messages
- `session_id` (string, optional): Session ID for Azure Service Bus sessions

**Returns:** List of message objects

**Example:**

```python
load("mq", "receive")

# Receive messages with long polling
messages = receive(
    queue_name="my-queue",
    max_messages=5,
    wait_time_seconds=20,
    visibility_timeout=60
)

for msg in messages:
    print("Message ID:", msg.id)
    print("Content:", msg.body)
    print("Attributes:", msg.attributes)
    print("Received:", msg.received_time)
```

#### `delete(queue_name, receipt_handle)`

Delete a message from the queue after processing.

**Parameters:**

- `queue_name` (string, required): Name of the queue
- `receipt_handle` (string, required): Receipt handle from received message

**Returns:** Boolean indicating success

**Example:**

```python
load("mq", "receive", "delete")

messages = receive("my-queue")
for msg in messages:
    # Process message
    print("Processing:", msg.body)
    
    # Delete after successful processing
    if delete("my-queue", msg.receipt_handle):
        print("Message deleted successfully")
```

#### `send_batch(queue_name, messages, **kwargs)`

Send multiple messages in a single batch operation.

**Parameters:**

- `queue_name` (string, required): Name of the queue or topic
- `messages` (list, required): List of message objects or strings
- `attributes` (dict, optional): Default attributes for all messages

**Returns:** Batch results with successful and failed messages

**Example:**

```python
load("mq", "send_batch")

# Send batch of messages
messages = [
    {"body": "Message 1", "attributes": {"priority": "high"}},
    {"body": "Message 2", "attributes": {"priority": "low"}},
    "Simple message 3"
]

results = send_batch("my-queue", messages)
print("Successful:", len(results.successful))
print("Failed:", len(results.failed))
```

### Queue Management Functions

#### `create_queue(queue_name, **kwargs)`

Create a new queue with specified properties.

**Parameters:**

- `queue_name` (string, required): Name of the queue to create
- `fifo` (bool, optional): Create FIFO queue (AWS SQS)
- `visibility_timeout` (int, optional): Default visibility timeout
- `message_retention_period` (int, optional): Message retention period in seconds
- `max_message_size` (int, optional): Maximum message size in bytes
- `dead_letter_queue` (string, optional): Dead letter queue name
- `max_receive_count` (int, optional): Max receive count for dead letter queue
- `duplicate_detection` (bool, optional): Enable duplicate detection (Azure)
- `requires_session` (bool, optional): Require sessions (Azure)

**Returns:** Queue URL or details

**Example:**

```python
load("mq", "create_queue")

# Create a standard queue
queue_url = create_queue(
    queue_name="my-processing-queue",
    visibility_timeout=60,
    message_retention_period=1209600,  # 14 days
    dead_letter_queue="my-dlq",
    max_receive_count=3
)

# Create a FIFO queue (AWS SQS)
fifo_url = create_queue(
    queue_name="my-ordered-queue.fifo",
    fifo=True,
    visibility_timeout=30
)
```

#### `delete_queue(queue_name)`

Delete a queue and all its messages.

**Parameters:**

- `queue_name` (string, required): Name of the queue to delete

**Returns:** Boolean indicating success

#### `list_queues(prefix="")`

List all queues, optionally filtered by prefix.

**Parameters:**

- `prefix` (string, optional): Queue name prefix filter

**Returns:** List of queue names

**Example:**

```python
load("mq", "list_queues")

# List all queues
all_queues = list_queues()
print("All queues:", all_queues)

# List queues with prefix
app_queues = list_queues(prefix="myapp-")
print("App queues:", app_queues)
```

#### `get_queue_attributes(queue_name)`

Get queue properties and statistics.

**Parameters:**

- `queue_name` (string, required): Name of the queue

**Returns:** Dictionary with queue attributes

**Example:**

```python
load("mq", "get_queue_attributes")

attrs = get_queue_attributes("my-queue")
print("Messages available:", attrs.approximate_number_of_messages)
print("Messages in flight:", attrs.approximate_number_of_messages_not_visible)
print("Created time:", attrs.created_timestamp)
```

### Topic Management Functions (Azure Service Bus)

#### `create_topic(topic_name, **kwargs)`

Create a new topic for publish/subscribe messaging.

**Parameters:**

- `topic_name` (string, required): Name of the topic to create
- `max_size_in_mb` (int, optional): Maximum topic size in megabytes
- `duplicate_detection` (bool, optional): Enable duplicate detection
- `requires_session` (bool, optional): Require sessions

**Returns:** Topic details

#### `create_subscription(topic_name, subscription_name, **kwargs)`

Create a subscription to a topic.

**Parameters:**

- `topic_name` (string, required): Name of the topic
- `subscription_name` (string, required): Name of the subscription
- `max_delivery_count` (int, optional): Maximum delivery attempts
- `filter_expression` (string, optional): SQL filter expression
- `requires_session` (bool, optional): Require sessions

**Returns:** Subscription details

**Example:**

```python
load("mq", "create_topic", "create_subscription")

# Create topic
create_topic("notifications", max_size_in_mb=1024)

# Create subscription with filter
create_subscription(
    topic_name="notifications",
    subscription_name="high-priority",
    filter_expression="priority = 'high'"
)

# Create subscription for all messages
create_subscription(
    topic_name="notifications",
    subscription_name="all-messages"
)
```

### Utility Functions

#### `peek(queue_name, **kwargs)`

Peek at messages without removing them from the queue.

**Parameters:**

- `queue_name` (string, required): Name of the queue
- `max_messages` (int, optional): Maximum number of messages to peek

**Returns:** List of message objects (without receipt handles)

#### `purge_queue(queue_name)`

Delete all messages in a queue.

**Parameters:**

- `queue_name` (string, required): Name of the queue to purge

**Returns:** Boolean indicating success

#### `change_message_visibility(queue_name, receipt_handle, visibility_timeout)`

Change the visibility timeout of a message.

**Parameters:**

- `queue_name` (string, required): Name of the queue
- `receipt_handle` (string, required): Receipt handle of the message
- `visibility_timeout` (int, required): New visibility timeout in seconds

**Returns:** Boolean indicating success

## Data Structures

### Message Object

```python
{
    "id": "message-id-123",
    "body": "Message content",
    "attributes": {"key": "value"},
    "receipt_handle": "receipt-handle-456",
    "received_time": time.now(),
    "sent_time": time.now(),
    "receive_count": 1,
    "md5_of_body": "hash-value",
    "message_group_id": "group1",  # FIFO queues only
    "message_deduplication_id": "dedup-123",  # FIFO queues only
    "session_id": "session-123",  # Azure Service Bus sessions
    "correlation_id": "corr-123",
    "reply_to": "reply-queue",
    "content_type": "text/plain",
    "time_to_live": 3600
}
```

### Batch Results Object

```python
{
    "successful": [
        {"id": "msg-1", "message_id": "aws-msg-id-1"},
        {"id": "msg-2", "message_id": "aws-msg-id-2"}
    ],
    "failed": [
        {"id": "msg-3", "error_code": "InvalidMessageContents", "error_message": "Message too large"}
    ]
}
```

### Queue Attributes Object

```python
{
    "queue_arn": "arn:aws:sqs:us-east-1:123456789012:my-queue",
    "approximate_number_of_messages": 5,
    "approximate_number_of_messages_not_visible": 2,
    "approximate_number_of_messages_delayed": 0,
    "created_timestamp": time.now(),
    "last_modified_timestamp": time.now(),
    "visibility_timeout": 30,
    "message_retention_period": 1209600,
    "max_message_size": 262144,
    "delay_seconds": 0,
    "dead_letter_target_arn": "arn:aws:sqs:us-east-1:123456789012:my-dlq",
    "max_receive_count": 3
}
```

## Implementation Plan

### Phase 1: Core Infrastructure

1. **Base Module Setup**
   - Create module structure using `base` package
   - Define configuration options
   - Set up error handling types
   - Implement module loading and initialization

2. **AWS SQS Client**
   - Implement AWS SQS client wrapper
   - Handle authentication (IAM roles, access keys)
   - Implement basic queue operations
   - Add error handling and retries

3. **Azure Service Bus Client**
   - Implement Azure Service Bus client wrapper
   - Handle authentication (connection strings, managed identity)
   - Implement basic queue operations
   - Add error handling and retries

### Phase 2: Core Operations

1. **Message Operations**
   - Implement `send()` function
   - Implement `receive()` function
   - Implement `delete()` function
   - Add batch operations (`send_batch()`)

2. **Queue Management**
   - Implement `create_queue()` function
   - Implement `delete_queue()` function
   - Implement `list_queues()` function
   - Implement `get_queue_attributes()` function

### Phase 3: Advanced Features

1. **Dead Letter Queues**
   - Configure dead letter queue settings
   - Handle message redrive policies
   - Add dead letter queue monitoring

2. **Scheduled Messages**
   - Implement message scheduling (Azure Service Bus)
   - Add delay seconds support (AWS SQS)
   - Handle scheduled message management

3. **Message Sessions**
   - Implement session-based messaging (Azure Service Bus)
   - Add session management functions
   - Handle session locking and unlocking

### Phase 4: Topics and Subscriptions (Azure Service Bus)

1. **Topic Management**
   - Implement `create_topic()` function
   - Implement `delete_topic()` function
   - Implement `list_topics()` function

2. **Subscription Management**
   - Implement `create_subscription()` function
   - Implement `delete_subscription()` function
   - Implement subscription filtering

### Phase 5: Utility Functions

1. **Additional Operations**
   - Implement `peek()` function
   - Implement `purge_queue()` function
   - Implement `change_message_visibility()` function

2. **Monitoring and Metrics**
   - Add queue statistics retrieval
   - Implement health checks
   - Add performance metrics

### Phase 6: Testing and Documentation

1. **Unit Tests**
   - Test all functions with mocked services
   - Test error handling scenarios
   - Test configuration variations

2. **Integration Tests**
   - Test with real AWS SQS (using localstack)
   - Test with Azure Service Bus emulator
   - Test cross-service scenarios

3. **Documentation**
   - Complete README.md with examples
   - Add godoc documentation
   - Create usage examples

## File Structure

```
mq/
├── PLAN.md                 # This file
├── README.md              # Module documentation
├── LICENSE                # MIT License
├── go.mod                 # Go module file
├── go.sum                 # Go module checksums
├── mq.go                  # Main module implementation
├── mq_test.go            # Unit tests
├── example_test.go       # Example tests
├── aws_sqs.go            # AWS SQS implementation
├── azure_servicebus.go   # Azure Service Bus implementation
├── types.go              # Common types and structures
├── errors.go             # Error definitions
└── internal/
    ├── aws/
    │   ├── sqs_client.go     # AWS SQS client wrapper
    │   └── sqs_client_test.go
    └── azure/
        ├── servicebus_client.go     # Azure Service Bus client wrapper
        └── servicebus_client_test.go
```

## Dependencies

### Go Dependencies

- `github.com/aws/aws-sdk-go-v2/service/sqs` - AWS SQS SDK
- `github.com/aws/aws-sdk-go-v2/config` - AWS configuration
- `github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus` - Azure Service Bus SDK
- `github.com/Azure/azure-sdk-for-go/sdk/azidentity` - Azure authentication
- `github.com/starpkg/base` - Base configuration package
- `github.com/1set/starlet` - Starlark runtime
- `go.starlark.net/starlark` - Starlark language support

### Testing Dependencies

- `github.com/stretchr/testify` - Testing utilities
- `github.com/testcontainers/testcontainers-go` - Integration testing

## Error Handling

### Error Types

- `ErrInvalidProvider` - Invalid message queue provider
- `ErrConnectionFailed` - Failed to connect to service
- `ErrQueueNotFound` - Queue or topic not found
- `ErrMessageTooLarge` - Message exceeds size limit
- `ErrInvalidMessageFormat` - Invalid message format
- `ErrAccessDenied` - Insufficient permissions
- `ErrRateLimitExceeded` - Rate limit exceeded
- `ErrTimeout` - Operation timeout

### Retry Logic

- Exponential backoff for transient errors
- Configurable retry count and delays
- Different retry strategies for different error types
- Circuit breaker pattern for persistent failures

## Security Considerations

### Authentication

- **AWS SQS**: IAM roles, access keys, session tokens
- **Azure Service Bus**: Connection strings, managed identity, service principal

### Encryption

- Support for encryption in transit (TLS)
- Support for encryption at rest (service-managed keys)
- Message-level encryption for sensitive data

### Access Control

- Proper IAM/RBAC permission validation
- Principle of least privilege
- Secure credential storage and rotation

## Performance Considerations

### Batch Operations

- Efficient batch sending and receiving
- Optimal batch sizes for different services
- Parallel processing for large batches

### Connection Pooling

- Reuse connections across operations
- Connection health monitoring
- Automatic connection recovery

### Caching

- Cache queue URLs and metadata
- Cache authentication tokens
- Invalidate cache on errors

## Monitoring and Observability

### Metrics

- Message throughput (sent/received per second)
- Queue depth and age
- Error rates and types
- Operation latency

### Logging

- Structured logging with context
- Configurable log levels
- Sensitive data redaction

### Health Checks

- Service connectivity checks
- Queue availability checks
- Performance threshold monitoring

## Examples

### Basic Usage

```python
load("mq", "send", "receive", "delete")

# Configure the service
mq.set_provider("aws_sqs")
mq.set_aws_region("us-west-2")

# Send a message
msg_id = send("my-queue", "Hello, World!")
print("Sent message:", msg_id)

# Receive messages
messages = receive("my-queue", max_messages=10)
for msg in messages:
    print("Received:", msg.body)
    # Process message...
    delete("my-queue", msg.receipt_handle)
```

### Batch Operations

```python
load("mq", "send_batch")

# Send multiple messages
messages = [
    {"body": "Message 1", "attributes": {"type": "order"}},
    {"body": "Message 2", "attributes": {"type": "notification"}},
    {"body": "Message 3", "attributes": {"type": "alert"}}
]

results = send_batch("my-queue", messages)
print("Sent {} messages, {} failed".format(
    len(results.successful), 
    len(results.failed)
))
```

### Azure Service Bus Topics

```python
load("mq", "create_topic", "create_subscription", "send", "receive")

# Configure Azure Service Bus
mq.set_provider("azure_bus")
mq.set_connection_string("Endpoint=sb://...")

# Create topic and subscriptions
create_topic("notifications")
create_subscription("notifications", "high-priority", 
                   filter_expression="priority = 'high'")
create_subscription("notifications", "all-messages")

# Send to topic
send("notifications", "Important update!", 
     attributes={"priority": "high", "category": "system"})

# Receive from subscription
messages = receive("notifications/subscriptions/high-priority")
for msg in messages:
    print("High priority message:", msg.body)
```

### Error Handling

```python
load("mq", "send", "receive")

def safe_send(queue, message):
    try:
        return send(queue, message)
    except Exception as e:
        print("Failed to send message:", e)
        return None

def process_queue(queue_name):
    messages = receive(queue_name, max_messages=10)
    for msg in messages:
        try:
            # Process message
            process_message(msg.body)
            delete(queue_name, msg.receipt_handle)
        except Exception as e:
            print("Failed to process message:", e)
            # Message will become visible again after visibility timeout
```

## Testing Strategy

### Unit Tests

- Test each function with mocked services
- Test error handling and edge cases
- Test configuration validation
- Test message serialization/deserialization

### Integration Tests

- Use localstack for AWS SQS testing
- Use Azure Service Bus emulator for Azure testing
- Test cross-service compatibility
- Test performance under load

### Example Tests

- Comprehensive examples in `example_test.go`
- Real-world usage scenarios
- Best practices demonstration

This comprehensive plan provides a solid foundation for implementing the MQ module with support for both AWS SQS and Azure Service Bus, following the established patterns from existing modules in the starpkg project.
