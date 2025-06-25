# 📨 mq - Unified Message Queue Interface for Starlark

**Module Name**: `mq`  
**Emoji**: 📨  
**Description**: Unified interface for AWS SQS and Azure Service Bus message queue operations  
**Tagline**: "Queue it up, deliver it right - unified message queue operations made simple"

## Executive Summary

The `mq` module provides Starlark scripts with a unified, high-performance interface for interacting with cloud-based message queue services. By abstracting the complexities of AWS SQS and Azure Service Bus behind a consistent API, developers can build portable message-driven applications without vendor lock-in.

This module addresses the growing need for reliable, scalable message processing in distributed systems while maintaining the simplicity and safety that Starlark provides. Whether you're building event-driven architectures, implementing work queues, or managing pub/sub messaging patterns, the `mq` module delivers enterprise-grade messaging capabilities with minimal configuration overhead.

Key differentiators include automatic retry handling, unified dead letter queue management, cross-platform message attribute handling, and built-in support for both point-to-point and publish/subscribe messaging patterns. The module leverages Go's concurrency features for optimal performance while maintaining Starlark's deterministic execution model.

## Core Design Principles

1. **Unified Interface Design**: Provide a consistent API that abstracts service-specific differences while preserving access to unique features when needed
2. **Performance-First Architecture**: Leverage Go routines and connection pooling for high-throughput scenarios while maintaining thread-safe operations
3. **Security by Default**: All credentials and sensitive configuration values are handled securely using the base package's secret management capabilities
4. **Resilient Operations**: Built-in retry logic, exponential backoff, and comprehensive error handling ensure reliable message delivery
5. **Flexible Message Routing**: Support both simple queue operations and advanced topic-based routing with subscription management
6. **Resource Management**: Automatic connection lifecycle management with proper cleanup and resource disposal

## Starlark Constraints & Adaptations

- **No Classes → Service Factory Pattern**: Use `connect()` function to create service-specific clients with method-like behavior through closures
- **No f-strings → Format Method**: All string formatting uses `"template {}".format(value)` instead of f-string syntax
- **No try/except → fail() Function**: Error conditions terminate script execution with descriptive messages via `fail()`
- **No is/is not → Equality Comparison**: Use `== None` and `!= None` for null checks instead of identity comparisons
- **No While Loops → For Range**: Use `range()` with for loops for retry logic and batch operations
- **Immutable After Load → Configuration at Connect**: All service configuration happens during connection establishment

## API Design

### Service Compatibility Overview

The `mq` module provides a unified interface, but not all features are available on both services. The tables below show exactly what's supported where.

#### Core Features Compatibility Matrix

| Feature Category | AWS SQS | Azure Service Bus | Notes |
|------------------|---------|-------------------|-------|
| **Basic Queues** | ✅ | ✅ | Full support on both |
| **FIFO Queues** | ✅ | ❌ | SQS only - use Sessions in Azure |
| **Topics/Subscriptions** | ❌ | ✅ | Azure Service Bus only |
| **Dead Letter Queues** | ✅ | ✅ | Different configuration methods |
| **Message Attributes** | ✅ | ✅ | Different limits and types |
| **Batch Operations** | ✅ (max 10) | ✅ (max 100) | Different batch sizes |
| **Long Polling** | ✅ | ✅ | Different parameter names |
| **Delayed Messages** | ✅ | ✅ | Different mechanisms |
| **Message Sessions** | ❌ | ✅ | Azure Service Bus only |
| **Duplicate Detection** | ✅ (FIFO only) | ✅ | Different implementations |

### Core Module Functions

#### Connection Management

```python
# Primary connection function - auto-detects service type
connect(
    service_type="auto",     # "aws_sqs", "azure_servicebus", "auto"
    connection_string=None,  # Azure Service Bus connection string
    aws_region=None,         # AWS region for SQS
    aws_access_key=None,     # AWS access key ID
    aws_secret_key=None,     # AWS secret access key
    timeout=30,              # Connection timeout in seconds
    max_retries=3            # Maximum retry attempts
) -> Client

# Utility functions
get_supported_services() -> list    # Returns ["aws_sqs", "azure_servicebus"]
get_client_info(client) -> dict    # Returns client connection details
```

### Client Object API

#### Queue Operations

```python
# Queue management
create_queue(name, **options) -> Queue
delete_queue(name) -> bool
list_queues(prefix="") -> list
get_queue(name) -> Queue
queue_exists(name) -> bool

# Queue information
get_queue_attributes(name) -> dict
set_queue_attributes(name, attributes) -> bool
get_queue_url(name) -> string  # AWS SQS specific
```

#### Message Operations

```python
# Single message operations
send_message(queue_name, body, **options) -> MessageResult
receive_messages(queue_name, max_count=1, **options) -> list
delete_message(queue_name, message_id, **options) -> bool
peek_messages(queue_name, max_count=1) -> list

# Batch operations for performance
send_messages(queue_name, messages) -> list  # List of MessageResult
delete_messages(queue_name, message_ids) -> list  # List of success/failure

# Message visibility management
change_message_visibility(queue_name, message_id, timeout) -> bool
extend_message_visibility(queue_name, message_id, timeout) -> bool
```

#### Advanced Message Features

```python
# Scheduled/delayed messages
schedule_message(queue_name, body, delay_seconds, **options) -> MessageResult
cancel_scheduled_message(queue_name, message_id) -> bool

# Dead letter queue operations
get_dead_letter_messages(queue_name, max_count=10) -> list
requeue_dead_letter_message(queue_name, message_id) -> bool
purge_dead_letter_queue(queue_name) -> bool
```

#### Topic Operations (Azure Service Bus)

```python
# Topic management
create_topic(name, **options) -> Topic
delete_topic(name) -> bool
list_topics(prefix="") -> list
topic_exists(name) -> bool

# Subscription management
create_subscription(topic_name, subscription_name, **options) -> Subscription
delete_subscription(topic_name, subscription_name) -> bool
list_subscriptions(topic_name) -> list

# Publish/Subscribe operations
publish_message(topic_name, body, **options) -> MessageResult
subscribe_messages(topic_name, subscription_name, max_count=1, **options) -> list
```

#### Message Filtering and Routing

```python
# Message attributes and properties
set_message_attributes(message, attributes) -> Message
get_message_attributes(message) -> dict
add_message_property(message, key, value) -> Message

# Subscription rules (Azure Service Bus)
add_subscription_rule(topic_name, subscription_name, rule_name, filter_expression) -> bool
remove_subscription_rule(topic_name, subscription_name, rule_name) -> bool
list_subscription_rules(topic_name, subscription_name) -> list
```

### Function Support by Service

#### Queue Operations Compatibility

| Function | AWS SQS | Azure Service Bus | AWS Notes | Azure Notes |
|----------|---------|-------------------|-----------|-------------|
| `create_queue()` | ✅ | ✅ | - | - |
| `delete_queue()` | ✅ | ✅ | - | - |
| `list_queues()` | ✅ | ✅ | - | - |
| `get_queue()` | ✅ | ✅ | - | - |
| `queue_exists()` | ✅ | ✅ | - | - |
| `get_queue_attributes()` | ✅ | ✅ | - | Different attribute names |
| `set_queue_attributes()` | ✅ | ✅ | - | Limited attributes |
| `get_queue_url()` | ✅ | ❌ | Returns SQS URL | Not applicable |

#### Message Operations Compatibility

| Function | AWS SQS | Azure Service Bus | AWS Limits | Azure Limits |
|----------|---------|-------------------|------------|--------------|
| `send_message()` | ✅ | ✅ | 256KB body | 256KB body (1MB Premium) |
| `receive_messages()` | ✅ | ✅ | max_count ≤ 10 | max_count ≤ 32 |
| `delete_message()` | ✅ | ✅ | - | - |
| `peek_messages()` | ❌ | ✅ | Not supported | Supported |
| `send_messages()` | ✅ | ✅ | max 10 messages | max 100 messages |
| `delete_messages()` | ✅ | ✅ | max 10 messages | max 100 messages |
| `change_message_visibility()` | ✅ | ❌ | 12 hours max | Use locks instead |
| `extend_message_visibility()` | ✅ | ❌ | 12 hours max | Use locks instead |

#### Advanced Message Features Compatibility

| Function | AWS SQS | Azure Service Bus | AWS Implementation | Azure Implementation |
|----------|---------|-------------------|-------------------|---------------------|
| `schedule_message()` | ✅ | ✅ | DelaySeconds (≤15 min) | ScheduledEnqueueTime |
| `cancel_scheduled_message()` | ❌ | ✅ | Not supported | Supported |
| `get_dead_letter_messages()` | ✅ | ✅ | Separate DLQ | Built-in DLQ |
| `requeue_dead_letter_message()` | ✅ | ✅ | Manual process | Built-in feature |
| `purge_dead_letter_queue()` | ✅ | ✅ | - | - |

#### Topic Operations Compatibility

| Function | AWS SQS | Azure Service Bus | Notes |
|----------|---------|-------------------|-------|
| `create_topic()` | ❌ | ✅ | Use SNS separately | Native support |
| `delete_topic()` | ❌ | ✅ | Use SNS separately | Native support |
| `list_topics()` | ❌ | ✅ | Use SNS separately | Native support |
| `topic_exists()` | ❌ | ✅ | Use SNS separately | Native support |
| `create_subscription()` | ❌ | ✅ | Use SNS separately | Native support |
| `delete_subscription()` | ❌ | ✅ | Use SNS separately | Native support |
| `list_subscriptions()` | ❌ | ✅ | Use SNS separately | Native support |
| `publish_message()` | ❌ | ✅ | Use SNS separately | Native support |
| `subscribe_messages()` | ❌ | ✅ | Use SNS separately | Native support |

### Parameter and Option Support

#### Queue Creation Options

| Option | AWS SQS | Azure Service Bus | AWS Values | Azure Values |
|--------|---------|-------------------|------------|--------------|
| `visibility_timeout` | ✅ | ❌ | 0-43200 seconds | Use LockDuration |
| `lock_duration` | ❌ | ✅ | Not applicable | 5 seconds - 5 minutes |
| `message_retention_period` | ✅ | ✅ | 60s - 1209600s | 1 minute - 14 days |
| `max_delivery_count` | ✅ | ✅ | 1-1000 | 1-2000 |
| `dead_letter_queue` | ✅ | ✅ | Queue ARN | Queue name |
| `fifo_queue` | ✅ | ❌ | true/false | Use Sessions |
| `content_based_deduplication` | ✅ | ✅ | FIFO only | Native support |
| `duplicate_detection_window` | ✅ | ✅ | FIFO only | 20 seconds - 7 days |
| `max_size` | ❌ | ✅ | Not configurable | 1GB - 80GB |
| `enable_batched_operations` | ❌ | ✅ | Always enabled | true/false |

#### Send Message Options

| Option | AWS SQS | Azure Service Bus | AWS Format | Azure Format |
|--------|---------|-------------------|------------|--------------|
| `attributes` | ✅ | ✅ | Key-value pairs | Properties |
| `delay_seconds` | ✅ | ❌ | 0-900 seconds | Use scheduled_enqueue_time |
| `scheduled_enqueue_time` | ❌ | ✅ | Not supported | ISO 8601 timestamp |
| `message_group_id` | ✅ | ❌ | FIFO queues only | Use SessionId |
| `session_id` | ❌ | ✅ | Not supported | String |
| `correlation_id` | ❌ | ✅ | Not supported | String |
| `reply_to` | ❌ | ✅ | Not supported | String |
| `time_to_live` | ❌ | ✅ | Not supported | Duration |
| `message_deduplication_id` | ✅ | ❌ | FIFO queues only | Use MessageId |

#### Receive Message Options

| Option | AWS SQS | Azure Service Bus | AWS Format | Azure Format |
|--------|---------|-------------------|------------|--------------|
| `max_count` | ✅ | ✅ | 1-10 | 1-32 |
| `wait_time` | ✅ | ✅ | 0-20 seconds | 0-60 seconds |
| `visibility_timeout` | ✅ | ❌ | 0-43200 seconds | Use lock_duration |
| `receive_mode` | ❌ | ✅ | Always peek_lock | peek_lock/receive_delete |
| `include_dead_letter` | ✅ | ✅ | Separate queue | Built-in flag |

### Service-Specific Limitations

#### AWS SQS Limitations

| Limitation | Description | Workaround |
|------------|-------------|------------|
| **No Topics** | SQS doesn't support pub/sub natively | Use Amazon SNS + SQS |
| **Batch Size** | Maximum 10 messages per batch | Multiple batch calls |
| **Message Size** | 256KB maximum | Use S3 for large messages |
| **Delay Limit** | Maximum 15 minutes delay | Use CloudWatch Events |
| **Visibility Timeout** | Maximum 12 hours | Design for shorter processing |
| **No Peek** | Cannot peek without receiving | Receive and re-queue |
| **FIFO Throughput** | 300 TPS (3000 with batching) | Use multiple queues |

#### Azure Service Bus Limitations

| Limitation | Description | Workaround |
|------------|-------------|------------|
| **No FIFO Queues** | No dedicated FIFO queue type | Use Sessions for ordering |
| **Premium Tier Required** | Some features need Premium | Plan tier accordingly |
| **Connection Limits** | Connection string based auth | Use managed identity |
| **Message Size** | 256KB (1MB Premium) | Use blob storage |
| **Session Concurrency** | One processor per session | Multiple sessions |
| **Subscription Limits** | 2000 subscriptions per topic | Design topic hierarchy |

### Error Handling Differences

#### Common Error Scenarios

| Error Type | AWS SQS Response | Azure Service Bus Response | Unified Handling |
|------------|------------------|----------------------------|------------------|
| **Queue Not Found** | `QueueDoesNotExist` | `MessagingEntityNotFound` | `fail("Queue not found")` |
| **Message Too Large** | `InvalidParameterValue` | `MessageSizeExceeded` | `fail("Message too large")` |
| **Access Denied** | `AccessDeniedError` | `UnauthorizedAccessException` | `fail("Access denied")` |
| **Throttling** | `RequestThrottled` | `ServerBusyException` | Automatic retry with backoff |
| **Malformed Request** | `InvalidParameterValue` | `ArgumentException` | `fail("Invalid parameters")` |
| **Service Unavailable** | `ServiceUnavailable` | `ServiceBusyException` | Automatic retry with backoff |

### Migration and Compatibility Guide

#### AWS SQS to Azure Service Bus

| SQS Feature | Azure Equivalent | Migration Notes |
|-------------|------------------|-----------------|
| Standard Queue | Queue | Direct migration |
| FIFO Queue | Queue with Sessions | Requires code changes |
| Message Groups | Sessions | Similar functionality |
| DelaySeconds | ScheduledEnqueueTime | Different time format |
| VisibilityTimeout | LockDuration | Different ranges |
| Dead Letter Queue | Dead Letter Subqueue | Different configuration |

#### Azure Service Bus to AWS SQS

| Azure Feature | AWS Equivalent | Migration Notes |
|---------------|----------------|-----------------|
| Queue | Standard Queue | Direct migration |
| Topic | SNS + SQS | Requires architecture change |
| Subscription | SQS Queue | Manual fan-out needed |
| Sessions | FIFO Message Groups | Limited functionality |
| Scheduled Messages | DelaySeconds (limited) | Time restrictions apply |

### Data Structures

#### MessageResult

```python
{
    "message_id": "unique-message-identifier",
    "body": "message content",
    "attributes": {"key": "value"},
    "properties": {"system_property": "value"},
    "receipt_handle": "service-specific-handle",
    "enqueue_time": "2024-01-01T12:00:00Z",
    "delivery_count": 1,
    "expires_at": "2024-01-01T13:00:00Z",
    "success": True,
    "error": None
}
```

#### Queue

```python
{
    "name": "queue-name",
    "url": "service-specific-url",
    "message_count": 42,
    "visibility_timeout": 30,
    "max_delivery_count": 10,
    "dead_letter_queue": "dlq-name",
    "created_time": "2024-01-01T10:00:00Z",
    "attributes": {"custom": "values"}
}
```

## Configuration System

The module integrates with the base package configuration system:

```go
// Primary service configuration
ServiceType        *ConfigOption[string] // "aws_sqs", "azure_servicebus", "auto"
ConnectionString   *ConfigOption[string] // Azure Service Bus connection string
Timeout           *ConfigOption[int]    // Connection timeout in seconds
MaxRetries        *ConfigOption[int]    // Maximum retry attempts

// AWS SQS Configuration
AWSRegion         *ConfigOption[string] // AWS region
AWSAccessKey      *ConfigOption[string] // AWS access key ID (secret)
AWSSecretKey      *ConfigOption[string] // AWS secret key (secret)
AWSSessionToken   *ConfigOption[string] // AWS session token (secret)

// Azure Service Bus Configuration
AzureNamespace    *ConfigOption[string] // Service Bus namespace
AzureSharedKey    *ConfigOption[string] // Shared access key (secret)
AzureKeyName      *ConfigOption[string] // Shared access key name

// Performance and Behavior
DefaultVisibilityTimeout *ConfigOption[int] // Default message visibility timeout
DefaultBatchSize         *ConfigOption[int] // Default batch size for operations
EnableDeadLetterQueue    *ConfigOption[bool] // Enable dead letter queue support
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MQ_SERVICE_TYPE` | Service type (aws_sqs, azure_servicebus) | auto |
| `MQ_CONNECTION_STRING` | Azure Service Bus connection string | - |
| `MQ_AWS_REGION` | AWS region for SQS | us-east-1 |
| `AWS_ACCESS_KEY_ID` | AWS access key ID | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key | - |
| `AWS_SESSION_TOKEN` | AWS session token (optional) | - |
| `MQ_TIMEOUT` | Connection timeout in seconds | 30 |
| `MQ_MAX_RETRIES` | Maximum retry attempts | 3 |

## Complete Usage Examples

### Basic Queue Operations

```python
load("mq", "connect")

def main():
    # Connect to AWS SQS
    client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2"
    )
    
    # Create a queue
    queue = client.create_queue("my-work-queue", {
        "visibility_timeout": 60,
        "max_delivery_count": 5,
        "dead_letter_queue": "my-dlq"
    })
    
    if queue == None:
        fail("Failed to create queue")
    
    # Send a message
    result = client.send_message("my-work-queue", "Hello, World!", {
        "attributes": {
            "priority": "high",
            "sender": "worker-1"
        }
    })
    
    print("Message sent with ID: {}".format(result.message_id))
    
    # Receive messages
    messages = client.receive_messages("my-work-queue", max_count=5, {
        "wait_time": 20,  # Long polling
        "visibility_timeout": 30
    })
    
    for message in messages:
        print("Processing message: {}".format(message.body))
        
        # Process the message...
        success = process_message(message)
        
        if success:
            # Delete message after successful processing
            client.delete_message("my-work-queue", message.message_id)
        else:
            # Let it become visible again for retry
            print("Message processing failed, will retry")

def process_message(message):
    # Simulate message processing
    print("Processing: {}".format(message.body))
    return True

main()
```

### Azure Service Bus with Topics

```python
load("mq", "connect")

def main():
    # Connect to Azure Service Bus
    client = connect(
        service_type="azure_servicebus",
        connection_string="Endpoint=sb://namespace.servicebus.windows.net/;..."
    )
    
    # Create topic and subscriptions
    topic = client.create_topic("order-events", {
        "max_size": "1GB",
        "ttl": 86400  # 24 hours
    })
    
    # Create subscriptions with filters
    client.create_subscription("order-events", "payment-processor", {
        "filter": "event_type = 'payment'"
    })
    
    client.create_subscription("order-events", "inventory-manager", {
        "filter": "event_type IN ('order_created', 'order_cancelled')"
    })
    
    # Publish messages
    events = [
        {"event_type": "payment", "order_id": "12345", "amount": 99.99},
        {"event_type": "order_created", "order_id": "12346", "items": 3},
        {"event_type": "payment", "order_id": "12346", "amount": 149.50}
    ]
    
    for event in events:
        result = client.publish_message("order-events", encode_json(event), {
            "properties": {
                "event_type": event["event_type"],
                "timestamp": get_current_time()
            }
        })
        print("Published event {} with ID: {}".format(event["event_type"], result.message_id))
    
    # Process subscription messages
    payment_messages = client.subscribe_messages("order-events", "payment-processor", max_count=10)
    for msg in payment_messages:
        print("Payment processor received: {}".format(msg.body))
        client.delete_message("order-events", msg.message_id, {"subscription": "payment-processor"})

def encode_json(obj):
    # Simple JSON encoding (would use json module in real scenario)
    return str(obj)

def get_current_time():
    return "2024-01-01T12:00:00Z"

main()
```

### Batch Operations for High Throughput

```python
load("mq", "connect")

def main():
    client = connect(service_type="aws_sqs")
    
    # Create batch of messages
    messages = []
    for i in range(100):
        messages.append({
            "body": "Batch message {}".format(i),
            "attributes": {
                "batch_id": "batch-001",
                "sequence": str(i)
            }
        })
    
    # Send messages in batches (SQS allows up to 10 per batch)
    batch_size = 10
    total_sent = 0
    
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        results = client.send_messages("batch-queue", batch)
        
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        total_sent += len(successful)
        
        if len(failed) > 0:
            print("Failed to send {} messages in batch {}".format(len(failed), i // batch_size))
            for failure in failed:
                print("Error: {}".format(failure.error))
    
    print("Successfully sent {} out of {} messages".format(total_sent, len(messages)))
    
    # Batch receive and process
    while True:
        messages = client.receive_messages("batch-queue", max_count=10, {
            "wait_time": 5  # Short polling for batch processing
        })
        
        if len(messages) == 0:
            break
        
        # Process messages
        processed_ids = []
        for msg in messages:
            if process_batch_message(msg):
                processed_ids.append(msg.message_id)
        
        # Batch delete successful messages
        if len(processed_ids) > 0:
            delete_results = client.delete_messages("batch-queue", processed_ids)
            successful_deletes = [r for r in delete_results if r]
            print("Successfully deleted {} messages".format(len(successful_deletes)))

def process_batch_message(message):
    # Simulate processing
    return True

main()
```

### Dead Letter Queue Management

```python
load("mq", "connect")

def main():
    client = connect()
    
    # Set up main queue with DLQ
    main_queue = client.create_queue("processing-queue", {
        "visibility_timeout": 30,
        "max_delivery_count": 3,
        "dead_letter_queue": "processing-dlq"
    })
    
    # Create the dead letter queue
    dlq = client.create_queue("processing-dlq")
    
    # Process main queue
    while True:
        messages = client.receive_messages("processing-queue", max_count=5)
        if len(messages) == 0:
            break
        
        for msg in messages:
            try:
                success = process_with_potential_failure(msg)
                if success:
                    client.delete_message("processing-queue", msg.message_id)
                else:
                    # Let it retry (will eventually go to DLQ)
                    print("Processing failed for message {}, will retry".format(msg.message_id))
            except Exception as e:
                print("Error processing message: {}".format(str(e)))
    
    # Handle dead letter messages
    dead_messages = client.get_dead_letter_messages("processing-queue", max_count=10)
    print("Found {} messages in dead letter queue".format(len(dead_messages)))
    
    for dead_msg in dead_messages:
        print("Dead letter message: {}".format(dead_msg.body))
        print("Delivery count: {}".format(dead_msg.delivery_count))
        
        # Decide what to do with dead letter messages
        if should_requeue(dead_msg):
            # Move back to main queue
            client.requeue_dead_letter_message("processing-queue", dead_msg.message_id)
            print("Requeued message {}".format(dead_msg.message_id))
        else:
            # Log and remove
            log_dead_message(dead_msg)
            # Message will be automatically removed from DLQ

def process_with_potential_failure(message):
    # Simulate processing that might fail
    import random
    return random.random() > 0.3  # 70% success rate

def should_requeue(message):
    # Simple logic for requeuing
    return message.delivery_count < 5

def log_dead_message(message):
    print("Logging dead letter message: {} - {}".format(message.message_id, message.body))

main()
```

### Multi-Service Configuration and Feature Detection

```python
load("mq", "connect", "get_supported_services", "get_client_info")

def main():
    # List supported services
    services = get_supported_services()
    print("Supported services: {}".format(services))
    
    # Connect to multiple services
    aws_client = connect(
        service_type="aws_sqs",
        aws_region="us-east-1",
        timeout=45
    )
    
    azure_client = connect(
        service_type="azure_servicebus",
        connection_string=get_env_var("AZURE_SERVICEBUS_CONNECTION")
    )
    
    # Get client information to detect capabilities
    aws_info = get_client_info(aws_client)
    azure_info = get_client_info(azure_client)
    
    print("AWS Client: {}".format(aws_info["service_type"]))
    print("Azure Client: {}".format(azure_info["service_type"]))
    
    # Demonstrate service-specific feature handling
    handle_service_differences(aws_client, azure_client)
    
    # Cross-service message forwarding with feature detection
    forward_messages_with_feature_detection(aws_client, azure_client)

def handle_service_differences(aws_client, azure_client):
    """Demonstrate handling of service-specific features"""
    
    # AWS SQS: Use visibility timeout (not available in Azure)
    messages = aws_client.receive_messages("test-queue", max_count=5, {
        "visibility_timeout": 60,  # AWS SQS specific
        "wait_time": 10           # Long polling
    })
    
    for msg in messages:
        # AWS: Change visibility timeout (extend processing time)
        aws_client.change_message_visibility("test-queue", msg.message_id, 120)
        
        # Process message...
        if process_message(msg):
            aws_client.delete_message("test-queue", msg.message_id)
    
    # Azure Service Bus: Use lock duration and peek (not available in AWS)
    try:
        # Peek messages without receiving them (Azure only)
        peeked = azure_client.peek_messages("test-queue", max_count=5)
        print("Peeked {} messages from Azure queue".format(len(peeked)))
        
        # Receive with lock duration (Azure equivalent of visibility timeout)
        azure_messages = azure_client.receive_messages("test-queue", max_count=5, {
            "wait_time": 30,     # Azure supports longer wait times
            "receive_mode": "peek_lock"
        })
        
        for msg in azure_messages:
            # Process message with session support (Azure only)
            if msg.session_id != None:
                print("Processing session message: {}".format(msg.session_id))
            
            if process_message(msg):
                azure_client.delete_message("test-queue", msg.message_id)
                
    except Exception as e:
        print("Azure-specific operation failed: {}".format(str(e)))

def forward_messages_with_feature_detection(source_client, dest_client):
    """Forward messages between services handling different capabilities"""
    
    # Get source client info to adapt behavior
    source_info = get_client_info(source_client)
    dest_info = get_client_info(dest_client)
    
    # Adjust batch size based on service capabilities
    if source_info["service_type"] == "aws_sqs":
        batch_size = 10  # AWS SQS limit
    else:
        batch_size = 32  # Azure Service Bus limit
    
    messages = source_client.receive_messages("source-queue", max_count=batch_size)
    
    for msg in messages:
        # Build message options based on destination service
        send_options = {
            "attributes": msg.attributes if msg.attributes else {}
        }
        
        # Handle service-specific message properties
        if dest_info["service_type"] == "azure_servicebus":
            # Azure Service Bus specific options
            if msg.correlation_id != None:
                send_options["correlation_id"] = msg.correlation_id
            if msg.session_id != None:
                send_options["session_id"] = msg.session_id
            # Use scheduled enqueue time for delays
            send_options["scheduled_enqueue_time"] = get_future_timestamp(300)  # 5 min delay
            
        elif dest_info["service_type"] == "aws_sqs":
            # AWS SQS specific options
            send_options["delay_seconds"] = 300  # 5 min delay (max 15 min)
            if msg.message_group_id != None:
                send_options["message_group_id"] = msg.message_group_id
        
        # Forward message with appropriate options
        result = dest_client.send_message("destination-queue", msg.body, send_options)
        
        if result.success:
            # Delete from source after successful forward
            source_client.delete_message("source-queue", msg.message_id)
            print("Forwarded {} -> {} (from {} to {})".format(
                msg.message_id, 
                result.message_id,
                source_info["service_type"],
                dest_info["service_type"]
            ))
        else:
            print("Failed to forward message: {}".format(result.error))

def handle_topic_operations():
    """Demonstrate Azure Service Bus topic operations (not available in AWS SQS)"""
    
    client = connect(service_type="azure_servicebus")
    client_info = get_client_info(client)
    
    # Check if topics are supported
    if client_info["service_type"] == "azure_servicebus":
        # Create topic with subscriptions
        topic = client.create_topic("order-events", {
            "max_size": "1GB",
            "duplicate_detection_window": 300  # 5 minutes
        })
        
        # Create filtered subscriptions
        client.create_subscription("order-events", "high-priority", {
            "filter": "priority = 'high'"
        })
        
        client.create_subscription("order-events", "payment-events", {
            "filter": "event_type = 'payment'"
        })
        
        # Publish messages with different properties
        events = [
            {"type": "order", "priority": "high", "id": "12345"},
            {"type": "payment", "priority": "normal", "id": "12346"}
        ]
        
        for event in events:
            result = client.publish_message("order-events", encode_json(event), {
                "properties": {
                    "event_type": event["type"],
                    "priority": event["priority"]
                }
            })
            print("Published {} event: {}".format(event["type"], result.message_id))
        
        # Process subscription messages
        high_priority = client.subscribe_messages("order-events", "high-priority", max_count=10)
        payment_events = client.subscribe_messages("order-events", "payment-events", max_count=10)
        
        print("High priority messages: {}".format(len(high_priority)))
        print("Payment event messages: {}".format(len(payment_events)))
        
    else:
        print("Topic operations not supported with {}".format(client_info["service_type"]))
        print("Consider using Amazon SNS + SQS for pub/sub patterns")

def process_message(message):
    # Simulate message processing
    return True

def get_env_var(name):
    # Would use runtime.getenv in real scenario
    return "connection-string-value"

def get_timestamp():
    return "2024-01-01T12:00:00Z"

def get_future_timestamp(seconds_ahead):
    return "2024-01-01T12:05:00Z"  # Simplified for example

def encode_json(obj):
    return str(obj)

main()
```

## Implementation Architecture

### File Structure

```
mq/
├── mq.go              # Main module implementation
├── client.go          # Client interface and factory
├── aws_sqs.go         # AWS SQS implementation
├── azure_servicebus.go # Azure Service Bus implementation
├── message.go         # Message data structures
├── queue.go           # Queue operations
├── topic.go           # Topic/subscription operations (Azure)
├── errors.go          # Error handling and types
├── config.go          # Configuration management
├── retry.go           # Retry logic and backoff
├── mq_test.go         # Unit tests
├── example_test.go    # Example tests
├── go.mod             # Go module definition
├── go.sum             # Go module checksums
├── README.md          # Documentation
└── LICENSE            # MIT License
```

### Core Components

#### Module Structure

```go
type Module struct {
    *base.ConfigurableModule
    
    // Configuration options
    ServiceType    *base.ConfigOption[string]
    ConnectionString *base.ConfigOption[string]
    Timeout        *base.ConfigOption[int]
    MaxRetries     *base.ConfigOption[int]
    
    // AWS SQS specific
    AWSRegion      *base.ConfigOption[string]
    AWSAccessKey   *base.ConfigOption[string]
    AWSSecretKey   *base.ConfigOption[string]
    
    // Azure Service Bus specific
    AzureNamespace *base.ConfigOption[string]
    AzureSharedKey *base.ConfigOption[string]
}
```

#### Client Interface

```go
type Client interface {
    // Queue operations
    CreateQueue(name string, options map[string]interface{}) (*Queue, error)
    DeleteQueue(name string) error
    ListQueues(prefix string) ([]*Queue, error)
    GetQueue(name string) (*Queue, error)
    QueueExists(name string) (bool, error)
    
    // Message operations
    SendMessage(queueName, body string, options map[string]interface{}) (*MessageResult, error)
    ReceiveMessages(queueName string, maxCount int, options map[string]interface{}) ([]*MessageResult, error)
    DeleteMessage(queueName, messageID string, options map[string]interface{}) error
    
    // Batch operations
    SendMessages(queueName string, messages []map[string]interface{}) ([]*MessageResult, error)
    DeleteMessages(queueName string, messageIDs []string) ([]bool, error)
    
    // Topic operations (Azure Service Bus)
    CreateTopic(name string, options map[string]interface{}) (*Topic, error)
    PublishMessage(topicName, body string, options map[string]interface{}) (*MessageResult, error)
    CreateSubscription(topicName, subscriptionName string, options map[string]interface{}) (*Subscription, error)
    SubscribeMessages(topicName, subscriptionName string, maxCount int, options map[string]interface{}) ([]*MessageResult, error)
    
    // Connection management
    Close() error
    HealthCheck() error
}
```

#### Service Implementations

```go
// AWS SQS Client
type SQSClient struct {
    client    *sqs.Client
    region    string
    timeout   time.Duration
    retryConfig retry.Config
}

// Azure Service Bus Client
type ServiceBusClient struct {
    client     *azservicebus.Client
    namespace  string
    timeout    time.Duration
    retryConfig retry.Config
}
```

### Dependencies

```go
// External dependencies
require (
    github.com/aws/aws-sdk-go-v2 v1.24.0
    github.com/aws/aws-sdk-go-v2/service/sqs v1.29.0
    github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.5.0
    github.com/Azure/azure-sdk-for-go/sdk/azcore v1.9.0
    github.com/1set/starlet v0.16.0
    go.starlark.net v0.0.0-20231121155337-90ade8b19d09
)

// Internal dependencies
require (
    github.com/starpkg/base v0.1.0
)
```

## Development Plan

### Phase 1: Foundation and AWS SQS Support (Critical)

**Timeline**: 2-3 weeks  
**Success Criteria**: Basic queue operations working with AWS SQS

- [ ] Set up module structure and base package integration
- [ ] Implement core configuration system with environment variables
- [ ] Develop AWS SQS client implementation
- [ ] Create basic queue operations (create, delete, list, send, receive)
- [ ] Implement message data structures and conversion
- [ ] Add error handling and retry logic
- [ ] Write comprehensive unit tests for SQS operations
- [ ] Create example tests demonstrating basic usage

### Phase 2: Azure Service Bus Queue Support (High)

**Timeline**: 2-3 weeks  
**Success Criteria**: Feature parity between AWS SQS and Azure Service Bus queues

- [ ] Implement Azure Service Bus client for queue operations
- [ ] Add connection string parsing and authentication
- [ ] Create unified client interface abstraction
- [ ] Implement batch operations for performance
- [ ] Add visibility timeout and dead letter queue support
- [ ] Write comprehensive tests for Azure Service Bus queues
- [ ] Create cross-service compatibility tests

### Phase 3: Advanced Features and Performance (High)

**Timeline**: 2-3 weeks  
**Success Criteria**: Production-ready performance and reliability features

- [ ] Implement batch operations for high throughput scenarios
- [ ] Add message scheduling and delayed delivery
- [ ] Create dead letter queue management functionality
- [ ] Add message attribute and property handling
- [ ] Implement connection pooling and resource management
- [ ] Add comprehensive logging and monitoring hooks
- [ ] Performance testing and optimization
- [ ] Memory usage optimization and leak detection

### Phase 4: Azure Service Bus Topics and Subscriptions (Medium)

**Timeline**: 2-3 weeks  
**Success Criteria**: Full pub/sub pattern support with Azure Service Bus

- [ ] Implement topic creation and management
- [ ] Add subscription creation and rule management
- [ ] Create message filtering and routing functionality
- [ ] Implement publish/subscribe operations
- [ ] Add subscription rule management (SQL filters)
- [ ] Create topic-specific example tests
- [ ] Add documentation for pub/sub patterns

### Phase 5: Production Readiness and Documentation (Medium)

**Timeline**: 1-2 weeks  
**Success Criteria**: Complete documentation and production deployment support

- [ ] Complete README.md with comprehensive examples
- [ ] Add troubleshooting guide and common patterns
- [ ] Create migration guides from other message queue libraries
- [ ] Add performance benchmarking suite
- [ ] Security audit and best practices documentation
- [ ] Create deployment examples for different environments
- [ ] Final integration testing across all supported services

## Testing Strategy

### Unit Tests

- **Coverage Target**: 90%+ line coverage
- **Focus Areas**: Configuration parsing, message serialization, error handling
- **Mock Strategy**: Mock external service calls using interfaces
- **Test Data**: Comprehensive test fixtures for different message types

### Integration Tests

- **Real Services**: Tests against actual AWS SQS and Azure Service Bus
- **Environment Setup**: Docker containers for local testing when possible
- **Credential Management**: Secure handling of test credentials
- **Clean-up**: Automatic resource cleanup after test runs

### Example Tests

- **Documentation Validation**: All README examples must pass tests
- **Error Scenarios**: Test common error conditions and recovery
- **Performance Tests**: Batch operation throughput and latency
- **Cross-Service Tests**: Message forwarding between services

### Performance Testing

- **Throughput Benchmarks**: Messages per second for different scenarios
- **Memory Usage**: Monitor memory consumption under load
- **Connection Pooling**: Validate connection reuse and lifecycle
- **Latency Measurement**: End-to-end message delivery times

## Security & Performance

### Security Considerations

- **Credential Management**: All API keys and connection strings handled as secrets
- **Transport Security**: Enforce TLS for all service communications
- **Message Content**: Support for message encryption/decryption helpers
- **Access Control**: Proper IAM integration for AWS services
- **Audit Logging**: Comprehensive logging of all operations for security monitoring

### Performance Optimizations

- **Connection Pooling**: Reuse HTTP connections across operations
- **Batch Operations**: Leverage service-native batching for high throughput
- **Async Operations**: Use Go routines for concurrent message processing
- **Memory Management**: Efficient message buffering and streaming
- **Retry Logic**: Exponential backoff with jitter to avoid thundering herd

### Best Practices

- **Resource Cleanup**: Automatic connection and resource disposal
- **Error Recovery**: Graceful degradation and circuit breaker patterns
- **Monitoring Integration**: Hooks for metrics collection and alerting
- **Configuration Validation**: Early validation of service configuration
- **Documentation**: Clear guidance on production deployment and scaling

This comprehensive plan provides a roadmap for implementing a production-ready message queue module that serves as a unified interface for cloud messaging services while maintaining the simplicity and safety of the Starlark runtime environment.
