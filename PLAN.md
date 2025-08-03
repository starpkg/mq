# 📨 MQ Module for Starlark

[![Go Reference](https://pkg.go.dev/badge/github.com/starpkg/mq.svg)](https://pkg.go.dev/github.com/starpkg/mq)
[![Go Report Card](https://goreportcard.com/badge/github.com/starpkg/mq)](https://goreportcard.com/report/github.com/starpkg/mq)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Unified message queue operations for Starlark scripts - seamlessly connect to AWS SQS and Azure Service Bus!**

The MQ module provides a comprehensive, easy-to-use interface for interacting with cloud message queue services from Starlark scripts. It supports Amazon SQS and Azure Service Bus with advanced features like batch operations, dead letter queue management, and unified message handling across different cloud providers.

## ✨ Features

- **🌐 Universal Compatibility**: Works with AWS SQS and Azure Service Bus with automatic service detection
- **🔒 Secure Configuration**: Module-level configuration with secret handling and environment variable support
- **📨 Unified Queue Operations**: Create, delete, list queues with consistent API across cloud providers
- **💌 Advanced Message Operations**: Send, receive, delete messages with unified parameter handling
- **⚡ High Performance**: Intelligent batch operations with auto-adaptation to service limits
- **🔄 Smart Retry Logic**: Automatic retry handling with exponential backoff for resilient operations
- **💀 Dead Letter Queue Support**: Complete DLQ management with unified interface across providers
- **🏷️ Message Properties**: Full support for message attributes, scheduling, and session handling
- **🔐 Message Lock Management**: Unified visibility timeout and lock duration handling
- **🛠️ Rich Utility Functions**: Service detection, feature validation, and configuration helpers
- **🧠 Intelligent Adaptation**: Graceful degradation for service-specific features and limitations
- **🎯 Starlark Native**: Designed specifically for Starlark with proper error handling and type safety

## Core Design Principles

1. **Unified Abstraction**: Abstract common messaging concepts into a single, consistent API regardless of underlying service implementation
2. **Concept Normalization**: Map service-specific features to unified concepts (e.g., visibility_timeout + lock_duration → lock_duration)
3. **Performance-First Architecture**: Leverage Go routines and connection pooling for high-throughput scenarios while maintaining thread-safe operations
4. **Security by Default**: All credentials and sensitive configuration values are handled securely using the base package's secret management capabilities
5. **Intelligent Adaptation**: Automatically adapt unified API calls to service-specific implementations and limitations
6. **Essential Features Only**: Focus on commonly used features and provide elegant fallbacks for service-specific limitations

## Starlark Constraints & Adaptations

- **No Classes → Service Factory Pattern**: Use `connect()` function to create service-specific clients with method-like behavior through closures
- **No f-strings → Format Method**: All string formatting uses `"template {}".format(value)` instead of f-string syntax
- **No try/except → Conditional Checks**: Use conditional logic instead of exception handling and `fail()` if can't handle the error
- **No is/is not → Equality Comparison**: Use `== None` and `!= None` for null checks instead of identity comparisons
- **No While Loops → Bounded For Loops**: Use `for i in range(n):` with break conditions for iterative processing
- **No Import Statements → Built-in Functions**: Use runtime-provided functions instead of Python's import system
- **No Random Module → Deterministic Logic**: Use predictable logic instead of random number generation
- **Immutable After Load → Configuration at Connect**: All service configuration happens during connection establishment

## API Design

### Service Compatibility Overview

The `mq` module provides a unified interface for queue operations across AWS SQS and Azure Service Bus. The table below shows what's supported on each service.

#### Core Features Compatibility Matrix

| Feature Category | AWS SQS | Azure Service Bus | Notes |
|------------------|---------|-------------------|-------|
| **Standard Queues** | ✅ | ✅ | Full support on both services |
| **FIFO Queues** | ✅ | ❌ | SQS only - use sessions in Azure for ordering |
| **Dead Letter Queues** | ✅ | ✅ | Different configuration methods |
| **Message Attributes** | ✅ | ✅ | Different limits and types |
| **Batch Operations** | ✅ (max 10) | ✅ (max 100) | Auto-adaptation to service limits |
| **Long Polling** | ✅ | ✅ | Different parameter names |
| **Delayed Messages** | ✅ (≤15min) | ✅ | Different mechanisms and limits |
| **Message Sessions** | ❌ | ✅ | Azure Service Bus only |
| **Duplicate Detection** | ✅ (FIFO only) | ✅ | Different implementations |
| **Peek Messages** | ❌ | ✅ | Azure only - AWS returns local error |

## Unified API Design

### Concept Normalization Strategy

The module normalizes different service concepts into unified abstractions:

| Unified Concept | AWS SQS Implementation | Azure Service Bus Implementation | Normalization Logic |
|-----------------|------------------------|-----------------------------------|-------------------|
| **Message Lock** | `visibility_timeout` | `lock_duration` | Use `lock_duration` as unified parameter |
| **Message Scheduling** | `delay_seconds` (≤15min) | `scheduled_enqueue_time` | Use `scheduled_time` with automatic fallback |
| **Message Grouping** | `message_group_id` (FIFO) | `session_id` | Use `session_id` as unified parameter |
| **Message Properties** | `message_attributes` | `properties` | Use `properties` as unified parameter |
| **Dead Letter Handling** | Separate DLQ + config | Built-in subqueue | Unified `dead_letter_config` |
| **Batch Processing** | Max 10 messages | Max 100 messages | Auto-split based on service limits |

### Core Module Functions

#### Connection Management

```python
# Primary connection function - auto-detects service type
connect(
    service_type="auto",         # "aws_sqs", "azure_servicebus", "auto", or None
    connection_string=None,      # Azure Service Bus connection string
    aws_region=None,             # AWS region for SQS
    aws_access_key=None,         # AWS access key ID
    aws_secret_key=None,         # AWS secret access key
    timeout=30,                  # Connection timeout in seconds
    max_retries=3                # Maximum retry attempts
) -> Client

# Utility functions
get_supported_services() -> list    # Returns ["aws_sqs", "azure_servicebus"]
get_client_info(client) -> dict     # Returns client connection details
check_feature_support(client, feature_name) -> bool  # Check if feature is supported
```

### Unified Client API

#### Queue Operations

```python
# Queue management
create_queue(name, lock_duration=30, retention_period=1209600, max_delivery_count=10, 
             dead_letter_config=None, enable_sessions=False, duplicate_detection=False) -> Queue
delete_queue(name) -> bool
list_queues(prefix="") -> list
get_queue(name) -> Queue
exists(name) -> bool
purge(name) -> bool
get_info(name) -> dict    # Unified queue information
```

#### Unified Message Operations

```python
# Core message operations
send(queue_name, body, properties=None, scheduled_time=None, session_id=None, 
     correlation_id=None, reply_to=None, time_to_live=None, message_id=None) -> MessageResult
receive(queue_name, max_count=1, wait_time=0, lock_duration=None, peek_only=False) -> list
delete(queue_name, message_ids) -> list  # Accepts single ID string or list of IDs

# Message lock management (unified visibility/lock concept)
lock(queue_name, message_id, lock_duration) -> bool
unlock(queue_name, message_id) -> bool

# Smart batch operations (auto-adapts to service limits)
batch_send(queue_name, messages) -> list  # Auto-handles batch size limits

# Message inspection (available when supported)
peek(queue_name, max_count=1) -> list    # Returns error on AWS SQS
```

#### Unified Scheduling and Dead Letter Queue

```python
# Message scheduling (unified approach)
schedule(queue_name, body, scheduled_time, properties=None, session_id=None) -> MessageResult
cancel(queue_name, message_id) -> bool  # Where supported (Azure only)

# Dead letter queue operations (unified interface)
dlq_receive(queue_name, max_count=10) -> list
dlq_reprocess(queue_name, message_id) -> bool
dlq_purge(queue_name) -> bool
```

### Unified API Implementation

#### Core Operations (Universal Support)

| Function | Implementation Strategy | AWS SQS Mapping | Azure Service Bus Mapping |
|----------|-------------------------|-----------------|---------------------------|
| `send()` | Direct mapping | Native SQS API | Native Service Bus API |
| `receive()` | Adaptive batch size | max_count ≤ 10 | max_count ≤ 32 |
| `delete()` | Auto-batch handling | Native delete | Native complete |
| `lock()` | Unified lock concept | Change visibility timeout | Renew message lock |
| `unlock()` | Early release | Change to 0 seconds | Abandon message |

#### Queue Management (Universal Support)

| Function | Implementation Strategy | AWS SQS Mapping | Azure Service Bus Mapping |
|----------|-------------------------|-----------------|---------------------------|
| `create_queue()` | Unified options mapping | CreateQueue API | CreateQueue API |
| `delete_queue()` | Direct mapping | DeleteQueue API | DeleteQueue API |
| `list_queues()` | Direct mapping | ListQueues API | ListQueues API |
| `get_info()` | Normalized attributes | GetQueueAttributes | GetQueueRuntimeProperties |
| `purge()` | Direct mapping | PurgeQueue API | Native purge |

#### Smart Adaptations

| Function | AWS SQS Behavior | Azure Service Bus Behavior | Unified Behavior |
|----------|------------------|----------------------------|------------------|
| `peek()` | Return local error | Native peek | Conditional support |
| `schedule()` | delay_seconds (≤15min) | scheduled_enqueue_time | Pass to service for validation |
| `cancel()` | Return local error | Native cancel | Conditional support |
| `batch_send()` | Auto-split to batches of 10 | Auto-split to batches of 100 | Transparent handling |

#### Error Handling Strategy

| Scenario | AWS SQS | Azure Service Bus | Implementation |
|----------|---------|-------------------|----------------|
| **Unsupported features** | Local error with guidance | Local error with guidance | Immediate fail() |
| **Service limits exceeded** | Pass to service | Pass to service | Let service return error |
| **Invalid parameters** | Local validation | Local validation | Early parameter validation |

### Unified Parameter Design

#### Queue Configuration (Normalized)

| Unified Parameter | Description | AWS SQS Mapping | Azure Service Bus Mapping | Default |
|-------------------|-------------|-----------------|---------------------------|---------|
| `lock_duration` | Message lock time | `visibility_timeout` | `lock_duration` | 30 seconds |
| `retention_period` | Message retention | `message_retention_period` | `default_message_time_to_live` | 14 days |
| `max_delivery_count` | Max delivery attempts | `max_receive_count` | `max_delivery_count` | 10 |
| `dead_letter_config` | DLQ configuration | `redrive_policy` | Built-in DLQ | None |
| `enable_sessions` | Ordered processing | `fifo_queue` | `requires_session` | false |
| `duplicate_detection` | Deduplication window | `content_based_deduplication` | `requires_duplicate_detection` | false |
| `max_queue_size` | Queue size limit | Not configurable | `max_size_in_megabytes` | Unlimited |

#### Message Options (Normalized)

| Unified Parameter | Description | AWS SQS Mapping | Azure Service Bus Mapping | Default |
|-------------------|-------------|-----------------|---------------------------|---------|
| `properties` | Message metadata | `message_attributes` | `application_properties` | {} |
| `scheduled_time` | Delivery time | `delay_seconds` (≤15min) | `scheduled_enqueue_time` | Immediate |
| `session_id` | Message grouping | `message_group_id` (FIFO) | `session_id` | None |
| `correlation_id` | Request correlation | Not supported | `correlation_id` | None |
| `reply_to` | Response destination | Not supported | `reply_to` | None |
| `time_to_live` | Message TTL | Use queue retention | `time_to_live` | Queue default |
| `message_id` | Duplicate detection | `message_deduplication_id` | `message_id` | Auto-generated |

#### Receive Options (Normalized)

| Unified Parameter | Description | AWS SQS Mapping | Azure Service Bus Mapping | Default |
|-------------------|-------------|-----------------|---------------------------|---------|
| `max_count` | Max messages | `max_number_of_messages` (≤10) | `max_message_count` (≤32) | 1 |
| `wait_time` | Polling timeout | `wait_time_seconds` (≤20) | `max_wait_time` (≤60) | 0 |
| `lock_duration` | Processing time | `visibility_timeout` | Inherited from queue | Queue default |
| `peek_only` | Peek without receive | Not supported (graceful) | `peek_lock` vs `receive_delete` | false |

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
| **Connection String Auth** | Connection string based auth | Use managed identity when possible |
| **Message Size** | 256KB (1MB Premium) | Use blob storage for large messages |
| **Session Concurrency** | One processor per session | Use multiple sessions for parallelism |

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

### Normalized Data Structures

#### MessageResult (Unified)

```python
{
    "message_id": "unique-message-identifier",
    "body": "message content",
    "properties": {"key": "value"},           # Unified message properties
    "session_id": "session-123",             # Unified grouping/session
    "correlation_id": "correlation-456",     # Request correlation
    "reply_to": "response-queue",            # Response destination
    "enqueue_time": "2024-01-01T12:00:00Z",
    "scheduled_time": "2024-01-01T12:05:00Z", # When message becomes available
    "delivery_count": 1,
    "lock_expires_at": "2024-01-01T12:30:00Z", # When lock expires
    "time_to_live": 3600,                    # Message TTL in seconds
    "receipt_handle": "service-specific-handle", # Internal use
    "success": True,
    "error": None
}
```

#### Queue (Unified)

```python
{
    "name": "queue-name",
    "service_type": "aws_sqs",               # Which service backs this queue
    "url": "service-specific-url",
    "message_count": 42,
    "lock_duration": 30,                     # Unified lock/visibility timeout
    "retention_period": 1209600,             # Message retention in seconds
    "max_delivery_count": 10,
    "dead_letter_config": {                  # Unified DLQ configuration
        "enabled": True,
        "max_delivery_count": 10,
        "queue_name": "dlq-name"
    },
    "enable_sessions": False,                # Unified ordered processing
    "duplicate_detection": {                 # Unified deduplication
        "enabled": True,
        "window_seconds": 300
    },
    "max_queue_size": 1073741824,           # Queue size in bytes (unlimited = -1)
    "created_time": "2024-01-01T10:00:00Z",
    "modified_time": "2024-01-01T10:00:00Z"
}
```

## Configuration System

The module integrates with the base package configuration system:

```go
// Primary service configuration
ServiceType      *ConfigOption[string] // "aws_sqs", "azure_servicebus", "auto"
ConnectionString *ConfigOption[string] // Azure Service Bus connection string (secret)
Timeout          *ConfigOption[int]    // Connection timeout in seconds
MaxRetries       *ConfigOption[int]    // Maximum retry attempts

// AWS SQS Configuration
AWSRegion        *ConfigOption[string] // AWS region
AWSAccessKey     *ConfigOption[string] // AWS access key ID (secret)
AWSSecretKey     *ConfigOption[string] // AWS secret key (secret)
AWSSessionToken  *ConfigOption[string] // AWS session token (secret)

// Performance and Behavior
DefaultLockDuration *ConfigOption[int]  // Default message lock duration in seconds
DefaultBatchSize    *ConfigOption[int]  // Default batch size for operations
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MQ_SERVICE_TYPE` | Service type (aws_sqs, azure_servicebus, auto) | auto |
| `MQ_CONNECTION_STRING` | Azure Service Bus connection string | - |
| `MQ_AWS_REGION` | AWS region for SQS | us-east-1 |
| `AWS_ACCESS_KEY_ID` | AWS access key ID | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key | - |
| `AWS_SESSION_TOKEN` | AWS session token (optional) | - |
| `MQ_TIMEOUT` | Connection timeout in seconds | 30 |
| `MQ_MAX_RETRIES` | Maximum retry attempts | 3 |
| `MQ_DEFAULT_LOCK_DURATION` | Default message lock duration | 30 |
| `MQ_DEFAULT_BATCH_SIZE` | Default batch size for operations | 10 |

## 🚀 Quick Start

### Basic Usage

```python
load("mq", "connect")

def main():
    # Connect to AWS SQS with auto-detection
    client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2"
    )
    
    # Create a queue with unified parameters
    queue = client.create_queue(
        "my-work-queue",
        lock_duration=60,
        max_delivery_count=5,
        dead_letter_config={
            "enabled": True,
            "queue_name": "my-dlq"
        }
    )
    
    if queue == None:
        fail("Failed to create queue")
    
    # Send a message with unified properties
    result = client.send(
        "my-work-queue", 
        "Hello, World!",
        properties={
            "priority": "high",
            "sender": "worker-1"
        }
    )
    
    print("Message sent with ID: {}".format(result.message_id))
    
    # Receive messages with unified options
    messages = client.receive(
        "my-work-queue", 
        max_count=5,
        wait_time=20,
        lock_duration=30
    )
    
    for message in messages:
        print("Processing message: {}".format(message.body))
        print("Message properties: {}".format(message.properties))
        
        # Process the message...
        success = process_message(message)
        
        if success:
            # Delete message after successful processing
            client.delete("my-work-queue", message.message_id)
        else:
            # Extend lock duration for retry
            client.lock("my-work-queue", message.message_id, 60)
            print("Extended lock for message {}, will retry".format(message.message_id))

def process_message(message):
    # Simulate message processing
    print("Processing: {}".format(message.body))
    return True

main()
```

### Azure Service Bus with Sessions

```python
load("mq", "connect")

def main():
    # Connect to Azure Service Bus
    client = connect(
        service_type="azure_servicebus",
        connection_string="Endpoint=sb://namespace.servicebus.windows.net/;..."
    )
    
    # Create a queue with session support for message ordering
    queue = client.create_queue(
        "order-processing",
        lock_duration=60,
        max_delivery_count=5,
        enable_sessions=True,
        duplicate_detection=True
    )
    
    # Send messages with session grouping
    orders = [
        {"order_id": "12345", "event": "created", "amount": 99.99},
        {"order_id": "12345", "event": "paid", "amount": 99.99},
        {"order_id": "12346", "event": "created", "amount": 149.50}
    ]
    
    for order in orders:
        result = client.send(
            "order-processing",
            encode_json(order),
            properties={
                "event_type": order["event"],
                "timestamp": get_current_time()
            },
            session_id=order["order_id"],  # Ensures ordering per order
            correlation_id="batch-001"
        )
        print("Sent order event: {} for order {}".format(order["event"], order["order_id"]))
    
    # Receive messages by session (guarantees order)
    messages = client.receive(
        "order-processing",
        max_count=10,
        wait_time=30,
        lock_duration=60
    )
    
    for msg in messages:
        print("Processing order event: {}".format(msg.body))
        print("Session ID: {}".format(msg.session_id))
        print("Correlation ID: {}".format(msg.correlation_id))
        
        # Process and delete
        if process_order_event(msg):
            client.delete("order-processing", msg.message_id)
        else:
            # Extend lock for retry
            client.lock("order-processing", msg.message_id, 60)

def process_order_event(message):
    # Simulate order event processing
    return True

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
            "properties": {
                "batch_id": "batch-001",
                "sequence": str(i)
            }
        })
    
    # Send messages in batches (auto-adapts to service limits)
    results = client.batch_send("batch-queue", messages)
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    
    print("Successfully sent {} out of {} messages".format(len(successful), len(messages)))
    
    if len(failed) > 0:
        print("Failed to send {} messages".format(len(failed)))
        for failure in failed:
            print("Error: {}".format(failure.error))
    
    # Batch receive and process (using for loop with range for bounded processing)
    for batch_round in range(100):  # Process up to 100 batches
        messages = client.receive(
            "batch-queue", 
            max_count=10,
            wait_time=5  # Short polling for batch processing
        )
        
        if len(messages) == 0:
            print("No more messages to process")
            break
        
        # Process messages
        processed_ids = []
        for msg in messages:
            if process_batch_message(msg):
                processed_ids.append(msg.message_id)
        
        # Batch delete successful messages
        if len(processed_ids) > 0:
            delete_results = client.delete("batch-queue", processed_ids)
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
    main_queue = client.create_queue(
        "processing-queue",
        lock_duration=30,
        max_delivery_count=3,
        dead_letter_config={
            "enabled": True,
            "queue_name": "processing-dlq"
        }
    )
    
    # Create the dead letter queue
    dlq = client.create_queue("processing-dlq")
    
    # Process main queue (using for loop with range for bounded processing)
    for processing_round in range(50):  # Process up to 50 rounds
        messages = client.receive("processing-queue", max_count=5)
        if len(messages) == 0:
            print("No more messages to process")
            break
        
        for msg in messages:
            success = process_with_potential_failure(msg)
            if success:
                client.delete("processing-queue", msg.message_id)
            else:
                # Let it retry (will eventually go to DLQ)
                print("Processing failed for message {}, will retry".format(msg.message_id))
    
    # Handle dead letter messages
    dead_messages = client.dlq_receive("processing-queue", max_count=10)
    print("Found {} messages in dead letter queue".format(len(dead_messages)))
    
    for dead_msg in dead_messages:
        print("Dead letter message: {}".format(dead_msg.body))
        print("Delivery count: {}".format(dead_msg.delivery_count))
        
        # Decide what to do with dead letter messages
        if should_requeue(dead_msg):
            # Move back to main queue
            client.dlq_reprocess("processing-queue", dead_msg.message_id)
            print("Reprocessed message {}".format(dead_msg.message_id))
        else:
            # Log and remove
            log_dead_message(dead_msg)
            # Message will be automatically removed from DLQ

def process_with_potential_failure(message):
    # Simulate processing that might fail
    return True  # Simplified for Starlark compatibility

def should_requeue(message):
    # Simple logic for requeuing
    return message.delivery_count < 5

def log_dead_message(message):
    print("Logging dead letter message: {} - {}".format(message.message_id, message.body))

main()
```

### Multi-Service Configuration and Feature Detection

```python
load("mq", "connect", "get_supported_services", "get_client_info", "check_feature_support")

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
    forward_messages_between_services(aws_client, azure_client)

def handle_service_differences(aws_client, azure_client):
    """Demonstrate handling of service-specific features"""
    
    # AWS SQS: Standard message processing
    messages = aws_client.receive(
        "test-queue", 
        max_count=5,
        wait_time=10,
        lock_duration=60
    )
    
    for msg in messages:
        # Process message and extend lock if needed
        if process_message(msg):
            aws_client.delete("test-queue", msg.message_id)
        else:
            aws_client.lock("test-queue", msg.message_id, 120)
    
    # Azure Service Bus: Check peek support
    if check_feature_support(azure_client, "peek"):
        # Peek messages without receiving them (Azure only)
        peeked = azure_client.peek("test-queue", max_count=5)
        print("Peeked {} messages from Azure queue".format(len(peeked)))
    
    # Receive with session support (Azure Service Bus)
    azure_messages = azure_client.receive(
        "test-queue", 
        max_count=5,
        wait_time=30,
        lock_duration=60
    )
    
    for msg in azure_messages:
        # Process message with session support (Azure only)
        if msg.session_id != None:
            print("Processing session message: {}".format(msg.session_id))
        
        if process_message(msg):
            azure_client.delete("test-queue", msg.message_id)

def forward_messages_between_services(source_client, dest_client):
    """Forward messages between services handling different capabilities"""
    
    # Get source client info to adapt behavior
    source_info = get_client_info(source_client)
    dest_info = get_client_info(dest_client)
    
    # Receive messages from source
    messages = source_client.receive("source-queue", max_count=10)
    
    for msg in messages:
        # Forward message with unified properties
        result = dest_client.send(
            "destination-queue",
            msg.body,
            properties=msg.properties,
            session_id=msg.session_id,
            correlation_id=msg.correlation_id
        )
        
        if result.success:
            # Delete from source after successful forward
            source_client.delete("source-queue", msg.message_id)
            print("Forwarded {} -> {} (from {} to {})".format(
                msg.message_id, 
                result.message_id,
                source_info["service_type"],
                dest_info["service_type"]
            ))
        else:
            print("Failed to forward message: {}".format(result.error))

def process_message(message):
    # Simulate message processing
    return True

def get_env_var(name):
    # Would use runtime.getenv in real scenario
    return "connection-string-value"

main()
```

## Implementation Architecture

### File Structure

```text
mq/
├── mq.go              # Main module implementation and Starlark integration
├── client.go          # ClientWrapper implementing starlark.Value
├── aws_sqs.go         # AWS SQS client implementation
├── azure_servicebus.go # Azure Service Bus client implementation
├── config.go          # Configuration management with base package
├── message.go         # Message data structures and conversions
├── queue.go           # Queue operations and utilities
├── errors.go          # Error handling and types
├── utils.go           # Utility functions and helpers
├── example_test.go    # Example tests with Starlark scripts
├── go.mod             # Go module definition
├── go.sum             # Go module checksums
├── README.md          # Documentation
└── LICENSE            # MIT License
```

### Core Components

#### Module Structure

```go
type Module struct {
    cfgMod *base.ConfigurableModule
    ext    *base.ConfigurableModuleExt
}

// NewModule creates a new instance of Module with default configurations
func NewModule() *Module {
    return newModuleWithOptions(
        genConfigOption(configKeyServiceType, "Service type (aws_sqs, azure_servicebus, auto)", "auto"),
        genSecretConfigOption(configKeyConnectionString, "Azure Service Bus connection string", ""),
        genConfigOption(configKeyTimeout, "Connection timeout in seconds", 30),
        genConfigOption(configKeyMaxRetries, "Maximum retry attempts", 3),
        genConfigOption(configKeyAWSRegion, "AWS region for SQS", "us-east-1"),
        genSecretConfigOption(configKeyAWSAccessKey, "AWS access key ID", ""),
        genSecretConfigOption(configKeyAWSSecretKey, "AWS secret access key", ""),
        genSecretConfigOption(configKeyAWSSessionToken, "AWS session token", ""),
        genConfigOption(configKeyDefaultLockDuration, "Default message lock duration", 30),
        genConfigOption(configKeyDefaultBatchSize, "Default batch size for operations", 10),
    )
}
```

#### ClientWrapper for Starlark Integration

```go
// ClientWrapper wraps the message queue client for Starlark
type ClientWrapper struct {
    client    Client
    methodMap map[string]func() starlark.Value
    allNames  []string
}

// Implements starlark.Value and starlark.HasAttrs interfaces
func (cw *ClientWrapper) String() string {
    return "<mq.Client>"
}

func (cw *ClientWrapper) Type() string {
    return "mq.Client"
}

func (cw *ClientWrapper) Attr(name string) (starlark.Value, error) {
    if methodFunc, exists := cw.methodMap[name]; exists {
        return methodFunc(), nil
    }
    return nil, starlark.NoSuchAttrError(fmt.Sprintf("%s has no .%s attribute", cw.Type(), name))
}
```

#### Client Interface

```go
type Client interface {
    // Queue operations
    CreateQueue(ctx context.Context, name string, options QueueOptions) (*Queue, error)
    DeleteQueue(ctx context.Context, name string) error
    ListQueues(ctx context.Context, prefix string) ([]*Queue, error)
    GetQueue(ctx context.Context, name string) (*Queue, error)
    Exists(ctx context.Context, name string) (bool, error)
    
    // Message operations
    Send(ctx context.Context, queueName, body string, options MessageOptions) (*MessageResult, error)
    Receive(ctx context.Context, queueName string, options ReceiveOptions) ([]*MessageResult, error)
    Delete(ctx context.Context, queueName string, messageIDs []string) ([]bool, error)
    
    // Message lock management
    Lock(ctx context.Context, queueName, messageID string, duration int) error
    Unlock(ctx context.Context, queueName, messageID string) error
    
    // Batch operations
    BatchSend(ctx context.Context, queueName string, messages []BatchMessage) ([]*MessageResult, error)
    
    // Dead letter queue operations
    DLQReceive(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error)
    DLQReprocess(ctx context.Context, queueName, messageID string) error
    DLQPurge(ctx context.Context, queueName string) error
    
    // Connection management
    Close() error
    GetClientInfo() map[string]interface{}
}
```

### Dependencies

```go
// External dependencies
require (
    github.com/aws/aws-sdk-go-v2 v1.24.0
    github.com/aws/aws-sdk-go-v2/service/sqs v1.29.0
    github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.8.0
    github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin v1.8.0
    github.com/1set/starlet v0.16.0
    go.starlark.net v0.0.0-20231121155337-90ade8b19d09
)

// Internal dependencies
require (
    github.com/starpkg/base v0.1.0
)
```

## API Normalization Strategy Summary

### Essential Unified Concepts

The `mq` module implements the following key normalizations to provide a consistent experience:

#### 1. Message Lock Management (Most Important)

- **Unified Parameter**: `lock_duration`
- **AWS SQS**: Maps to `visibility_timeout`
- **Azure Service Bus**: Maps to `lock_duration`
- **API Functions**: `lock()`, `unlock()`
- **Implementation**: Unified visibility/lock timeout concept

#### 2. Message Scheduling

- **Unified Parameter**: `scheduled_time` (ISO 8601 timestamp)
- **AWS SQS**: Converts to `delay_seconds` (passes to service for validation)
- **Azure Service Bus**: Maps to `scheduled_enqueue_time`
- **API Function**: `schedule()`
- **Error Handling**: Let service return errors for invalid values

#### 3. Message Properties

- **Unified Parameter**: `properties` (dict)
- **AWS SQS**: Maps to `message_attributes`
- **Azure Service Bus**: Maps to `application_properties`
- **Consistent Interface**: Single properties parameter across services

#### 4. Message Grouping/Sessions

- **Unified Parameter**: `session_id`
- **AWS SQS**: Maps to `message_group_id` (FIFO queues only)
- **Azure Service Bus**: Maps to `session_id`
- **Ordering**: Enables message ordering per session

#### 5. Dead Letter Queue Configuration

- **Unified Parameter**: `dead_letter_config` (object)
- **AWS SQS**: Creates separate DLQ + redrive policy
- **Azure Service Bus**: Configures built-in dead letter subqueue
- **API Functions**: `dlq_receive()`, `dlq_reprocess()`, `dlq_purge()`

#### 6. Batch Operations

- **Unified Behavior**: Automatic batch size adaptation
- **AWS SQS**: Auto-splits to batches of ≤10 messages
- **Azure Service Bus**: Auto-splits to batches of ≤100 messages
- **API Function**: `batch_send()`, `delete()` (handles single ID or list)
- **User Experience**: Transparent handling of service limits

### Graceful Degradation Patterns

| Feature | AWS SQS Fallback | Azure Service Bus Fallback |
|---------|------------------|----------------------------|
| `peek()` | Local error with guidance | Native support |
| `cancel()` | Local error with guidance | Native support |
| `correlation_id` | Ignored (logs warning) | Native support |
| `reply_to` | Ignored (logs warning) | Native support |

### Implementation Strategy

| Scenario | Implementation | Rationale |
|----------|---------------|-----------|
| **Unsupported features** | Local fail() with clear guidance | Immediate feedback to user |
| **Service limits exceeded** | Pass to service, return service error | Let cloud provider validate |
| **Invalid parameters** | Early validation where possible | Catch obvious errors quickly |
| **Service-specific errors** | Normalize common error types | Consistent error experience |

## Security & Performance

### Security Considerations

- **Credential Management**: All API keys and connection strings handled as secrets using base package
- **Transport Security**: Enforce TLS for all service communications
- **Context Handling**: Proper context propagation from Starlark thread using `dataconv.GetThreadContext()`
- **Access Control**: Proper IAM integration for AWS services and Azure authentication
- **Input Validation**: Sanitize and validate all user inputs before passing to cloud services

### Performance Optimizations

- **Connection Pooling**: Reuse HTTP connections across operations for better performance
- **Batch Operations**: Leverage service-native batching with automatic size adaptation
- **Context Propagation**: Use thread context from `dataconv.GetThreadContext()` for proper cancellation
- **Memory Management**: Efficient message handling and conversion between Starlark and Go types
- **Retry Logic**: Exponential backoff with jitter for resilient operations

### Implementation Guidelines

- **Error Handling**: Let cloud services validate parameters and return normalized errors
- **Feature Detection**: Use `check_feature_support()` to gracefully handle service differences
- **Starlark Integration**: Follow s3 module patterns for ClientWrapper and method exposure
- **Configuration**: Use base package configuration system with proper secret handling
- **Testing**: Comprehensive example tests with real Starlark scripts
