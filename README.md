# 📨 `mq` - Unified Message Queue Module

[![godoc](https://pkg.go.dev/badge/github.com/starpkg/mq.svg)](https://pkg.go.dev/github.com/starpkg/mq)
[![license](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.23-blue.svg)](https://golang.org/)

A unified Starlark module for message queue operations across **AWS SQS** and **Azure Service Bus**. Provides a consistent API interface for common message queue operations including queue management, message sending/receiving, and dead letter queue handling.

> **Where this fits.** `starpkg` provides *support for necessary local operations* plus *simple abstractions over common online services, for ease of use*. `mq` is squarely in the **online-service** half: it wraps two managed cloud queue services (AWS SQS, Azure Service Bus) behind one Starlark-friendly surface so a script can send, receive, and manage messages without learning either vendor SDK. It is an **L4 domain module**, depending downward on `starpkg/base` (the module/config system), `1set/starlet` (the Machine runner + `dataconv`), and transitively `1set/starlight` + `go.starlark.net`.

## ⭐ Features

### Unified API
- **Single Interface**: Same API for AWS SQS and Azure Service Bus
- **Auto-Detection**: Automatically detects service type based on credentials
- **Consistent Error Handling**: Unified error messages across services

### Message Queue Operations
- **Queue Management**: Create, delete, list, and inspect queues
- **Message Operations**: Send, receive, delete, and schedule messages
- **Batch Operations**: Send multiple messages efficiently
- **Dead Letter Queues**: Handle failed message processing

### Advanced Features
- **Message Scheduling**: Schedule messages for future delivery
- **Session Support**: Ordered message processing (Azure)
- **Duplicate Detection**: Prevent duplicate message processing
- **Lock Management**: Control message visibility and processing time

## 🗺️ Script-facing surface

The module exposes three load-time builtins and one client object whose methods cover the full message-queue lifecycle. Each symbol is documented in detail in the [API Reference](#-api-reference) below.

- **Module builtins** — `connect`, `get_supported_services`, `get_client_info`
- **Queue operations** (client methods) — `create_queue`, `delete_queue`, `list_queues`, `get_queue`, `exists`, `purge`, `get_info`
- **Message operations** — `send`, `receive`, `delete`, `batch_send`, `schedule`, `cancel`, `peek`
- **Lock management** — `lock`, `unlock`
- **Dead letter queue** — `dead_letter_receive`, `dead_letter_requeue`, `dead_letter_purge`

The client object also exposes `get_client_info` as a method (same payload as the module builtin of the same name).

## 🚀 Installation

Add to your Go module:

```bash
go get github.com/starpkg/mq
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MQ_SERVICE_TYPE` | Service type (aws_sqs, azure_servicebus, auto) | auto |
| `MQ_TIMEOUT` | Connection timeout in seconds | 30 |
| `MQ_MAX_RETRIES` | Maximum retry attempts | 3 |
| `MQ_CONNECTION_STRING` | Azure Service Bus connection string | "" |
| `MQ_AWS_REGION` | AWS region for SQS | us-east-1 |
| `MQ_AWS_ACCESS_KEY` | AWS access key ID | "" |
| `MQ_AWS_SECRET_KEY` | AWS secret access key | "" |
| `MQ_AWS_SESSION_TOKEN` | AWS session token | "" |
| `MQ_DEFAULT_LOCK_DURATION` | Default message lock duration (seconds) | 30 |
| `MQ_DEFAULT_BATCH_SIZE` | Default batch size for operations | 10 |

## 📚 API Reference

### Module Functions

These functions are available directly from the `mq` module:

#### `connect(service_type?, connection_string?, aws_region?, aws_access_key?, aws_secret_key?, timeout?, max_retries?) -> Client`

Creates and returns a message queue client.

**Parameters:**
- `service_type` (string, optional): Service type - "aws_sqs", "azure_servicebus", or "auto" (default: "auto")
- `connection_string` (string, optional): Azure Service Bus connection string (default: "")
- `aws_region` (string, optional): AWS region for SQS (default: "us-east-1")
- `aws_access_key` (string, optional): AWS access key ID (default: "")
- `aws_secret_key` (string, optional): AWS secret access key (default: "")
- `timeout` (int, optional): Connection timeout in seconds (default: 30)
- `max_retries` (int, optional): Maximum retry attempts (default: 3)

**Returns:** Client object for message queue operations

**Example:**
```python
# AWS SQS
client = mq.connect(
    service_type="aws_sqs",
    aws_region="us-west-2",
    aws_access_key="your-access-key",
    aws_secret_key="your-secret-key"
)

# Azure Service Bus
client = mq.connect(
    service_type="azure_servicebus",
    connection_string="Endpoint=sb://..."
)

# Auto-detect (based on provided credentials)
client = mq.connect(aws_region="us-east-1")  # Will use AWS SQS
```

#### `get_supported_services() -> list`

Returns a list of supported message queue services.

**Returns:** List of supported service types: ["aws_sqs", "azure_servicebus"]

**Example:**
```python
services = mq.get_supported_services()
print(services)  # ["aws_sqs", "azure_servicebus"]
```

#### `get_client_info(client) -> dict`

Returns information about a client instance. Equivalent to calling `get_client_info()` as a method on the client object.

**Parameters:**
- `client` (Client): The client object to inspect (must be the value returned by `connect`)

**Returns:** Dictionary containing client information. For AWS SQS the keys are `service_type`, `region`, `timeout`, `max_retries`; for Azure Service Bus they are `service_type`, `namespace`, `timeout`, `max_retries`.

**Example:**
```python
info = mq.get_client_info(client)
print(info["service_type"])  # "aws_sqs" or "azure_servicebus"
```

---

### Client Methods

Once you have a client object, you can call these methods:

#### Queue Operations

##### `create_queue(name, lock_duration?, retention_period?, max_delivery_count?, dead_letter_config?, enable_sessions?, duplicate_detection?, duplicate_window_secs?, max_queue_size?) -> Queue`

Creates a new message queue.

**Parameters:**
- `name` (string, required): Queue name
- `lock_duration` (int, optional): Message lock duration in seconds (default: 30)
- `retention_period` (int, optional): Message retention period in seconds (default: 1209600 = 14 days)
- `max_delivery_count` (int, optional): Maximum delivery attempts before moving to DLQ (default: 10)
- `dead_letter_config` (dict, optional): Dead letter queue configuration
- `enable_sessions` (bool, optional): Enable message sessions/ordering (default: false)
- `duplicate_detection` (bool, optional): Enable duplicate detection (default: false)
- `duplicate_window_secs` (int, optional): Duplicate detection window in seconds (default: `0`, which Azure Service Bus interprets as its 300-second window)
- `max_queue_size` (int, optional): Maximum queue size in bytes (default: 0 = unlimited)

**Dead Letter Config Structure:**
```python
{
    "enabled": True,
    "queue_name": "my-dlq",  # Dead letter queue name
    "max_delivery_count": 5   # Max attempts before moving to DLQ
}
```

**Returns:** Queue object with queue information

**Example:**
```python
# Basic queue
queue = client.create_queue("orders")

# Queue with DLQ
queue = client.create_queue(
    "priority-orders",
    lock_duration=60,
    max_delivery_count=3,
    dead_letter_config={
        "enabled": True,
        "queue_name": "priority-orders-dlq",
        "max_delivery_count": 3
    }
)
```

##### `delete_queue(name) -> bool`

Deletes a queue.

**Parameters:**
- `name` (string, required): Queue name to delete

**Returns:** True if successful

##### `list_queues(prefix?) -> list`

Lists queues, optionally filtered by name prefix.

**Parameters:**
- `prefix` (string, optional): Filter queues by name prefix (default: "" = all queues)

**Returns:** List of Queue objects

##### `get_queue(name) -> Queue`

Gets information about a specific queue.

**Parameters:**
- `name` (string, required): Queue name

**Returns:** Queue object or None if not found

##### `exists(name) -> bool`

Checks if a queue exists.

**Parameters:**
- `name` (string, required): Queue name

**Returns:** True if queue exists, False otherwise

##### `purge(name) -> bool`

Purges all messages from a queue.

**Parameters:**
- `name` (string, required): Queue name

**Returns:** True if successful

**Note:** ⚠️ AWS SQS implementation is not yet complete.

##### `get_info(name) -> dict`

Gets detailed queue statistics and information.

**Parameters:**
- `name` (string, required): Queue name

**Returns:** Dictionary with queue statistics

#### Message Operations

##### `send(queue_name, body, properties?, scheduled_time?, session_id?, correlation_id?, reply_to?, time_to_live?, message_id?) -> MessageResult`

Sends a message to a queue.

**Parameters:**
- `queue_name` (string, required): Target queue name
- `body` (string, required): Message body content
- `properties` (dict, optional): Custom message properties/attributes
- `scheduled_time` (string, optional): ISO 8601 timestamp for scheduled delivery
- `session_id` (string, optional): Session ID for message ordering
- `correlation_id` (string, optional): Correlation ID for request-response patterns
- `reply_to` (string, optional): Reply queue name
- `time_to_live` (int, optional): Message TTL in seconds
- `message_id` (string, optional): Custom message ID

**Returns:** MessageResult object with send result

**Example:**
```python
# Basic message
result = client.send("orders", "New order received")

# Message with properties
result = client.send(
    "orders",
    "Priority order",
    properties={"priority": "high", "customer_id": "12345"},
    time_to_live=3600
)

# Scheduled message
result = client.send(
    "reminders",
    "Daily reminder",
    scheduled_time="2024-01-01T10:00:00Z"
)
```

##### `receive(queue_name, max_count?, wait_time?, lock_duration?, peek_only?) -> list`

Receives messages from a queue.

**Parameters:**
- `queue_name` (string, required): Source queue name
- `max_count` (int, optional): Maximum number of messages to receive (default: 1)
- `wait_time` (int, optional): Long polling wait time in seconds (default: 0)
- `lock_duration` (int, optional): Message lock duration in seconds (default: uses queue default)
- `peek_only` (bool, optional): Peek without removing messages (default: false)

**Returns:** List of MessageResult objects

**Example:**
```python
# Receive one message
messages = client.receive("orders")

# Receive multiple with long polling
messages = client.receive("orders", max_count=10, wait_time=20)

# Peek without removing
messages = client.receive("orders", peek_only=True)
```

##### `delete(queue_name, message_ids) -> list`

Deletes one or more messages from a queue.

**Parameters:**
- `queue_name` (string, required): Source queue name
- `message_ids` (string or list, required): a single message ID, or a list of message IDs, to delete

**Returns:** A list of booleans, one per requested message ID, where each element reports whether that message was deleted successfully. (A single-string `message_ids` still yields a one-element list.)

**Example:**
```python
# Delete single message
results = client.delete("orders", "msg-123")        # -> [True]

# Delete multiple messages
results = client.delete("orders", ["msg-123", "msg-456"])  # -> [True, True]
```

**Note:** ⚠️ On Azure Service Bus, deletion by message ID always returns `False` for every entry: completing a Service Bus message requires the original received-message object, which this ID-based API cannot supply. AWS SQS deletes via receipt handles (real receipt handles are sent to `DeleteMessageBatch`; short test-style IDs of ≤20 chars are treated as successful without a call).

Sends multiple messages in a batch operation.

**Parameters:**
- `queue_name` (string, required): Target queue name
- `messages` (list, required): List of message dictionaries

**Message Dictionary Structure:**
```python
{
    "body": "Message content",
    "properties": {"key": "value"},  # optional
    "session_id": "session1",        # optional
    "scheduled_time": "2024-01-01T10:00:00Z"  # optional
}
```

**Returns:** List of MessageResult objects

**Note:** ⚠️ AWS SQS implementation is not yet complete.

##### `schedule(queue_name, body, scheduled_time, properties?, session_id?) -> MessageResult`

Schedules a message for future delivery.

**Parameters:**
- `queue_name` (string, required): Target queue name
- `body` (string, required): Message body content
- `scheduled_time` (string, required): ISO 8601 timestamp for delivery
- `properties` (dict, optional): Custom message properties
- `session_id` (string, optional): Session ID for ordering

**Returns:** MessageResult object

##### `cancel(queue_name, message_id) -> bool`

Cancels a scheduled message.

**Parameters:**
- `queue_name` (string, required): Queue name
- `message_id` (string, required): Message ID to cancel

**Returns:** True if successful

**Note:** ❌ AWS SQS returns an `unsupported` error (SQS has no scheduled-message-cancel API). ⚠️ On Azure Service Bus this is currently a stub that returns success without calling `CancelScheduledMessage`.

##### `peek(queue_name, max_count?) -> list`

Peeks at messages without receiving them.

**Parameters:**
- `queue_name` (string, required): Source queue name
- `max_count` (int, optional): Maximum messages to peek (default: 1)

**Returns:** List of MessageResult objects

**Note:** ❌ AWS SQS returns an `unsupported` error (SQS has no peek API). ⚠️ On Azure Service Bus this is currently a stub that returns an empty list instead of calling `PeekMessages`.

#### Message Lock Management

##### `lock(queue_name, message_id, lock_duration) -> bool`

Extends the lock duration of a message.

**Parameters:**
- `queue_name` (string, required): Queue name
- `message_id` (string, required): Message ID
- `lock_duration` (int, required): New lock duration in seconds

**Returns:** True if successful

**Note:** ⚠️ On AWS SQS this is currently a stub that returns success without calling `ChangeMessageVisibility`. ❌ Azure Service Bus returns an `unsupported` error: lock renewal needs the original received-message object, which this message-ID-based API cannot supply.

##### `unlock(queue_name, message_id) -> bool`

Releases the lock on a message.

**Parameters:**
- `queue_name` (string, required): Queue name
- `message_id` (string, required): Message ID

**Returns:** True if successful

**Note:** ⚠️ On AWS SQS this is currently a stub that returns success without resetting the visibility timeout. ❌ Azure Service Bus returns an `unsupported` error: message abandonment needs the original received-message object, which this message-ID-based API cannot supply.

#### Dead Letter Queue Operations

##### `dead_letter_receive(queue_name, max_count?) -> list`

Receives messages from the dead letter queue.

**Parameters:**
- `queue_name` (string, required): Main queue name (DLQ is auto-resolved)
- `max_count` (int, optional): Maximum messages to receive (default: 10)

**Returns:** List of MessageResult objects from DLQ

##### `dead_letter_requeue(queue_name, message_id) -> bool`

Moves a message back from dead letter queue to main queue.

**Parameters:**
- `queue_name` (string, required): Main queue name
- `message_id` (string, required): Message ID to requeue

**Returns:** True if successful

**Note:** ⚠️ Both AWS SQS and Azure Service Bus implementations are not yet complete.

##### `dead_letter_purge(queue_name) -> bool`

Purges all messages from the dead letter queue.

**Parameters:**
- `queue_name` (string, required): Main queue name (DLQ is auto-resolved)

**Returns:** True if successful

**Note:** ⚠️ On AWS SQS this routes through the (still-stubbed) queue `purge`, so it is currently a no-op; on Azure Service Bus it drains the DLQ for real.

##### `get_client_info() -> dict`

Returns information about the client.

**Returns:** Dictionary containing client information

---

### Data Structures

#### Queue Object

Represents a message queue with unified properties across services.

**Properties:**
- `name` (string): Queue name
- `service_type` (string): Service type ("aws_sqs" or "azure_servicebus")
- `url` (string): Service-specific queue URL/identifier
- `message_count` (int): Number of messages in queue
- `lock_duration` (int): Message lock duration in seconds
- `retention_period` (int): Message retention period in seconds
- `max_delivery_count` (int): Maximum delivery attempts before DLQ
- `dead_letter_config` (dict): Dead letter queue configuration
- `enable_sessions` (bool): Whether message sessions are enabled
- `duplicate_detection` (dict): Duplicate detection configuration
- `max_queue_size` (int): Maximum queue size in bytes
- `created_time` (string): Queue creation timestamp
- `modified_time` (string): Last modification timestamp

#### MessageResult Object

Represents a message received from or sent to a queue.

**Properties:**
- `message_id` (string): Unique message identifier
- `body` (string): Message body content
- `properties` (dict): Custom message properties/attributes
- `session_id` (string): Session ID for message ordering
- `correlation_id` (string): Correlation ID for request-response
- `reply_to` (string): Reply queue name
- `enqueue_time` (string): When message was enqueued
- `scheduled_time` (string): Scheduled delivery time (if any)
- `lock_expires_at` (string): When message lock expires
- `delivery_count` (int): Number of delivery attempts
- `time_to_live` (int): Message TTL in seconds
- `receipt_handle` (string): Service-specific receipt handle
- `success` (bool): Whether operation was successful
- `error` (string): Error message (if any)

---

### Implementation Status

#### ✅ Fully Implemented
- Basic queue operations (create, delete, list, get, exists)
- Message send and receive operations
- Message delete on AWS SQS (Azure delete-by-ID always reports `False` — see below)
- Dead letter queue receive (both services); DLQ purge on Azure (AWS DLQ purge is still a no-op stub)
- Client connection and configuration
- Azure Service Bus core functionality

#### ⚠️ Partially Implemented (stubs that return mock data — see ⚠️ TODO in the matrix)
- **AWS SQS**: purge, lock/unlock, batch_send, dead_letter_requeue
- **Azure Service Bus**: delete (always returns `False` — needs the original received-message object), cancel, peek, dead_letter_requeue

#### ❌ Unsupported (returns an `unsupported` error — the service has no equivalent / the API needs the original message object)
- **AWS SQS**: cancel, peek (SQS has no scheduled-message-cancel or peek API)
- **Azure Service Bus**: lock, unlock (lock renewal / abandonment need the original received-message object, not just a message ID)

#### 📋 Feature Compatibility Matrix

Legend: ✅ Full — implemented against the live service. ⚠️ TODO — stub that returns mock data without a real call (usually success; Azure `delete` returns all-`False`). ❌ Unsupported — returns an `unsupported` error.

| Feature | AWS SQS | Azure Service Bus |
|---------|---------|-------------------|
| Queue Operations | ✅ Full | ✅ Full |
| Send Message | ✅ Full | ✅ Full |
| Receive Message | ✅ Full | ✅ Full |
| Delete Message | ✅ Full | ⚠️ TODO |
| Batch Send | ⚠️ TODO | ✅ Full |
| Schedule Message | ✅ Full | ✅ Full |
| Cancel Message | ❌ Unsupported | ⚠️ TODO |
| Peek Message | ❌ Unsupported | ⚠️ TODO |
| Lock Management | ⚠️ TODO | ❌ Unsupported |
| DLQ Receive | ✅ Full | ✅ Full |
| DLQ Requeue | ⚠️ TODO | ⚠️ TODO |
| DLQ Purge | ⚠️ TODO | ✅ Full |
| Queue Purge | ⚠️ TODO | ✅ Full |

## 🎯 Quick Start

### Basic Usage

```python
load("mq", "connect")

def main():
    # Connect to your message queue service
    client = connect(
        service_type="azure_servicebus",
        connection_string="Endpoint=sb://..."
    )
    
    # Create a queue
    queue = client.create_queue("orders")
    
    # Send a message
    result = client.send("orders", "New order received")
    print("Message sent: {}".format(result["message_id"]))
    
    # Receive and process messages
    messages = client.receive("orders", max_count=10)
    for msg in messages:
        print("Processing: {}".format(msg["body"]))
        # Process message here...
        client.delete("orders", msg["message_id"])

main()
```

### Connection Examples

```python
load("mq", "connect", "get_supported_services")

def main():
    # Check supported services
    print("Supported: {}".format(get_supported_services()))
    
    # AWS SQS
    aws_client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2",
        aws_access_key="YOUR_ACCESS_KEY",
        aws_secret_key="YOUR_SECRET_KEY"
    )
    
    # Azure Service Bus
    azure_client = connect(
        service_type="azure_servicebus", 
        connection_string="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
    )
    
    # Auto-detect (uses first available credentials)
    auto_client = connect(aws_region="us-east-1")  # Will use AWS SQS

main()
```

### More Examples

```python
# Dead Letter Queue Example
queue = client.create_queue(
    "orders",
    max_delivery_count=3,
    dead_letter_config={
        "enabled": True,
        "queue_name": "orders-dlq",
        "max_delivery_count": 3
    }
)

# Batch sending
messages = [
    {"body": "Order #1", "properties": {"priority": "high"}},
    {"body": "Order #2", "properties": {"priority": "normal"}}
]
results = client.batch_send("orders", messages)

# Scheduled messages
client.schedule(
    "reminders",
    "Meeting reminder", 
    "2024-12-31T09:00:00Z",
    properties={"type": "meeting"}
)
```

## 🚨 Service-Specific Notes

### AWS SQS
- Maximum message delay: 15 minutes
- Maximum batch size: 10 messages  
- FIFO queues require `.fifo` suffix
- Message deduplication only for FIFO queues

### Azure Service Bus
- Message scheduling up to 7 days in advance
- Sessions enable ordered processing
- Built-in duplicate detection per queue
- Native dead letter queue support

## 📈 Performance Tips

1. **Use batch operations** for sending multiple messages
2. **Configure appropriate timeouts** for your use case
3. **Enable long polling** to reduce costs and improve responsiveness
4. **Use sessions/FIFO queues** only when message ordering is required
5. **Monitor dead letter queues** for failed message processing
6. **Set appropriate message TTL** to prevent queue bloat

## 🧪 Testing Considerations

When writing tests for your message queue operations, consider using shorter timeouts for faster test execution:

```python
# For testing: Use shorter lock duration to speed up tests
test_queue = client.create_queue(
    "test-queue",
    lock_duration=5,        # 5 seconds instead of default 30
    max_delivery_count=2    # Lower threshold for DLQ testing
)

# Production: Use default values for reliability
prod_queue = client.create_queue("prod-queue")  # Uses 30-second default
```

### Testing Best Practices

1. **Use explicit short timeouts** in test queues to speed up test execution
2. **Clean up test queues** after each test to avoid conflicts
3. **Use unique queue names** with timestamps to prevent collisions
4. **Test DLQ behavior** with lower max_delivery_count values
5. **Mock external dependencies** when testing queue integration

## 🐛 Troubleshooting

### Common Issues

**Connection timeouts:**
- Increase the `timeout` configuration
- Check network connectivity
- Verify credentials and permissions

**Message not appearing:**
- Check visibility timeout settings
- Verify queue exists and is correct
- Check dead letter queue for failed messages

**Authentication errors:**
- Verify AWS credentials or Azure connection string
- Check IAM permissions for AWS SQS
- Verify shared access policy for Azure Service Bus

For more detailed troubleshooting, enable debug logging and check the error messages returned by the module functions.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.