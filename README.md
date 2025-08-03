# 🔄 MQ - Message Queue Module for Starlark

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.18-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

A unified Starlark module for message queue operations across AWS SQS and Azure Service Bus, providing a consistent interface for common messaging patterns.

## 🚀 Features

### Unified API
- **Single Interface**: Work with AWS SQS and Azure Service Bus using the same API
- **Automatic Service Detection**: Auto-detect service type from configuration
- **Normalized Concepts**: Unified terminology across different services (lock_duration vs visibility_timeout)

### Core Operations
- **Queue Management**: Create, delete, list, and manage queues
- **Message Operations**: Send, receive, delete messages with unified options
- **Batch Processing**: Send/receive multiple messages efficiently
- **Message Scheduling**: Schedule messages for future delivery
- **Dead Letter Queues**: Handle failed messages with DLQ support

### AWS SQS Support ✅
- Standard and FIFO queues
- Message attributes and properties
- Visibility timeout management
- Dead letter queue configuration
- Batch operations (up to 10 messages)
- Message scheduling (up to 15 minutes)
- Content-based deduplication

### Azure Service Bus Support 🚧
- Queue and topic/subscription patterns
- Message sessions and correlation IDs
- Scheduled message delivery
- Message filtering and routing
- Dead letter queue management
- *(Coming in Phase 2)*

## 📦 Installation

Add the module to your project:

```bash
go get github.com/starpkg/mq
```

## 🔧 Configuration

The module supports multiple configuration methods with the following priority:

1. **Explicit values** (highest priority)
2. **Environment variables**
3. **Default values** (lowest priority)

### Environment Variables

#### Common Configuration
- `MQ_SERVICE_TYPE`: Service type (`aws_sqs`, `azure_servicebus`, `auto`)
- `MQ_TIMEOUT`: Connection timeout in seconds (default: 30)
- `MQ_MAX_RETRIES`: Maximum retry attempts (default: 3)

#### AWS SQS Configuration
- `AWS_REGION`: AWS region (default: us-east-1)
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_SESSION_TOKEN`: AWS session token (optional)

#### Azure Service Bus Configuration
- `MQ_CONNECTION_STRING`: Azure Service Bus connection string
- `MQ_AZURE_NAMESPACE`: Azure Service Bus namespace
- `MQ_AZURE_SHARED_KEY`: Azure Service Bus shared access key
- `MQ_AZURE_KEY_NAME`: Azure Service Bus key name

## 📚 Usage Examples

### Basic Queue Operations

```python
load("mq", "connect", "get_supported_services")

def main():
    # Get supported services
    services = get_supported_services()
    print("Supported services:", services)
    
    # Connect to AWS SQS
    client = connect(service_type="aws_sqs", aws_region="us-west-2")
    
    # Create a queue
    queue = client.create_queue("my-queue", {
        "lock_duration": 300,  # 5 minutes
        "retention_period": 1209600,  # 14 days
        "max_delivery_count": 3
    })
    print("Created queue:", queue.name)
    
    # Send a message
    result = client.send_message("my-queue", "Hello, World!", {
        "properties": {"source": "starlark", "priority": "high"}
    })
    print("Message sent:", result.message_id)
    
    # Receive messages
    messages = client.receive_messages("my-queue", max_count=10)
    for msg in messages:
        print("Received:", msg.body)
        # Process message...
        client.delete_message("my-queue", msg.receipt_handle)
    
    # Clean up
    client.close()

main()
```

### FIFO Queue with Sessions

```python
load("mq", "connect")

def main():
    client = connect(service_type="aws_sqs")
    
    # Create FIFO queue
    queue = client.create_queue("my-orders.fifo", {
        "enable_sessions": True,
        "duplicate_detection": {
            "enabled": True,
            "window_seconds": 300
        }
    })
    
    # Send messages with session grouping
    orders = [
        {"id": "order-1", "customer": "alice"},
        {"id": "order-2", "customer": "alice"},
        {"id": "order-3", "customer": "bob"}
    ]
    
    for order in orders:
        client.send_message("my-orders.fifo", order["id"], {
            "session_id": order["customer"],
            "deduplication_id": order["id"],
            "properties": {"customer": order["customer"]}
        })
    
    print("Orders sent with session grouping")

main()
```

### Scheduled Messages

```python
load("mq", "connect")
load("time", "now")

def main():
    client = connect(service_type="aws_sqs")
    
    # Schedule a message for 10 minutes from now
    future_time = now().unix() + 600
    scheduled_time = time.unix(future_time).format("2006-01-02T15:04:05Z")
    
    result = client.send_scheduled_message(
        "reminder-queue",
        "Time for your meeting!",
        scheduled_time,
        {"properties": {"type": "reminder"}}
    )
    
    print("Scheduled message:", result.message_id)

main()
```

### Batch Processing

```python
load("mq", "connect")

def main():
    client = connect(service_type="aws_sqs")
    
    # Prepare batch messages
    messages = []
    for i in range(5):
        messages.append({
            "body": "Batch message {}".format(i),
            "properties": {"batch_id": "batch-001", "sequence": str(i)}
        })
    
    # Send batch
    results = client.send_messages_batch("batch-queue", messages)
    
    for i, result in enumerate(results):
        if result.success:
            print("Message {} sent: {}".format(i, result.message_id))
        else:
            print("Message {} failed: {}".format(i, result.error))

main()
```

### Dead Letter Queue Handling

```python
load("mq", "connect")

def main():
    client = connect(service_type="aws_sqs")
    
    # Create queue with DLQ configuration
    queue = client.create_queue("process-queue", {
        "dead_letter_config": {
            "enabled": True,
            "max_delivery_count": 3,
            "queue_name": "process-queue-dlq"
        }
    })
    
    # Check for dead letter messages
    dlq_messages = client.get_dead_letter_messages("process-queue", max_count=10)
    
    for msg in dlq_messages:
        print("Dead letter message:", msg.body)
        print("Delivery count:", msg.delivery_count)
        
        # Decide whether to reprocess or discard
        if should_reprocess(msg):
            client.reprocess_dead_letter_message("process-queue", msg.message_id)
        else:
            client.delete_message("process-queue-dlq", msg.receipt_handle)

def should_reprocess(msg):
    # Custom logic to determine if message should be reprocessed
    return msg.delivery_count < 5

main()
```

## 🔍 Feature Support Matrix

| Feature | AWS SQS | Azure Service Bus |
|---------|---------|------------------|
| Basic Queues | ✅ | 🚧 Coming Soon |
| Topics/Subscriptions | ❌ | 🚧 Coming Soon |
| FIFO/Sessions | ✅ | 🚧 Coming Soon |
| Batch Operations | ✅ | 🚧 Coming Soon |
| Scheduled Messages | ✅ (15min max) | 🚧 Coming Soon |
| Dead Letter Queues | ✅ | 🚧 Coming Soon |
| Message Filtering | ❌ | 🚧 Coming Soon |
| Peek Messages | ❌ | 🚧 Coming Soon |

## 🛠️ Development

### Running Tests

```bash
# Run all tests
go test -v .

# Run Starlark integration tests
go test -v . -run TestStarlarkScripts
```

### Test Scripts

The module includes Starlark test scripts in the `test/mq/` directory:

- `test-*.star`: Scripts that should succeed
- `panic-*.star`: Scripts that should fail with errors

## 📋 Roadmap

### Phase 1: Foundation (Current) ✅
- [x] AWS SQS basic operations
- [x] Unified configuration system
- [x] Error handling and normalization
- [x] Starlark integration
- [x] FIFO queue support

### Phase 2: Azure Service Bus 🚧
- [ ] Azure Service Bus queue operations
- [ ] Topic/subscription support
- [ ] Message filtering and routing
- [ ] Session management

### Phase 3: Advanced Features 📋
- [ ] Connection pooling and retry logic
- [ ] Message transformation pipeline
- [ ] Monitoring and metrics
- [ ] Local testing/mocking support

## 🤝 Contributing

We welcome contributions! Please feel free to submit issues, feature requests, or pull requests.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Projects

- [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)
- [Azure SDK for Go](https://github.com/Azure/azure-sdk-for-go)
- [Starlark Language](https://github.com/bazelbuild/starlark)
- [StarPkg Base](https://github.com/starpkg/base)