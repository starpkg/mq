# 🔗 MQ Module for Starlark

[![Go Version](https://img.shields.io/github/go-mod/go-version/starpkg/mq)](https://golang.org/doc/devel/release.html)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/starpkg/mq)](https://goreportcard.com/report/github.com/starpkg/mq)

A powerful and unified Starlark module for message queue operations, providing seamless integration with AWS SQS and Azure Service Bus through a consistent API.

## 🚀 Features

- **Unified API**: Single interface for multiple message queue services
- **Multi-Cloud Support**: AWS SQS and Azure Service Bus integration
- **Auto-Detection**: Automatically detect service type from configuration
- **Queue Management**: Create, delete, list, and manage queues
- **Message Operations**: Send, receive, delete, and schedule messages
- **Batch Operations**: Efficient batch message processing
- **Dead Letter Queues**: Built-in support for message failure handling
- **Session Support**: Ordered message processing (FIFO queues/sessions)
- **Properties & Metadata**: Rich message metadata and custom properties
- **Type Safety**: Strong typing with comprehensive error handling

## 📦 Installation

```bash
go get github.com/starpkg/mq
```

## 🔧 Configuration

The module supports configuration through multiple methods with clear precedence:

1. **Explicit values** (highest priority)
2. **Dynamic getters**
3. **Environment variables**
4. **Default values** (lowest priority)

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

## 🎯 Usage

### Basic Connection

```python
load("mq", "connect", "get_supported_services")

def main():
    # Get supported services
    services = get_supported_services()
    print("Supported services: {}".format(services))
    
    # Connect to AWS SQS
    client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2",
        aws_access_key="YOUR_ACCESS_KEY",
        aws_secret_key="YOUR_SECRET_KEY"
    )
    
    # Connect to Azure Service Bus
    azure_client = connect(
        service_type="azure_servicebus",
        connection_string="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
    )
    
    # Auto-detection based on provided credentials
    auto_client = connect(
        service_type="auto",
        aws_region="us-west-2",
        aws_access_key="YOUR_ACCESS_KEY",
        aws_secret_key="YOUR_SECRET_KEY"
    )

main()
```

### Queue Operations

```python
load("mq", "connect")

def main():
    client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2",
        aws_access_key="YOUR_ACCESS_KEY",
        aws_secret_key="YOUR_SECRET_KEY"
    )
    
    # Create a queue
    queue = client.create_queue(
        "my-test-queue",
        lock_duration=60,
        retention_period=86400,  # 1 day
        max_delivery_count=5
    )
    print("Created queue: {}".format(queue["name"]))
    
    # List queues
    queues = client.list_queues(prefix="my-")
    print("Found {} queues".format(len(queues)))
    
    # Check if queue exists
    if client.exists("my-test-queue"):
        print("Queue exists!")
    
    # Get queue information
    info = client.get_queue("my-test-queue")
    print("Queue URL: {}".format(info["url"]))
    
    # Delete queue (cleanup)
    client.delete_queue("my-test-queue")

main()
```

### Message Operations

```python
load("mq", "connect")
load("time", "now")

def main():
    client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2",
        aws_access_key="YOUR_ACCESS_KEY",
        aws_secret_key="YOUR_SECRET_KEY"
    )
    
    queue_name = "my-message-queue"
    
    # Create queue first
    client.create_queue(queue_name)
    
    # Send a message
    result = client.send(
        queue_name,
        "Hello, World!",
        properties={"sender": "starlark", "priority": "high"},
        correlation_id="req-123",
        time_to_live=3600  # 1 hour
    )
    print("Sent message: {}".format(result["message_id"]))
    
    # Send scheduled message
    future_time = "2024-12-31T23:59:59Z"
    scheduled = client.schedule(
        queue_name,
        "Happy New Year!",
        future_time,
        properties={"event": "new_year"}
    )
    print("Scheduled message: {}".format(scheduled["message_id"]))
    
    # Receive messages
    messages = client.receive(
        queue_name,
        max_count=10,
        wait_time=5,
        lock_duration=30
    )
    
    for msg in messages:
        print("Received: {}".format(msg["body"]))
        print("Properties: {}".format(msg["properties"]))
        
        # Process message...
        
        # Delete after processing
        client.delete(queue_name, [msg["receipt_handle"]])
    
    # Batch send messages
    batch_messages = [
        {
            "body": "Message 1",
            "properties": {"batch": "1"},
            "correlation_id": "batch-1"
        },
        {
            "body": "Message 2", 
            "properties": {"batch": "2"},
            "correlation_id": "batch-2"
        }
    ]
    
    results = client.batch_send(queue_name, batch_messages)
    print("Sent {} messages in batch".format(len(results)))

main()
```

### Dead Letter Queue Configuration

```python
load("mq", "connect")

def main():
    client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2",
        aws_access_key="YOUR_ACCESS_KEY",
        aws_secret_key="YOUR_SECRET_KEY"
    )
    
    # Create queue with dead letter queue
    queue = client.create_queue(
        "main-queue",
        max_delivery_count=3,
        dead_letter_config={
            "enabled": True,
            "queue_name": "main-queue-dlq",
            "max_delivery_count": 3
        }
    )
    
    # Send a message that will fail processing
    client.send("main-queue", "This will fail processing")
    
    # Simulate failed processing by receiving and not deleting
    for attempt in range(4):  # Exceed max delivery count
        messages = client.receive("main-queue", max_count=1)
        if len(messages) > 0:
            print("Attempt {}: {}".format(attempt + 1, messages[0]["body"]))
            # Don't delete - let it go back to queue
    
    # Check dead letter queue
    dlq_messages = client.dead_letter_receive("main-queue", max_count=10)
    print("Messages in DLQ: {}".format(len(dlq_messages)))
    
    # Requeue from dead letter queue
    for msg in dlq_messages:
        client.dead_letter_requeue("main-queue", msg["message_id"])
        print("Requeued message: {}".format(msg["message_id"]))

main()
```

### Azure Service Bus Sessions

```python
load("mq", "connect")

def main():
    client = connect(
        service_type="azure_servicebus",
        connection_string="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
    )
    
    # Create queue with sessions enabled
    queue = client.create_queue(
        "session-queue",
        enable_sessions=True,
        duplicate_detection=True,
        duplicate_window_secs=300
    )
    
    # Send messages with session ID for ordering
    session_id = "customer-123"
    
    messages = [
        "Order created",
        "Payment processed", 
        "Order shipped",
        "Order delivered"
    ]
    
    for i, msg in enumerate(messages):
        result = client.send(
            "session-queue",
            msg,
            session_id=session_id,
            message_id="order-{}-step-{}".format(session_id, i),
            properties={"step": i + 1}
        )
        print("Sent: {}".format(result["message_id"]))
    
    # Receive messages (they'll be delivered in order)
    session_messages = client.receive(
        "session-queue",
        max_count=10
    )
    
    for msg in session_messages:
        print("Processing step {}: {}".format(
            msg["properties"]["step"], 
            msg["body"]
        ))

main()
```

## 🔍 API Reference

### Module Functions

#### `connect(**kwargs) -> Client`
Creates a new message queue client.

**Parameters:**
- `service_type` (str): Service type ("aws_sqs", "azure_servicebus", "auto")
- `connection_string` (str): Azure Service Bus connection string
- `aws_region` (str): AWS region
- `aws_access_key` (str): AWS access key ID
- `aws_secret_key` (str): AWS secret access key
- `aws_session_token` (str): AWS session token (optional)
- `timeout` (int): Connection timeout in seconds
- `max_retries` (int): Maximum retry attempts

#### `get_supported_services() -> List[str]`
Returns list of supported message queue services.

#### `get_client_info(client) -> Dict`
Returns information about the given client.

### Client Methods

#### Queue Operations
- `create_queue(name, **options) -> Queue`
- `delete_queue(name) -> bool`
- `list_queues(prefix="") -> List[Queue]`
- `get_queue(name) -> Queue`
- `exists(name) -> bool`
- `purge(name) -> bool`
- `get_info(name) -> Queue`

#### Message Operations
- `send(queue_name, body, **options) -> MessageResult`
- `receive(queue_name, **options) -> List[MessageResult]`
- `delete(queue_name, message_ids) -> List[bool]`
- `batch_send(queue_name, messages) -> List[MessageResult]`
- `schedule(queue_name, body, scheduled_time, **options) -> MessageResult`
- `cancel(queue_name, message_id) -> bool`
- `peek(queue_name, max_count=1) -> List[MessageResult]`

#### Message Lock Management
- `lock(queue_name, message_id, duration) -> bool`
- `unlock(queue_name, message_id) -> bool`

#### Dead Letter Queue Operations
- `dead_letter_receive(queue_name, max_count=10) -> List[MessageResult]`
- `dead_letter_requeue(queue_name, message_id) -> bool`
- `dead_letter_purge(queue_name) -> bool`

## 🛠️ Development

### Building

```bash
go build .
```

### Testing

```bash
# Run all tests
go test -v ./...

# Run specific test
go test -v -run TestStarlarkScripts/test-basic_module_load.star
```

### Linting

```bash
golangci-lint run
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Projects

- [Starlark Language](https://github.com/bazelbuild/starlark)
- [Starlet](https://github.com/1set/starlet) - Starlark Libraries and Extensions
- [AWS SDK for Go](https://github.com/aws/aws-sdk-go)
- [Azure SDK for Go](https://github.com/Azure/azure-sdk-for-go)

## ⚠️ Compatibility Notes

### AWS SQS
- Maximum message delay: 15 minutes
- Maximum batch size: 10 messages
- FIFO queues require `.fifo` suffix
- Message deduplication only available for FIFO queues

### Azure Service Bus
- Sessions required for message ordering
- Duplicate detection configurable per queue
- Built-in dead letter queue support
- Message scheduling up to 7 days in advance

## 📈 Performance Tips

1. **Use batch operations** for sending multiple messages
2. **Configure appropriate timeouts** for your use case
3. **Enable long polling** to reduce costs and improve responsiveness
4. **Use sessions/FIFO queues** only when message ordering is required
5. **Monitor dead letter queues** for failed message processing
6. **Set appropriate message TTL** to prevent queue bloat

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