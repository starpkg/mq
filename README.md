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

## 🚀 Quick Start

### Implementation Status

- **✅ AWS SQS**: **Production Ready** with real AWS SDK v1 integration and automatic test mode detection
- **✅ Azure Service Bus**: **Production Ready** with real Azure SDK integration and comprehensive thread-safety

### Basic Usage

```python
load("mq", "connect")

def main():
    # Connect to AWS SQS (production ready with test mode for development)
    client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2",
        aws_access_key="your-aws-access-key",     # Real credentials for production
        aws_secret_key="your-aws-secret-key"     # Use "test-*" credentials for testing
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
    
    print("Message sent with ID: {}".format(result["message_id"]))
    
    # Receive messages with unified options
    messages = client.receive(
        "my-work-queue", 
        max_count=5,
        wait_time=20,
        lock_duration=30
    )
    
    for message in messages:
        print("Processing message: {}".format(message["body"]))
        print("Message properties: {}".format(message["properties"]))
        
        # Process the message...
        success = process_message(message)
        
        if success:
            # Delete message after successful processing
            client.delete("my-work-queue", message["message_id"])
        else:
            # Extend lock duration for retry
            client.lock("my-work-queue", message["message_id"], 60)
            print("Extended lock for message {}, will retry".format(message["message_id"]))

def process_message(message):
    # Simulate message processing
    print("Processing: {}".format(message["body"]))
    return True

main()
```

## 📋 Service Support

The `mq` module provides a unified interface for queue operations across AWS SQS and Azure Service Bus:

### Core Features Compatibility Matrix

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

## 🏗️ Implementation Details

### AWS SQS (Production Ready)

The AWS SQS implementation is **fully functional** and production-ready:

- **✅ Real AWS SDK Integration**: Uses AWS SDK for Go v1 for full compatibility with Go 1.18
- **✅ Automatic Account Detection**: Uses AWS STS to get real account ID for proper ARN construction
- **✅ Complete Queue Management**: Create, delete, list, and manage queues with all SQS features
- **✅ Message Operations**: Send, receive, delete, batch operations with proper error handling
- **✅ Dead Letter Queue Support**: Full DLQ management with automatic DLQ creation
- **✅ FIFO Queue Support**: Handles session-based ordering through FIFO queues
- **✅ Service Adaptation**: Properly handles AWS SQS specific limitations and features
- **✅ Thread Safety**: Safe for concurrent use
- **✅ Error Handling**: Comprehensive error mapping and reporting

### Azure Service Bus (Production Ready)

The Azure Service Bus implementation is **fully functional** and production-ready:

- **✅ Real Azure SDK Integration**: Uses Azure SDK for Go v1.4.1 compatible with Go 1.18
- **✅ Thread-Safe Architecture**: Uses mutexes following Azure SDK best practices from reference implementation
- **✅ Complete Interface**: All client methods implemented with proper signatures and real SDK calls
- **✅ Queue Management**: Full queue lifecycle management with Service Bus admin operations
- **✅ Message Operations**: Send, receive, delete with proper Service Bus features
- **✅ Session Support**: Built-in session management for ordered message processing
- **✅ Dead Letter Queue**: Native DLQ support with requeue capabilities

### Production Usage

Both implementations use real cloud service APIs:

```python
# AWS SQS with real credentials and automatic account ID detection
client = connect(
    service_type="aws_sqs",
    aws_region="us-west-2",
    aws_access_key="AKIAIOSFODNN7EXAMPLE",  # Real AWS credentials required
    aws_secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

# Azure Service Bus with real connection string
client = connect(
    service_type="azure_servicebus",
    connection_string="Endpoint=sb://your-namespace.servicebus.windows.net/;..."
)

# Auto-detection based on provided parameters
client = connect(
    service_type="auto",
    aws_region="us-west-2",
    aws_access_key="AKIAIOSFODNN7EXAMPLE",
    aws_secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)
```

## 🔧 Configuration

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

## 📖 API Reference

### Module Functions

#### `connect()`

Creates a message queue client connection.

```python
client = connect(
    service_type="auto",         # "aws_sqs", "azure_servicebus", "auto"
    connection_string=None,      # Azure Service Bus connection string
    aws_region=None,             # AWS region for SQS
    aws_access_key=None,         # AWS access key ID
    aws_secret_key=None,         # AWS secret access key
    timeout=30,                  # Connection timeout in seconds
    max_retries=3                # Maximum retry attempts
)
```

#### `get_supported_services()`

Returns a list of supported message queue services.

```python
services = get_supported_services()
# Returns: ["aws_sqs", "azure_servicebus"]
```

#### `get_client_info(client)`

Returns information about a client connection.

```python
info = get_client_info(client)
# Returns: {"service_type": "aws_sqs", "region": "us-west-2", ...}
```

## 🎯 Best Practices

### 1. Use Appropriate Batch Sizes

```python
# Good: Let the module auto-adapt batch sizes
results = client.batch_send(queue_name, large_message_list)

# The module automatically splits into service-appropriate batches:
# - AWS SQS: max 10 messages per batch
# - Azure Service Bus: max 100 messages per batch
```

### 2. Use Dead Letter Queues for Reliability

```python
# Always configure DLQ for production workloads
queue = client.create_queue(
    "production-queue",
    max_delivery_count=3,  # Reasonable retry limit
    dead_letter_config={
        "enabled": True,
        "queue_name": "production-dlq"
    }
)
```

### 3. Implement Proper Message Processing

```python
def process_messages_safely(client, queue_name):
    messages = client.receive(queue_name, max_count=10)
    
    for msg in messages:
        # Process message
        result = process_message(msg)
        
        if result:
            # Only delete after successful processing
            client.delete(queue_name, msg["message_id"])
        else:
            # Extend lock for retry
            client.lock(queue_name, msg["message_id"], 60)
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.