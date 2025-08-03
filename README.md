# 🔄 MQ - Message Queue Module for Starlark

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
