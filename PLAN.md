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

## Provider Compatibility Overview

The MQ module provides a unified API across AWS SQS and Azure Service Bus, but not all features are available on both providers. The tables below show what's supported where.

### Function Support by Provider

| Function | AWS SQS | Azure Service Bus | Notes |
|----------|---------|-------------------|-------|
| `configure()` | ✅ | ✅ | Different config parameters |
| `send()` | ✅ | ✅ | Some parameters provider-specific |
| `receive()` | ✅ | ✅ | Different polling mechanisms |
| `delete()` | ✅ | ✅ | Different handle formats |
| `peek()` | ✅ | ✅ | Implementation differs |
| `send_batch()` | ✅ | ✅ | Different batch limits |
| `delete_batch()` | ✅ | ✅ | - |
| `create_queue()` | ✅ | ✅ | Different queue properties |
| `delete_queue()` | ✅ | ✅ | - |
| `list_queues()` | ✅ | ✅ | - |
| `get_queue_attributes()` | ✅ | ✅ | Different attribute sets |
| `purge_queue()` | ✅ | ✅ | - |
| `change_message_visibility()` | ✅ | ✅ | - |
| `create_topic()` | ❌ | ✅ | SQS doesn't support topics |
| `create_subscription()` | ❌ | ✅ | SQS doesn't support subscriptions |
| `delete_topic()` | ❌ | ✅ | SQS doesn't support topics |
| `delete_subscription()` | ❌ | ✅ | SQS doesn't support subscriptions |
| `list_topics()` | ❌ | ✅ | SQS doesn't support topics |
| `list_subscriptions()` | ❌ | ✅ | SQS doesn't support subscriptions |
| `get_dead_letter_messages()` | ✅ | ✅ | Different implementations |
| `redrive_messages()` | ✅ | ✅ | Different redrive mechanisms |

### Parameter Support by Function

#### `send()` Function Parameters

| Parameter | AWS SQS | Azure Service Bus | Notes |
|-----------|---------|-------------------|-------|
| `queue_name` | ✅ | ✅ | Queue or topic name |
| `message` | ✅ | ✅ | Message body |
| `attributes` | ✅ | ✅ | Different attribute types |
| `delay_seconds` | ✅ | ❌ | Use `scheduled_time` for Azure |
| `message_group_id` | ✅ (FIFO only) | ❌ | Use `session_id` for Azure |
| `message_deduplication_id` | ✅ (FIFO only) | ❌ | Azure has built-in deduplication |
| `scheduled_time` | ❌ | ✅ | Use `delay_seconds` for AWS |
| `session_id` | ❌ | ✅ | Use `message_group_id` for AWS FIFO |
| `content_type` | ❌ | ✅ | AWS uses attributes |
| `correlation_id` | ❌ | ✅ | AWS uses attributes |
| `reply_to` | ❌ | ✅ | AWS uses attributes |
| `time_to_live` | ❌ | ✅ | AWS uses queue settings |
| `priority` | ❌ | ✅ | AWS doesn't support priorities |
| `label` | ❌ | ✅ | Azure-specific feature |

#### `receive()` Function Parameters

| Parameter | AWS SQS | Azure Service Bus | Notes |
|-----------|---------|-------------------|-------|
| `queue_name` | ✅ | ✅ | Queue or subscription name |
| `max_messages` | ✅ (1-10) | ✅ (1-256) | Different limits |
| `wait_time_seconds` | ✅ (0-20) | ✅ (0-∞) | Long polling |
| `visibility_timeout` | ✅ | ✅ | Different default values |
| `peek_only` | ✅ | ✅ | - |
| `session_id` | ❌ | ✅ | Session-based receiving |
| `receive_timeout` | ✅ | ✅ | - |

#### `create_queue()` Function Parameters

| Parameter | AWS SQS | Azure Service Bus | Notes |
|-----------|---------|-------------------|-------|
| `queue_name` | ✅ | ✅ | - |
| `fifo` | ✅ | ❌ | Azure queues are FIFO by default |
| `visibility_timeout` | ✅ | ✅ | Different ranges |
| `message_retention_period` | ✅ | ✅ | Different limits |
| `max_message_size` | ✅ | ✅ | Different limits |
| `dead_letter_queue` | ✅ | ✅ | Different configuration |
| `max_receive_count` | ✅ | ✅ | - |
| `duplicate_detection` | ❌ | ✅ | SQS FIFO has built-in dedup |
| `requires_session` | ❌ | ✅ | Azure-specific feature |

### Provider-Specific Limitations

#### AWS SQS Limitations

| Feature | Limitation | Workaround |
|---------|------------|------------|
| Topics/Subscriptions | Not supported | Use separate queues or SNS+SQS |
| Message Priority | Not supported | Use separate queues by priority |
| Scheduled Messages | Max 15 minutes delay | Use external scheduler for longer delays |
| Message Size | 256KB max | Use S3 for large payloads |
| Batch Size | 10 messages max | Process in multiple batches |
| FIFO Throughput | 300 TPS with batching | Use standard queues for higher throughput |
| Content Type | Not natively supported | Use message attributes |
| Sessions | FIFO message groups only | Limited compared to Azure sessions |

#### Azure Service Bus Limitations

| Feature | Limitation | Workaround |
|---------|------------|------------|
| Message Group ID | Sessions only | Use sessions for ordering |
| Delay Seconds | Use scheduled messages | Different API pattern |
| FIFO Queues | All queues maintain order | Cannot disable ordering |
| Queue Names | Strict naming rules | Validate names before creation |
| Connection String | Required for auth | Cannot use individual credentials |
| Peek Lock Duration | Different from visibility timeout | Use appropriate timeouts |

### Cross-Provider Feature Mapping

| AWS SQS Feature | Azure Service Bus Equivalent | Notes |
|-----------------|------------------------------|-------|
| Standard Queue | Queue | Basic message queue |
| FIFO Queue | Queue with Sessions | Ordering guaranteed |
| Message Groups | Sessions | Message grouping/ordering |
| Dead Letter Queue | Dead Letter Queue | Failed message handling |
| Delay Seconds | Scheduled Messages | Future delivery |
| Long Polling | Receive with Timeout | Efficient polling |
| Message Attributes | Message Properties | Metadata |
| SNS + SQS | Topics + Subscriptions | Pub/sub pattern |

## Configuration Options

| Option | Type | Description | Default | AWS SQS | Azure Service Bus |
|--------|------|-------------|---------|---------|-------------------|
| `provider` | string | Service provider ("aws_sqs", "azure_bus") | "" | ✅ | ✅ |
| `connection_string` | string | Azure Service Bus connection string | "" | ❌ | ✅ |
| `aws_region` | string | AWS region for SQS | "us-east-1" | ✅ | ❌ |
| `aws_access_key_id` | string | AWS access key ID | "" | ✅ | ❌ |
| `aws_secret_access_key` | string | AWS secret access key | "" | ✅ | ❌ |
| `aws_session_token` | string | AWS session token (optional) | "" | ✅ | ❌ |
| `default_timeout` | int | Default timeout in seconds | 30 | ✅ | ✅ |
| `max_retries` | int | Maximum retry attempts | 3 | ✅ | ✅ |
| `visibility_timeout` | int | Default visibility timeout in seconds | 30 | ✅ | ✅ |
| `wait_time_seconds` | int | Long polling wait time | 0 | ✅ | ✅ |
| `max_messages` | int | Maximum messages per receive operation | 10 | ✅ | ✅ |

## Starlark API Design

### Core Module Functions

```python
# Configuration functions
configure(provider, **config) -> None      # Configure the MQ provider
get_config(key) -> value                   # Get configuration value

# Core messaging operations
send(queue_name, message, **kwargs) -> MessageID
receive(queue_name, **kwargs) -> [Message]
delete(queue_name, receipt_handle) -> bool
peek(queue_name, **kwargs) -> [Message]

# Batch operations
send_batch(queue_name, messages, **kwargs) -> BatchResult
delete_batch(queue_name, receipt_handles) -> BatchResult

# Queue management
create_queue(queue_name, **kwargs) -> QueueInfo
delete_queue(queue_name) -> bool
list_queues(prefix="") -> [string]
get_queue_attributes(queue_name) -> QueueAttributes
purge_queue(queue_name) -> bool

# Topic management (Azure Service Bus)
create_topic(topic_name, **kwargs) -> TopicInfo
create_subscription(topic_name, subscription_name, **kwargs) -> SubscriptionInfo
delete_topic(topic_name) -> bool
delete_subscription(topic_name, subscription_name) -> bool
list_topics(prefix="") -> [string]
list_subscriptions(topic_name) -> [string]

# Utility functions
change_message_visibility(queue_name, receipt_handle, timeout) -> bool
get_dead_letter_messages(queue_name) -> [Message]
redrive_messages(source_queue, target_queue, max_messages=10) -> int
```

## Provider-Specific Usage Guidance

### Writing Cross-Provider Compatible Code

When writing Starlark scripts that should work with both providers, follow these patterns:

```python
load("mq", "configure", "send", "receive", "get_config")
load("time")

def send_delayed_message(queue, message, delay_minutes=5):
    """Send a delayed message that works on both providers"""
    provider = get_config("provider")
    
    if provider == "aws_sqs":
        # AWS SQS uses delay_seconds (max 15 minutes)
        delay_seconds = min(delay_minutes * 60, 900)  # Cap at 15 minutes
        return send(queue, message, delay_seconds=delay_seconds)
    elif provider == "azure_bus":
        # Azure Service Bus uses scheduled_time
        future_time = time.now().add(minutes=delay_minutes)
        return send(queue, message, scheduled_time=future_time)
    else:
        fail("Unsupported provider: {}".format(provider))

def send_ordered_message(queue, message, group_key):
    """Send an ordered message that works on both providers"""
    provider = get_config("provider")
    
    if provider == "aws_sqs":
        # AWS SQS FIFO queues use message_group_id
        if not queue.endswith(".fifo"):
            fail("AWS SQS ordered messages require FIFO queue (*.fifo)")
        return send(queue, message, 
                   message_group_id=group_key,
                   message_deduplication_id="{}-{}".format(group_key, time.now().unix))
    elif provider == "azure_bus":
        # Azure Service Bus uses sessions
        return send(queue, message, session_id=group_key)
    else:
        fail("Unsupported provider: {}".format(provider))

def receive_ordered_messages(queue, group_key=None):
    """Receive ordered messages that works on both providers"""
    provider = get_config("provider")
    
    if provider == "aws_sqs":
        # AWS SQS FIFO automatically maintains order within message groups
        return receive(queue, max_messages=10)
    elif provider == "azure_bus":
        # Azure Service Bus uses session_id for ordering
        if group_key != None:
            return receive(queue, session_id=group_key, max_messages=10)
        else:
            return receive(queue, max_messages=10)
    else:
        fail("Unsupported provider: {}".format(provider))
```

### Provider Detection and Validation

```python
load("mq", "get_config")

def validate_provider_features():
    """Validate that required features are available on current provider"""
    provider = get_config("provider")
    
    required_features = {
        "topics": False,  # Set to True if your app needs topics
        "priorities": False,  # Set to True if your app needs message priorities
        "sessions": False,  # Set to True if your app needs sessions
        "large_batches": False  # Set to True if you need >10 messages per batch
    }
    
    if provider == "aws_sqs":
        unsupported = []
        if required_features["topics"]:
            unsupported.append("topics (use SNS+SQS or separate queues)")
        if required_features["priorities"]:
            unsupported.append("message priorities (use separate queues)")
        if required_features["sessions"]:
            unsupported.append("sessions (use FIFO message groups)")
        
        if len(unsupported) > 0:
            fail("AWS SQS doesn't support: {}".format(", ".join(unsupported)))
    
    elif provider == "azure_bus":
        unsupported = []
        if required_features["large_batches"]:
            print("Note: Azure Service Bus supports larger batches (256 messages)")
        
        if len(unsupported) > 0:
            fail("Azure Service Bus doesn't support: {}".format(", ".join(unsupported)))
    
    print("Provider {} supports all required features".format(provider))

def get_provider_limits():
    """Get provider-specific limits for planning"""
    provider = get_config("provider")
    
    if provider == "aws_sqs":
        return {
            "max_message_size": 256 * 1024,  # 256KB
            "max_batch_size": 10,
            "max_delay_seconds": 900,  # 15 minutes
            "max_visibility_timeout": 43200,  # 12 hours
            "max_retention_period": 1209600  # 14 days
        }
    elif provider == "azure_bus":
        return {
            "max_message_size": 1024 * 1024,  # 1MB
            "max_batch_size": 256,
            "max_delay_seconds": None,  # No limit with scheduled messages
            "max_visibility_timeout": 300,  # 5 minutes default
            "max_retention_period": 2419200  # 28 days
        }
    else:
        fail("Unknown provider: {}".format(provider))
```

### Error Handling for Provider Differences

```python
load("mq", "send", "get_config")

def safe_send_with_attributes(queue, message, attributes):
    """Safely send a message handling provider-specific attribute limitations"""
    provider = get_config("provider")
    
    # Validate attributes based on provider
    if provider == "aws_sqs":
        # AWS SQS has specific attribute value type requirements
        safe_attributes = {}
        for key, value in attributes.items():
            # AWS SQS attributes must be strings
            safe_attributes[key] = str(value)
        return send(queue, message, attributes=safe_attributes)
    
    elif provider == "azure_bus":
        # Azure Service Bus supports more attribute types
        return send(queue, message, attributes=attributes)
    
    else:
        fail("Unsupported provider: {}".format(provider))

def safe_create_queue_with_dlq(queue_name, dlq_name):
    """Create a queue with dead letter queue support for both providers"""
    provider = get_config("provider")
    
    if provider == "aws_sqs":
        # Create DLQ first
        dlq_result = create_queue(dlq_name)
        # Create main queue with DLQ reference
        return create_queue(queue_name, 
                           dead_letter_queue=dlq_name,
                           max_receive_count=3)
    
    elif provider == "azure_bus":
        # Azure Service Bus creates DLQ automatically
        return create_queue(queue_name, max_delivery_count=3)
    
    else:
        fail("Unsupported provider: {}".format(provider))
```

### Runtime Provider Capability Checks

```python
load("mq", "get_config")

def check_topic_support():
    """Check if current provider supports topics"""
    provider = get_config("provider")
    return provider == "azure_bus"

def check_message_priority_support():
    """Check if current provider supports message priorities"""
    provider = get_config("provider")
    return provider == "azure_bus"

def check_large_batch_support():
    """Check if current provider supports large batches (>10 messages)"""
    provider = get_config("provider")
    return provider == "azure_bus"

def get_max_batch_size():
    """Get maximum batch size for current provider"""
    provider = get_config("provider")
    if provider == "aws_sqs":
        return 10
    elif provider == "azure_bus":
        return 256
    else:
        return 1  # Conservative fallback

# Usage example
def send_large_batch_safely(queue, messages):
    """Send a large batch of messages respecting provider limits"""
    max_batch = get_max_batch_size()
    
    for i in range(0, len(messages), max_batch):
        batch = messages[i:i + max_batch]
        result = send_batch(queue, batch)
        print("Sent batch {}: {} messages".format(i // max_batch + 1, len(batch)))
```

### Migration Between Providers

The MQ module supports migrating messages and configurations between AWS SQS and Azure Service Bus:

```python
# Example: Migrating from AWS SQS to Azure Service Bus
load("mq", "configure", "send", "receive", "delete", "send_batch")
load("time")

def migrate_queue_messages(source_provider, target_provider, queue_name, target_queue=None):
    """Migrate messages from one provider to another"""
    
    if target_queue == None:
        target_queue = queue_name
    
    print("Starting migration from {} to {}".format(source_provider, target_provider))
    
    # Configure source provider
    configure(source_provider)
    
    # Receive all messages from source
    all_messages = []
    while True:
        messages = receive(queue_name, max_messages=10, wait_time_seconds=5)
        if len(messages) == 0:
            break
        
        for msg in messages:
            # Transform message for target provider
            transformed = transform_message_for_provider(msg, target_provider)
            all_messages.append(transformed)
            
            # Delete from source (commit the migration)
            delete(queue_name, msg.receipt_handle)
        
        print("Migrated {} messages...".format(len(all_messages)))
    
    # Configure target provider
    configure(target_provider)
    
    # Send messages to target
    batch_size = 10 if target_provider == "aws_sqs" else 100
    for i in range(0, len(all_messages), batch_size):
        batch = all_messages[i:i + batch_size]
        result = send_batch(target_queue, batch)
        print("Sent batch {}: {} successful, {} failed".format(
            i // batch_size + 1, 
            len(result.successful), 
            len(result.failed)
        ))
    
    print("Migration completed: {} messages migrated".format(len(all_messages)))

def transform_message_for_provider(msg, target_provider):
    """Transform message attributes for target provider compatibility"""
    
    if target_provider == "aws_sqs":
        # Transform Azure Service Bus message to AWS SQS format
        transformed = {
            "body": msg.body,
            "attributes": {}
        }
        
        # Map Azure-specific fields to attributes
        if hasattr(msg, "content_type") and msg.content_type != None:
            transformed["attributes"]["content_type"] = msg.content_type
        if hasattr(msg, "correlation_id") and msg.correlation_id != None:
            transformed["attributes"]["correlation_id"] = msg.correlation_id
        if hasattr(msg, "session_id") and msg.session_id != None:
            transformed["attributes"]["session_id"] = msg.session_id
        
        # Copy existing attributes
        for key, value in msg.attributes.items():
            transformed["attributes"][key] = str(value)  # AWS requires string values
        
        return transformed
    
    elif target_provider == "azure_bus":
        # Transform AWS SQS message to Azure Service Bus format
        transformed = {
            "body": msg.body,
            "attributes": {}
        }
        
        # Extract Azure-specific fields from attributes
        attrs = dict(msg.attributes)
        content_type = attrs.pop("content_type", None)
        correlation_id = attrs.pop("correlation_id", None)
        session_id = attrs.pop("session_id", None)
        
        if content_type != None:
            transformed["content_type"] = content_type
        if correlation_id != None:
            transformed["correlation_id"] = correlation_id
        if session_id != None:
            transformed["session_id"] = session_id
        
        # Copy remaining attributes
        transformed["attributes"] = attrs
        
        return transformed
    
    else:
        fail("Unsupported target provider: {}".format(target_provider))

def migrate_queue_configuration(source_provider, target_provider, queue_configs):
    """Migrate queue configurations between providers"""
    
    migrated_configs = []
    
    for config in queue_configs:
        queue_name = config["name"]
        
        if source_provider == "aws_sqs" and target_provider == "azure_bus":
            # AWS SQS to Azure Service Bus migration
            azure_config = {
                "name": queue_name,
                "max_delivery_count": config.get("max_receive_count", 3),
                "duplicate_detection": True if queue_name.endswith(".fifo") else False
            }
            
            # Map retention periods (different units)
            if "message_retention_period" in config:
                azure_config["default_message_ttl"] = config["message_retention_period"]
            
            migrated_configs.append(azure_config)
            
        elif source_provider == "azure_bus" and target_provider == "aws_sqs":
            # Azure Service Bus to AWS SQS migration
            aws_config = {
                "name": queue_name,
                "max_receive_count": config.get("max_delivery_count", 3),
                "fifo": config.get("duplicate_detection", False)
            }
            
            # Ensure FIFO queue naming
            if aws_config["fifo"] and not queue_name.endswith(".fifo"):
                aws_config["name"] = queue_name + ".fifo"
            
            # Map retention periods
            if "default_message_ttl" in config:
                aws_config["message_retention_period"] = config["default_message_ttl"]
            
            migrated_configs.append(aws_config)
    
    return migrated_configs
```

### Provider-Specific Best Practices

#### AWS SQS Best Practices

```python
# 1. Use FIFO queues for ordering
def create_ordered_queue_aws(queue_name):
    """Create an ordered queue on AWS SQS"""
    if not queue_name.endswith(".fifo"):
        queue_name = queue_name + ".fifo"
    
    return create_queue(
        queue_name=queue_name,
        fifo=True,
        visibility_timeout=60,
        dead_letter_queue=queue_name.replace(".fifo", "-dlq.fifo"),
        max_receive_count=3
    )

# 2. Optimize for throughput with batching
def high_throughput_send_aws(queue_name, messages):
    """Send messages optimized for AWS SQS throughput"""
    
    # AWS SQS batch limit is 10 messages
    batch_size = 10
    
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        
        # Add deduplication IDs for FIFO queues
        if queue_name.endswith(".fifo"):
            for j, msg in enumerate(batch):
                if "message_deduplication_id" not in msg:
                    msg["message_deduplication_id"] = "batch-{}-{}".format(i, j)
        
        result = send_batch(queue_name, batch)
        print("AWS batch {}: {} sent".format(i // batch_size + 1, len(result.successful)))

# 3. Handle long polling efficiently
def poll_efficiently_aws(queue_name, process_function):
    """Efficiently poll AWS SQS with long polling"""
    
    while True:
        messages = receive(
            queue_name=queue_name,
            max_messages=10,
            wait_time_seconds=20,  # Long polling
            visibility_timeout=300  # 5 minutes processing time
        )
        
        if len(messages) == 0:
            continue
        
        for msg in messages:
            try:
                process_function(msg)
                delete(queue_name, msg.receipt_handle)
            except Exception as e:
                print("Processing failed: {}".format(str(e)))
                # Message will become visible again after visibility timeout
```

#### Azure Service Bus Best Practices

```python
# 1. Use sessions for message ordering
def create_ordered_queue_azure(queue_name):
    """Create an ordered queue on Azure Service Bus"""
    return create_queue(
        queue_name=queue_name,
        requires_session=True,
        max_delivery_count=5,
        duplicate_detection=True
    )

# 2. Optimize large batches
def high_throughput_send_azure(queue_name, messages):
    """Send messages optimized for Azure Service Bus throughput"""
    
    # Azure Service Bus supports larger batches
    batch_size = 100
    
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        result = send_batch(queue_name, batch)
        print("Azure batch {}: {} sent".format(i // batch_size + 1, len(result.successful)))

# 3. Use topics for pub/sub patterns
def setup_notification_system_azure():
    """Setup a notification system using Azure Service Bus topics"""
    
    # Create topic
    create_topic("notifications", max_size_in_mb=1024)
    
    # Create filtered subscriptions
    create_subscription(
        topic_name="notifications",
        subscription_name="high-priority",
        filter_expression="priority IN ('high', 'critical')"
    )
    
    create_subscription(
        topic_name="notifications", 
        subscription_name="email-notifications",
        filter_expression="type = 'email'"
    )
    
    create_subscription(
        topic_name="notifications",
        subscription_name="audit-log"  # No filter = all messages
    )

# 4. Handle scheduled messages
def schedule_message_azure(queue_name, message, delay_hours):
    """Schedule a message for future delivery on Azure Service Bus"""
    
    future_time = time.now().add(hours=delay_hours)
    return send(
        queue_name=queue_name,
        message=message,
        scheduled_time=future_time,
        attributes={"scheduled_at": time.now().format(time.RFC3339)}
    )
```

### Core Functions with Complete Examples

#### `configure(provider, **config)`

Configure the MQ provider and connection settings.

**Parameters:**

- `provider` (string, required): "aws_sqs" or "azure_bus"
- `**config`: Provider-specific configuration options

**Example:**

```python
load("mq", "configure")

# Configure AWS SQS
configure("aws_sqs",
    region="us-west-2",
    access_key_id="AKIAIOSFODNN7EXAMPLE",
    secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    default_timeout=30,
    max_retries=3
)

# Configure Azure Service Bus
configure("azure_bus",
    connection_string="Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EXAMPLE",
    default_timeout=30,
    max_retries=3
)

# Configure with environment variables (automatically detected)
configure("aws_sqs")  # Uses AWS_REGION, AWS_ACCESS_KEY_ID, etc.
```

#### `send(queue_name, message, **kwargs)`

Send a message to a queue or topic.

**Parameters:**

- `queue_name` (string, required): Name of the queue or topic
- `message` (string, required): Message content
- `attributes` (dict, optional): Message attributes/properties
- `delay_seconds` (int, optional): Delay before message becomes available (0-900)
- `message_group_id` (string, optional): Message group ID for FIFO queues
- `message_deduplication_id` (string, optional): Deduplication ID for FIFO queues
- `scheduled_time` (time, optional): Schedule message for future delivery (Azure)
- `session_id` (string, optional): Session ID for Azure Service Bus sessions
- `content_type` (string, optional): Content type of the message
- `correlation_id` (string, optional): Correlation ID for message tracking
- `reply_to` (string, optional): Reply-to queue/topic name
- `time_to_live` (int, optional): Message time-to-live in seconds
- `priority` (int, optional): Message priority (0-255, Azure Service Bus)
- `label` (string, optional): Message label (Azure Service Bus)

**Returns:** Message ID string

**Complete Examples:**

```python
load("mq", "configure", "send")
load("time")

def main():
    # Configure provider
    configure("aws_sqs", region="us-west-2")
    
    # 1. Simple message
    msg_id = send("orders", "New order received")
    print("Sent message: {}".format(msg_id))
    
    # 2. Message with attributes
    order_id = send(
        queue_name="orders",
        message="Order #12345 created",
        attributes={
            "customer_id": "cust-789",
            "priority": "high",
            "order_type": "premium",
            "amount": "149.99"
        }
    )
    
    # 3. Delayed message (AWS SQS)
    reminder_id = send(
        queue_name="reminders",
        message="Payment reminder for order #12345",
        delay_seconds=300,  # 5 minutes
        attributes={"type": "payment_reminder"}
    )
    
    # 4. FIFO queue message (AWS SQS)
    fifo_id = send(
        queue_name="order-processing.fifo",
        message="Process order #12345",
        message_group_id="customer-789",
        message_deduplication_id="order-12345-{}".format(time.now().unix),
        attributes={"order_id": "12345", "step": "payment"}
    )
    
    # 5. Scheduled message (Azure Service Bus)
    configure("azure_bus", 
        connection_string="Endpoint=sb://example.servicebus.windows.net/...")
    
    future_time = time.now().add(hours=2)
    scheduled_id = send(
        queue_name="scheduled-tasks",
        message="Execute daily report",
        scheduled_time=future_time,
        attributes={"task_type": "daily_report", "date": time.now().format("2006-01-02")}
    )

main()
```

#### `receive(queue_name, **kwargs)`

Receive messages from a queue or subscription.

**Parameters:**

- `queue_name` (string, required): Name of the queue or subscription
- `max_messages` (int, optional): Maximum number of messages to receive (1-10)
- `wait_time_seconds` (int, optional): Long polling wait time (0-20)
- `visibility_timeout` (int, optional): Visibility timeout for received messages
- `peek_only` (bool, optional): Peek without removing messages (default: false)
- `session_id` (string, optional): Session ID for Azure Service Bus sessions
- `receive_timeout` (int, optional): Timeout for receive operation

**Returns:** List of Message objects

**Complete Examples:**

```python
load("mq", "configure", "receive", "delete", "send")
load("time")

def main():
    configure("aws_sqs", region="us-west-2")
    
    # 1. Simple receive
    messages = receive("orders")
    for msg in messages:
        print("Message: {}".format(msg.body))
        print("ID: {}".format(msg.id))
        print("Receipt: {}".format(msg.receipt_handle))
    
    # 2. Batch receive with long polling
    messages = receive(
        queue_name="high-priority",
        max_messages=10,
        wait_time_seconds=20,
        visibility_timeout=60
    )
    
    print("Received {} messages".format(len(messages)))
    for msg in messages:
        print("Processing message: {}".format(msg.body))
        
        # Access message attributes
        priority = msg.attributes.get("priority", "normal")
        customer_id = msg.attributes.get("customer_id")
        
        if priority == "high":
            process_high_priority_order(msg.body, customer_id)
        else:
            process_normal_order(msg.body, customer_id)
        
        # Delete after processing
        if delete("high-priority", msg.receipt_handle):
            print("Message deleted successfully")
        else:
            print("Failed to delete message")
    
    # 3. Peek messages without removing
    preview = receive(
        queue_name="orders",
        max_messages=5,
        peek_only=True
    )
    
    print("Queue has {} messages waiting".format(len(preview)))
    for msg in preview:
        print("Preview: {}".format(msg.body[:50]))  # First 50 chars

def process_high_priority_order(body, customer_id):
    print("High priority processing for customer: {}".format(customer_id))
    return True

def process_normal_order(body, customer_id):
    print("Normal processing for customer: {}".format(customer_id))
    return True

main()
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

## Complete Usage Examples

### 1. Basic Message Queue Operations

```python
load("mq", "configure", "send", "receive", "delete")

def main():
    # Configure AWS SQS
    configure("aws_sqs", region="us-west-2")
    
    # Send a message
    msg_id = send("my-queue", "Hello, World!")
    print("Sent message: {}".format(msg_id))
    
    # Receive and process messages
    messages = receive("my-queue", max_messages=5)
    for msg in messages:
        print("Processing: {}".format(msg.body))
        
        # Delete after processing
        if delete("my-queue", msg.receipt_handle):
            print("Message processed and deleted")

main()
```

### 2. E-commerce Order Processing System

```python
load("mq", "configure", "send", "receive", "delete", "send_batch")
load("time", "json")

def main():
    configure("aws_sqs", region="us-west-2")
    
    # Simulate order creation
    orders = [
        {"order_id": "ORD-001", "customer": "alice", "amount": 99.99, "items": 3},
        {"order_id": "ORD-002", "customer": "bob", "amount": 149.50, "items": 2},
        {"order_id": "ORD-003", "customer": "charlie", "amount": 299.99, "items": 5}
    ]
    
    # Send orders to processing queue
    order_messages = []
    for order in orders:
        order_messages.append({
            "body": json.encode(order),
            "attributes": {
                "order_id": order["order_id"],
                "customer": order["customer"],
                "priority": "high" if order["amount"] > 200 else "normal",
                "created_at": time.now().format(time.RFC3339)
            }
        })
    
    # Batch send orders
    result = send_batch("order-processing", order_messages)
    print("Sent {} orders for processing".format(len(result.successful)))
    
    # Process orders
    while True:
        orders = receive(
            queue_name="order-processing",
            max_messages=10,
            wait_time_seconds=5,
            visibility_timeout=300  # 5 minute processing window
        )
        
        if len(orders) == 0:
            print("No more orders to process")
            break
            
        for order_msg in orders:
            order_data = json.decode(order_msg.body)
            print("Processing order: {}".format(order_data["order_id"]))
            
            # Simulate processing time
            processing_success = process_order(order_data)
            
            if processing_success:
                # Send to fulfillment queue
                fulfillment_id = send(
                    queue_name="fulfillment",
                    message=json.encode({
                        "order_id": order_data["order_id"],
                        "status": "ready_for_fulfillment",
                        "processed_at": time.now().format(time.RFC3339)
                    }),
                    attributes={
                        "order_id": order_data["order_id"],
                        "priority": order_msg.attributes.get("priority", "normal")
                    }
                )
                
                # Delete from processing queue
                delete("order-processing", order_msg.receipt_handle)
                print("Order {} sent to fulfillment: {}".format(
                    order_data["order_id"], fulfillment_id))
            else:
                print("Order {} processing failed, will retry".format(
                    order_data["order_id"]))

def process_order(order):
    # Simulate order processing logic
    print("  - Validating order: {}".format(order["order_id"]))
    print("  - Checking inventory for {} items".format(order["items"]))
    print("  - Processing payment: ${}".format(order["amount"]))
    return True  # Simulate success

main()
```

### 3. Multi-Provider Message Router

```python
load("mq", "configure", "send", "receive", "delete")
load("time", "json")

def main():
    # Configure multiple providers
    setup_providers()
    
    # Route messages between different providers
    route_messages()

def setup_providers():
    """Setup both AWS SQS and Azure Service Bus"""
    print("Setting up message queue providers...")
    
    # This function would be called based on environment or config
    provider = get_current_provider()
    
    if provider == "aws":
        configure("aws_sqs",
            region="us-west-2",
            default_timeout=30,
            max_retries=3
        )
        print("Configured AWS SQS")
    elif provider == "azure":
        configure("azure_bus",
            connection_string=get_azure_connection_string(),
            default_timeout=30,
            max_retries=3
        )
        print("Configured Azure Service Bus")

def route_messages():
    """Route messages from input to different output queues"""
    
    while True:
        # Receive from main input queue
        messages = receive(
            queue_name="message-router-input",
            max_messages=10,
            wait_time_seconds=10
        )
        
        if len(messages) == 0:
            continue
            
        for msg in messages:
            try:
                # Parse message routing info
                routing_data = json.decode(msg.body)
                destination = routing_data.get("destination", "default")
                message_type = routing_data.get("type", "unknown")
                
                print("Routing {} message to {}".format(message_type, destination))
                
                # Route to appropriate queue
                target_queue = get_target_queue(destination, message_type)
                
                routed_id = send(
                    queue_name=target_queue,
                    message=json.encode(routing_data.get("payload", {})),
                    attributes={
                        "source": "message-router",
                        "original_message_id": msg.id,
                        "routed_at": time.now().format(time.RFC3339),
                        "type": message_type
                    }
                )
                
                print("  Routed to {} with ID: {}".format(target_queue, routed_id))
                
                # Delete from input queue
                delete("message-router-input", msg.receipt_handle)
                
            except Exception as e:
                print("Failed to route message {}: {}".format(msg.id, str(e)))
                # Message will become visible again for retry

def get_current_provider():
    # In real implementation, this would check environment/config
    return "aws"  # or "azure"

def get_azure_connection_string():
    # In real implementation, this would get from secure config
    return "Endpoint=sb://example.servicebus.windows.net/..."

def get_target_queue(destination, message_type):
    routing_table = {
        ("notifications", "email"): "email-notifications",
        ("notifications", "sms"): "sms-notifications", 
        ("analytics", "event"): "analytics-events",
        ("billing", "invoice"): "billing-invoices",
        ("default", "unknown"): "dead-letter-processing"
    }
    
    return routing_table.get((destination, message_type), "dead-letter-processing")

main()
```

### 4. Dead Letter Queue Management

```python
load("mq", "configure", "send", "receive", "delete", "get_dead_letter_messages", "redrive_messages")
load("time", "json")

def main():
    configure("aws_sqs", region="us-west-2")
    
    # Monitor and process dead letter messages
    monitor_dead_letters()

def monitor_dead_letters():
    """Monitor and process dead letter queues"""
    
    dead_letter_queues = [
        "order-processing-dlq",
        "payment-processing-dlq", 
        "notification-dlq"
    ]
    
    for dlq in dead_letter_queues:
        print("Checking dead letter queue: {}".format(dlq))
        
        # Get dead letter messages
        dead_messages = get_dead_letter_messages(dlq)
        
        if len(dead_messages) > 0:
            print("Found {} dead letter messages in {}".format(len(dead_messages), dlq))
            
            # Analyze failures
            analyze_failures(dead_messages, dlq)
            
            # Attempt to redrive some messages
            redrive_recoverable_messages(dlq)

def analyze_failures(messages, queue_name):
    """Analyze dead letter messages to identify patterns"""
    
    failure_patterns = {}
    
    for msg in messages:
        # Get failure reason from attributes
        failure_reason = msg.attributes.get("failure_reason", "unknown")
        
        if failure_reason not in failure_patterns:
            failure_patterns[failure_reason] = 0
        failure_patterns[failure_reason] = failure_patterns[failure_reason] + 1
        
        # Log detailed failure info
        print("  Dead message {} - Reason: {} - Receive count: {}".format(
            msg.id[:8], 
            failure_reason,
            msg.receive_count
        ))
    
    # Print failure summary
    print("Failure patterns for {}:".format(queue_name))
    for reason, count in failure_patterns.items():
        print("  {}: {} messages".format(reason, count))

def redrive_recoverable_messages(dlq_name):
    """Redrive messages that might be recoverable"""
    
    # Determine source queue from DLQ name
    source_queue = dlq_name.replace("-dlq", "")
    
    # Get messages from DLQ
    dlq_messages = receive(dlq_name, max_messages=10, peek_only=True)
    
    recoverable_count = 0
    for msg in dlq_messages:
        # Check if message might be recoverable
        if is_message_recoverable(msg):
            recoverable_count = recoverable_count + 1
    
    if recoverable_count > 0:
        print("Attempting to redrive {} recoverable messages from {} to {}".format(
            recoverable_count, dlq_name, source_queue))
        
        # Redrive messages back to source queue
        redriven = redrive_messages(
            source_queue=dlq_name,
            target_queue=source_queue,
            max_messages=recoverable_count
        )
        
        print("Successfully redriven {} messages".format(redriven))

def is_message_recoverable(msg):
    """Determine if a dead letter message might be recoverable"""
    
    # Check age - don't redrive very old messages
    if msg.receive_count > 5:
        return False
        
    # Check for specific failure reasons that might be temporary
    failure_reason = msg.attributes.get("failure_reason", "")
    temporary_failures = ["timeout", "rate_limit", "service_unavailable"]
    
    for temp_failure in temporary_failures:
        if temp_failure in failure_reason.lower():
            return True
    
    return False

main()
```

### 5. Azure Service Bus Topics and Subscriptions

```python
load("mq", "configure", "create_topic", "create_subscription", "send", "receive", "delete")
load("time", "json")

def main():
    # Configure Azure Service Bus
    configure("azure_bus", 
        connection_string="Endpoint=sb://example.servicebus.windows.net/...")
    
    # Setup topic and subscriptions
    setup_notification_system()
    
    # Send notifications
    send_notifications()
    
    # Process subscriptions
    process_subscriptions()

def setup_notification_system():
    """Setup topics and subscriptions for notifications"""
    
    # Create main notifications topic
    topic_info = create_topic(
        topic_name="notifications",
        max_size_in_mb=1024,
        duplicate_detection=True
    )
    print("Created topic: {}".format(topic_info.name))
    
    # Create subscription for high priority notifications
    high_priority_sub = create_subscription(
        topic_name="notifications",
        subscription_name="high-priority",
        filter_expression="priority = 'high' OR priority = 'critical'",
        max_delivery_count=5
    )
    print("Created high priority subscription")
    
    # Create subscription for email notifications
    email_sub = create_subscription(
        topic_name="notifications", 
        subscription_name="email-notifications",
        filter_expression="type = 'email'",
        max_delivery_count=3
    )
    print("Created email subscription")
    
    # Create subscription for SMS notifications
    sms_sub = create_subscription(
        topic_name="notifications",
        subscription_name="sms-notifications", 
        filter_expression="type = 'sms' AND priority = 'high'",
        max_delivery_count=3
    )
    print("Created SMS subscription")
    
    # Create catch-all subscription
    all_sub = create_subscription(
        topic_name="notifications",
        subscription_name="audit-log",
        max_delivery_count=1
    )
    print("Created audit log subscription")

def send_notifications():
    """Send various types of notifications to the topic"""
    
    notifications = [
        {
            "message": "Welcome to our service!",
            "type": "email",
            "priority": "normal",
            "user_id": "user-123",
            "template": "welcome"
        },
        {
            "message": "Your payment was successful",
            "type": "email", 
            "priority": "high",
            "user_id": "user-456",
            "template": "payment_success"
        },
        {
            "message": "Security alert: New login detected",
            "type": "sms",
            "priority": "high", 
            "user_id": "user-789",
            "template": "security_alert"
        },
        {
            "message": "System maintenance scheduled",
            "type": "email",
            "priority": "critical",
            "broadcast": True,
            "template": "maintenance"
        }
    ]
    
    for notification in notifications:
        msg_id = send(
            queue_name="notifications",  # Topic name
            message=json.encode(notification),
            attributes={
                "type": notification["type"],
                "priority": notification["priority"],
                "user_id": notification.get("user_id", ""),
                "template": notification["template"],
                "sent_at": time.now().format(time.RFC3339)
            },
            label="NotificationEvent",
            content_type="application/json"
        )
        
        print("Sent notification: {} - Type: {} - Priority: {}".format(
            msg_id, notification["type"], notification["priority"]))

def process_subscriptions():
    """Process messages from different subscriptions"""
    
    # Process high priority notifications
    print("\nProcessing high priority notifications...")
    high_priority_messages = receive("notifications/subscriptions/high-priority")
    for msg in high_priority_messages:
        notification = json.decode(msg.body)
        print("HIGH PRIORITY: {} for user {}".format(
            notification["message"], 
            notification.get("user_id", "all users")
        ))
        delete("notifications/subscriptions/high-priority", msg.receipt_handle)
    
    # Process email notifications
    print("\nProcessing email notifications...")
    email_messages = receive("notifications/subscriptions/email-notifications")
    for msg in email_messages:
        notification = json.decode(msg.body)
        send_email(notification)
        delete("notifications/subscriptions/email-notifications", msg.receipt_handle)
    
    # Process SMS notifications  
    print("\nProcessing SMS notifications...")
    sms_messages = receive("notifications/subscriptions/sms-notifications")
    for msg in sms_messages:
        notification = json.decode(msg.body)
        send_sms(notification)
        delete("notifications/subscriptions/sms-notifications", msg.receipt_handle)

def send_email(notification):
    """Simulate sending email"""
    print("📧 Sending email: {} to user {}".format(
        notification["message"],
        notification.get("user_id", "broadcast")
    ))

def send_sms(notification):
    """Simulate sending SMS"""
    print("📱 Sending SMS: {} to user {}".format(
        notification["message"],
        notification.get("user_id", "unknown")
    ))

main()
```

## Configuration System

Using base package configuration pattern:

```go
type Config struct {
    // Provider settings
    Provider            *base.ConfigOption[string]     // "aws_sqs" or "azure_bus"
    
    // AWS SQS settings
    AWSRegion           *base.ConfigOption[string]     // Default: "us-east-1"
    AWSAccessKeyID      *base.ConfigOption[string]     // AWS access key
    AWSSecretAccessKey  *base.ConfigOption[string]     // AWS secret key (secret)
    AWSSessionToken     *base.ConfigOption[string]     // AWS session token (optional)
    
    // Azure Service Bus settings
    ConnectionString    *base.ConfigOption[string]     // Azure connection string (secret)
    
    // General settings
    DefaultTimeout      *base.ConfigOption[int]        // Default: 30 seconds
    MaxRetries          *base.ConfigOption[int]        // Default: 3
    VisibilityTimeout   *base.ConfigOption[int]        // Default: 30 seconds
    WaitTimeSeconds     *base.ConfigOption[int]        // Default: 0 (no long polling)
    MaxMessages         *base.ConfigOption[int]        // Default: 10
    
    // Performance settings
    ConnectionPoolSize  *base.ConfigOption[int]        // Default: 10
    BatchSize          *base.ConfigOption[int]        // Default: 10
    MaxConcurrency     *base.ConfigOption[int]        // Default: 100
    
    // Security settings
    EnableTLS          *base.ConfigOption[bool]       // Default: true
    VerifyCertificates *base.ConfigOption[bool]       // Default: true
}
```

### Environment Variable Configuration

```bash
# Provider configuration
export MQ_PROVIDER="aws_sqs"                    # or "azure_bus"

# AWS SQS configuration
export MQ_AWS_REGION="us-west-2"
export MQ_AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export MQ_AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export MQ_AWS_SESSION_TOKEN="optional-session-token"

# Azure Service Bus configuration
export MQ_CONNECTION_STRING="Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EXAMPLE"

# General configuration
export MQ_DEFAULT_TIMEOUT="30"
export MQ_MAX_RETRIES="3"
export MQ_VISIBILITY_TIMEOUT="30"
export MQ_WAIT_TIME_SECONDS="20"
export MQ_MAX_MESSAGES="10"

# Performance configuration
export MQ_CONNECTION_POOL_SIZE="10"
export MQ_BATCH_SIZE="10"
export MQ_MAX_CONCURRENCY="100"

# Security configuration
export MQ_ENABLE_TLS="true"
export MQ_VERIFY_CERTIFICATES="true"
```

## Implementation Architecture

### File Structure

```
mq/
├── PLAN.md                 # This comprehensive plan
├── README.md              # User documentation and examples
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

- `github.com/aws/aws-sdk-go-v2/service/sqs` - AWS SQS SDK v2
- `github.com/aws/aws-sdk-go-v2/config` - AWS configuration
- `github.com/aws/aws-sdk-go-v2/credentials` - AWS credentials
- `github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus` - Azure Service Bus SDK
- `github.com/Azure/azure-sdk-for-go/sdk/azidentity` - Azure authentication
- `github.com/starpkg/base` - Base configuration package
- `github.com/1set/starlet` - Starlark runtime
- `go.starlark.net/starlark` - Starlark language support

### Testing Dependencies

- `github.com/stretchr/testify` - Testing utilities
- `github.com/testcontainers/testcontainers-go` - Integration testing
- `github.com/localstack/localstack-go-client` - AWS local testing

### Core Components

#### 1. Module Structure

```go
type Module struct {
    cfgMod   *base.ConfigurableModule
    ext      *base.ConfigurableModuleExt
    provider Provider
    clients  map[string]Client
    mu       sync.RWMutex
}

type Provider interface {
    Send(ctx context.Context, params *SendParams) (*SendResult, error)
    Receive(ctx context.Context, params *ReceiveParams) (*ReceiveResult, error)
    Delete(ctx context.Context, params *DeleteParams) error
    SendBatch(ctx context.Context, params *SendBatchParams) (*SendBatchResult, error)
    CreateQueue(ctx context.Context, params *CreateQueueParams) (*QueueInfo, error)
    DeleteQueue(ctx context.Context, queueName string) error
    ListQueues(ctx context.Context, prefix string) ([]string, error)
    GetQueueAttributes(ctx context.Context, queueName string) (*QueueAttributes, error)
}
```

#### 2. AWS SQS Implementation

```go
type SQSProvider struct {
    client    *sqs.Client
    config    *Config
    queueURLs map[string]string
    mu        sync.RWMutex
}

type SQSClient struct {
    svc       *sqs.Client
    region    string
    endpoints map[string]string
}
```

#### 3. Azure Service Bus Implementation

```go
type ServiceBusProvider struct {
    client       *azservicebus.Client
    adminClient  *azservicebus.AdminClient
    config       *Config
    senders      map[string]*azservicebus.Sender
    receivers    map[string]*azservicebus.Receiver
    mu           sync.RWMutex
}

type ServiceBusClient struct {
    namespace     string
    connString    string
    tokenProvider azcore.TokenCredential
}
```

#### 4. Data Structures

```go
type Message struct {
    ID              string
    Body            string
    Attributes      map[string]string
    ReceiptHandle   string
    MD5OfBody       string
    ReceivedTime    time.Time
    SentTime        time.Time
    ReceiveCount    int
    MessageGroupID  string
    SessionID       string
    CorrelationID   string
    ReplyTo         string
    ContentType     string
    TimeToLive      int
}

type BatchResult struct {
    Successful []BatchEntry
    Failed     []BatchFailure
}

type BatchEntry struct {
    ID        string
    MessageID string
}

type BatchFailure struct {
    ID           string
    ErrorCode    string
    ErrorMessage string
}

type QueueAttributes struct {
    QueueARN                           string
    ApproximateNumberOfMessages        int
    ApproximateNumberOfMessagesNotVisible int
    ApproximateNumberOfMessagesDelayed int
    CreatedTimestamp                   time.Time
    LastModifiedTimestamp              time.Time
    VisibilityTimeout                  int
    MessageRetentionPeriod             int
    MaxMessageSize                     int
    DelaySeconds                       int
    DeadLetterTargetARN                string
    MaxReceiveCount                    int
}
```

## Error Handling

### Error Types

```go
// Core error types
var (
    ErrInvalidProvider         = errors.New("invalid message queue provider")
    ErrConnectionFailed        = errors.New("failed to connect to message queue service")
    ErrQueueNotFound          = errors.New("queue not found")
    ErrTopicNotFound          = errors.New("topic not found") 
    ErrSubscriptionNotFound   = errors.New("subscription not found")
    ErrMessageTooLarge        = errors.New("message exceeds size limit")
    ErrInvalidMessageFormat   = errors.New("invalid message format")
    ErrAccessDenied           = errors.New("insufficient permissions")
    ErrRateLimitExceeded      = errors.New("rate limit exceeded")
    ErrTimeout                = errors.New("operation timeout")
    ErrInvalidConfiguration   = errors.New("invalid configuration")
    ErrProviderNotConfigured  = errors.New("provider not configured")
    ErrBatchSizeExceeded      = errors.New("batch size exceeds limit")
    ErrInvalidQueueName       = errors.New("invalid queue name")
    ErrDuplicateMessage       = errors.New("duplicate message detected")
    ErrMessageNotFound        = errors.New("message not found")
    ErrSessionLocked          = errors.New("session is locked by another receiver")
)

// Provider-specific error wrapping
type ProviderError struct {
    Provider string
    Code     string
    Message  string
    Err      error
}

func (e *ProviderError) Error() string {
    return fmt.Sprintf("[%s:%s] %s: %v", e.Provider, e.Code, e.Message, e.Err)
}

func (e *ProviderError) Unwrap() error {
    return e.Err
}
```

### Starlark Error Handling

Since Starlark doesn't support try/except, the module uses these patterns:

```python
# Pattern 1: Use fail() for critical errors
def send_critical(queue, message):
    if queue == "":
        fail("Queue name cannot be empty")
    
    result = send(queue, message)
    if result == None:
        fail("Failed to send critical message")
    
    return result

# Pattern 2: Return None for optional operations
def try_receive(queue):
    messages = receive(queue, wait_time_seconds=1)
    if len(messages) == 0:
        return None
    return messages[0]

# Pattern 3: Use boolean returns for success/failure
def safe_delete(queue, receipt_handle):
    success = delete(queue, receipt_handle)
    if not success:
        print("Warning: Failed to delete message")
    return success
```

### Retry Logic

```go
type RetryConfig struct {
    MaxRetries    int
    InitialDelay  time.Duration
    MaxDelay      time.Duration
    Multiplier    float64
    Jitter        bool
}

func (p *SQSProvider) SendWithRetry(ctx context.Context, params *SendParams) (*SendResult, error) {
    var lastErr error
    
    for attempt := 0; attempt <= p.config.MaxRetries; attempt++ {
        result, err := p.send(ctx, params)
        if err == nil {
            return result, nil
        }
        
        lastErr = err
        
        // Don't retry on certain errors
        if isNonRetryableError(err) {
            break
        }
        
        if attempt < p.config.MaxRetries {
            delay := calculateBackoffDelay(attempt, p.config.RetryConfig)
            select {
            case <-time.After(delay):
                continue
            case <-ctx.Done():
                return nil, ctx.Err()
            }
        }
    }
    
    return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}
```

## Development Plan

### Phase 1: Core Infrastructure (Week 1)

**Priority**: Critical  
**Effort**: 25-30 hours

#### Deliverables

- Base module setup with configuration system
- AWS SQS client implementation
- Azure Service Bus client implementation
- Core error handling framework
- Basic connection management

#### Success Criteria

```python
configure("aws_sqs", region="us-west-2")
msg_id = send("test-queue", "Hello, World!")
messages = receive("test-queue")
delete("test-queue", messages[0].receipt_handle)
```

#### Tasks

1. **Setup module structure** (4-5 hours)
   - Create Go module with base package integration
   - Define configuration options with environment variable support
   - Implement module loading and Starlark integration

2. **AWS SQS client** (8-10 hours)
   - Implement SQS client wrapper with AWS SDK v2
   - Handle authentication (IAM roles, access keys, session tokens)
   - Implement connection pooling and URL caching
   - Add comprehensive error handling

3. **Azure Service Bus client** (8-10 hours)
   - Implement Service Bus client wrapper with Azure SDK
   - Handle authentication (connection strings, managed identity)
   - Implement sender/receiver management
   - Add topic and subscription support

4. **Core messaging operations** (5-5 hours)
   - Implement `configure()` function
   - Implement `send()` function for both providers
   - Implement `receive()` function for both providers
   - Implement `delete()` function for both providers

### Phase 2: Advanced Operations (Week 2)

**Priority**: High  
**Effort**: 20-25 hours

#### Deliverables

- Batch operations for efficiency
- Queue management functions
- Message peeking and visibility control
- Dead letter queue support

#### Success Criteria

```python
# Batch operations
results = send_batch("orders", [msg1, msg2, msg3])
queue_url = create_queue("new-queue", dead_letter_queue="dlq")
attrs = get_queue_attributes("orders")
```

#### Tasks

1. **Batch operations** (6-8 hours)
   - Implement `send_batch()` with automatic chunking
   - Implement `delete_batch()` for efficient cleanup
   - Handle partial failures gracefully
   - Add batch size optimization

2. **Queue management** (8-10 hours)
   - Implement `create_queue()` with provider-specific options
   - Implement `delete_queue()` with safety checks
   - Implement `list_queues()` with prefix filtering
   - Implement `get_queue_attributes()` with comprehensive stats

3. **Advanced messaging** (6-7 hours)
   - Implement `peek()` for message preview
   - Implement `change_message_visibility()`
   - Implement `purge_queue()` for cleanup
   - Add message scheduling support (Azure Service Bus)

### Phase 3: Enterprise Features (Week 3)

**Priority**: High  
**Effort**: 20-25 hours

#### Deliverables

- Azure Service Bus topics and subscriptions
- Dead letter queue management
- Message sessions and ordering
- Advanced filtering and routing

#### Success Criteria

```python
# Topics and subscriptions
create_topic("notifications")
create_subscription("notifications", "email-alerts", filter_expression="type = 'email'")
dlq_messages = get_dead_letter_messages("orders-dlq")
redriven = redrive_messages("orders-dlq", "orders", max_messages=10)
```

#### Tasks

1. **Topics and subscriptions** (10-12 hours)
   - Implement `create_topic()` for Azure Service Bus
   - Implement `create_subscription()` with filtering
   - Implement topic message sending and subscription receiving
   - Add subscription management functions

2. **Dead letter queue management** (6-8 hours)
   - Implement `get_dead_letter_messages()`
   - Implement `redrive_messages()` for recovery
   - Add dead letter queue monitoring
   - Implement automatic redrive policies

3. **Sessions and ordering** (4-5 hours)
   - Implement session-based messaging for Azure Service Bus
   - Add FIFO queue support for AWS SQS
   - Implement message ordering guarantees
   - Add session lock management

### Phase 4: Performance & Reliability (Week 4)

**Priority**: Medium  
**Effort**: 15-20 hours

#### Deliverables

- Connection pooling and optimization
- Comprehensive retry logic
- Circuit breaker pattern
- Performance monitoring

#### Success Criteria

- Handle 1000+ messages/second
- Automatic failover on connection issues
- Sub-100ms latency for simple operations
- Memory usage under 50MB under normal load

#### Tasks

1. **Performance optimization** (8-10 hours)
   - Implement connection pooling with health checks
   - Add message batching optimizations
   - Implement concurrent message processing
   - Add connection reuse and caching

2. **Reliability features** (7-10 hours)
   - Implement comprehensive retry logic with exponential backoff
   - Add circuit breaker pattern for failure isolation
   - Implement automatic connection recovery
   - Add health check endpoints

### Phase 5: Testing & Documentation (Week 5)

**Priority**: Medium  
**Effort**: 20-25 hours

#### Deliverables

- Comprehensive test suite
- Integration tests with real services
- Complete documentation
- Performance benchmarks

#### Success Criteria

- 90%+ test coverage
- All examples working with real services
- Complete API documentation
- Performance benchmarks documented

#### Tasks

1. **Unit testing** (8-10 hours)
   - Test all functions with mocked services
   - Test error handling scenarios
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
