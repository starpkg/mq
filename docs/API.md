# `mq` — Starlark API Reference

The complete reference for every script-facing builtin, client-object method,
and configuration accessor exposed by the `mq` module. For an overview,
installation, and a quickstart, see the [README](../README.md).

The module exposes three load-time builtins via `load("mq", …)` — `connect`,
`get_supported_services`, and `get_client_info` — plus a set of configuration
accessors (`get_<key>` / `set_<key>`) generated from the module's options.
`connect` returns a **client object** that carries the message-queue methods
(queue management, send/receive, lock, schedule, dead-letter ops). Both backends
— AWS SQS and Azure Service Bus — implement the same client surface and return
the same `Queue` / `MessageResult` shapes, so a script written against one
service mostly ports to the other; the per-operation gaps are tabulated in the
[Implementation status](#implementation-status) matrix.

## Contents

- [Module functions](#module-functions)
- [Client methods — queue operations](#client-methods--queue-operations)
- [Client methods — message operations](#client-methods--message-operations)
- [Client methods — lock management](#client-methods--lock-management)
- [Client methods — dead letter queue](#client-methods--dead-letter-queue)
- [Client methods — client information](#client-methods--client-information)
- [Data structures](#data-structures)
- [Implementation status](#implementation-status)
- [Configuration](#configuration)

## Module functions

These functions are loaded directly from the `mq` module via `load("mq", …)`.

### `connect(service_type?, connection_string?, aws_region?, aws_access_key?, aws_secret_key?, timeout?, max_retries?)`

Creates and returns a message queue client. Any argument omitted (or left at its
zero value) falls back to the corresponding module config option; a non-zero
argument overrides it. When `service_type` resolves to `"auto"`, the service is
inferred from the supplied credentials (a connection string ⇒ Azure Service Bus,
otherwise AWS SQS).

**Parameters:**

- `service_type` (string, optional): Service type — `"aws_sqs"`,
  `"azure_servicebus"`, or `"auto"` (default: module config, `"auto"`)
- `connection_string` (string, optional): Azure Service Bus connection string
  (default: module config)
- `aws_region` (string, optional): AWS region for SQS (default: module config,
  `"us-east-1"`)
- `aws_access_key` (string, optional): AWS access key ID (default: module config)
- `aws_secret_key` (string, optional): AWS secret access key (default: module
  config)
- `timeout` (int, optional): Connection timeout in seconds (default: module
  config, `30`)
- `max_retries` (int, optional): Maximum retry attempts (default: module config,
  `3`)

**Returns:** Client object for message queue operations.

**Errors:** Fails if the resolved configuration is invalid — e.g. AWS SQS without
a region, AWS access/secret key supplied without the other, an AWS session token
without explicit access/secret keys, or Azure Service Bus without a connection
string — or if the backend client cannot be created.

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

### `get_supported_services()`

Returns the list of supported message queue services.

**Parameters:** None

**Returns:** List of supported service types: `["aws_sqs", "azure_servicebus"]`.

**Example:**

```python
services = mq.get_supported_services()
print(services)  # ["aws_sqs", "azure_servicebus"]
```

### `get_client_info(client)`

Returns information about a client instance. Equivalent to calling
`get_client_info()` as a method on the client object.

**Parameters:**

- `client` (Client, required): The client object to inspect (must be the value
  returned by `connect`)

**Returns:** Dictionary containing client information. For AWS SQS the keys are
`service_type`, `region`, `timeout`, `max_retries`; for Azure Service Bus they
are `service_type`, `namespace`, `timeout`, `max_retries`.

**Errors:** Fails if the argument is not an `mq.Client`.

**Example:**

```python
info = mq.get_client_info(client)
print(info["service_type"])  # "aws_sqs" or "azure_servicebus"
```

## Client methods — queue operations

Once you hold a client object (from `connect`), call these methods on it.

### `create_queue(name, lock_duration?, retention_period?, max_delivery_count?, dead_letter_config?, enable_sessions?, duplicate_detection?, duplicate_window_secs?, max_queue_size?)`

Creates a new message queue.

**Parameters:**

- `name` (string, required): Queue name
- `lock_duration` (int, optional): Message lock duration in seconds (default: 30)
- `retention_period` (int, optional): Message retention period in seconds
  (default: 1209600 = 14 days)
- `max_delivery_count` (int, optional): Maximum delivery attempts before moving
  to DLQ (default: 10)
- `dead_letter_config` (dict, optional): Dead letter queue configuration
- `enable_sessions` (bool, optional): Enable message sessions/ordering (default:
  `False`)
- `duplicate_detection` (bool, optional): Enable duplicate detection (default:
  `False`)
- `duplicate_window_secs` (int, optional): Duplicate detection window in seconds
  (default: `0`, which Azure Service Bus interprets as its 300-second window)
- `max_queue_size` (int, optional): Maximum queue size in bytes (default: 0 =
  unlimited)

**Dead letter config structure:**

```python
{
    "enabled": True,
    "queue_name": "my-dlq",   # Dead letter queue name
    "max_delivery_count": 5   # Max attempts before moving to DLQ
}
```

**Returns:** Queue object with queue information.

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

### `delete_queue(name)`

Deletes a queue.

**Parameters:**

- `name` (string, required): Queue name to delete

**Returns:** `True` if successful.

### `list_queues(prefix?)`

Lists queues, optionally filtered by name prefix.

**Parameters:**

- `prefix` (string, optional): Filter queues by name prefix (default: `""` = all
  queues)

**Returns:** List of Queue objects.

### `get_queue(name)`

Gets information about a specific queue.

**Parameters:**

- `name` (string, required): Queue name

**Returns:** Queue object, or `None` if not found.

### `exists(name)`

Checks if a queue exists.

**Parameters:**

- `name` (string, required): Queue name

**Returns:** `True` if the queue exists, `False` otherwise.

### `purge(name)`

Purges all messages from a queue.

**Parameters:**

- `name` (string, required): Queue name

**Returns:** `True` if successful.

> **Note:** ⚠️ AWS SQS implementation is not yet complete (stub).

### `get_info(name)`

Gets detailed queue statistics and information.

**Parameters:**

- `name` (string, required): Queue name

**Returns:** Queue object with queue statistics, or `None` if not found.

## Client methods — message operations

### `send(queue_name, body, properties?, scheduled_time?, session_id?, correlation_id?, reply_to?, time_to_live?, message_id?)`

Sends a message to a queue.

**Parameters:**

- `queue_name` (string, required): Target queue name
- `body` (string, required): Message body content
- `properties` (dict, optional): Custom message properties/attributes
- `scheduled_time` (string, optional): RFC3339 timestamp for scheduled delivery
- `session_id` (string, optional): Session ID for message ordering
- `correlation_id` (string, optional): Correlation ID for request-response
  patterns
- `reply_to` (string, optional): Reply queue name
- `time_to_live` (int, optional): Message TTL in seconds
- `message_id` (string, optional): Custom message ID

**Returns:** MessageResult object with the send result.

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

### `receive(queue_name, max_count?, wait_time?, lock_duration?, peek_only?)`

Receives messages from a queue.

**Parameters:**

- `queue_name` (string, required): Source queue name
- `max_count` (int, optional): Maximum number of messages to receive (default: 1)
- `wait_time` (int, optional): Long polling wait time in seconds (default: 0)
- `lock_duration` (int, optional): Message lock duration in seconds (default: uses
  the queue default)
- `peek_only` (bool, optional): Peek without removing messages (default: `False`)

**Returns:** List of MessageResult objects.

**Example:**

```python
# Receive one message
messages = client.receive("orders")

# Receive multiple with long polling
messages = client.receive("orders", max_count=10, wait_time=20)

# Peek without removing
messages = client.receive("orders", peek_only=True)
```

### `delete(queue_name, message_ids)`

Deletes one or more messages from a queue.

**Parameters:**

- `queue_name` (string, required): Source queue name
- `message_ids` (string or list, required): a single message ID, or a list of
  message IDs, to delete

**Returns:** A list of booleans, one per requested message ID, where each element
reports whether that message was deleted successfully. (A single-string
`message_ids` still yields a one-element list.)

**Example:**

```python
# Delete single message
results = client.delete("orders", "msg-123")               # -> [True]

# Delete multiple messages
results = client.delete("orders", ["msg-123", "msg-456"])  # -> [True, True]
```

> **Note:** ⚠️ On Azure Service Bus, deletion by message ID always returns
> `False` for every entry: completing a Service Bus message requires the original
> received-message object, which this ID-based API cannot supply. AWS SQS deletes
> via receipt handles (real receipt handles are sent to `DeleteMessageBatch`;
> short test-style IDs of ≤20 chars are treated as successful without a call).

### `batch_send(queue_name, messages)`

Sends multiple messages in a batch operation.

**Parameters:**

- `queue_name` (string, required): Target queue name
- `messages` (list, required): List of message dictionaries

**Message dictionary structure:**

```python
{
    "body": "Message content",
    "properties": {"key": "value"},           # optional
    "session_id": "session1",                 # optional
    "correlation_id": "corr-1",               # optional
    "reply_to": "responses",                  # optional
    "time_to_live": 3600,                     # optional
    "message_id": "msg-1"                     # optional
}
```

**Returns:** List of MessageResult objects.

**Example:**

```python
messages = [
    {"body": "Order #1", "properties": {"priority": "high"}},
    {"body": "Order #2", "properties": {"priority": "normal"}}
]
results = client.batch_send("orders", messages)
```

> **Note:** ⚠️ AWS SQS implementation is not yet complete (stub).

### `schedule(queue_name, body, scheduled_time, properties?, session_id?)`

Schedules a message for future delivery.

**Parameters:**

- `queue_name` (string, required): Target queue name
- `body` (string, required): Message body content
- `scheduled_time` (string, required): RFC3339 timestamp for delivery
- `properties` (dict, optional): Custom message properties
- `session_id` (string, optional): Session ID for ordering

**Returns:** MessageResult object.

**Example:**

```python
client.schedule(
    "reminders",
    "Meeting reminder",
    "2024-12-31T09:00:00Z",
    properties={"type": "meeting"}
)
```

### `cancel(queue_name, message_id)`

Cancels a scheduled message.

**Parameters:**

- `queue_name` (string, required): Queue name
- `message_id` (string, required): Message ID to cancel

**Returns:** `True` if successful.

> **Note:** ❌ AWS SQS returns an `unsupported` error (SQS has no
> scheduled-message-cancel API). ⚠️ On Azure Service Bus this is currently a stub
> that returns success without calling `CancelScheduledMessage`.

### `peek(queue_name, max_count?)`

Peeks at messages without receiving them.

**Parameters:**

- `queue_name` (string, required): Source queue name
- `max_count` (int, optional): Maximum messages to peek (default: 1)

**Returns:** List of MessageResult objects.

> **Note:** ❌ AWS SQS returns an `unsupported` error (SQS has no peek API).
> ⚠️ On Azure Service Bus this is currently a stub that returns an empty list
> instead of calling `PeekMessages`.

## Client methods — lock management

### `lock(queue_name, message_id, lock_duration)`

Extends the lock duration of a message.

**Parameters:**

- `queue_name` (string, required): Queue name
- `message_id` (string, required): Message ID
- `lock_duration` (int, required): New lock duration in seconds

**Returns:** `True` if successful.

> **Note:** ⚠️ On AWS SQS this is currently a stub that returns success without
> calling `ChangeMessageVisibility`. ❌ Azure Service Bus returns an
> `unsupported` error: lock renewal needs the original received-message object,
> which this message-ID-based API cannot supply.

### `unlock(queue_name, message_id)`

Releases the lock on a message.

**Parameters:**

- `queue_name` (string, required): Queue name
- `message_id` (string, required): Message ID

**Returns:** `True` if successful.

> **Note:** ⚠️ On AWS SQS this is currently a stub that returns success without
> resetting the visibility timeout. ❌ Azure Service Bus returns an `unsupported`
> error: message abandonment needs the original received-message object, which
> this message-ID-based API cannot supply.

## Client methods — dead letter queue

### `dead_letter_receive(queue_name, max_count?)`

Receives messages from the dead letter queue.

**Parameters:**

- `queue_name` (string, required): Main queue name (DLQ is auto-resolved)
- `max_count` (int, optional): Maximum messages to receive (default: 10)

**Returns:** List of MessageResult objects from the DLQ.

### `dead_letter_requeue(queue_name, message_id)`

Moves a message back from the dead letter queue to the main queue.

**Parameters:**

- `queue_name` (string, required): Main queue name
- `message_id` (string, required): Message ID to requeue

**Returns:** `True` if successful.

> **Note:** ⚠️ Both AWS SQS and Azure Service Bus implementations are not yet
> complete (stubs).

### `dead_letter_purge(queue_name)`

Purges all messages from the dead letter queue.

**Parameters:**

- `queue_name` (string, required): Main queue name (DLQ is auto-resolved)

**Returns:** `True` if successful.

> **Note:** ⚠️ On AWS SQS this routes through the (still-stubbed) queue `purge`,
> so it is currently a no-op; on Azure Service Bus it drains the DLQ for real.

## Client methods — client information

### `get_client_info()`

Returns information about the client. (This is the client-object method form; the
module-level `get_client_info(client)` builtin returns the same payload.)

**Parameters:** None

**Returns:** Dictionary containing client information. For AWS SQS the keys are
`service_type`, `region`, `timeout`, `max_retries`; for Azure Service Bus they
are `service_type`, `namespace`, `timeout`, `max_retries`.

**Example:**

```python
info = client.get_client_info()
print(info["service_type"])  # "aws_sqs" or "azure_servicebus"
```

## Data structures

### Queue object

Represents a message queue with unified properties across services.

**Properties:**

- `name` (string): Queue name
- `service_type` (string): Service type (`"aws_sqs"` or `"azure_servicebus"`)
- `url` (string): Service-specific queue URL/identifier
- `message_count` (int): Number of messages in the queue
- `lock_duration` (int): Message lock duration in seconds
- `retention_period` (int): Message retention period in seconds
- `max_delivery_count` (int): Maximum delivery attempts before DLQ
- `dead_letter_config` (dict): Dead letter queue configuration
- `enable_sessions` (bool): Whether message sessions are enabled
- `duplicate_detection` (dict): Duplicate detection configuration
- `max_queue_size` (int): Maximum queue size in bytes
- `created_time` (string): Queue creation timestamp (RFC3339)
- `modified_time` (string): Last modification timestamp (RFC3339)

### MessageResult object

Represents a message received from or sent to a queue.

**Properties:**

- `message_id` (string): Unique message identifier
- `body` (string): Message body content
- `properties` (dict): Custom message properties/attributes
- `session_id` (string): Session ID for message ordering
- `correlation_id` (string): Correlation ID for request-response
- `reply_to` (string): Reply queue name
- `enqueue_time` (string): When the message was enqueued (RFC3339)
- `scheduled_time` (string): Scheduled delivery time, if any (RFC3339)
- `lock_expires_at` (string): When the message lock expires (RFC3339)
- `delivery_count` (int): Number of delivery attempts
- `time_to_live` (int): Message TTL in seconds
- `receipt_handle` (string): Service-specific receipt handle
- `success` (bool): Whether the operation was successful
- `error` (string): Error message, if any

## Implementation status

Some operations are fully implemented against the live service; others are stubs
on one backend that return mock data without a real call (documented, not
silently wrong); a few are unsupported and return an `unsupported` error.

Legend: ✅ Full — implemented against the live service. ⚠️ TODO — stub that
returns mock data without a real call (usually success; Azure `delete` returns
all-`False`). ❌ Unsupported — returns an `unsupported` error.

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

## Configuration

Each module configuration option is exposed to scripts as generated accessor
builtins (loaded from the `mq` module alongside the functions above):

- **`get_<key>()`** — returns the current value of the option.
- **`set_<key>(value)`** — sets the option (returns `None`).

An option's value resolves in priority order: an explicit `set_<key>` value, the
environment variable, then the default. These options serve as defaults that
`connect` uses when the corresponding argument is not provided.

**Secret options expose only `set_<key>` — never a getter.** The four
credential/secret options (`connection_string`, `aws_access_key`,
`aws_secret_key`, `aws_session_token`) have **no** `get_<key>` builtin, so a
script can set a secret but cannot read it back. Go host code can still read them
via the config API.

| Option | Getter | Setter | Type | Env var | Default | Description |
|--------|--------|--------|------|---------|---------|-------------|
| `service_type` | `get_service_type` | `set_service_type` | string | `MQ_SERVICE_TYPE` | `auto` | Service type (`aws_sqs`, `azure_servicebus`, `auto`) |
| `timeout` | `get_timeout` | `set_timeout` | int | `MQ_TIMEOUT` | `30` | Connection timeout in seconds |
| `max_retries` | `get_max_retries` | `set_max_retries` | int | `MQ_MAX_RETRIES` | `3` | Maximum retry attempts |
| `connection_string` | _(secret — none)_ | `set_connection_string` | string | `MQ_CONNECTION_STRING` | `""` | Azure Service Bus connection string |
| `aws_region` | `get_aws_region` | `set_aws_region` | string | `MQ_AWS_REGION` | `us-east-1` | AWS region for SQS |
| `aws_access_key` | _(secret — none)_ | `set_aws_access_key` | string | `MQ_AWS_ACCESS_KEY` | `""` | AWS access key ID |
| `aws_secret_key` | _(secret — none)_ | `set_aws_secret_key` | string | `MQ_AWS_SECRET_KEY` | `""` | AWS secret access key |
| `aws_session_token` | _(secret — none)_ | `set_aws_session_token` | string | `MQ_AWS_SESSION_TOKEN` | `""` | AWS session token |
| `default_lock_duration` | `get_default_lock_duration` | `set_default_lock_duration` | int | `MQ_DEFAULT_LOCK_DURATION` | `30` | Default message lock duration in seconds |
| `default_batch_size` | `get_default_batch_size` | `set_default_batch_size` | int | `MQ_DEFAULT_BATCH_SIZE` | `10` | Default batch size for operations |

**Example:**

```python
load(
    "mq",
    "connect",
    # getters (non-secret options only)
    "get_service_type", "get_timeout", "get_max_retries", "get_aws_region",
    "get_default_lock_duration", "get_default_batch_size",
    # setters (secrets are set-only)
    "set_service_type", "set_timeout", "set_max_retries", "set_connection_string",
    "set_aws_region", "set_aws_access_key", "set_aws_secret_key",
    "set_aws_session_token", "set_default_lock_duration", "set_default_batch_size",
)

set_service_type("aws_sqs")
set_aws_region("us-west-2")
print(get_aws_region())  # "us-west-2"

client = connect()  # opens AWS SQS in us-west-2
```

## Service-specific notes

### AWS SQS

- Maximum message delay: 15 minutes
- Maximum batch size: 10 messages
- FIFO queues require a `.fifo` suffix
- Message deduplication only for FIFO queues

### Azure Service Bus

- Message scheduling up to 7 days in advance
- Sessions enable ordered processing
- Built-in duplicate detection per queue
- Native dead letter queue support
