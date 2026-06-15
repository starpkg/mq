# 📨 `mq` — Unified Message Queue Module

[![godoc](https://pkg.go.dev/badge/github.com/starpkg/mq.svg)](https://pkg.go.dev/github.com/starpkg/mq)
[![license](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.23-blue.svg)](https://golang.org/)
[![codecov](https://codecov.io/gh/starpkg/mq/graph/badge.svg)](https://codecov.io/gh/starpkg/mq)
![binary footprint](https://img.shields.io/badge/binary_footprint-%2B5.6_MB-blue)

A unified Starlark module for message queue operations across **AWS SQS** and
**Azure Service Bus**. It gives scripts one consistent surface for queue
management, message send/receive, scheduling, lock management, and dead letter
queue handling — without learning either vendor SDK.

## Overview

- **One interface, two backends** — the same client API drives AWS SQS and
  Azure Service Bus, with auto-detection from the supplied credentials.
- **Queue management** — `create_queue`, `delete_queue`, `list_queues`,
  `get_queue`, `exists`, `purge`, `get_info`.
- **Message operations** — `send`, `receive`, `delete`, `batch_send`,
  `schedule`, `cancel`, `peek`.
- **Lock & dead-letter** — `lock` / `unlock`, plus `dead_letter_receive` /
  `dead_letter_requeue` / `dead_letter_purge`.
- **Unified results & errors** — both backends return the same `Queue` /
  `MessageResult` shapes and a normalized error taxonomy.

> **Where this fits.** `starpkg` provides *support for necessary local
> operations* plus *simple abstractions over common online services, for ease
> of use*. `mq` is squarely in the **online-service** half: it wraps two managed
> cloud queue services behind one Starlark-friendly surface. It is an **L4
> domain module**, depending downward on `starpkg/base` (the module/config
> system), `1set/starlet` (the Machine runner + `dataconv`), and transitively
> `1set/starlight` + `go.starlark.net`.

For the complete per-builtin / per-method reference — signatures, parameters,
returns, errors, examples — and the configuration accessors, see
**[docs/API.md](docs/API.md)**.

## Installation

```bash
go get github.com/starpkg/mq
```

## Quick Start

```python
load("mq", "connect")

def main():
    # Connect to your message queue service
    client = connect(
        service_type="azure_servicebus",
        connection_string="Endpoint=sb://..."
    )

    # Create a queue
    client.create_queue("orders")

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

Connect to AWS SQS, or let the module auto-detect from the credentials:

```python
load("mq", "connect", "get_supported_services")

def main():
    print("Supported: {}".format(get_supported_services()))

    # AWS SQS
    aws_client = connect(
        service_type="aws_sqs",
        aws_region="us-west-2",
        aws_access_key="YOUR_ACCESS_KEY",
        aws_secret_key="YOUR_SECRET_KEY"
    )

    # Auto-detect (a connection string ⇒ Azure, otherwise AWS SQS)
    auto_client = connect(aws_region="us-east-1")  # Will use AWS SQS

main()
```

## Starlark API at a glance

Module builtins (`load("mq", …)`):

- `connect(service_type?, connection_string?, aws_region?, aws_access_key?, aws_secret_key?, timeout?, max_retries?)` — open a client (returns a client object).
- `get_supported_services()` — list the supported services (`["aws_sqs", "azure_servicebus"]`).
- `get_client_info(client)` — info dict for a client (same payload as the client method).

Client object methods (queue operations):

- `create_queue(name, lock_duration?, retention_period?, max_delivery_count?, dead_letter_config?, enable_sessions?, duplicate_detection?, duplicate_window_secs?, max_queue_size?)` — create a queue.
- `delete_queue(name)` — delete a queue.
- `list_queues(prefix?)` — list queues (optionally by prefix).
- `get_queue(name)` — queue info, or `None`.
- `exists(name)` — whether a queue exists.
- `purge(name)` — purge all messages from a queue.
- `get_info(name)` — detailed queue statistics.

Client object methods (message operations):

- `send(queue_name, body, properties?, scheduled_time?, session_id?, correlation_id?, reply_to?, time_to_live?, message_id?)` — send a message.
- `receive(queue_name, max_count?, wait_time?, lock_duration?, peek_only?)` — receive messages.
- `delete(queue_name, message_ids)` — delete by ID(s); returns a list of bools.
- `batch_send(queue_name, messages)` — send a list of message dicts.
- `schedule(queue_name, body, scheduled_time, properties?, session_id?)` — schedule a message.
- `cancel(queue_name, message_id)` — cancel a scheduled message.
- `peek(queue_name, max_count?)` — peek without receiving.

Client object methods (lock, dead letter, info):

- `lock(queue_name, message_id, lock_duration)` — extend a message lock.
- `unlock(queue_name, message_id)` — release a message lock.
- `dead_letter_receive(queue_name, max_count?)` — receive from the DLQ.
- `dead_letter_requeue(queue_name, message_id)` — move a message back to the main queue.
- `dead_letter_purge(queue_name)` — purge the DLQ.
- `get_client_info()` — info dict for the client.

Some operations are stubs or unsupported on one backend (e.g. AWS `purge` /
`lock` / `unlock` / `batch_send`, Azure `delete` / `cancel` / `peek`). See the
[implementation status matrix](docs/API.md#implementation-status) in
**[docs/API.md](docs/API.md)** for the full signatures, return values, errors,
and per-service behaviour of every builtin and method above.

## Configuration

The module's options (`service_type`, `timeout`, `max_retries`,
`connection_string`, `aws_region`, `aws_access_key`, `aws_secret_key`,
`aws_session_token`, `default_lock_duration`, `default_batch_size`) are
configured via environment variables (`MQ_*`) or per-option `get_<key>` /
`set_<key>` accessor builtins, and serve as defaults for `connect`. Secret
options (the connection string and AWS credentials) expose **only** `set_<key>`
— never a getter. See the
[Configuration section of docs/API.md](docs/API.md#configuration) for the full
option table, defaults, accessors, and secret rules.

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file
for details.
