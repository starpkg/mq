# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`starpkg/mq` is an **L4 domain module** of the Star\* ecosystem: it exposes message-queue operations to Starlark scripts behind one unified surface. A script imports the module, opens a client with `connect`, then creates queues and sends/receives/schedules messages — without ever touching a vendor SDK.

`starpkg` provides *support for necessary local operations* plus *simple abstractions over common online services, for ease of use*. `mq` sits firmly on the **online-service** side: there is no local queue engine here — every operation is a thin, normalized wrapper over a managed cloud service. Two backends share the same `Client` interface:

- **AWS SQS** via `aws-sdk-go-v2` (`service/sqs`, `config`, `credentials`, `sts`).
- **Azure Service Bus** via `azure-sdk-for-go/.../azservicebus` (data-plane sender/receiver + admin client).

The service is chosen by `service_type` (`aws_sqs`, `azure_servicebus`, `auto`); `auto` infers from which credentials/config are present (`detectServiceType`): a connection string ⇒ Azure, otherwise AWS (AWS is also the final fallback). Because both backends speak through the same `Client` interface and return the same `Queue`/`MessageResult` dicts, a script written against one service mostly ports to the other; the per-operation gaps are tabulated in the README compatibility matrix.

Layer position: depends downward on `starpkg/base` (the `ConfigurableModule`/config-option system + the `RunStarlarkTests` harness), `1set/starlet` (the Machine + `dataconv` for Go⇄Starlark marshalling), and transitively `1set/starlight` + `go.starlark.net`. Nothing in the ecosystem depends on it.

## Dev commands

Pure Go library with a Makefile. From this repo:

```bash
make test                                  # -race -cover -count 1, the working bar
make ci                                    # race+cover profile + bench compile (what CI runs)
go test ./... -run TestNormalizeProperties # a single test
go run github.com/1set/meta/doccov@master .   # the documentation-coverage gate
gofmt -l . && go vet ./...                 # must be clean before commit
```

**Verify on the go floor in Docker** — this repo's floor is **go 1.23** (see Release discipline), and may differ from the local toolchain; behavior on the floor must be checked in a container:

```bash
docker run --rm -v "$PWD":/src -v "$HOME/go/pkg/mod":/go/pkg/mod -w /src golang:1.23 go test -race -count=1 ./...
```

The Go unit tests (`utils_test.go`) are self-contained (pure helpers and conversion, no network). `TestStarlarkScripts` drives the `../test/mq/*.star` integration scripts that live in the **private `starpkg/test` repo** (checked out under `test/mq/{aws-sqs,azure-servicebus}` when present): `test-*.star` must succeed, `panic-*.star` must fail. When that directory is absent (CI, fresh clone) the `base.RunStarlarkTests` harness `t.Skip`s, so a green local run does not depend on the private fixtures. Scripts reaching live AWS/Azure read credentials from a `.env` the harness loads from the test dir — never commit credentials.

## Architecture (the part that spans files)

The module is a **two-backend, one-interface bridge**: every script call goes through the `Client` interface (`client.go`), and the wrapper that scripts hold (`ClientWrapper`) dispatches each method onto whichever backend `connect` chose.

- **`mq.go`** — the module entry. `Module` wraps a `base.ConfigurableModule`; `NewModule()` registers the ten config options (with `MQ_*` env vars; AWS/Azure secrets flagged via `genSecretConfigOption`). `LoadModule()` exposes three load-time builtins: **`connect`**, **`get_supported_services`**, **`get_client_info`**. `starConnect` merges module config with per-call overrides (`getConfigValue`), auto-detects the service, validates, then builds the backend client (`createClient`) and returns a `ClientWrapper`.
- **`client.go`** — the `Client` interface (the unified contract: queue ops, message ops, lock, batch, schedule/cancel/peek, dead-letter ops, `Close`/`GetClientInfo`) and `ClientWrapper`, the `starlark.Value`/`HasAttrs` object scripts hold. Its `methodMap` registers all 21 object methods as `starlark.NewBuiltin("mq.<method>", …)` builtins; each method unpacks args, fetches the thread context, calls the interface, and marshals the result (or routes errors through `NormalizeError`).
- **`aws_sqs.go`** — the AWS SQS backend (`AWSSQSClient`, `NewAWSSQSClient`). SQS-specific request context/retry, queue-URL resolution, and SQS attribute mapping.
- **`azure_servicebus.go`** — the Azure Service Bus backend (`AzureServiceBusClient`, `NewAzureServiceBusClient`): data-plane sender/receiver plus the admin client for queue management.
- **`config.go`** — `ClientConfig` (+ `Validate`/`Copy`) and the option structs that cross the interface: `QueueOptions`, `MessageOptions`, `ReceiveOptions`, `BatchMessage`, `DeadLetterConfig`.
- **`queue.go`** / **`message.go`** — the unified result types `Queue` and `MessageResult` and their `Struct()` marshalling (Go → Starlark dict via `dataconv.Marshal`); timestamps are emitted as RFC3339 strings.
- **`errors.go`** — the unified error taxonomy: `MQError`, the `ErrorType` categories, the sentinel `Err*` values, and `NormalizeError` → `normalizeAWSError`/`normalizeAzureError` (keyword-matching service errors into stable categories). `IsRetryableError`/`IsTemporaryError` classify transient failures.
- **`utils.go`** — pure helpers: queue-name/body validation, property normalization, batch splitting and per-service batch-limit adaptation, message-ID generation, and the slice→Starlark converters.

**Data flow (one call):** script `client.send(...)` → `ClientWrapper.send` unpacks kwargs and parses `properties`/`scheduled_time` → builds `MessageOptions` → `Client.Send` on the chosen backend → backend calls the vendor SDK → returns a `*MessageResult` → `MessageResult.Struct()` marshals to a Starlark dict. Errors anywhere are wrapped by `NormalizeError` into an `MQError` carrying `{service, operation, type}`.

**Config & `connect` precedence.** The module holds ten options (`config.go` / registered in `mq.go`): `service_type`, `timeout`, `max_retries`, `connection_string` (secret), `aws_region`, `aws_access_key`/`aws_secret_key`/`aws_session_token` (secrets), `default_lock_duration`, `default_batch_size`. Each has an `MQ_<UPPER>` env var. `connect(...)` accepts a subset as kwargs (`service_type`, `connection_string`, `aws_region`, `aws_access_key`, `aws_secret_key`, `timeout`, `max_retries`); a non-zero kwarg overrides the module value via `getConfigValue`, otherwise the configured/env value wins. `ClientConfig.Validate` enforces the rules (AWS needs a region and AK/SK either both-or-neither; Azure needs a connection string; session token only with explicit AK/SK).

## Invariants / hardening (preserve when editing)

1. **Backward compatibility is the iron rule.** `NewModule()` and the existing config keys/env vars are the public surface; the builtin names and their argument shapes are load-bearing for scripts. Method builtins are registered as the literal `"mq.<method>"` — byte-identical to the historical `ModuleName + "." + name` form. Keep that literal: the doccov gate statically enumerates these names, and changing them is an observable behavior change.
2. **Service abstraction stays unified.** Both backends implement the same `Client` interface and return the same `Queue`/`MessageResult` shapes; never let a backend leak a vendor-specific type across the interface. New operations are added to the interface first, then both backends.
3. **Errors are normalized, never raw.** Every method routes failures through `NormalizeError(service, operation, err)` so scripts see a stable `MQError` taxonomy rather than vendor strings. New methods must do the same.
4. **Deterministic, bounded marshalling.** Results cross to Starlark only through `*.Struct()` / the slice converters (`dataconv.Marshal`), so a `nil` queue/message becomes `None` and timestamps are RFC3339 — not ad-hoc conversions in the method bodies.
5. **Partial backends are documented, not silently wrong.** Some operations are stubs on one backend (see the README compatibility matrix — e.g. AWS `purge`/`lock`/`unlock`/`batch_send`/`dead_letter_requeue`, Azure `cancel`/`peek`/`dead_letter_requeue`). A stub returns success/mock data by design; when you implement one, update the README matrix in the same PR.

## Test organization

Group by functional goal — **do not add one `*_test.go` per fix.** Two homes exist:

- **`utils_test.go`** — table-driven unit tests for the pure helpers (`coalesceInt`, `generateMessageID`, `normalizeProperties`, `adaptBatchSize`, `min`/`max`). Add a new helper test as a **section here**.
- **`example_test.go`** — `TestStarlarkScripts`, the `../test/mq` integration harness (private `starpkg/test` repo, auto-skips when absent).

Tests are table/example-driven; no third-party test framework. Keep functions small (Codacy's `nloc`/cyclomatic rules). New script-visible behavior gets a `test-*.star` (must pass) or `panic-*.star` (must fail) fixture in the private test repo, not a new Go test file here.

## Documentation

Three layers must stay in sync (enforced by the doc standard, `plan/starpkg文档标准（DOC-STD）`):

- **`README.md`** — every script-facing builtin and client method documented as a backtick whole-word (the doccov gate parses backtick spans; the fence-free "Script-facing surface" list near the top is what guarantees coverage). Names, signatures, and return shapes must match the code — e.g. `delete` returns a **list of bools**, not a single bool.
- **GoDoc** — package comment + a doc comment on every exported symbol whose first word is the symbol name (Go convention; gated by `revive`'s `exported` rule in CI).
- **CLAUDE.md** — this file.

The doc-coverage gate runs in CI via the centralized `1set/meta` reusable workflow (`doc-coverage: true` in `.github/workflows/build.yml`). Run it locally with `go run github.com/1set/meta/doccov@master .` before committing doc/API changes.

## Release discipline

- **Floor = go 1.23**, set in `go.mod` and the CI `go-floor`. The AWS and Azure SDK pins track recent releases; raising the floor or bumping a major SDK is a deliberate, isolated change.
- **CI matrix** = `[1.23.x, latest stable]` via the centralized reusable workflow in `1set/meta` (pinned by commit SHA for supply-chain safety; bump the pin when meta's workflow changes).
- **Bumping the version, the go floor, or tagging are user-confirmed actions** — never tag autonomously; the `go.starlark.net`/SDK pin upgrade is its own last PR before any tag; default to patch bumps; published tags are immutable in the module proxy.
