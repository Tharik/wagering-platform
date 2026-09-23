# Wagering Platform

## Overview

This repository implements a Go wagering service for wallets and `BET`, `WIN`, `LOSS`, `REFUND`, and `ROLLBACK` transactions. Commands can arrive through HTTP or SQS and share the same PostgreSQL-backed processing and idempotency rules.

Financial state is stored as integer minor units. Wallet mutation, wager state, ledger entries, Inbox records, and Outbox events are committed through explicit database transaction boundaries. PostgreSQL row locking protects concurrent wallet updates, while durable pending-reference processing handles out-of-order referenced transactions.

For detailed design decisions and trade-offs, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Architecture at a glance

- Go and Uber Fx provide the application and lifecycle wiring.
- PostgreSQL is the source of truth for wallets, wagers, the append-only ledger, Inbox, Outbox, and pending-reference state.
- Keycloak provides OAuth2/OIDC client-credentials authentication and provider isolation.
- LocalStack supplies local SQS FIFO queues and dead-letter queues.
- A durable Inbox protects inbound at-least-once processing.
- A transactional Outbox publishes committed events with stable event IDs.
- Background workers consume commands, publish Outbox events, resolve pending references, and monitor the command DLQ.

## Prerequisites

For the primary local workflow:

- Docker with Docker Compose
- `curl` for the examples

A local Go installation matching `go.mod` is required to run tests directly on the host. The token examples use standard shell tools and do not require `jq`.

## Quick start

Build and start the complete local stack:

```bash
docker compose up --build
```

On a fresh environment, Compose starts PostgreSQL, runs all migrations through the one-shot `migrate` service, imports the local Keycloak realm, provisions the LocalStack queues, and starts the application after its required dependencies are ready.

Local ports:

| Service | Address |
| --- | --- |
| Wagering API | `http://localhost:8080` |
| Keycloak | `http://localhost:8081` |
| LocalStack | `http://localhost:4566` |
| PostgreSQL | `localhost:5432` |

From another terminal, check readiness:

```bash
curl -fsS http://localhost:8080/health/ready
```

Operational endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /health/live` | Process liveness |
| `GET /health/ready` | PostgreSQL and command-queue readiness |
| `GET /metrics` | Prometheus-compatible metrics |

Inspect or stop the stack with:

```bash
docker compose ps
docker compose down
```

## Authentication

Wallet administration and reconciliation require the `wagering-internal` client. Wagering endpoints require a provider client such as `provider-a`. The local realm issues tokens with the `wagering-api` audience.

Export reusable local tokens:

```bash
TOKEN_URL=http://localhost:8081/realms/wagering/protocol/openid-connect/token

INTERNAL_TOKEN=$(curl -fsS -X POST "$TOKEN_URL" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials' \
  -d 'client_id=wagering-internal' \
  -d 'client_secret=internal-secret' \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

PROVIDER_TOKEN=$(curl -fsS -X POST "$TOKEN_URL" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials' \
  -d 'client_id=provider-a' \
  -d 'client_secret=provider-a-secret' \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
```

These client secrets are deterministic local-development credentials from `keycloak/wagering-realm.json`. They are not production secrets or production configuration examples.

## API examples

The following commands assume the stack is running and the token variables above are set.

### Create and read a wallet

Create a BRL wallet and capture its ID:

```bash
WALLET_RESPONSE=$(curl -fsS -X POST http://localhost:8080/wallets \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "playerId": "player-123",
    "initialBalance": {
      "amount": "100.00",
      "currency": "BRL"
    }
  }')

WALLET_ID=$(printf '%s' "$WALLET_RESPONSE" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
printf '%s\n' "$WALLET_RESPONSE"
```

Representative response:

```json
{
  "id": "wallet-uuid",
  "playerId": "player-123",
  "balance": {
    "amount": "100.00",
    "currency": "BRL"
  },
  "version": 1
}
```

Read it:

```bash
curl -fsS "http://localhost:8080/wallets/$WALLET_ID" \
  -H "Authorization: Bearer $INTERNAL_TOKEN"
```

Wallets are unique by `(playerId, currency)`. Creating the same business key again returns `409 Conflict`.

### Submit and read a BET

Submit a `25.00` BET and capture the internal transaction ID:

```bash
WAGER_RESPONSE=$(curl -fsS -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-001' \
  -d "{
    \"providerId\": \"provider-a\",
    \"externalTransactionId\": \"bet-001\",
    \"playerId\": \"player-123\",
    \"walletId\": \"$WALLET_ID\",
    \"roundId\": \"round-987\",
    \"gameId\": \"fortune-chimp\",
    \"kind\": \"BET\",
    \"money\": {
      \"amount\": \"25.00\",
      \"currency\": \"BRL\"
    }
  }")

TRANSACTION_ID=$(printf '%s' "$WAGER_RESPONSE" | sed -n 's/.*"transactionId":"\([^"]*\)".*/\1/p')
printf '%s\n' "$WAGER_RESPONSE"
```

Representative first-processing response (`201 Created`):

```json
{
  "transactionId": "transaction-uuid",
  "status": "PROCESSED",
  "balance": {
    "amount": "75.00",
    "currency": "BRL"
  },
  "idempotentReplay": false
}
```

Submitting the same header and canonical business payload again returns the original result with `200 OK` and `"idempotentReplay": true`.

Read by internal transaction ID:

```bash
curl -fsS "http://localhost:8080/wagering/transactions/$TRANSACTION_ID" \
  -H "Authorization: Bearer $PROVIDER_TOKEN"
```

Read by provider and external transaction identity:

```bash
curl -fsS http://localhost:8080/providers/provider-a/wagering/transactions/bet-001 \
  -H "Authorization: Bearer $PROVIDER_TOKEN"
```

### Reconcile a wallet

```bash
curl -fsS -X POST "http://localhost:8080/wallets/$WALLET_ID/reconciliation" \
  -H "Authorization: Bearer $INTERNAL_TOKEN"
```

The diagnostic response reports stored and ledger-calculated balances, their signed difference, consistency, and checked entry count. It does not mutate the wallet.

Other wallet routes:

```text
GET /wallets/{id}/ledger?limit=50&cursor=...
```

Ledger pagination uses an opaque cursor.

## Wagering contract and semantics

### Wager kinds

| Kind | Semantics |
| --- | --- |
| `BET` | Debits the wallet and requires sufficient funds. |
| `WIN` | Credits the wallet. A reference is optional; when supplied it must identify a processed BET in the same provider, player, wallet, currency, and round context. |
| `LOSS` | Records a processed result with amount `0.00` and no wallet movement. |
| `REFUND` | Fully reverses a processed BET; partial refunds are not supported. |
| `ROLLBACK` | Applies the opposite movement of an eligible processed BET, WIN, or REFUND; duplicate successful reversals are prevented. |

`REFUND` and `ROLLBACK` require `referenceExternalTransactionId`. A referenced `WIN` supplies it optionally.

### Money

External money amounts are JSON strings in fixed decimal notation with exactly two decimal places. JSON numbers, signs, whitespace, and scientific notation are not accepted. Internally, amounts use signed 64-bit integer minor units and never floating-point arithmetic.

Valid amounts:

```json
"10.00"
"0.01"
```

Invalid amounts:

```text
10.00
"10"
"10.0"
"1e2"
```

The external API currently supports `BRL`.

### Idempotency

- `Idempotency-Key` is required for HTTP wagering commands.
- The same provider, key, and canonical business payload returns the original result.
- Reusing the key with a different canonical business payload returns `409 Conflict`.
- `(providerId, externalTransactionId)` is independently unique.
- A successful replay does not duplicate wallet, ledger, or Outbox effects.

See [ARCHITECTURE.md](ARCHITECTURE.md) for transaction boundaries and canonical payload/hash details.

### Reference lifecycle

A missing or unresolved eligible reference places the dependent wager in durable `PENDING_REFERENCE`. The resolver retries with persisted exponential backoff. If a valid processed reference becomes available, the dependent may become `PROCESSED`; an invalid terminal reference produces `REJECTED`.

The pending-reference TTL is five minutes. Expiry produces terminal `REJECTED` with failure code `REFERENCE_EXPIRED`.

The runtime actively writes `PENDING_REFERENCE`, `PROCESSED`, and `REJECTED`. `PENDING` and `FAILED` remain reserved compatibility states; deeper lifecycle semantics are documented in [ARCHITECTURE.md](ARCHITECTURE.md).

### Durable wager rejection codes

These failure codes represent durable, terminal `REJECTED` outcomes. Retrying the same transaction identity replays the persisted rejected result and its original result balance; correcting a logically invalid request or reference generally requires a new `externalTransactionId` and idempotency key. Some business conditions remain invalid even under a new identity, such as attempting a second successful reversal after the referenced transaction has already been reversed.

| failureCode | Applies to | Terminal condition | Retry semantics |
| --- | --- | --- | --- |
| `INSUFFICIENT_FUNDS` | `BET` | The wallet lacks funds for the debit. | The same identity replays the rejection. After funding, submit a new logical transaction with a new identity. |
| `REVERSAL_INSUFFICIENT_FUNDS` | `ROLLBACK` of a `WIN` or `REFUND` requiring a debit | The wallet lacks funds for the reversal debit. | The same identity replays the rejection. After funding, submit a new logical transaction with a new identity. |
| `REFERENCE_MISMATCH` | Referenced `WIN`, `REFUND`, or `ROLLBACK` | The processed reference differs in provider, player, wallet, round, or currency context. | The same identity replays the rejection. A corrected reference requires a new logical transaction and identity. |
| `REFERENCE_AMOUNT_MISMATCH` | `REFUND` or `ROLLBACK` | The amount differs from the referenced transaction. A referenced `WIN` intentionally need not equal the BET amount. | The same identity replays the rejection. A corrected amount requires a new logical transaction and identity. |
| `INVALID_REFERENCE_KIND` | Referenced `WIN`, `REFUND`, or `ROLLBACK` | A `WIN` or `REFUND` does not reference a `BET`, or a `ROLLBACK` does not reference a `BET`, `WIN`, or `REFUND`. | The same identity replays the rejection. A corrected reference requires a new logical transaction and identity. |
| `INVALID_REFERENCE` | Referenced `WIN`, `REFUND`, or `ROLLBACK` | Defensive stable fallback for an unmapped reference-validation error; known validation paths normally use a more specific code. | The same identity replays the rejection. A corrected request requires a new logical transaction and identity. |
| `REFERENCE_TERMINAL_UNSUCCESSFUL` | Referenced `WIN`, `REFUND`, or `ROLLBACK` | The referenced wager is already `REJECTED` or reserved `FAILED`. | The same identity replays the rejection. Referencing a different eligible transaction requires a new logical transaction and identity. |
| `ALREADY_REVERSED` | `REFUND` or `ROLLBACK` | Another successful reversal already references the transaction. | The same identity replays the rejection, and changing identity does not make a second successful reversal valid. |
| `REFERENCE_EXPIRED` | Pending referenced `WIN`, `REFUND`, or `ROLLBACK` | The unresolved reference exceeded the pending-reference TTL. | The same identity remains rejected even if the reference arrives later. A genuinely new logical transaction and identity are required. |

Contract validation errors are not durable rejection codes. Invalid wager kinds, invalid amounts (including an invalid `LOSS` amount), required or forbidden references, and malformed money or currency are rejected before durable wager persistence. HTTP returns a `4xx` response where applicable; SQS processing fails and its transaction rolls back. No wager, ledger entry, completed Inbox record, or Outbox event is committed.

### HTTP outcomes

| Situation | HTTP behavior |
| --- | --- |
| Missing or invalid token | `401 Unauthorized` |
| Provider/authorization mismatch | `403 Forbidden` |
| Idempotency or external transaction conflict | `409 Conflict` |
| First accepted wager processing | `201 Created` |
| Idempotent replay | `200 OK` |
| Business rejection such as insufficient funds | Successful processing response with `status: "REJECTED"` and a `failureCode` |
| Unexpected infrastructure failure | `5xx` |

## Messaging and LocalStack

The primary queues are:

| Queue | Purpose |
| --- | --- |
| `wager-transactions.fifo` | Inbound wager commands |
| `wager-transactions-dlq.fifo` | Poison-command redrive target |
| `wager-events.fifo` | Outbound committed events |

The local bootstrap also provisions `wager-events-dlq.fifo`; the application does not currently monitor or consume it.

With the Compose stack running, inspect the primary queues without receiving, deleting, or changing message visibility:

```bash
docker compose exec localstack awslocal sqs get-queue-url --queue-name wager-transactions.fifo
docker compose exec localstack awslocal sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/wager-transactions.fifo --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

docker compose exec localstack awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo
docker compose exec localstack awslocal sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

docker compose exec localstack awslocal sqs get-queue-url --queue-name wager-events.fifo
docker compose exec localstack awslocal sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/wager-events.fifo --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

Official inbound command shape:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "player-123",
    "walletId": "wallet-uuid",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": {
      "amount": "25.00",
      "currency": "BRL"
    }
  }
}
```

FIFO routing identity is part of the producer contract:

| Flow | `MessageGroupId` | `MessageDeduplicationId` | Rationale |
| --- | --- | --- | --- |
| Inbound `WagerTransactionRequested` | `walletId` | `messageId` | Commands for one wallet remain ordered, while different wallets can proceed independently. The `messageId` is also the transport identity used by the durable Inbox. |
| Outbound domain event | `aggregateId` | `eventId` | Events for one aggregate remain ordered. The `eventId` is generated and persisted with the Outbox row, so every retry uses the same identity. |

FIFO ordering and deduplication are additional transport guarantees; financial correctness does not depend on SQS FIFO deduplication. PostgreSQL transactionality, Inbox `messageId` plus raw-payload hash handling, persistent wager idempotency, wallet locking/versioning, and the Outbox's persisted `eventId` remain authoritative.

Outbox publication is intentionally at least once. An external publish can succeed while the PostgreSQL transaction that confirms `published_at` fails. The row then remains pending and may be published again with the same `eventId` and `MessageDeduplicationId`; downstream consumers must deduplicate that stable identity.

Inbound delivery is at least once. Inbox registration, wagering work, and Inbox completion share the database transaction, and an SQS message is deleted only after that transaction commits. Poison commands are moved by the native SQS redrive policy after `maxReceiveCount`.

Committed events are stored in the transactional Outbox before background publishers send them to `wager-events.fifo`. Publication is at least once; event IDs remain stable across retries for downstream deduplication.

## Database migrations

Migrations are plain SQL under `migrations/`. `docker compose up --build` runs all pending migrations automatically before the application starts. Migrations provide explicit up/down operations, but a rollback requires the existing data to be compatible with the target schema.

Apply all pending migrations explicitly:

```bash
docker compose run --rm migrate \
  -path=/migrations \
  -database='postgres://wagering:wagering@postgres:5432/wagering?sslmode=disable' \
  up
```

Revert one migration when its data-compatibility preconditions are satisfied:

```bash
docker compose run --rm migrate \
  -path=/migrations \
  -database='postgres://wagering:wagering@postgres:5432/wagering?sslmode=disable' \
  down 1
```

Apply pending migrations again with the `up` command above. Inspect version and dirty state with:

```bash
docker compose run --rm migrate \
  -path=/migrations \
  -database='postgres://wagering:wagering@postgres:5432/wagering?sslmode=disable' \
  version
```

A dirty migration state blocks normal subsequent migration until an operator investigates and repairs it deliberately.

Migration `000006` expands reference linkage to allow referenced `WIN` transactions. If rows with `kind = 'WIN'` and a non-null `referenced_transaction_id` exist, rolling back `000006` is not lossless or compatible: PostgreSQL rejects restoration of the older REFUND/ROLLBACK-only constraint. Operators must resolve or migrate incompatible data deliberately before attempting that rollback. The clean/fresh `down 1` and subsequent `up` example remains valid when no such version-6 data exists; deleting linkage, forcing the migration version, or resetting production data is not a recovery procedure.

## Testing

Start the local stack before running the integration suite. Tests use real PostgreSQL, Keycloak, and LocalStack services and isolate their PostgreSQL schemas by package.

```bash
go test ./...
go test ./... -count=3
go test -race ./...
go vet ./...
gofmt -l $(find . -name '*.go' -not -path './.git/*')
git diff --check
```

The normal suite covers financial concurrency, idempotency races, provider isolation, HTTP/SQS cross-channel identity, Inbox/Outbox recovery, SQS retry/redrive, pending references, reconciliation, migrations, and graceful shutdown. The dedicated harness below provides real process-level three-instance verification.

### Focused correctness verification

Run these commands from the repository root. The integration-test commands assume the Compose stack is already running.

```bash
# Money parsing, formatting, arithmetic, and wallet numeric boundaries.
go test ./internal/domain -run 'Test(ParseMoney|Money(Add|Subtract|String)|Wallet(Credit|Debit))' -count=1

# Canonical payload hashing and persistent idempotency behavior.
go test ./internal/domain -run 'Test(PayloadHash|FixedDecimalMoney|InvalidMoney)' -count=1
go test ./internal/application/wagering -run 'Test(ProcessedBetReplay|SameIdempotencyKey|SameBetFiftyTimes)' -count=1

# Pending-reference resolution, expiry, and restart recovery.
go test ./internal/application/wagering -run 'Test(MissingReference|PendingRollback|PendingReference)' -count=1

# Durable Inbox registration, replay, and conflicts.
go test ./internal/infrastructure/messaging/inbox -count=1

# Outbox retry, stable event IDs, and concurrent publisher coordination.
go test ./internal/infrastructure/messaging/outbox -run 'Test(PublishBatch|ConcurrentPublishers|PendingEvent)' -count=1

# SQS processing, visibility retry, and DLQ redrive.
go test ./internal/infrastructure/messaging/sqs -run 'TestConsumer' -count=1

# OIDC validation and provider authorization edge cases.
go test ./internal/infrastructure/httpapi -run 'Test(AuthMiddlewareValidatesOIDCEdgeCases|OIDCAuthenticationAndProviderIsolation)' -count=1

# Reconciliation consistency, divergence, and snapshot behavior.
go test ./internal/application/wallet -run 'TestReconcil' -count=1

# Three-process concurrency and idempotency harness.
./scripts/multi-instance-test.sh
```

Proof mapping:

| Behavior | Verification path |
| --- | --- |
| Money and canonical payload boundaries | Focused domain tests above |
| Persistent idempotency | Focused wagering integration tests above |
| Pending-reference lifecycle and recovery | Focused wagering integration tests above |
| Inbox replay/conflict handling | Inbox integration package above |
| Outbox retry and multi-publisher coordination | Focused Outbox integration tests above |
| SQS visibility retry and redrive | Focused SQS integration tests above |
| Authentication and provider isolation | Focused HTTP authentication tests above |
| Snapshot-consistent reconciliation | Focused wallet integration tests above |
| Multi-process financial concurrency | `./scripts/multi-instance-test.sh` |

## Multi-instance verification

Run the supported harness:

```bash
./scripts/multi-instance-test.sh
```

Run it from the repository root. It starts three independent application containers and repeats the process-level checks three times. The harness verifies two concurrent `80.00` BETs against a `100.00` wallet, cross-instance idempotency, independent-wallet processing, and direct database invariants. It intentionally leaves the Compose stack running for inspection.

Stop that multi-instance stack with the same Compose files used by the harness:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.multi.yml \
  down
```

Using only the base `docker compose down` after the harness may report `app2` and `app3` as orphan containers. To intentionally reset disposable local test data as well, add `--volumes` to the matching multi-file command; volume removal is not the default cleanup workflow.

## Failure and recovery

- **Idempotent replay:** a repeated HTTP or SQS command returns the persisted result without another financial effect.
- **Poison command:** processing failures leave the SQS message undeleted; native redrive eventually moves it to `wager-transactions-dlq.fifo`.
- **Pending reference:** retry metadata and expiry are durable, so resolution continues after restart or on another instance.
- **Outbox retry:** committed unpublished events remain in PostgreSQL and are retried; a crash after send may produce an at-least-once duplicate with the same event ID.
- **Graceful shutdown:** workers stop accepting new work, in-flight context-aware processing drains within the configured lifecycle deadline, and PostgreSQL resources close after workers stop.

See [ARCHITECTURE.md](ARCHITECTURE.md) for exact failure windows and transaction guarantees.

## Configuration

| Variable | Purpose | Local default/example | Production guidance |
| --- | --- | --- | --- |
| `DATABASE_URL` | PostgreSQL connection | `postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable` | Set an environment-specific connection with appropriate TLS and secret handling. |
| `AWS_REGION` | AWS SDK region | `us-east-1` | Set the deployment region explicitly when it differs. |
| `SQS_ENDPOINT` | Optional SQS-compatible endpoint override | `.env.example`: `http://localhost:4566`; Compose: `http://localstack:4566` | Normally leave unset so the SDK uses the regional AWS SQS endpoint. |
| `SQS_COMMANDS_QUEUE_URL` | Inbound command queue URL | LocalStack `wager-transactions.fifo` URL | Set the provisioned AWS queue URL. |
| `SQS_COMMANDS_DLQ_URL` | Command DLQ URL | LocalStack `wager-transactions-dlq.fifo` URL | Set the provisioned AWS DLQ URL. |
| `SQS_EVENTS_QUEUE_URL` | Outbound event queue URL | LocalStack `wager-events.fifo` URL | Set the provisioned AWS event queue URL. |
| `OIDC_ISSUER` | Trusted token issuer | `http://localhost:8081/realms/wagering` | Set the production issuer exactly. |
| `OIDC_JWKS_URL` | Optional explicit JWKS endpoint | Empty for issuer discovery; Compose uses the internal Keycloak URL | Leave empty for discovery or set a trusted reachable JWKS endpoint. |
| `HTTP_ADDRESS` | HTTP listen address | `:8080` | Set according to the runtime/network environment. |

`.env.example` is intentionally a local-development configuration:

```bash
set -a
source .env.example
set +a
```

## Production notes

The application includes developer-friendly defaults for localhost PostgreSQL, LocalStack queue URLs, the local Keycloak issuer, and `us-east-1`. Production deployments should configure all environment-specific dependencies explicitly rather than relying on those defaults.

LocalStack uses disposable `AWS_ACCESS_KEY_ID=test` and `AWS_SECRET_ACCESS_KEY=test` values only because the AWS SDK requires credentials when signing local requests. Production AWS authentication uses the standard SDK credential chain; IAM roles or workload identities with short-lived credentials are recommended instead of static long-lived keys.

The imported Keycloak realm and its client secrets are also local-development fixtures. Production requires managed identity configuration, secret management, TLS, restricted queue policies, and appropriate encryption. The least-privilege SQS permission model is documented in [ARCHITECTURE.md](ARCHITECTURE.md).

## Architecture documentation

For deeper design details, see [ARCHITECTURE.md](ARCHITECTURE.md), including:

- transaction boundaries and wallet row locking;
- financial concurrency and database constraints;
- Inbox and transactional Outbox behavior;
- reference lifecycle and resolver coordination;
- SQS retries, redrive, and recovery windows;
- messaging security and least-privilege IAM;
- application lifecycle and graceful shutdown;
- deliberate trade-offs and remaining limitations.
