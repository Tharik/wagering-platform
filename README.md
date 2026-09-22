# Wagering Platform

A backend wagering platform implemented in Go with a focus on financial integrity, concurrency, idempotency, reliable messaging, and failure recovery.

The service supports wallet management and wager processing through both HTTP and SQS while preserving the same transactional and idempotency guarantees across both channels.

## Architecture overview

The application is organized into the following layers:

- `internal/domain` — core domain concepts and invariants.
- `internal/application` — wallet and wagering use cases.
- `internal/infrastructure/httpapi` — HTTP API, authentication, health checks, and metrics.
- `internal/infrastructure/messaging` — SQS consumer/publisher, Inbox, and transactional Outbox.
- `internal/infrastructure/postgres` — PostgreSQL infrastructure.
- `internal/worker` — background workers.
- `cmd/wagering-api` — Uber Fx composition root and application lifecycle.

PostgreSQL is the source of truth for wallets, wagers, idempotency records, Inbox, Outbox, and the financial ledger.

More detailed design decisions and trade-offs are documented in `ARCHITECTURE.md`.

## Main guarantees

The implementation provides:

- Money represented as integer minor units internally; no floating-point arithmetic.
- Persistent idempotency.
- Per-wallet concurrency control using PostgreSQL row locking.
- Atomic wallet, wager, ledger, Inbox, and Outbox operations where applicable.
- Append-only financial ledger.
- At-least-once SQS processing.
- Transactional Outbox for reliable event publication.
- Durable Inbox for message deduplication.
- Durable pending-reference processing.
- Full REFUND and ROLLBACK validation.
- Protection against multiple successful reversals of the same transaction.
- OAuth2/OIDC authentication using Keycloak.
- Provider isolation based on authenticated identity.
- Graceful worker shutdown through Uber Fx lifecycle hooks.
- PostgreSQL and SQS readiness checks.
- Structured JSON logging and Prometheus-compatible metrics.

## Requirements

For local development:

- Go
- Docker
- Docker Compose
- PostgreSQL client (`psql`) is optional; migrations can also be executed through the PostgreSQL container.

## Local infrastructure

Start PostgreSQL, LocalStack, and Keycloak:

```bash
docker compose up -d
```

Check the services:

```bash
docker compose ps
```

The local environment provides:

- PostgreSQL on port `5432`
- LocalStack on port `4566`
- Keycloak on port `8081`
- Wagering API on port `8080` when started with the default configuration

Keycloak imports the development realm automatically from:

```text
keycloak/wagering-realm.json
```

LocalStack creates the required SQS queues from:

```text
localstack/init/01-create-queues.sh
```

The command queue and DLQ are:

```text
wager-transactions.fifo
wager-transactions-dlq.fifo
```

Outbound domain events are published to:

```text
wager-events.fifo
```

## Configuration

The application works with local defaults, but all relevant external configuration can be overridden through environment variables.

See `.env.example`.

```text
DATABASE_URL
AWS_REGION
SQS_ENDPOINT
SQS_COMMANDS_QUEUE_URL
SQS_COMMANDS_DLQ_URL
SQS_EVENTS_QUEUE_URL
OIDC_ISSUER
HTTP_ADDRESS
```

To load the example configuration into the current shell:

```bash
set -a
source .env.example
set +a
```

## Database migrations

Migrations live under `migrations/` and are deliberately plain SQL.

Apply them in order:

```bash
for migration in migrations/*.up.sql; do
  docker compose exec -T postgres \
    psql -U wagering -d wagering -v ON_ERROR_STOP=1 < "$migration"
done
```

Revert them in reverse order:

```bash
for migration in $(find migrations -name '*.down.sql' | sort -r); do
  docker compose exec -T postgres \
    psql -U wagering -d wagering -v ON_ERROR_STOP=1 < "$migration"
done
```

The migrations cover:

1. Initial wallet, wager, ledger, Inbox, and Outbox schema.
2. Database-enforced ledger immutability.
3. Reference transaction and reversal support.
4. Single-successful-reversal enforcement.

After reverting all migrations, they can be applied again using the UP command above.

## Running the application

After the infrastructure is running and migrations have been applied:

```bash
go run ./cmd/wagering-api
```

The default HTTP address is:

```text
:8080
```

A different instance can be started using another address:

```bash
HTTP_ADDRESS=:8082 go run ./cmd/wagering-api
```

This is useful for validating multi-instance concurrency behavior.

## Authentication

The API uses OAuth2/OIDC with Keycloak.

The local Keycloak realm contains development clients for:

```text
provider-a
provider-b
wagering-internal
```

The provider identity is derived from the validated access token. A caller cannot select another provider by simply changing a request field.

Provider clients are used for wagering operations, while the internal client is used for wallet administration and reconciliation.

The realm configuration under `keycloak/wagering-realm.json` is intended for local development only. Its development credentials must not be used in a production environment.

A client-credentials token can be obtained from the local Keycloak token endpoint using the corresponding client ID and secret configured in the realm file.

Example:

```bash
curl -s \
  -X POST \
  "http://localhost:8081/realms/wagering/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=provider-a" \
  -d "client_secret=provider-a-secret"
```

The API validates the token issuer and the `wagering-api` audience.

## HTTP API

### Health

```text
GET /health/live
GET /health/ready
```

Readiness checks both PostgreSQL and SQS.

### Metrics

```text
GET /metrics
```

The endpoint exposes Prometheus-compatible metrics.

### Wallets

Internal authentication is required.

```text
POST /wallets
GET /wallets/{walletId}
GET /wallets/{walletId}/ledger
POST /wallets/{walletId}/reconciliation
```

Ledger pagination supports `limit` and an opaque `cursor`.

Example:

```text
GET /wallets/{walletId}/ledger?limit=50&cursor=...
```

### Wagering

Provider authentication is required.

```text
POST /wagering/transactions
GET /wagering/transactions/{transactionId}
GET /providers/{providerId}/wagering/transactions/{externalTransactionId}
```

Supported wager types:

```text
BET
WIN
LOSS
REFUND
ROLLBACK
```

## Money

External monetary values are represented as fixed two-decimal strings.

The domain rejects invalid representations such as:

- negative values;
- excessive decimal scale;
- scientific notation;
- NaN or Infinity.

Internally, money is stored as signed 64-bit integer minor units.

This avoids floating-point rounding errors in wallet and ledger operations.

## Wallet and ledger

Each wallet is unique by:

```text
(playerId, currency)
```

Wallet balances cannot become negative.

Every successful financial movement produces an append-only ledger entry containing the balance before and after the movement.

The database enforces ledger immutability through triggers preventing UPDATE and DELETE operations.

Opening a wallet with a positive initial balance creates an internal `OPENING` transaction, ledger entry, and corresponding Outbox events atomically.

A zero opening balance creates no financial movement.

## Concurrency

Wallet mutation uses PostgreSQL row-level locking.

Operations affecting the same wallet are serialized by the database, while operations against different wallets can proceed independently.

This design does not depend on in-memory mutexes or a single application instance.

The implementation was validated with multiple independent database pools and with three separate application processes sharing the same PostgreSQL database.

For a wallet containing `100.00`, two concurrent distinct BET requests of `80.00` result in:

```text
one PROCESSED
one REJECTED with INSUFFICIENT_FUNDS
final balance = 20.00
one debit ledger movement
```

Replaying either request does not create an additional financial movement.

## Idempotency

Idempotency is persisted in PostgreSQL.

Requests are normalized before their canonical request hash is calculated.

A replay of the same logical operation returns the original result without changing the wallet or creating another ledger entry.

Reusing the same idempotency identity with a different canonical payload is rejected.

HTTP and SQS processing share the same wagering use case and persistent idempotency guarantees.

## REFUND and ROLLBACK

A REFUND reverses a successfully processed BET.

A ROLLBACK reverses a successfully processed BET, WIN, or REFUND using the opposite financial movement.

Reference validation includes:

- provider;
- player;
- wallet;
- currency;
- round;
- reference transaction state and type.

Partial reversals are not supported.

The database prevents more than one successful REFUND/ROLLBACK reversal for the same referenced transaction.

If a reversal requires a debit and the wallet does not contain sufficient funds, it is rejected with a distinct insufficient-funds result.

## Pending references

A REFUND or ROLLBACK may arrive before the referenced transaction because delivery is at least once and messages may arrive out of order.

Instead of immediately rejecting such a request, the transaction enters:

```text
PENDING_REFERENCE
```

The state is durable in PostgreSQL.

A background resolver retries eligible pending transactions with backoff. Processing survives application restarts because retry state is persisted.

If the reference becomes available, the transaction is resolved normally.

If the configured retry/expiry policy is exhausted, the transaction becomes `REJECTED`.

Multiple resolver instances safely coordinate through PostgreSQL locking.

## SQS processing

Inbound wagering messages are consumed from:

```text
wager-transactions.fifo
```

The message envelope contains:

```json
{
  "messageId": "...",
  "type": "...",
  "occurredAt": "...",
  "data": {}
}
```

The consumer uses a persistent Inbox keyed by consumer name and message ID.

Inbox registration, wager processing, and Inbox completion occur within the same database transaction.

The SQS message is deleted only after the database transaction commits successfully.

Therefore, a crash after commit but before `DeleteMessage` causes a redelivery that is safely recognized as a replay rather than creating a second financial movement.

Repeated processing failures are handled by the queue redrive policy and eventually move the message to:

```text
wager-transactions-dlq.fifo
```

## Transactional Outbox

Domain events are written to the Outbox in the same PostgreSQL transaction as the corresponding business state.

Background publishers claim unpublished records and publish them to SQS.

Events are marked as published only after successful publication.

If the process crashes after sending an event but before marking the Outbox record as published, the event may be published again. This is intentional at-least-once behavior.

The event ID remains stable across publication retries so downstream consumers can deduplicate safely.

Multiple Outbox publishers coordinate through PostgreSQL locking.

## Events

Outbound events use an envelope containing:

```text
eventId
eventType
aggregateId
correlationId
causationId
occurredAt
version
data
```

Events include transaction processing/rejection, wallet balance changes, and pending-reference state changes.

## Reconciliation

The reconciliation endpoint reconstructs the expected wallet balance from the ledger and compares it with the current wallet state.

It also validates ledger continuity and movement arithmetic.

Reconciliation is diagnostic: it reports divergence but does not silently modify financial state.

Detected divergences are exposed through metrics.

## Observability

Application logs use structured JSON.

Where available, contextual fields include:

```text
correlationId
messageId
transactionId
walletId
providerId
```

Logs intentionally avoid complete financial payloads and sensitive authentication data.

Metrics include:

- wager results by status;
- idempotent replays;
- processing latency;
- SQS retries and processing errors;
- DLQ message count;
- concurrency conflicts;
- Outbox publications, retries, errors, and lag;
- reconciliation divergences.

## Failure and recovery scenarios

### PostgreSQL unavailable

Readiness becomes unhealthy and transactional processing cannot proceed.

### SQS unavailable

Readiness becomes unhealthy. Outbox records remain durable in PostgreSQL and can be retried after SQS becomes available again.

For local testing:

```bash
docker compose stop localstack
```

After restarting it:

```bash
docker compose start localstack
```

readiness should recover automatically.

### Consumer crash after database commit

The SQS message is redelivered because it was not deleted.

The Inbox and persistent wagering idempotency prevent duplicate financial effects.

### Outbox publisher crash after publish

The Outbox record may be published again because it was not marked as published.

The stable event ID allows downstream deduplication.

### Missing reference

The operation remains durable as `PENDING_REFERENCE` and is retried by the background resolver.

## Testing

Start the local infrastructure and apply migrations before running integration tests.

Run the complete test suite:

```bash
go test ./... -p=1
```

Run repeatedly to expose timing or isolation issues:

```bash
go test ./... -p=1 -count=3
```

Run the race detector:

```bash
go test -race ./... -p=1
```

Run static analysis:

```bash
go vet ./...
```

Check formatting:

```bash
gofmt -w ./cmd ./internal
```

Check whitespace errors before committing:

```bash
git diff --check
```

The integration suite exercises real PostgreSQL, Keycloak, and LocalStack infrastructure.

It covers, among other scenarios:

- repeated identical wagers;
- same-wallet concurrent debits;
- concurrent operations on different wallets;
- concurrent REFUND/ROLLBACK attempts;
- Inbox concurrency;
- multiple Outbox publishers;
- SQS redelivery after commit;
- DLQ redrive;
- pending-reference resolution and expiry;
- pending-reference recovery after application restart;
- OAuth2/OIDC provider isolation;
- HTTP/SQS cross-channel idempotency;
- reconciliation divergence;
- ledger pagination.

Because integration tests use shared local infrastructure, do not leave a separately running wagering application consuming the same development resources while executing the complete test suite.

## Multi-instance validation

The service does not rely on process-local synchronization.

Multiple instances can be started against the same PostgreSQL and SQS infrastructure:

```bash
HTTP_ADDRESS=:8082 go run ./cmd/wagering-api
HTTP_ADDRESS=:8083 go run ./cmd/wagering-api
HTTP_ADDRESS=:8084 go run ./cmd/wagering-api
```

Requests sent concurrently through different instances still use PostgreSQL as the concurrency authority.

The same-wallet `100.00` / two concurrent `80.00` BET scenario was validated across separate application processes.

## Graceful shutdown

Uber Fx owns application startup and shutdown.

On shutdown:

1. the shared worker context is cancelled;
2. background workers stop;
3. the application waits for worker termination;
4. the HTTP server shuts down;
5. PostgreSQL resources are released through the Fx lifecycle.

The HTTP listener is acquired synchronously during startup. If the configured port cannot be bound, application startup fails instead of leaving background workers running without a functional HTTP server.

## Local development notes

The Keycloak realm contains development-only client credentials.

LocalStack uses development AWS credentials and must not be treated as production security configuration.

The architecture intentionally favors database-backed correctness over process-local coordination so the service remains safe when multiple instances are running.

See `ARCHITECTURE.md` for the detailed design, transactional boundaries, failure semantics, and trade-offs.