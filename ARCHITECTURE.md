# Architecture

## Goals and invariants

The primary design goal is correct financial state under concurrency, retries, duplicate delivery, out-of-order references, process failure, and multiple application instances. PostgreSQL is the financial source of truth and the durable coordination boundary.

The core invariants are:

- A wallet balance cannot become negative.
- A wallet starts at version `1`; after creation, its version increments only when its balance moves.
- The financial ledger is append-only.
- Each relevant financial movement has at most one ledger entry for its wallet/transaction identity.
- Duplicate or replayed requests cannot duplicate wallet, ledger, or Outbox effects.
- Wager state, wallet mutation, ledger movement, Inbox state, and Outbox records commit atomically where applicable.
- Referenced transactions must match the required provider, player, wallet, currency, round, kind, state, and amount rules.
- At most one successful `REFUND`/`ROLLBACK` reversal can exist for a referenced transaction.
- Outbound event delivery is at least once, not exactly once.

Correctness does not depend on process-local locks, caches, or a single application instance. Application validation provides meaningful outcomes, while database locks, constraints, and transactions remain authoritative under races and failures.

## System components

The current deployment is one Go application composed with Uber Fx. The following are logical modules within that process unless noted otherwise:

- **HTTP API:** authenticated wallet administration, wagering, reconciliation, ledger reads, health, and metrics.
- **Wallet application service:** wallet creation, reads, ledger pagination, and reconciliation.
- **Wagering application service:** idempotency, reference validation, wallet mutation, wager persistence, ledger insertion, and Outbox event creation.
- **PostgreSQL:** wallets, wagers, idempotency identity, append-only ledger, Inbox, Outbox, and pending-reference retry state.
- **SQS consumer:** receives FIFO wager commands and invokes the wagering use case.
- **Durable Inbox:** protects message identity and completed-command replay.
- **Transactional Outbox and publisher:** commits event intent with business state and publishes it asynchronously.
- **Pending-reference resolver:** retries durable out-of-order references.
- **DLQ monitor:** observes command dead-letter queue depth.
- **OIDC middleware:** validates tokens and enforces provider/internal-client boundaries.
- **Lifecycle, health, metrics, and logging:** operational control and visibility.

Keycloak and PostgreSQL run as external dependencies. AWS SQS is the production broker model; LocalStack provides SQS-compatible development infrastructure.

## Request and transaction flow

HTTP first authenticates and authorizes the caller, parses the transport DTO, validates the provider boundary, and constructs a `domain.WagerRequest`. SQS validates its envelope and fields, parses money, and constructs the same domain request under the existing trusted-message provider model.

A typical wagering database transaction performs the following logical work:

1. Validate command and business inputs and calculate the canonical payload hash.
2. Look up `(providerId, idempotencyKey)` and return the persisted result when this is a valid replay.
3. Load and lock the wallet row so decisions for the same wallet serialize.
4. Enforce external transaction uniqueness and wallet/player/currency rules.
5. Resolve and validate a reference when the wager requires or supplies one.
6. Apply the domain wallet debit or credit when there is a monetary movement.
7. Persist the wager result and update the wallet where applicable.
8. Append one ledger entry when money moved.
9. Insert the corresponding Outbox event records.
10. Commit the complete operation.

The exact SQL statement order varies by outcome, but no partial financial result is committed. For example, the system cannot commit a wallet debit without its wager and ledger movement, or a processed wager without its event intent.

SQS processing extends the transaction to include Inbox registration and completion. The SQS message is deleted only after durable completion commits. SQS network calls and later Outbox publication are not part of the financial database transaction.

Mutating business transactions use PostgreSQL's normal `READ COMMITTED` behavior together with explicit wallet row locking and constraints. Reconciliation is deliberately different: it uses one read-only `REPEATABLE READ` transaction so wallet and ledger reads share a consistent snapshot.

## Money, wallets, and ledger

External money is a JSON string containing one or more ASCII digits, a decimal point, and exactly two ASCII fractional digits. Signs, whitespace, floating-point JSON numbers, and scientific notation are rejected. Parsing occurs before the domain request can establish persisted idempotency identity.

Internally, money uses signed 64-bit integer minor units and checked integer arithmetic. No financial calculation uses floating point. External wagering currently supports `BRL`, and currency equality is enforced across wallet and wager operations.

A wallet is created at version `1`. A positive opening balance creates an internal `OPENING` transaction, a credit ledger entry, `WagerTransactionProcessed`, and `WalletBalanceChanged`, all atomically. A zero opening balance creates no opening transaction, ledger movement, or event.

After creation, a successful debit or credit increments the wallet version exactly once. These outcomes do not increment the version:

- `LOSS`, because it has no monetary movement;
- `REJECTED` processing;
- `PENDING_REFERENCE` processing;
- idempotent replay.

Checked balance and version arithmetic reject overflow before mutating domain state. Insufficient funds reject a debit before balance or version changes.

Each ledger entry records wallet, transaction, direction, amount, balance before, balance after, and creation time. Database constraints enforce positive movement amounts, valid credit/debit arithmetic, non-negative wallet balances, and one movement per wallet/transaction. Triggers reject ledger `UPDATE` and `DELETE` operations.

No ledger entry is created for `LOSS`, `REJECTED`, or `PENDING_REFERENCE` outcomes.

## Concurrency and multi-instance correctness

Wager processing locks its wallet row with `SELECT ... FOR UPDATE` for the duration of the transaction. Concurrent financial decisions for the same wallet therefore serialize in PostgreSQL, even when requests reach different application processes.

For a wallet containing `100.00`, two distinct concurrent BETs of `80.00` cannot both succeed. One transaction debits and commits first. The other then observes `20.00` and becomes `REJECTED` with `INSUFFICIENT_FUNDS`. The final balance is `20.00`, with one debit ledger movement.

Different wallets lock different rows and can proceed independently. No process-local wallet mutex participates in correctness.

Database constraints provide race-safe secondary boundaries for:

- wallet uniqueness by player/currency;
- provider idempotency and external transaction identities;
- non-negative balances and valid ledger arithmetic;
- one ledger movement per wallet/transaction;
- one successful `REFUND`/`ROLLBACK` per reference;
- one opening transaction per wallet;
- unique Inbox identity.

Pending resolvers and Outbox publishers use PostgreSQL coordination so multiple instances can cooperate without claiming the same active work during normal execution. A dedicated three-container harness validates same-wallet contention, cross-instance idempotency, and different-wallet parallelism with direct database invariant checks.

## Wager lifecycle and kinds

The runtime actively writes these wager states:

- `PENDING_REFERENCE`: durable unresolved reference.
- `PROCESSED`: accepted terminal result.
- `REJECTED`: terminal business rejection.

`PENDING` and `FAILED` remain reserved persisted compatibility states. The runtime does not currently create either one. Readers treat `PENDING` as unresolved and `FAILED` as terminal unsuccessful. Infrastructure failures roll back instead of creating a durable `FAILED` wager, and no `WagerTransactionFailed` event exists.

Wager kinds have these effects:

| Kind | Financial and reference behavior |
| --- | --- |
| `BET` | Debit; requires sufficient balance. |
| `WIN` | Credit; reference is optional. |
| `LOSS` | No monetary movement; amount must be `0.00`. |
| `REFUND` | Full reversal of a processed BET; partial refund is unsupported. |
| `ROLLBACK` | Opposite movement of an eligible processed BET, WIN, or REFUND. |

A database partial unique index prevents more than one successful `REFUND`/`ROLLBACK` for the same reference. Multiple distinct WINs may reference the same valid BET because WIN references are not reversals.

When a reversal direction requires a debit, insufficient wallet balance produces the durable rejection `REVERSAL_INSUFFICIENT_FUNDS` instead of allowing a negative balance.

## Reference resolution

References are resolved within the requesting provider. An identically named external transaction belonging to another provider is neither used nor inspected to reject the request.

| Reference condition | Outcome |
| --- | --- |
| Reference is optional and omitted | Process normally. |
| Same-provider referenced transaction is absent | Persist `PENDING_REFERENCE`. |
| Reference state is `PENDING` or `PENDING_REFERENCE` | Persist/remain pending and retry. |
| Reference is `PROCESSED` and context/business rules are valid | Continue processing. |
| Reference is `PROCESSED` but kind, context, or amount rules are invalid | Persist `REJECTED` with the corresponding stable failure code. |
| Reference is `REJECTED` or reserved `FAILED` | Persist `REJECTED` with `REFERENCE_TERMINAL_UNSUCCESSFUL`. |
| Successful reversal already exists | Persist `REJECTED` with `ALREADY_REVERSED`. |
| Required reversal debit lacks funds | Persist `REJECTED` with `REVERSAL_INSUFFICIENT_FUNDS`. |
| Five-minute pending-reference TTL expires | Persist `REJECTED` with `REFERENCE_EXPIRED`. |

Reference context includes provider, player, wallet, currency, and round. REFUND and ROLLBACK amounts must equal the referenced amount. A referenced WIN must point to a processed BET in the same context, but its credited amount may differ from the BET amount.

Pending-reference state includes retry attempts, next-attempt time, expiry, and correlation/causation metadata. The resolver polls due rows and claims them with `FOR UPDATE SKIP LOCKED`. Retry delay starts at five seconds, grows exponentially, is capped at one minute, and never schedules beyond expiry.

Because state is durable, resolution survives process restart and may be performed by another instance. Final processed or rejected events preserve the original correlation and optional causation metadata.

## Canonical wagering idempotency

Wagering idempotency is durable across HTTP retries, SQS redelivery, restarts, multiple instances, and cross-channel replay.

The canonical wager hash is deterministic JSON serialization of one concrete Go struct, not a generic canonical-JSON scheme. `encoding/json` serializes its declared fields in this exact order:

1. `providerId`
2. `externalTransactionId`
3. `playerId`
4. `walletId`
5. `roundId`
6. `gameId`
7. `kind`
8. `amount`
9. `currency`
10. `referenceExternalTransactionId`, only when non-empty

The compact bytes for a BET without a reference are equivalent to:

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "ext-123",
  "playerId": "player-1",
  "walletId": "wallet-1",
  "roundId": "round-1",
  "gameId": "game-1",
  "kind": "BET",
  "amount": "25.00",
  "currency": "BRL"
}
```

The implementation hashes those bytes with SHA-256 and stores lowercase hexadecimal in `wager_transactions.payload_hash`, a `CHAR(64)` column. This is deterministic request identity comparison, not password hashing. Collision risk is accepted as negligible; canonical field selection is the important business contract.

Money is parsed before the domain request is built. Canonical amount uses fixed two-decimal `Money.String()`, and currency uses `Money.Currency()`. Invalid external formats never establish a persisted wager hash.

`referenceExternalTransactionId` uses `omitempty`. Missing and explicitly empty references both become the empty Go string and are omitted, so they hash identically. A non-empty reference appears last and changes the hash.

The hash excludes the `Idempotency-Key` itself, transaction ID, state, result balance, failure code, timestamps, correlation ID, causation ID, HTTP headers, authentication token, SQS message ID, receipt metadata, and raw transport JSON.

The persisted idempotency identity is `(providerId, idempotencyKey)`. The key selects the record but is not part of the payload hash; provider ID participates in both identity and hash.

- No record: continue processing.
- Same provider/key and hash: replay the original transaction ID, state, result balance, and failure code with `idempotentReplay: true`.
- Same provider/key and different hash: return `ErrIdempotencyConflict`.

HTTP returns `201` for first accepted processing and `200` for replay. It maps idempotency conflict to `409 Conflict`.

Provider external identity, `(providerId, externalTransactionId)`, is independently unique. A different idempotency key cannot reuse it; that produces `ErrExternalTransactionExists`, also mapped to HTTP `409`.

HTTP and SQS parse into the same `domain.WagerRequest` and call the same application service, so logically identical commands share the wager hash. SQS additionally has a separate raw-body Inbox hash for `(consumerName, messageId)`. A textual message-body change can therefore conflict at the Inbox boundary even when its domain fields would produce the same wager hash.

## Inbox, Outbox, and events

### Inbox

Inbound SQS identity is `(consumerName, messageId)`. The Inbox stores a SHA-256 hash of the exact raw message body. Reusing a message ID with different bytes produces an Inbox payload conflict rather than silently treating different messages as one.

For a new command, Inbox registration, wagering processing, financial persistence, Outbox insertion, and Inbox completion occur in the same PostgreSQL transaction. A completed redelivery can be acknowledged without repeating financial effects. Malformed or conflicting messages do not reach durable completion and follow retry/DLQ behavior.

### Outbox

Events are inserted into PostgreSQL in the same transaction as their corresponding committed state. A background publisher later sends them to `wager-events.fifo` and marks them published only after `SendMessage` succeeds.

Each event ID is generated at persistence time and remains stable across publication retries. Multiple publishers coordinate through PostgreSQL locking. If sending succeeds but the process fails before marking the row published, the same event may be sent again. Downstream consumers must therefore deduplicate and tolerate at-least-once delivery.

The envelope contains `eventId`, `eventType`, `aggregateId`, `correlationId`, optional `causationId`, `occurredAt`, `version`, and `data`.

Current event types are:

- `WagerTransactionProcessed`
- `WagerTransactionRejected`
- `WagerTransactionPendingReference`
- `WalletBalanceChanged`

`LOSS` emits `WagerTransactionProcessed` but no `WalletBalanceChanged`. Rejected wagers emit `WagerTransactionRejected` and no ledger movement or balance-change event. A positive opening balance emits processed-opening and balance-change events; zero opening balance emits neither.

## SQS retry, DLQ, and recovery

Inbound commands use `wager-transactions.fifo`; poison commands redrive to `wager-transactions-dlq.fifo`. The consumer long-polls for up to ten seconds and requests one message at a time. Delivery is at least once.

After a normal runtime processing failure, the consumer adjusts message visibility using deterministic delays based on receive count: 5, 10, 20, 40, then at most 60 seconds. It does not delete the message. The local redrive policy uses `maxReceiveCount=3`; production provisioning owns that policy.

SQS native redrive moves poison messages. The application does not call `SendMessage` on the command DLQ. The DLQ monitor reads only approximate queue depth and does not receive or delete dead-letter messages.

Durable success is followed by `DeleteMessage`. Failure before durable completion, malformed input, message-ID payload conflict, or transport interruption leaves the message available for retry and eventual redrive. A crash after database commit but before delete is safe: Inbox completion and wagering idempotency identify the redelivery without another financial effect.

The local environment also provisions `wager-events-dlq.fifo`, but the application does not currently monitor or consume it.

## Authentication and authorization

The HTTP API uses OAuth2/OIDC client-credentials identities. Token verification uses issuer discovery or an explicitly configured trusted JWKS URL and validates signature, issuer, `wagering-api` audience, expiry, and not-before constraints through the OIDC verifier.

The authenticated client identity is authoritative:

- allowlisted provider clients may perform wagering operations;
- `wagering-internal` may administer and reconcile wallets;
- a body or URL provider ID must equal the authenticated provider;
- one provider cannot query or mutate another provider's transactions.

HTTP returns `401` for missing or invalid authentication and `403` for authorization or provider mismatches. The imported Keycloak realm and its deterministic client secrets are development fixtures, not production credentials.

SQS preserves a separate trusted-message boundary: `data.providerId` is passed to the application service without applying HTTP/OIDC behavior.

## Messaging security and IAM

Runtime modules share one AWS identity today, but their permissions are separable:

| Component | Queue | Required operations | Reason |
| --- | --- | --- | --- |
| Inbound consumer | `wager-transactions.fifo` | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` | Receive commands, acknowledge durable completion, and schedule retry visibility. |
| Readiness check | `wager-transactions.fifo` | `sqs:GetQueueAttributes` | Verify command-queue access. |
| Outbox publisher | `wager-events.fifo` | `sqs:SendMessage` | Publish committed Outbox events. |
| DLQ monitor | `wager-transactions-dlq.fifo` | `sqs:GetQueueAttributes` | Observe approximate DLQ depth. |

A combined least-privilege policy for the current runtime identity is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConsumeWagerCommands",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:<region>:<account-id>:wager-transactions.fifo"
    },
    {
      "Sid": "PublishWagerEvents",
      "Effect": "Allow",
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:<region>:<account-id>:wager-events.fifo"
    },
    {
      "Sid": "ObserveCommandDLQ",
      "Effect": "Allow",
      "Action": "sqs:GetQueueAttributes",
      "Resource": "arn:aws:sqs:<region>:<account-id>:wager-transactions-dlq.fifo"
    }
  ]
}
```

The runtime does not need queue creation, deletion, purge, redrive configuration, or `SendMessage` access to the command DLQ. Provisioning/bootstrap identities own `sqs:CreateQueue`, `sqs:GetQueueUrl`, `sqs:GetQueueAttributes`, and `sqs:SetQueueAttributes`; integration-test cleanup also uses `sqs:DeleteQueue`.

Production uses the AWS SDK default credential chain with short-lived IAM role or workload-identity credentials, such as ECS task roles, EKS IRSA/Pod Identity, or EC2 instance roles. Static long-lived access keys are not recommended. LocalStack's `test/test` credentials are disposable local values.

When `SQS_ENDPOINT` is unset, the SDK uses the normal regional AWS endpoint. An explicit endpoint selects LocalStack or another intentional compatible service. Production traffic uses TLS; queues must not be publicly readable or writable. Server-side encryption should be enabled as appropriate. Customer-managed KMS keys require corresponding KMS permissions, and a VPC endpoint is an optional network-hardening measure.

Messages contain business identifiers and monetary values. Queue and log access should be restricted accordingly; the repository does not establish a legal PII classification.

## Reconciliation and ledger reads

Reconciliation starts one read-only PostgreSQL `REPEATABLE READ` transaction. It reads the stored wallet balance and ordered ledger entries from that same snapshot, validates entry continuity and debit/credit arithmetic, and calculates:

- stored balance;
- ledger-derived balance;
- signed difference, stored minus calculated;
- consistency;
- checked entry count.

The operation is diagnostic and never rewrites financial state. Divergence produces a structured warning and increments the existing reconciliation divergence metric.

Ledger API reads use stable keyset pagination ordered by `(created_at, id)` and expose an opaque cursor. This avoids the shifting rows and increasing cost associated with large `OFFSET` values.

## Lifecycle and graceful shutdown

Uber Fx constructs dependencies and owns startup/shutdown. Startup acquires the HTTP listener before launching the SQS consumer, Outbox publisher, pending-reference resolver, and DLQ monitor. If binding fails, startup returns an error and workers are not left running.

Shutdown first gives the SQS consumer its Fx shutdown context, then cancels the shared worker context. The consumer uses two contexts:

- **Receive/acceptance context:** cancellation stops long polling and prevents newly received work from beginning processing.
- **Processing context:** already-started work receives a drain opportunity independent of receive cancellation.

The drain budget is ten seconds or the earlier Fx shutdown deadline. Successful context-aware processing during drain may commit and delete its message. Failed or canceled drain processing does not delete the message or change its visibility after shutdown cancellation; normal runtime failures retain visibility-backoff behavior.

At the drain deadline the worker cancels the processing context. Production PostgreSQL and AWS operations observe cancellation and are expected to return promptly. Go cannot forcibly terminate an arbitrary synchronous function that ignores context.

Fx waits for workers before shutting down HTTP and before PostgreSQL lifecycle cleanup. This prevents background work from using already-closed database resources.

## Database migrations and recovery

Schema evolution uses ordered, source-controlled SQL up/down migrations under `migrations/`. The Compose `migrate` service applies pending migrations before application startup, and the application service depends on its successful completion.

Migration version and dirty state are stored in PostgreSQL. A failed/dirty migration prevents clean progression and causes the startup dependency to fail rather than launching the application against an uncertain schema. Recovery requires investigating the failed change and deliberately repairing the migration state; force/reset is not the normal workflow.

Migrations include explicit up/down definitions, but rollback is schema- and data-compatibility dependent. They allow the same ordered history to be reproduced on a fresh database when those compatibility preconditions hold. Migration `000006` is a concrete example: referenced `WIN` data is valid in version 6 but violates the older REFUND/ROLLBACK-only reference constraint restored by its down migration. PostgreSQL therefore rejects that rollback until operators deliberately resolve or migrate the incompatible data. This is normal forward-schema compatibility behavior, not a financial-integrity defect. Database constraints, triggers, enum values, and indexes remain versioned alongside application expectations.

## Observability and health

The application emits structured JSON logs with selected identifiers such as correlation ID, message ID, transaction ID, wallet ID, and provider ID. It does not log AWS credentials, bearer tokens, or complete SQS message bodies. Complete financial request payloads are intentionally excluded.

Prometheus-compatible metrics cover wager outcomes, replay counts, processing latency, SQS retries/errors, DLQ depth, concurrency conflicts, Outbox publication/retries/lag, and reconciliation divergence.

Liveness reports that the process is alive. Readiness verifies PostgreSQL connectivity and `GetQueueAttributes` access to the command queue. The DLQ monitor independently observes approximate command-DLQ depth. Observability describes behavior but is not part of the correctness mechanism; distributed tracing is not implemented.

## Failure semantics

The system distinguishes failure categories rather than forcing every problem into a wager state:

| Category | Examples | Durable/transport behavior |
| --- | --- | --- |
| Terminal business rejection | Insufficient BET funds, invalid/mismatched reference, duplicate reversal, expired reference, insufficient reversal debit funds | Persist `REJECTED` with original result balance and failure code; no financial movement. |
| Durable unresolved reference | Missing or non-terminal reference | Persist `PENDING_REFERENCE`; retry durably until resolution or expiry. |
| Identity conflict | Idempotency hash mismatch or duplicate provider/external identity | Return a distinct conflict; database uniqueness remains authoritative under races. |
| Infrastructure/transport failure | PostgreSQL, SQS, context, or unexpected runtime error | Roll back uncommitted work; retry according to HTTP/SQS/worker behavior; do not synthesize `FAILED`. |

Important crash windows are intentional and bounded:

- Before database commit, rollback prevents partial visible financial state.
- After SQS database commit but before message delete, Inbox/idempotency makes redelivery safe.
- After event publish but before Outbox acknowledgement, duplicate publication may occur with the same stable event ID.
- During an SQS outage, committed Outbox rows remain durable for later publication and readiness reports the dependency unavailable.
- During a PostgreSQL outage, transactional processing stops and readiness reports failure.

Domain rejection is not necessarily an HTTP transport error. For example, insufficient funds is an accepted wagering result with state `REJECTED`; identity conflicts map to HTTP `409`, while unexpected infrastructure failures map to `5xx`.

## Trade-offs and limitations

- **PostgreSQL coordination:** centralizing financial truth simplifies durable correctness but makes database availability necessary for processing.
- **Pessimistic wallet locking:** same-wallet work is serialized to prioritize correctness; different wallets retain parallelism.
- **`READ COMMITTED` mutations:** explicit aggregate locking and constraints avoid globally paying the cost of `SERIALIZABLE`; reconciliation opts into `REPEATABLE READ` for its snapshot.
- **At-least-once messaging:** Inbox and stable event IDs make duplicates manageable, but neither SQS nor the Outbox claims exactly-once delivery.
- **Outbox publication transaction:** holding a database claim while calling SQS is simple and safe for this scope but can keep a transaction open during network latency.
- **Polling resolvers:** pending references and Outbox work use durable polling/backoff rather than an external scheduler.
- **Cooperative shutdown:** context-aware production operations drain predictably, but Go cannot force-stop context-ignoring synchronous code.
- **Contract-sensitive hashing:** changing canonical fields, order, names, money formatting, or omission rules changes persisted idempotency identity.
- **Reserved states:** `PENDING` and `FAILED` remain for compatibility even though current runtime paths do not create them.
- **Deployment shape:** multiple logical modules currently share one application process rather than being independently deployed microservices.
- **Development defaults:** LocalStack, the imported Keycloak realm, localhost-oriented URLs, and deterministic credentials support local reproducibility and must be overridden or replaced in production.
