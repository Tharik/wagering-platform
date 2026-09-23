# Architecture

## 1. Overview

The Wagering Platform is designed around one primary requirement: financial state must remain correct under concurrency, retries, duplicate delivery, out-of-order messages, application crashes, and multiple application instances.

PostgreSQL is the system of record and the main coordination mechanism.

The application intentionally does not rely on process-local locks or in-memory idempotency because those mechanisms would not remain correct when multiple application instances are running.

The main architectural building blocks are:

- Go application composed with Uber Fx;
- PostgreSQL for wallets, wagers, ledger, Inbox, Outbox, and retry state;
- SQS FIFO through LocalStack for local asynchronous messaging;
- Keycloak for OAuth2/OIDC authentication;
- background workers for message consumption, Outbox publication, pending-reference resolution, and DLQ monitoring.

The code is separated into domain, application, infrastructure, worker, and composition-root concerns.

---

## 2. Financial representation

Money is represented internally using signed 64-bit integer minor units.

For example:

```text
100.00 BRL -> 10000
80.00 BRL  -> 8000
```

Floating-point arithmetic is deliberately avoided.

External monetary values are parsed from fixed-decimal strings and validated before becoming domain money values.

The parser rejects unsupported representations such as negative values, excessive decimal scale, scientific notation, NaN, and Infinity.

This makes wallet and ledger arithmetic deterministic and avoids floating-point rounding errors.

Currency is part of the financial identity and is validated across wallet and wager operations.

---

## 3. PostgreSQL as the consistency boundary

PostgreSQL is used not only for persistence but also as the consistency and coordination boundary.

Important state is persisted rather than kept only in application memory:

- wallet balances and versions;
- wager transactions and results;
- idempotency identity and canonical request hash;
- ledger movements;
- Inbox processing state;
- Outbox publication state;
- pending-reference retry state.

Transactions use PostgreSQL's default `READ COMMITTED` isolation level.

Correctness for wallet mutations does not depend on raising the global isolation level. Instead, the application explicitly locks the wallet row while processing financial operations and combines that locking with database uniqueness and arithmetic constraints.

This keeps the locking scope aligned with the aggregate that requires serialization.

---

## 4. Wallet concurrency

Operations that mutate a wallet acquire a PostgreSQL row lock using:

```sql
SELECT ... FOR UPDATE
```

The lock is held for the duration of the database transaction.

As a result, concurrent operations targeting the same wallet are serialized by PostgreSQL.

Consider a wallet containing `100.00` and two independent BET transactions of `80.00`.

Both requests may reach different application processes at the same time.

One transaction acquires the wallet lock first, observes `100.00`, debits `80.00`, writes its ledger entry, and commits.

The second transaction then acquires the lock and observes the new balance of `20.00`. It is rejected for insufficient funds.

The expected result is therefore:

```text
BET A: PROCESSED
BET B: REJECTED / INSUFFICIENT_FUNDS
wallet balance: 20.00
successful debit ledger entries: 1
```

This behavior does not depend on which application instance receives the request.

Operations against different wallets lock different rows and can proceed independently.

This approach was validated using independent database pools and three separately running application processes.

---

## 5. Transaction boundaries

Financial mutations are performed inside PostgreSQL transactions.

For a successful wager, the transaction can include:

1. idempotency and transaction validation;
2. wallet locking;
3. wager state persistence;
4. wallet balance/version mutation;
5. append-only ledger insertion;
6. Outbox event insertion.

The transaction commits only after the complete business operation has been persisted.

If any step fails, the transaction is rolled back.

This prevents states such as:

```text
wallet debited but wager missing
wallet changed but ledger missing
wager processed but event intent missing
```

SQS processing extends this boundary by also including Inbox state in the same transaction.

---

## 6. Canonical wagering idempotency

Idempotency is persisted in PostgreSQL and therefore works across:

- application restarts;
- multiple application instances;
- HTTP retries;
- SQS redeliveries;
- HTTP/SQS cross-channel retries.

The wager hash is produced by deterministic JSON serialization of a concrete Go struct. This is not a generic canonical-JSON scheme. The struct fields are declared and serialized in this order:

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

For example, a BET without a reference serializes to compact JSON with the following logical content and field order:

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

The implementation uses `encoding/json`, SHA-256, and lowercase hexadecimal encoding. The resulting 64-character value is stored in `wager_transactions.payload_hash`, a `CHAR(64)` column. The hash provides deterministic request identity comparison; it is not password hashing. Cryptographic collision risk is accepted as negligible, while the selected canonical fields define the business identity.

External money is parsed before the domain request is built. The canonical amount is the fixed two-decimal result of `Money.String()`, and currency comes from `Money.Currency()`. Invalid external money formats fail before a persisted wager identity or hash is established.

`referenceExternalTransactionId` uses `omitempty`. A missing reference and an explicitly empty reference both become an empty Go string and are omitted, so they hash identically. A non-empty reference is serialized as the final field and changes the hash.

The wager hash excludes the `Idempotency-Key` itself, transaction ID, state, result balance, failure code, timestamps, correlation ID, causation ID, HTTP headers, authentication token, SQS message ID, receipt metadata, and raw transport JSON.

The persisted idempotency identity is:

```text
(providerId, idempotencyKey)
```

The key selects the idempotency record but is not part of the canonical payload hash. `providerId` is both part of the hash and part of the database identity. No existing record allows processing to continue. The same provider/key and hash replays the original transaction ID, state, result balance, and failure code, with `idempotentReplay: true`; a different hash returns `ErrIdempotencyConflict`. HTTP returns `201` for first accepted processing and `200` for replay.

Provider external transaction identity is an independent database constraint:

```text
(providerId, externalTransactionId)
```

Using another idempotency key does not permit reuse of that identity. A different key with the same provider/external transaction returns `ErrExternalTransactionExists`. HTTP maps both conflict classes to `409 Conflict`.

HTTP and SQS parse their transport inputs into the same `domain.WagerRequest` and call the same wagering application service, so they use the same canonical wager hash. HTTP obtains the authoritative provider from the authenticated principal after matching it against the body; SQS preserves its existing trusted-message provider model.

SQS also has a separate Inbox identity based on `(consumerName, messageId)`. The Inbox hashes the exact raw message body to detect reuse of a message ID with different transport content. That transport-level hash is distinct from wagering idempotency: a textual SQS-body change can conflict at the Inbox boundary even when its logical wager fields would produce the same canonical wager hash.

Database uniqueness constraints provide the race-safe boundary for concurrent requests attempting to create the same idempotency or external transaction identity.

Idempotency is therefore not dependent on a local cache or mutex.

---

## 7. Financial ledger

Every successful financial movement produces an append-only ledger entry.

A ledger entry records:

- wallet;
- transaction;
- direction;
- amount;
- balance before;
- balance after.

Database constraints verify the movement arithmetic.

Conceptually:

```text
CREDIT:
balanceAfter = balanceBefore + amount

DEBIT:
balanceAfter = balanceBefore - amount
```

The database also enforces non-negative balances.

A wallet/transaction combination can produce at most one ledger movement.

The ledger is made append-only at the database level using triggers that reject `UPDATE` and `DELETE`.

Rejected transactions such as insufficient-funds BETs do not produce financial ledger movements.

### Opening balance

A wallet created with a positive initial balance creates an internal `OPENING` transaction and corresponding ledger movement.

The wallet, opening transaction, ledger entry, and Outbox events are committed atomically.

A wallet created with zero initial balance does not create a fake financial movement.

---

## 8. Wallet version

Wallets contain a version number starting at `1`.

The version changes only when a financial movement changes the wallet.

Rejected operations and idempotent replays do not increment the wallet version.

The version is useful both for external state representation and for describing the sequence of successful wallet mutations.

---

## 9. REFUND and ROLLBACK

Reversal operations reference an existing transaction.

A `REFUND` performs a full reversal of a successfully processed BET.

A `ROLLBACK` performs the opposite financial movement of a successfully processed supported reference transaction.

Reference validation verifies that the reference belongs to the expected:

- provider;
- player;
- wallet;
- currency;
- round.

The reference transaction must also be in a valid state and of a supported type.

Partial reversals are intentionally not supported.

### Single successful reversal

Application validation alone would not be sufficient because a REFUND and ROLLBACK could race in different processes.

A database partial unique index therefore guarantees that only one successful REFUND/ROLLBACK can exist for a referenced transaction.

This makes the invariant safe even under concurrent execution.

### Reversal debit

Some reversals require debiting the wallet.

If the wallet no longer contains enough funds for the reversal, the transaction is rejected with a distinct insufficient-funds result instead of making the wallet negative.

### Persisted wager lifecycle

The runtime actively writes `PENDING_REFERENCE`, `PROCESSED`, and `REJECTED` wager states.

`PENDING` and `FAILED` remain in the persisted state type for compatibility. The current runtime does not write either state. Readers treat `PENDING` as unresolved and `FAILED` as terminal unsuccessful when encountered.

Infrastructure failures roll back the transaction instead of creating a durable `FAILED` wager. There is currently no `WagerTransactionFailed` event.

---

## 10. Out-of-order references

At-least-once distributed messaging does not guarantee that a referenced transaction will always be available before its reversal is received.

For example:

```text
ROLLBACK arrives
BET has not arrived yet
```

Immediately rejecting the ROLLBACK could produce an incorrect result if the BET arrives shortly afterward.

For that reason, missing-reference reversals enter:

```text
PENDING_REFERENCE
```

The pending transaction and retry metadata are stored in PostgreSQL.

A background resolver searches for due pending references and retries them.

---

## 11. Pending-reference concurrency and recovery

Pending-reference resolution is safe across multiple resolver instances.

Eligible records are claimed using PostgreSQL locking with:

```sql
FOR UPDATE SKIP LOCKED
```

This allows several application instances to run the resolver without processing the same pending transaction simultaneously.

When the referenced transaction becomes available, normal reference validation and wallet processing are performed.

If the reference remains unavailable beyond the retry/expiry policy, the pending transaction becomes `REJECTED`.

Because retry attempts and next-attempt timestamps are persisted, pending-reference processing survives application restarts.

The behavior was tested by:

1. creating a pending reversal;
2. shutting down the original database/application context;
3. creating a new application context;
4. making the reference available;
5. resolving the persisted pending transaction.

---

## 12. Pending-reference correlation continuity

Incoming HTTP and SQS operations carry correlation information used for observability and event propagation.

When a reversal enters `PENDING_REFERENCE`, its original `correlationId` and optional `causationId` are persisted with the wager transaction in PostgreSQL together with the durable retry state.

The pending-reference resolver loads those persisted identifiers when it later resolves or rejects the transaction.

As a result, asynchronous completion preserves the original tracing context across:

- retry delays;
- application restarts;
- execution by a different application instance;
- successful reference resolution;
- reference expiry and rejection.

The final `WagerTransactionProcessed` or `WagerTransactionRejected` event therefore retains the correlation context of the operation that originally created the pending transaction rather than creating an unrelated tracing chain.

This behavior is covered by integration tests that persist a pending reversal, recreate the application/database context, resolve or expire the transaction, and verify the correlation metadata in the resulting Outbox event.

The database columns supporting this behavior are introduced by migration `000005_pending_reference_correlation`.

---

## 13. SQS delivery model

The inbound command queue is:

```text
wager-transactions.fifo
```

with:

```text
wager-transactions-dlq.fifo
```

as its dead-letter queue.

The system assumes at-least-once delivery.

Therefore, duplicate messages are expected behavior rather than exceptional behavior.

A message is deleted from SQS only after the corresponding database transaction commits successfully.

### Messaging security and IAM

The runtime components share one process and AWS identity today, but their permissions can be understood independently:

| Component | Queue | Required operations | Reason |
| --- | --- | --- | --- |
| Inbound consumer | `wager-transactions.fifo` | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` | Receive commands, acknowledge durable processing, and schedule retry visibility. |
| Readiness check | `wager-transactions.fifo` | `sqs:GetQueueAttributes` | Verify access to the configured command queue. |
| Outbox publisher | `wager-events.fifo` | `sqs:SendMessage` | Publish committed Outbox events. |
| DLQ monitor | `wager-transactions-dlq.fifo` | `sqs:GetQueueAttributes` | Observe approximate DLQ depth for metrics. |

A combined least-privilege policy for the current single runtime identity is:

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

SQS native redrive moves poison commands to `wager-transactions-dlq.fifo` after `maxReceiveCount`. The application does not require `sqs:SendMessage` on that DLQ; its monitor only reads queue depth and does not receive or delete messages. The local environment also provisions `wager-events-dlq.fifo`, but the application does not currently monitor or consume it.

Queue administration belongs to a separate infrastructure, deployment, bootstrap, or test identity. Local provisioning uses `sqs:CreateQueue`, `sqs:GetQueueUrl`, `sqs:GetQueueAttributes`, and `sqs:SetQueueAttributes`; integration-test cleanup also uses `sqs:DeleteQueue`. These administrative actions, redrive configuration, and `sqs:PurgeQueue` are not normal runtime permissions.

Production credentials should be short-lived and supplied through an IAM role or workload identity using the standard AWS SDK credential chain. Suitable mechanisms include ECS task roles, EKS IRSA or Pod Identity, and EC2 instance roles. The `test` credentials in Docker Compose are disposable LocalStack values, not production examples. The LocalStack endpoint override is development-only; production uses AWS SQS service endpoints over TLS. Private connectivity through a VPC endpoint is an optional deployment hardening measure, and queues must not be publicly readable or writable.

Production queues should use SQS server-side encryption as appropriate. A customer-managed KMS key is optional according to deployment requirements and requires corresponding KMS permissions for the relevant producers and consumers.

Command and event payloads contain business and operational data, including player IDs, wallet IDs, transaction IDs, and monetary values. Queue and log access should therefore be limited accordingly. The application does not log AWS credentials, bearer tokens, or complete SQS message bodies. It does log selected operational identifiers for observability.

---

## 14. Transactional Inbox

SQS deduplication is backed by a persistent Inbox.

Inbox identity is based on:

```text
consumerName + messageId
```

and the Inbox also stores the message payload hash.

For a new message, the database transaction includes:

1. Inbox registration;
2. wager processing;
3. wallet/ledger/Outbox changes when applicable;
4. Inbox completion.

Only after this transaction commits is the SQS message deleted.

### Crash after commit

Consider:

```text
database COMMIT succeeds
process crashes
DeleteMessage never happens
```

SQS redelivers the message.

The Inbox identifies the message as already completed, so the consumer can safely acknowledge the replay without producing another financial effect.

This behavior is durable across process restarts.

---

## 15. Transactional Outbox

Publishing an event directly to SQS before committing the business transaction creates an unsafe failure window.

For example:

```text
event published
database transaction fails
```

A consumer would then observe an event describing a state that never committed.

The application therefore uses a transactional Outbox.

Domain events are inserted into PostgreSQL in the same transaction as the corresponding business state.

A background publisher later publishes those records to SQS.

After successful publication, the Outbox record is marked as published.

### Crash after publish

Another unavoidable failure window exists:

```text
SendMessage succeeds
process crashes
Outbox row was not marked published
```

After restart, the event may be published again.

This is expected at-least-once behavior.

The Outbox event ID is generated when the event is persisted and remains stable across publication retries.

Downstream consumers can therefore deduplicate using `eventId`.

### Multiple publishers

Multiple Outbox publishers coordinate using PostgreSQL locking.

Records are claimed in a way that prevents active publishers from simultaneously publishing the same claimed row during normal execution.

The implementation deliberately accepts the possibility of duplicate publication after an ambiguous crash because eliminating that failure window would require distributed exactly-once semantics between PostgreSQL and SQS.

---

## 16. Event envelope

Outbound events use a common envelope containing:

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

`causationId` is optional where no meaningful upstream cause exists.

The stable `eventId` is particularly important for Outbox retries and downstream deduplication.

Wallet balance-change events include the relevant wallet/currency state without requiring consumers to reconstruct the financial movement from an unstructured payload.

---

## 17. HTTP and SQS share the application use case

HTTP and SQS are transport mechanisms rather than separate implementations of wagering rules.

Both eventually execute the same wagering application logic and use the same PostgreSQL state.

This prevents transport-specific differences in:

- financial validation;
- idempotency;
- wallet locking;
- ledger creation;
- reversal behavior.

Cross-channel idempotency was explicitly tested by submitting a transaction through HTTP and replaying the same logical transaction through SQS.

Only one financial movement is produced.

---

## 18. Authentication and provider isolation

OAuth2/OIDC authentication is delegated to Keycloak in the local environment.

The API validates:

- token signature through OIDC discovery;
- issuer;
- expected `wagering-api` audience.

The authenticated client identity determines the provider.

The provider is not trusted simply because a request payload or URL claims a particular provider ID.

Development provider clients are explicitly allowlisted.

Wallet administration and reconciliation require the internal client identity.

This creates two trust boundaries:

```text
provider clients -> wagering operations
internal client  -> wallet administration
```

Provider isolation prevents one authenticated provider from accessing another provider's transactions.

The Keycloak realm and development credentials stored in the repository are local-development configuration and are not intended as production secrets.

---

## 19. Reconciliation

Reconciliation reconstructs financial state from the append-only ledger and compares it with the current wallet state.

The process validates:

- ledger continuity;
- debit/credit arithmetic;
- reconstructed final balance;
- current wallet balance.

Reconciliation is diagnostic.

If divergence is detected, the application reports it and increments the reconciliation divergence metric.

It does not automatically overwrite financial state.

Automatic correction could hide the root cause of a financial inconsistency and is therefore intentionally avoided.

---

## 20. Ledger pagination

Ledger reads use stable keyset pagination.

The ordering key combines:

```text
created_at
id
```

The client receives an opaque cursor rather than database pagination details.

Keyset pagination avoids the instability and increasing cost associated with large `OFFSET` values while preserving deterministic traversal when multiple rows have nearby timestamps.

---

## 21. Observability

The application uses structured JSON logging.

Contextual identifiers are included when available, including:

```text
correlationId
messageId
transactionId
walletId
providerId
```

Sensitive authentication information and complete financial request payloads are intentionally excluded from logs.

Prometheus-compatible metrics cover:

- processed/rejected/pending wager results;
- idempotent replays;
- processing latency;
- SQS retries and processing errors;
- DLQ depth;
- concurrency conflicts;
- Outbox publication results and retries;
- Outbox publication lag;
- reconciliation divergences.

Observability is designed to describe system behavior without becoming part of the correctness mechanism.

---

## 22. Health and readiness

Liveness and readiness have different purposes.

Liveness indicates that the process is alive.

Readiness verifies dependencies required for useful processing.

The readiness endpoint checks:

- PostgreSQL;
- SQS command queue accessibility.

If LocalStack/SQS becomes unavailable, readiness becomes unhealthy.

When SQS becomes available again, readiness recovers without requiring an application restart.

---

## 23. Uber Fx and lifecycle management

Uber Fx is used as the application composition root.

Infrastructure and application dependencies are constructed through Fx providers.

Fx lifecycle hooks own startup and shutdown.

On startup, the HTTP listener is acquired before background workers are launched.

If the HTTP address cannot be bound, startup returns an error and Fx rolls back instead of leaving workers active behind a non-functional HTTP service.

Background components include:

- SQS consumer worker;
- Outbox publisher worker;
- pending-reference resolver worker;
- DLQ monitor.

On shutdown:

1. the shared worker context is cancelled;
2. the application waits for workers to terminate;
3. the HTTP server shuts down;
4. PostgreSQL resources are released through lifecycle hooks.

This ordering avoids workers attempting to use database resources that have already been closed.

---

## 24. Multi-instance design

The application is intentionally designed so correctness does not depend on a single process.

Coordination mechanisms are external and durable:

```text
wallet serialization       -> PostgreSQL row locks
idempotency                -> PostgreSQL
reversal uniqueness        -> PostgreSQL constraints
message deduplication      -> Inbox
event delivery intent      -> Outbox
pending-reference recovery -> PostgreSQL
```

There are no process-local wallet mutexes acting as the source of financial correctness.

This was validated by running three independent application processes against the same PostgreSQL and messaging infrastructure.

---

## 25. Failure semantics

The architecture assumes components can fail at inconvenient moments.

### Crash before database commit

The transaction rolls back and no partial financial state becomes visible.

### Crash after database commit

Committed financial state remains authoritative.

For SQS, redelivery is handled through Inbox/idempotency.

### Crash after event publish but before Outbox acknowledgement

The event may be published again with the same stable event ID.

### PostgreSQL outage

Transactional processing cannot continue and readiness reports the dependency failure.

### SQS outage

Committed Outbox records remain durable and publication can resume when SQS recovers.

Readiness reports SQS as unavailable.

### Missing reference

The transaction remains durable as `PENDING_REFERENCE` and can be resolved by any application instance after the reference becomes available.

### Poison message

Repeated failures eventually cause SQS redrive to the configured DLQ.

---

## 26. Database constraints as a final safety boundary

Important invariants are protected both by application logic and, where practical, by PostgreSQL.

Examples include:

- wallet balance cannot be negative;
- wallet version cannot be below its initial value;
- wallet uniqueness by player/currency;
- ledger amounts must be positive;
- ledger before/after arithmetic must be valid;
- one ledger movement per wallet/transaction;
- ledger mutation is forbidden;
- Inbox identity is unique;
- provider external transaction identity is unique;
- provider idempotency identity is unique;
- only one successful reversal can exist for a reference;
- only one opening transaction can exist for a wallet.

Application validation provides useful domain errors, while database constraints remain the final defense against races and programming mistakes.

---

## 27. Deliberate trade-offs

### Pessimistic wallet locking

`SELECT ... FOR UPDATE` can reduce throughput for a single extremely hot wallet because mutations are serialized.

That is intentional.

For a financial aggregate, deterministic correctness is preferred over allowing concurrent balance modifications that later require conflict repair.

Different wallets remain independently concurrent.

### PostgreSQL default isolation level

The service uses PostgreSQL's default `READ COMMITTED` isolation level rather than globally using `SERIALIZABLE`.

Explicit aggregate locking and database constraints are used for the invariants that require serialization.

This avoids unnecessary serialization failures across unrelated operations.

### Outbox transaction during publication

The publisher coordinates claimed Outbox rows through PostgreSQL while performing external publication.

This keeps ownership semantics simple and safe for the challenge but can keep database transactions open while waiting for SQS.

A higher-throughput production design could introduce explicit claim/lease state so network publication occurs outside the claim transaction.

That design would require additional lease-expiry and crash-recovery semantics.

### Development infrastructure

LocalStack and the imported Keycloak realm prioritize deterministic local reproducibility.

Production deployment would use managed/configured infrastructure, secret management, hardened identity configuration, TLS, and environment-specific provisioning.

---

## 28. Correctness philosophy

The design follows a simple hierarchy:

```text
financial correctness
        >
durable recovery
        >
multi-instance safety
        >
operational visibility
        >
implementation convenience
```

The system assumes:

- requests can be duplicated;
- messages can be redelivered;
- references can arrive out of order;
- application processes can crash;
- several application instances can process work concurrently;
- external infrastructure can temporarily fail.

Correctness therefore lives in durable transactional state and database-enforced invariants rather than in assumptions about request ordering or process lifetime.
