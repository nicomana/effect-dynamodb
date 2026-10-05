---
"effect-dynamodb": minor
---

pr: 129

`Transaction.transactWrite` accepts `update` / `patch`, and deletes of entities with `unique`, `versioned: { retain: true }` or `softDelete`

Both used to be refused with a `ValidationError` (a delete of those entities with
**EDD-9048**). That left no way to commit an update, or a constraint-safe delete,
atomically with anything else. A typical casualty is an audit or outbox row that
must be written if and only if the business change is.

They now write exactly what the same op writes on its own. `transactWrite` runs
the entity's own op with its write recorded instead of sent, and the recorded
items join the transaction. That includes the guarded update or put of the row,
sentinel rotations and owned releases, the retain snapshot and the soft-delete
tombstone. Every read the standalone op makes still happens, so its outcomes
surface before anything is sent: `ItemNotFound`, `ConditionalCheckFailed`
(`patch` of a missing item), `OptimisticLockError` (a stale `expectedVersion`)
and `RefNotFound`. Because the transactional write is the standalone write, the
two cannot drift.

The recorded items join the transaction as a guarded write and are read the way
a guarded put's are:

- A taken unique value is `UniqueConstraintViolation`.
- A cancelled main item is `TransactionCancelled` when the caller set a
  condition (`.condition()`, `patch`, `deleteIfExists`).
- Any other cancellation is a lost race, which is planned again from a fresh
  read.

A transaction whose updates all resolve to no write sends nothing, as the
standalone no-op update does.

A transaction refuses, with a `ValidationError`, an update `.cascade(...)`
(**EDD-9059**), a `.returnValues(...)` mode that returns an item on an update or
a delete (**EDD-9060**), and an update of an entity with `vectorIndexes`
(**EDD-9061**). The new `TransactWriteUpdateOp` type is `transactWrite`-only:
`EventStore.append({ additionalItems })` and `Batch.write` are unchanged.
