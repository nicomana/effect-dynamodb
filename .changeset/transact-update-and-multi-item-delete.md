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
and `RefNotFound`. A plain update, which standalone relies on its write's own
condition to find a missing row, is checked with one read instead, and a missing
row takes the op's own path: a complete `.set()` is planned as its create. Because the transactional write is the standalone write, the
two cannot drift.

The recorded items join the transaction as a guarded write and are read the way
a guarded put's are:

- A taken unique value is `UniqueConstraintViolation`.
- A cancelled main item is `TransactionCancelled` when the caller's own
  `.condition()` rejected a row that is still there.
- Any other cancellation is a lost race, which is planned again from a fresh
  read. A row deleted meanwhile then takes the op's missing-row path, and a
  pinned `expectedVersion` it no longer has fails `OptimisticLockError`.

A transaction whose updates all resolve to no write sends nothing, as the
standalone no-op update does.

A transaction refuses, with a `ValidationError`, an update `.cascade(...)`
(**EDD-9059**), a `.returnValues(...)` mode that returns an item on an update or
a delete (**EDD-9060**), and an update of an entity with `vectorIndexes`
(**EDD-9061**). The new `TransactWriteUpdateOp` type is `transactWrite`-only:
`EventStore.append({ additionalItems })` and `Batch.write` are unchanged.

The standalone `update` gets one fix, which transactions inherit through it: a
**`clearMap` on a versioned entity now conditions the write on the version its
read found**. Before, a bucket added between the read and the write survived the
clear, even though a comment and `DESIGN.md` described a version CAS. A
concurrent write is now an `OptimisticLockError`, and a stale `expectedVersion`
is refused against that read before anything is sent.
