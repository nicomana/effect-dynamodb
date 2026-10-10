# effect-dynamodb

## 1.25.0

### Minor Changes

- [#129](https://github.com/jmenga/effect-dynamodb/pull/129) Thanks [@nicomana](https://github.com/nicomana)! - `Transaction.transactWrite` accepts `update` / `patch`, and deletes of entities with `unique`, `versioned: { retain: true }` or `softDelete`

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
  row takes the op's own path: a complete `.set()` is planned as its create.
  Because the transactional write is the standalone write, the two cannot drift.

  The recorded items join the transaction as a guarded write and are read the way
  a guarded put's are:

  - A taken unique value is `UniqueConstraintViolation`.
  - A retain snapshot the row's history already holds is the same
    `ValidationError` the standalone op reports; re-reading would not change it.
  - A cancelled main item is `TransactionCancelled` only when the caller's own
    `.condition()` rejected it and the row is unchanged since the read: the
    stored item the cancellation returns is compared with the row the plan read
    (its version and incarnation, or the attributes the item's condition names).
  - Any other cancellation is a lost race, which is planned again from a fresh
    read. A row deleted meanwhile then takes the op's missing-row path, and a
    pinned `expectedVersion` it no longer has fails `OptimisticLockError`. A race
    lost on every attempt is `OptimisticLockError` with both versions on a
    versioned entity, otherwise `ConcurrentModification` naming what changed.

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

### Patch Changes

- Updated dependencies []:
  - @effect-dynamodb/schema@1.25.0

## 1.24.0

### Minor Changes

- [`4b8d210`](https://github.com/jmenga/effect-dynamodb/commit/4b8d210a4894e0a6be43906ffffa0098ee23628f) - EventStore command-path extensions: consistent reads, If-Match expected versions, inline projections, inline snapshots with single-request loads and stream indexes ([#136](https://github.com/jmenga/effect-dynamodb/issues/136), [#137](https://github.com/jmenga/effect-dynamodb/issues/137), [#138](https://github.com/jmenga/effect-dynamodb/issues/138), [#139](https://github.com/jmenga/effect-dynamodb/issues/139), [#140](https://github.com/jmenga/effect-dynamodb/issues/140))

  Everything is additive. Two defaults of `commandHandler` change, and neither
  changes a successful result: it now loads state with strongly consistent reads
  ([#139](https://github.com/jmenga/effect-dynamodb/issues/139)), and it folds the new events into state **before** appending them ([#137](https://github.com/jmenga/effect-dynamodb/issues/137)).
  - **Consistent reads ([#139](https://github.com/jmenga/effect-dynamodb/issues/139)).** `read`, `readFrom`, `currentVersion` and the new
    `readLatest` take `{ consistentRead?: boolean }`, which sets `ConsistentRead`
    on every `Query` page. `commandHandler` loads strongly consistently by
    default, so `decide` no longer runs against state missing the newest events;
    `commandHandler(decider, stream, { consistentRead: false })` opts out.
  - **If-Match expected versions ([#136](https://github.com/jmenga/effect-dynamodb/issues/136)).** `handle(streamId, command, { expectedVersion })`
    fails with `VersionConflict` **before `decide` runs** when the loaded version
    differs, and otherwise conditions the append on it. Neither conflict is
    retried, whatever the handler's `retry` says. `VersionConflict` gains an
    optional `actualVersion`, set by the pre-decide check (the loaded version) for
    a useful `412`. With `idempotency`, a redelivered command that already
    committed is reported as `DuplicateCommand`, not `VersionConflict`. A value
    that is not a non-negative integer fails with `ValidationError`.
  - **Inline projections ([#137](https://github.com/jmenga/effect-dynamodb/issues/137)).** A command handler's `additionalItems` may be a
    function of the `Decision` (`{ events, state, previous, version }`) returning
    transact ops, or an `Effect` of them whose error and requirements join the
    handler's. The items commit in the same transaction as the events, and the
    function is re-run on every retry. The state the handler returns, snapshots
    and projects is always the `evolve` fold of the stored and new events. `evolve`
    may mutate state in place; when it does, `previous` and `state` are the same
    object.
  - **Inline snapshots and `readLatest` ([#138](https://github.com/jmenga/effect-dynamodb/issues/138)).** `snapshot: { schema, mode: "inline" }`
    writes the snapshot in the append's own transaction (every append, or at an
    `every` cadence), so it is current after every command; `append` also accepts
    `{ snapshot: state }` directly. `stream.readLatest(streamId)` returns the
    snapshot, the events after it and the head version in one `Query` when the
    snapshot is current, and `commandHandler` now loads every snapshot-configured
    stream this way. On an `"after-append"` stream with a large `every`, a load
    reads up to `every + 1` items in exchange for fewer requests.
  - **Snapshot-only loads.** On a `mode: "inline"` stream without `every`,
    `commandHandler(decider, stream, { verifySnapshot: false })` and
    `readLatest(streamId, { verifySnapshot: false })` read the snapshot item
    alone with one `GetItem`, instead of a `Query` that also reads the newest
    event. A `Query` is charged for every item it reads, so with large events
    this roughly halves a command's read capacity. `readLatest` then returns the
    snapshot's `asOfVersion` as an **unverified** `version`. The handler
    returns nothing decided on an unverified snapshot until the head confirms
    it: a successful append confirms it by itself, and a domain error, a no-op
    or an append conflict is checked with one more read — a `Query` for events
    after the snapshot's version, which reads no items while the snapshot is
    current. It falls back to a
    verified load at once when there is no snapshot, and before reporting an
    If-Match mismatch, so `actualVersion` is the verified head; an If-Match at a
    lagging snapshot's version answers as the verified load would. Without
    `expectedVersion`, a decision found to be made on stale state is made again
    on the verified state, so `decide` (and a function-form `additionalItems`,
    when both decisions append) can run twice for one call; a snapshot at the
    head after an append conflict is a genuine race, left to the `retry`
    policy. Any other snapshot config is refused with `[EDD-9068]`; a stream
    without a snapshot config ignores the option.
  - **Stream indexes ([#140](https://github.com/jmenga/effect-dynamodb/issues/140)).** `makeStream({ indexes })` declares sub-streams of a
    stream's events ordered by a key derived from each event, on an LSI (default,
    strongly consistent) or a GSI (`type: "gsi"`, scoped to the same stream).
    `readIndex(name, streamId, { beginsWith | between, reverse, limit, consistentRead })`
    and `query.index(name, streamId)` read them, with index names type-checked.
    `EventStore.indexDefinitions(...streams)` returns the `CreateTable` fragments
    (projection `ALL`). An LSI must be created with the table, and a table with
    any LSI caps every partition key value's item collection at 10 GB. Index
    attributes are written only by `append`, so an index added later, or a
    changed `key`, covers only events appended from then on.
  - **Large commands are stepped commands.** An append stays one atomic
    transaction: it is never split. A command that decides more than one
    transaction holds fails with `AppendTooLarge` before anything is written,
    and its `count` / `limit` show the step size is too large. The new guidance
    (tutorial, `DESIGN.md`) runs such a command as stepped commands: fixed-size
    steps planned by the application, each an ordinary atomic `commandHandler`
    call chained by `expectedVersion`, with a `commandId` per step under
    idempotency — so a failure partway leaves a real, consistent state, and a
    redelivered command skips its committed steps (`DuplicateCommand`) and
    resumes at the first one not committed. One
    command → one decision → one atomic append. ([#141](https://github.com/jmenga/effect-dynamodb/issues/141)'s opt-in chunked append is
    not implemented.)

  New definition-time errors: `[EDD-9062]` invalid `snapshot.mode`, `[EDD-9063]`
  malformed stream index, `[EDD-9064]` index attribute owned by the stream,
  `[EDD-9065]` indexes sharing an index or attribute, `[EDD-9066]` undeclared
  index name, `[EDD-9067]` conflicting physical index definitions, and
  `[EDD-9068]` `verifySnapshot: false` on a stream that is not `mode: "inline"`
  without `every` (thrown when the handler is created; a defect from
  `readLatest`).

  Fix: snapshot state is now encoded with the same `decode → encode` fallback as
  events. A `Schema.Class` state folded by an immutable `evolve` that spreads
  (`({ ...s, balance })`) is a plain object, which the state schema's encoder
  alone refused; an after-append snapshot was then silently never written, and
  an inline one would have failed every command on the stream.

  Whether `decide` may mutate state ([#142](https://github.com/jmenga/effect-dynamodb/issues/142)) is left to the application: the
  library adds no read-only types or runtime guard.

  Docs: the event-sourcing tutorial gains a step for each feature, backed by the
  runnable `examples/event-sourcing.ts`, and the API reference and `DESIGN.md`
  cover the new options, types and errors.

### Patch Changes

- Updated dependencies [[`4b8d210`](https://github.com/jmenga/effect-dynamodb/commit/4b8d210a4894e0a6be43906ffffa0098ee23628f)]:
  - @effect-dynamodb/schema@1.24.0

## 1.23.0

### Minor Changes

- [#134](https://github.com/jmenga/effect-dynamodb/pull/134) [`a2904f9`](https://github.com/jmenga/effect-dynamodb/commit/a2904f9678ab71aa095959e520a5a9d529408401) Thanks [@jmenga](https://github.com/jmenga)! - Store nested dates and other transformed values in wire form, read the maps earlier versions wrote, and support nested sub-aggregates ([#133](https://github.com/jmenga/effect-dynamodb/issues/133))

  Earlier versions stored some `DateTime` values, and other values whose schema
  transforms them, as a marshalled copy of the domain object, for example
  `{ epochMilliseconds, "~effect/DateTime", _tag: "Utc" }` instead of an ISO
  string. On Effect 4.0.0 those rows either failed to read
  (`Expected DateTime.Utc`, for rows written under an Effect release candidate) or
  read back as plain objects that only looked like `DateTime`s. This release
  writes those values in their schema's wire form and reads the old maps back as
  real `DateTime`s.

  ### Before you upgrade

  - **Upgrade every reader before any writer, and don't roll back past this
    version once new rows are written.** A self date (`Schema.DateTimeUtc`,
    `Schema.Date`, or one with `storedAs`) inside a `NullOr` or other union, a
    `Record` or a `Tuple` is now stored as a string or number, on entities and
    aggregates alike. 1.22.0 fails to read it (`Expected DateTime.Utc`), or, in a
    union with a string member, reads it back as a plain string. The other shapes
    this release writes differently stay readable by 1.22.0: dates in arrays and
    arrays of classes, transform dates such as `DateTimeUtcFromString` in any
    container, refs in `many` elements declared as plain classes, and optional
    `NumberFromString` fields. An entity whose primary sort key has composites
    now keeps each item's version and soft-delete history under its own keys
    (below), which 1.22.0 doesn't read correctly: its `versions` and
    `deleted.get` mix siblings' rows with the item's, and its `getVersion` misses
    every version written in the new format.
  - **Keys are unchanged** for every existing entity and aggregate shape: `pk`,
    `sk`, GSI, unique, version, soft-delete, time-series, collection and
    list-index keys are composed byte-for-byte as before, with one exception:
    the version-snapshot and soft-delete keys of an entity whose primary sort
    key has composites (several items per partition) now carry the item's
    identity, so each item has its own history. History written by earlier
    releases stays readable and restorable. Entities without sort key
    composites keep exactly the keys they had. Rows an earlier release keyed
    differently from today's composer (an unpadded number composite written by
    1.15) are still read by queries and scans as they were. Details under
    "History of items that share a partition".
  - **Some attributes change stored type** on their next write, listed under each
    section below. For example, an optional `NumberFromString` holding `5` was
    stored as `{ "N": "5" }` and is now `{ "S": "5" }`, and a nested self date was
    a map (`M`) and is now a string or number. Until old rows are rewritten, a
    filter on such an attribute (a `filter` expression, or a `filterBy` predicate,
    which sees the stored value) can match old and new rows differently, and
    DynamoDB Streams consumers see the attribute change type.
  - **Two model shapes are now rejected at `make()`.** A union whose date member
    is stored as an epoch number next to a member also stored as a number
    (`Number`, a number literal, `BigInt`, another epoch date) fails with
    `EDD-9058`, because a stored number could belong to either member. On an
    aggregate this also covers a `NumberFromString` or `BigIntFromString` member:
    `update` re-decodes the aggregate's domain values, where those are numbers.
    Entities accept those two. Store the date as a string (the default for a self
    date) or remove the numeric member. A `DynamoModel.configure` `storedAs`
    override on a union field with more than one date member fails with
    `EDD-9057`; annotate the intended member instead.
  - **Update and delete errors changed.** A lost version race is now always an
    `OptimisticLockError` carrying the real `actualVersion`, a failed
    `.condition()` is always a `ConditionalCheckFailed`, and an update of a
    missing item is always an `ItemNotFound` (it used to be
    `OptimisticLockError(-1)` for a versioned update with `expectedVersion`).
    Two new errors exist: `ConcurrentModification` and `UpdateAppliedButUnreadable`.
    Don't retry the second one, because the write was applied. Review your
    `catchTag` handlers; the cases that changed are listed under "Updates and
    deletes".
  - **`update()` of a missing item no longer writes a partial row.** It fails
    with `ItemNotFound` and writes nothing, unless it is a plain `.set()` of a
    complete item, which the library creates through `create`. `patch()` still
    fails with `ConditionalCheckFailed` (see below).
  - **Good news if you enabled `versioned` on an existing table.** Items written
    before the entity was versioned read as version 0 on every path, and
    `expectedVersion(0)` addresses them. Their first versioned write conditions
    on no version existing, adds the incarnation token and writes version 1, and
    their retain snapshot is `v#0000000`. A race on that first write is an
    `OptimisticLockError`. Soft delete and restore work on them.
  - **Items whose version was removed are refused.** An item of a versioned
    entity that has the incarnation token (`__edd_i__`) but no version, because
    its version was removed outside the library, fails with a `ValidationError`
    on reads and on the writes that read or check the version, instead of
    reading as version 0. A query over a partition that holds one such item fails
    as a whole. Restore the version attribute to read it again. Items with
    neither (written before the entity was versioned) still read as version 0.
  - **A `put` over an existing item of a versioned or unique-constrained entity
    now reads it first and continues it** instead of resetting it to version 1.
    A concurrent write between the read and the put is retried, so the last
    writer wins as before; only a race lost on every attempt fails, with
    `OptimisticLockError` or `ConcurrentModification`, which the error channel
    gains. `create` doesn't read the item: it must be missing anyway. **A deleted
    retain item can be created again without `purge`**: it continues after the
    version history its key still holds. Details under "Puts, upserts and
    batches".
  - **Transactions and `EventStore` `additionalItems` now replace existing
    versioned and unique-constrained items.** A put there reads the item and
    writes it exactly as the entity's own `put` does, so a versioned read model
    can be kept in step with the events across appends. A taken unique value in
    `additionalItems` is now a `UniqueConstraintViolation`, not an
    `AdditionalItemConditionFailed`: that error is only for a condition you set.
    `Transaction.transactWrite` and `append` (and `commandHandler`) gain
    `OptimisticLockError` and `ConcurrentModification` (a race lost on every
    attempt), and `append` gains `UniqueConstraintViolation`.
  - **A unique sentinel is released only by the item that owns it.** An item can
    hold a unique value without owning its sentinel (the constraint was added
    later, or a `ttl`'d reservation expired and another item claimed the value).
    Every write that releases a sentinel (a put, update or upsert that changes
    the value, a delete, a soft delete, `purge`, a transaction put) now reads it
    first and releases it only if it names this item, so it can no longer delete
    another item's reservation and let the value be taken twice. That costs one
    consistent read per sentinel released.
  - **A hard delete of a retain entity snapshots the item it deletes.** The
    delete and the snapshot of the final state (`v#N`) are one transaction,
    guarded on the version read, so an item created again at the key continues
    at `N + 1` and a writer still holding version `N` can't overwrite it. That
    makes a retain hard delete a `GetItem` plus a two-item `TransactWriteItems`
    (twice the write capacity of a `DeleteItem`) instead of one `DeleteItem`.
    A delete you set no `.condition()` on is read and written again (up to three
    attempts) when another writer changes or deletes the item in between, as a
    put is; this holds for every delete that reads first (retain, unique and
    soft delete), and a delete that loses the item to a concurrent delete
    reports what a delete of a missing item does (success for retain only,
    `ItemNotFound` with unique constraints or soft delete). `deleteIfExists`
    asserts only that the item exists, which these deletes already check: it
    is retried the same way, and on a missing item it fails with
    `ConditionalCheckFailed` (on unique-constraint and soft-delete entities it
    used to fail with `ItemNotFound`; retain-only entities already gave
    `ConditionalCheckFailed`). A `.condition()` added to `deleteIfExists` is now
    ANDed with its existence check instead of replacing it. With any other
    `.condition()`, such a race fails, as before.
  - **`delete().returnValues("allOld")` returns the item it deleted** (the
    model, `undefined` when there was none), on every delete path — it returned
    nothing. The result is typed by the mode (any `ReturnValuesMode` still
    compiles). A mode DeleteItem doesn't support (`"allNew"`, `"updatedOld"`,
    `"updatedNew"`) fails with a `ValidationError` before anything is sent; it
    used to reach DynamoDB, which rejected it (or, through `Entity.returnValues`,
    was dropped). If the deleted item can't be decoded, the new
    `DeleteAppliedButUnreadable` reports it, with the item as stored: the delete
    WAS applied.
  - **Chained `.filter()`s no longer collide.** Each filter was compiled on its
    own, numbering its attribute placeholders from zero, so the second
    overwrote the first's name and the query matched the wrong attribute; they
    are now compiled as one expression.
  - **Projected names need no particular characters.** `select(["first-name"])`
    built the placeholder `#proj_first-name`, which DynamoDB rejects; a name
    that isn't letters, digits and underscores now gets a numbered placeholder.
  - **Collection queries keep their grouping through every combinator.**
    `db.collections.x(...).filter(...).collect()` returned a flat list of
    internal `{ _memberKey, _decoded }` wrappers (it lost the grouping by
    member); it is grouped like `collect()`, and so is `.fetch()`'s page (it
    returned the same wrappers, though typed as grouped). `.paginate()`, which
    streamed the wrappers too, streams each item tagged with its member
    (`{ member, item }`, typed `CollectionStreamItem`), and `CollectionQuery`
    now declares `.select()` (partial records grouped per member), `.count()`,
    `.paginate()`, `.maxPages()`, `.consistentRead()` and `.ignoreOwnership()`;
    `CollectionQuery`, `CollectionStreamItem`, `CollectionSelected` and
    `CollectionAccessors` are exported. A collection
    filter or select names each member's domain fields. A member without the
    field reads it as absent, exactly as before (so `not(...)` and
    `notExists(...)` still match its rows), and an attribute that member
    stores under that name never stands in for it.
  - **A filter can no longer widen an entity's ownership check.** A filter
    whose top level was an `OR` was ANDed with the `__edd_e__` check without
    parentheses (`#eddE IN (:et0) AND a OR b`), so another entity's rows in the
    same partition matched `b` — returned by `collect` (or failing to decode),
    counted, selected. The filter is now parenthesised.
  - **A `.condition()` no longer replaces an op's own guard.** On 1.22.0
    `create`'s not-exists check and `patch`'s and `deleteIfExists`'s exists
    check were held as the op's condition, so a `.condition()` replaced them:
    `create(item).condition(c)` overwrote an existing item whenever `c` held on
    it (`exists(n)`, say); `patch(key).condition(c)` with a `c` that holds on a
    missing item (`notExists(n)`) wrote a partial item and then failed to decode
    it; and `deleteIfExists(key).condition(c)` of a missing item succeeded,
    deleting nothing, whenever `c` held. They are now the op's own guards, ANDed
    with the caller's condition — bound, unbound, in a transaction and in
    `EventStore` additional items — so each of these fails with
    `ConditionalCheckFailed` (`TransactionCancelled` in a transaction) and writes
    nothing. As on every op, a later `.condition()` replaces an earlier one; the
    guard stays. `patch()` of a missing item now fails with
    `ConditionalCheckFailed` on every entity: one whose update reads first,
    such as a retain entity, failed with `ItemNotFound`. The exception is a
    lost race on a versioned entity — the item is deleted between the read and
    the write — which is an `OptimisticLockError`, like every lost version race.
  - **Empty conditions and filters.** On 1.22.0 `.condition({})` (or `and()`)
    sent an empty `ConditionExpression`, or `()` beside the library's guard
    (`… AND ()`), and an empty part under `or()` or `not()` was sent as
    `… OR ()` / `NOT ()`; DynamoDB rejected each with a `DynamoValidationError`,
    as it did an `isIn` with no values (`IN ()`). A condition that asserts
    nothing is now no condition — the op's own guard alone — on put, create,
    upsert, update, patch, delete, `deleteIfExists`, append and transaction ops,
    and an empty part directly under `and()` is left out. Anywhere else — under
    `or()` (it would match everything) or `not()` (nothing), an `or()` with no
    parts, or an `isIn` with no values — it is refused with a `ValidationError`
    before anything is sent, in conditions, entity and collection filters and
    aggregate `list` filters. Two of these are behaviour changes: a filter of
    `or()` with no parts was dropped, so the query matched everything, and an
    aggregate `list` filter of `or()` returned every aggregate; both now fail
    with a `ValidationError`. `.filter({})` is still no filter, and a
    `Transaction.check()` with an empty condition is refused before sending.
  - **`.consistentRead()` on a GSI is refused before sending.** DynamoDB reads
    a global secondary index only eventually consistently, and rejected the
    request with a `DynamoValidationError`; an entity index query or a
    collection with `.consistentRead()` now fails with a `ValidationError`
    without sending it. The table is read consistently, as before. An entity's
    indexes are always treated as GSIs (`db.tables.*.create()` creates them as
    GSIs), so one pointed at an LSI of a table created outside the library is
    refused too.
  - **`expectedVersion` on an entity that isn't `versioned` is refused
    (behaviour change).** On 1.22.0 a plain `update`, a `patch` and the unbound
    `Entity.expectedVersion` silently ignored it, so the update ran with no
    concurrency check at all; an update that read first (a unique-field change)
    compared it against the missing version and failed with
    `OptimisticLockError`. It now fails with a `ValidationError` before anything
    is read or sent. Add `versioned: true`, or use a `.condition()`.
  - **`Query.asParams` can fail, and `compileExpr` can throw (type-level
    change).** `asParams` now declares `ValidationError` (it declared `never`):
    it fails, as the query would, for a filter with an empty part under `or()` /
    `not()`, an `or()` with no parts or an `isIn` with no values — on 1.22.0 it
    returned params DynamoDB then rejected or, for an `or()` with no parts,
    silently dropped the filter, so the query matched every row. The exported
    `compileExpr` throws on those same expressions instead of compiling them to
    `… OR ()`, `NOT ()`, `IN ()` or an empty string.
  - **Bound queries filter and select renamed fields by their stored names.** A
    field renamed with `DynamoModel.configure(..., { field })` was projected and
    filtered under its domain name, so `select(["name"])` returned `{}` and
    `filter({ name })` matched nothing; they now use the stored attribute and
    hand items back under the domain names. On a collection whose members store
    one field under different names, a filter is judged per member and a select
    reads each member's own attribute.
  - **A transaction that touches one item twice, or passes DynamoDB's 4 MB, is
    refused before it is sent**, with a `ValidationError` naming the entity, in
    `Transaction.transactWrite` and `EventStore.append`. The items an op adds
    count: two puts that swap unique values touch the same sentinels, and a
    retain put counts twice (its item and its snapshot).
  - **Primary-key queries and scans no longer return history rows.** Version
    snapshots, soft-delete tombstones and time-series event items carry their
    entity's type, so a primary-key query with no (or a partial) sort key
    condition, a scan, and a collection on the primary key returned them as if
    they were items (a time-series partition query failed to decode its events).
    They're now left out. A row is left out only when it is positively history:
    its sort key is not the one its own stored composites compose AND it has the
    layout of a snapshot (`#v#…<version>`), a tombstone (`#deleted#…<timestamp>`)
    or an event (`#e#` under the live key). Every other row is read as before —
    including rows an earlier release keyed in a way the current composer
    doesn't reproduce (an unpadded number composite written by 1.15). This runs
    on the rows as they arrive, for queries and scans alike, so a projection
    also reads the sort key and its composites. `limit` is still sent as
    `Limit` on the first request, and each later request asks for twice the
    last, so a run of history rows costs requests logarithmic in its length;
    `maxPages` still bounds requests, so a capped query can return fewer items
    than `limit` when rows are left out. `.history()` still reads
    events. A primary-key `count()` of a retain, soft-delete or time-series
    entity reads the sort key and composites of each row to count them (the
    same read capacity as a server-side count), not whole items.
  - **`purge` removes only its own entity's rows.** It deleted every row in the
    partition, so with a collection on the primary key, purging an order also
    deleted its lines. It now deletes only rows of its own entity type.
  - **`Batch.write` sends puts of a `versioned` entity as transactions.** They go
    first, as create-only `TransactWriteItems` of up to 100 items (and under
    DynamoDB's 4 MB transaction payload), and each chunk costs twice the write
    capacity of a batch write. A put that would replace an existing item fails
    with a `ValidationError` and writes nothing from its chunk. Earlier chunks
    may already have been written, because `Batch.write` was never atomic across
    chunks. A batch that touches a versioned put's item more than once (a
    delete and a put of it, or two puts) is refused before anything is written,
    since the put runs in its own transaction and the order couldn't be kept.
  - **More updates are refused with a `ValidationError`**: a `.set()` that changes
    a primary-key composite (silently ignored before), a `.set()` that changes an
    immutable field (restating its current value is fine), and the path
    operations on index composites and unique fields listed under "Updates and
    deletes".
  - **`returnValues` is honoured, and typed by its mode.** `"none"` now returns
    `undefined` and `"updatedOld"` / `"updatedNew"` return a partial of the
    attributes written, on every update path. A retain update with `"allOld"`
    now returns the replaced item rather than the new one.
  - **Two new hidden attributes.** Versioned entities get `__edd_i__`, set on
    create and added to existing items on their next guarded write. An item whose
    unique field holds only a decoding default that is also an index composite
    gets `__edd_d__`, a string set naming those fields. Decoded models never
    include either, but raw readers, `asNative` and DynamoDB Streams consumers
    will see them.
  - **Writes are validated more strictly**, so some calls that used to succeed
    now fail with a `ValidationError`. Container `.check()` refinements on an
    array, a struct or a checked-struct class that holds a date or another
    substituted value were silently dropped, and are now enforced on every write
    (entities and aggregates; details below). Entity path updates are now
    validated like `.set()`, so a literal outside its set or a string under its
    `minLength` is rejected instead of stored. Reads don't enforce the container
    checks, so existing rows that break them still read. On an aggregate, though,
    such a row refuses every `update` until that same update makes the value
    valid; an entity `.set()` on other fields still succeeds on it.
  - **A canonical ISO string in a string member reads as a date.** In
    `Schema.Union([Schema.DateTimeUtc, Schema.String])`, the exact ISO form the
    library writes for a date (`"2000-01-01T00:00:00.000Z"`) reads back as a
    `DateTime`, even if it was written as a string. Other strings (`"2020"`, `"5"`,
    `"hello"`) stay strings. If a string field may hold ISO instants, use a tagged
    or discriminated shape.
  - **No backfill is performed.** Old rows read correctly as they are, and are
    rewritten in wire form when they are next written. For an aggregate that
    means the next `update` that changes the row's group (the root item or its
    sub-aggregate); an update that changes nothing writes nothing.
  - **Values that were lost stay lost.** A domain object with no enumerable state
    was stored as a map holding no value: a `Schema.Date` (`{M:{}}`), a `URL`, a
    `Duration`, a `BigDecimal`. These cannot be recovered, and reading them fails
    with a `ValidationError`.
  - **Nested sub-aggregates written by earlier versions are not read.** Rows below
    the first sub-aggregate level used different keys, and never read back
    before. Recreate those aggregates.

  ### Aggregates

  **`create` of an existing aggregate fails (behaviour change).** `create`
  wrote plain `Put`s, so creating an aggregate whose root item already existed
  silently overwrote it — and left the old aggregate's edge and sub-aggregate
  rows that the new one didn't rewrite. The root item is now written first, in
  the first transaction, conditioned on `attribute_not_exists`. An existing
  aggregate cancels that transaction, so nothing is written, and `create` fails
  with `ConditionalCheckFailed` (`entityType` is the root's, `key` its `pk` and
  `sk`). Edge and sub-aggregate rows carry no guard of their own; they are only
  written after the root's transaction commits. As before, each sub-aggregate is
  its own transaction: if a later one fails, the earlier ones stay written.
  Replace an existing aggregate with `update`, or `delete` it first. The guard
  sees the root only: edge rows left without a root (an earlier write that
  failed partway) don't stop `create`, and `get` reads them back merged with the
  new rows — `delete` the key first, which removes every row in the partition.

  **`delete` no longer leaves rows behind under throttling.** It ignored the
  `UnprocessedItems` DynamoDB returns from `BatchWriteItem`, so a throttled
  delete succeeded with rows still stored. They are retried with exponential
  backoff, as `Batch.write` retries them (up to 5 retries), and a `delete` that
  still can't remove them fails with a `DynamoError` saying how many remain.

  **Writes.** Aggregates now store every value in its wire form wherever it is
  nested: in root arrays (`Schema.Array(Schema.DateTimeUtcFromString)`), arrays of
  classes (`sessions[].startTime`), `NullOr` and other unions (also inside
  arrays), records, tuples, refs hydrated into `one` / `many` items
  (`player.dateOfBirth` on a `MatchPlayer` item), and edges declared without an
  `entity`. Before, only a field whose own schema was a transform was encoded.

  **Reads.** Date maps written by earlier versions are rebuilt into real
  `DateTime` values, whichever type-id key they carry; `Zoned` values keep their
  named or offset zone. An optional `BigIntFromString` and a plain `Schema.BigInt`
  stored as a number, which could not be read back at all, now read as a `bigint`.

  **Stored-type changes.** A top-level `Schema.optional(...)` or `Schema.NullOr(...)`
  around a non-date transform (such as `NumberFromString` or `BigIntFromString`),
  on the root item, an edge item or a `many` element's own fields, is now stored
  encoded rather than in its domain form. So is a `NumberFromString` nested inside
  a hydrated ref, and a self date inside a union, record or tuple.

  **Now working.** None of these worked on 1.22.0:

  - A ref in a `many` element, declared as `player: Player.pipe(DynamoModel.ref)`
    (it could not be read back even after a fresh write) or as the element itself,
    `Schema.Array(Player.pipe(DynamoModel.ref))` (`update` failed). The plain
    class `player: Player`, matched by name to the edge's entity, also
    round-trips.
  - `update`, even one that changed nothing, on models with unions, records or
    tuples around transforms (it failed with `Expected string`).
  - Unions that mix a date with another type, such as
    `Schema.Union([Schema.DateTimeUtcFromString, Schema.Number])` (`create`
    threw). Each value is now stored and read by the member it belongs to.
  - `create` input carrying a `DateTime`, an `Option` or another Effect value on
    an aggregate with a ref edge. The input was copied with `structuredClone`,
    which stripped those values, so they were rejected. Cyclic input is handled
    too.
  - A nested field that shares a name with a root ref edge (a `coach: Schema.String`
    inside an array, next to a root `coach` edge). Refs are now resolved by field
    schema, so it is no longer decoded as that edge's entity.

  **Container checks.** `create` and `update` enforce `.check()` refinements on
  arrays, structs and checked-struct classes that hold a date or another
  substituted value; earlier versions silently dropped them. A violating value
  fails with a `ValidationError`. Reads don't enforce them, so a stored row that
  breaks one still assembles. Every `update` of that aggregate fails until the
  same update makes the value valid, which repairs the row.

  **Known limitation.** A `many` edge with a custom `decompose` that renames
  element fields still stores the renamed values in their domain form, so a
  `DateTime` there is written as a map. Those values do read back as real
  `DateTime`s.

  ### Entities

  **Self dates in containers.** A self date inside a `NullOr` or other union, a
  nullable class (`NullOr(Stamp)`), an array of a union (`Array(NullOr(date))`), a
  `Record` value, or a `Tuple` / `TupleWithRest` / `StructWithRest` was stored as
  a `DateTime` map. It is now stored in its wire form (a number where `storedAs`
  says so), and existing map rows read back as real `DateTime`s. A
  `DynamoModel.configure` `storedAs` override on a union field now applies to its
  date member. Transform schemas such as `DateTimeUtcFromString` already stored
  their wire form and are unchanged. A `TupleWithRest` field is also no longer
  mis-derived as an array.

  **Container checks.** `.check()` refinements on arrays, structs and
  checked-struct classes that hold a date or another substituted value were
  silently dropped. They are now enforced on every write: `put`, `create`,
  `update` and `.set()`, path operations, `.append()`, Batch and Transaction. A
  violating value fails with a `ValidationError`. Reads don't enforce them, so
  existing rows that break one still read.

  **Path updates.** `pathSet`, `pathAppend`, `pathPrepend`, `pathIfNotExists` and
  the record-based `.append()` (including on versioned entities that retain
  snapshots) now encode their value through the schema at the path, as `.set()`
  does. Before, they wrote the raw value: a `DateTime` became a map even on a
  plain date field, and a `NumberFromString` value was stored as a number. Now:

  - A plain object on a class-typed field is encoded as that class, and a class
    instance is always encoded whole.
  - A plain object or array that mixes wire and domain parts, or holds an
    ambiguous wire part, is encoded part by part, so every `DateTime`, `Date` and
    `Redacted` inside it is stored in wire form.
  - A value already in wire form is normalised the way `.set()` normalises it:
    `NumberFromString` `"05"` is stored as `"5"`, `DateTimeUtcFromString`
    `"2000-01-01"` as `"2000-01-01T00:00:00.000Z"`, and a `Schema.Trim` field
    stores its trimmed form. Read-back values are identical; only the stored
    bytes differ, which filters and Streams consumers will see.
  - A value passes through as given only if it genuinely decodes as the wire form
    of a leaf transform with a primitive wire form AND is also a valid domain
    value, such as `"aGk="` on a `StringFromBase64` field; encoding it would
    double-encode it. A plain string that isn't valid wire (`"hi"`) is a domain
    value and is encoded.
  - Path values into and under a top-level `DynamoModel.ref` field are encoded
    through the ref target's model, like any other path. Only a path no schema
    describes (such as one under a dynamic key of an untyped value) is written as
    given.
  - A value set by path into a class that has lost its fields (one built with
    `.check()` or `.annotate()`, or a `DynamoModel.ref` nested inside a ref
    target) is written as given, in the same form `put` stores it, so the item
    stays readable.
  - Path values are validated like `.set()`, so an invalid value (a literal not
    in the set, a string under `minLength`, a broken container check) fails with a
    `ValidationError` instead of being stored. A key whose value is `undefined`
    is dropped before validation, as it is when stored. List `append` and
    `prepend` validate each element, but can't enforce list-level checks such as
    `maxLength`, because DynamoDB builds the list server-side. An append can
    therefore take a list past such a check. For a list whose check is enforced
    on read (any array holding no date or other substituted value), the item then
    fails to decode on the update's returned item and on every later read. Guard
    such lists with a condition on their size, e.g.
    `.condition((t, { lt }) => lt(t.tags.size(), 2))`, or use `.set()` with the
    full list. This is not new in this release.

  `ADD`, `DELETE` and `SUBTRACT` are unchanged.

  **Path updates on retain entities.** Path operations (`pathSet`,
  `pathAppend`, `pathPrepend`, `pathIfNotExists`, `pathAdd`, `pathSubtract`,
  `pathDelete`, `pathRemove`) on entities with `versioned: { retain: true }` were
  silently ignored: they returned success and wrote nothing. They are now sent to
  DynamoDB as the same update expression used for other entities, in one
  transaction with the version snapshot of the item they replace, so they behave
  exactly as DynamoDB defines: list indexes refer to the item before the update,
  a copy reads the old value, overlapping paths and appends to a missing list are
  rejected, and a rejected update writes no snapshot. `expectedVersion` and
  `.condition()` apply. One update cannot combine path operations with a change
  to a unique-constraint field or a computed change to an index composite; it
  fails with a `ValidationError`, so split it into two updates.

  **Legacy values read back.** The raw values earlier path updates left on
  transform fields now read: a number on a `NumberFromString` field, a
  safe-integer number on a `BigIntFromString` field, a `DateTime` map on a date
  transform. A plain `Schema.BigInt`, stored as a number, now reads back as a
  `bigint`.

  **Known limitation.** Plain dates inside a class that has lost its fields (see
  above), including whole-value `put`, `.set()` and `pathSet` of that class, are
  still stored as maps, as on 1.22.0, and read back as plain objects.

  **Zoned dates.** A zoned date with an offset zone (`+05:00`) now reads back
  with that offset, for both `DynamoModel.DateTimeZoned` and a self
  `Schema.DateTimeZoned`, on entities and aggregates; earlier versions read it
  back as UTC. Named zones and UTC round-trip as before, and the stored form is
  unchanged. Known limitation, as in earlier versions: an offset that isn't a
  whole minute (a historical local-mean-time offset, a sub-minute
  `zoneMakeOffset`) is rounded to the minute by Effect's ISO format, and the
  instant read back moves by the same amount.

  ### Updates and deletes

  **Path operations on index composites and unique fields.** A top-level
  `pathSet` of a value or `pathRemove`, and a numeric `pathAdd` or
  `pathSubtract`, on an index composite or unique field now recompose the keys
  and rotate the unique sentinels exactly as `.set()`, `.remove()`, `.add()` and
  `.subtract()` do. Operations whose result DynamoDB computes at write time
  (copies, `pathIfNotExists`, list and set operations) on such a field, paths
  below such a field, any path operation on a primary-key composite or an
  immutable field, and two operations on the same such field are rejected with a
  `ValidationError` naming the field. An update can't combine path operations
  with a change to a unique field or a computed change to an index composite;
  split it into two updates. `.add()`, `.subtract()`, `.append()` and
  `.deleteFromSet()` on an index composite now recompose the index key on every
  entity.

  **Error mapping.** On every update path a lost version race is an
  `OptimisticLockError` with the real `actualVersion`, a failed `.condition()` is
  a `ConditionalCheckFailed`, and a missing item is an `ItemNotFound`. Three cases
  used to be the other way round:

  - A versioned update with `expectedVersion` and a `.condition()` reported a
    failed condition as `OptimisticLockError(-1)`.
  - A retain update with a `.condition()` did the same.
  - A retain path update with a `.condition()` reported a lost version race as
    `ConditionalCheckFailed`.

  **Guarded read-then-write.** Updates that read the item first (a unique-field
  change, a computed change to an index composite, any retain update) write a
  guarded update of only what changed. A concurrent change to an unrelated
  attribute is preserved. A race on something the update read fails without
  writing: with the new `ConcurrentModification` on an unversioned entity, and
  with `OptimisticLockError` on a versioned one. Soft delete, and a hard delete
  of an entity with unique constraints or `retain`, are guarded the same way;
  with no `.condition()` such a delete reads the item again and is retried. `restore` fails with
  `ItemNotFound` if a concurrent restore won, and with `ItemNotDeleted` if a live
  item exists under the key.

  **Wide items.** An unversioned soft delete is never refused for width. When
  the full guard can't fit DynamoDB's condition limits (4 KB and 300 operators,
  counted on the actual condition including your `.condition()`), it falls back
  to the strongest guard that fits: the item exists, `updatedAt` is unchanged
  (with timestamps), then as many attributes as fit, unique-constraint fields
  first. With timestamps, that detects any concurrent library update except one
  in the same millisecond with an identical `updatedAt`. A writer outside the
  library that leaves `updatedAt` alone can change unguarded attributes
  undetected. Without timestamps, only the guarded attributes are protected. An
  update too wide for one expression writes the whole item, under the version
  condition (versioned) or the same fallback guard (unversioned). A concurrent
  write from outside the library to an attribute the guard doesn't cover is lost,
  which is also what 1.22.0 did for every such update. A `.condition()` too large
  to fit beside the guard fails before writing with a `ValidationError` stating
  its size. Operators are counted as DynamoDB counts them: in a condition, each
  comparison, `AND` / `OR` / `NOT`, `IN` and each function, with `BETWEEN`
  counted once (its own `AND` is part of it); in an update expression, each `+`,
  `-` and function, but not a `SET` clause's `=`.

  **Incarnation token.** Versioned entities carry a hidden `__edd_i__` attribute,
  set on create and backfilled on the next guarded write. It is never in decoded
  models (only in `asNative`). Version-checked writes require it, so an item
  deleted and recreated — at the same version on an entity without `retain`, or
  by a writer outside the library — is never mistaken for the original. (A
  retain item the library creates again continues after its retained history, so
  it never repeats a version.)
  An item that has the token but no version had its version removed outside the
  library. It is refused with a `ValidationError` rather than read as version 0,
  which would let the next update rewrite its history: by every read (`get`,
  queries, the `deleted` views, `decodeMarshalledItem`), and by every update,
  soft delete, hard delete of an entity with unique constraints or `retain`,
  `restore`, versioned `put` and `upsert`. A query over a partition holding one
  fails as a whole. A plain hard delete (no unique constraints, no `retain`) still
  removes it, since it reads nothing and writes no history; `purge` removes it on
  any entity.

  **Version history is never overwritten.** An update, soft or hard delete,
  restore or replacing `put` that would write a `v#N` snapshot holding a different state
  from the row already there fails with a `ValidationError` and writes nothing.
  Rewriting the same state (the same version, incarnation and, with timestamps,
  `updatedAt`) is allowed, which is what the first update after a retain `put`,
  and a restore, do. A write from outside the library that changes an item
  without bumping its version can still be captured into the next `v#N`
  snapshot.

  **Retain return values.** A retain update returns exactly the item it wrote,
  even if another writer has replaced it since. If that can't be proven, it fails
  with the new `UpdateAppliedButUnreadable`: the write WAS applied, so don't
  retry. `allOld` returns the replaced item; on record and unique-field retain
  updates it used to return the new one.

  **`returnValues` on every update path.** `"none"` returns `undefined`,
  `"updatedOld"` / `"updatedNew"` return only the top-level attributes written as
  a partial, and `"allOld"` / `"allNew"` return the whole item. The result type
  follows the mode (`UpdateReturn` is exported), and repeated
  `Entity.returnValues` calls are typed by the last one. A cascade with `allOld`
  or `updatedOld` cascades exactly what this update wrote; combined with path
  operations on an unversioned entity it is refused.

  **`update()` of a missing item** no longer leaves an undecodable partial row.
  A plain update requires the item to exist. If it's missing and the update is a
  plain `.set()` of a complete item (every required field and primary-key
  composite; a field with a decoding default doesn't count as required) with no
  other operations, `expectedVersion`, `.condition()`, cascade, `withVector` or
  old-image `returnValues`, the library creates it through `create` with the same
  payload, so the item is exactly what `put` writes. If another writer creates it
  in between, the update re-runs once on that item. Anything else fails with
  `ItemNotFound` and writes nothing, as do retain entities and updates that read
  first (a unique-field change and the like). `patch()` of a missing item fails
  with `ConditionalCheckFailed` on every path, except a lost race on a versioned
  entity (the item deleted between the read and the write), which is an
  `OptimisticLockError`.

  **Decoding defaults.** Fields with `Schema.withDecodingDefault` now survive on
  read: a `put` that omitted one used to write the item and then fail with a
  `ValidationError`. A defaulted `DateTimeUtc` is stored as an ISO string. A
  defaulted primary-key or index composite that a write omits is stored with its
  default, and keys are composed from it. Other defaulted fields are still not
  stored, and the default is applied on read.

  A default never creates a unique-constraint sentinel. A defaulted unique field
  that is also an index composite is stored and indexed, and is listed in a hidden
  string-set attribute, `__edd_d__`, which never appears in decoded models. It
  gets its sentinel only when a write supplies its value. A `.remove()` of a
  defaulted index composite stores the default again and keeps the item indexed
  under it; on a unique field it releases the old value's sentinel and creates
  none for the default.

  **`.set()` refusals.** A `.set()` of a changed primary-key composite is refused;
  it was silently ignored before. An immutable field can be restated with its
  current value, so spread records work; a different value is refused.

  **Known limitations.** Both are inherent:

  - On unversioned entities, nothing can prove an unguarded attribute unchanged.
    So the item a unique-field update returns may show stale values for
    attributes it neither reads nor writes, wide items use the fallback guard
    above, and the whole-item write of a wide update can overwrite writers
    outside the library. Use `versioned` where that matters.
  - A plain `.expectedVersion(n)` can't detect a delete-and-recreate that has
    climbed back to version `n` on an entity without `retain`: its versions
    restart at 1, so it takes `n − 1` updates after the recreate.

  ### Puts, upserts and batches

  **Replacing puts.** A `put` of a versioned or unique-constrained entity reads
  the item first. Over an existing item it continues that item: it takes the
  next version, keeps the same incarnation and `createdAt` (unless the input
  supplies one; unique-only entities keep `createdAt` too), writes a retain
  snapshot of the item it replaces, and rotates the unique sentinels of changed
  values. It never resets the item to version 1 or orphans a sentinel. The write
  is guarded by what it read; a put replaces the whole item, so when another
  writer creates, changes or deletes it in between, the put reads it again and
  retries, and the last writer wins. A race lost on every attempt fails with
  `OptimisticLockError` (versioned) or `ConcurrentModification` (unversioned) and
  writes nothing. A soft-deleted item counts as missing. `create` no longer reads
  the item first; it still fails with `ConditionalCheckFailed` on an existing
  item. A retain `create` runs one `Limit 1` query of its version history, and,
  when the primary sort key has composites, a second, keys-only `Limit 1` query
  for history an earlier release wrote (and reads that history only if it finds
  some).

  **Re-creating a deleted retain item.** Its version history outlives it, and
  the key can be used again without `purge`. Deleting it, hard or soft, snapshots
  its final state at its own version in the same transaction (1.22.0 wrote no
  snapshot on a hard delete). A `put`, `create`, `upsert` or transaction put of
  the missing item reads the highest version retained for its key and continues
  after it, with a new incarnation token: an item deleted at version 3 comes back
  at version 4 with its own `v#0000004` snapshot, and the earlier history is never
  overwritten. A writer still holding version 3 fails with `OptimisticLockError`.
  `restore` of the old tombstone while the new item is live fails with
  `ItemNotDeleted`. A hard delete of a missing retain item writes nothing, as
  before; with a `.condition()` the condition is judged against no item, and the
  delete never removes an item created since its read.

  **Sentinel ownership.** A sentinel is released only by the item that owns it
  (`_entity_pk` / `_entity_sk`): the write reads it first and conditions the
  release on that ownership. A release whose reservation changed hands in
  between fails an update, or a delete with a `.condition()`, with
  `ConcurrentModification` on the unique fields; a put, an `upsert`, a
  transaction put and an unconditioned delete plan the write again from a
  fresh read (up to three attempts, then `ConcurrentModification`). `purge` releases the sentinels of the live item and of
  every tombstone, each only if owned. Each sentinel a write would release costs
  one consistent `GetItem`. An update that changes the value of a unique
  constraint with a `ttl` now gives the new sentinel that expiry, as a put does
  (it was written without one).

  **`upsert` that reads first.** One `UpdateItem` can't write, rotate or check a
  sentinel, or snapshot the item it replaces, so `upsert` of an entity with
  unique constraints or `versioned: { retain: true }` reads the item once first.
  (A retain entity's upsert used to write no snapshot at all.) A missing item is
  created with its sentinels and snapshot. An existing item is updated from that
  same read: sentinels rotate for changed unique values and are left alone for
  unchanged ones, the replaced item is snapshotted, under the update's version
  and incarnation guards (versioned) or attribute guards (unversioned). The whole
  input is validated either way, so an `upsert` missing a required field fails
  with a `ValidationError` even when the item exists. A concurrent create or
  delete in between is retried the other way, and a sentinel release whose
  reservation changed hands is planned again from a fresh read, as a put's is; a
  concurrent change of the item itself fails the upsert, as it fails an update.
  A race lost on every attempt fails
  with `OptimisticLockError` or `ConcurrentModification` (never a
  `ConditionalCheckFailed` you didn't ask for), and a value another item holds
  with `UniqueConstraintViolation`. Its errors name the `upsert`. An `upsert`
  whose input omits a defaulted index composite also reads first: it stores the
  default only when it creates the item and keeps the stored value otherwise, on
  every entity (a plain upsert used to overwrite the stored value with the
  default). Other upserts are a single `UpdateItem`, as before.

  **Transactions.** In `Transaction.transactWrite` and `EventStore`
  `additionalItems`, a `put` of a versioned or unique-constrained entity is
  written exactly as the entity's own `put` writes it: the item is read, a
  replaced item continues its version, incarnation and `createdAt` and is
  snapshotted, changed sentinels rotate (releasing only owned ones), and a
  re-created retain item continues after its history — all in the one
  transaction. A race between the read and the transaction cancels it, and it is
  built and written again. A taken unique value is a `UniqueConstraintViolation`
  from both; only an op's own condition is `TransactionCancelled` from
  `transactWrite` and `AdditionalItemConditionFailed` from `append`.

  Both are checked before anything is sent. DynamoDB allows one operation per
  item in a transaction, and the reasons it gives for a repeated item can read as
  a lost race, so the transaction was retried and misreported (as
  `OptimisticLockError`, or a `DynamoValidationError`, depending on the backend).
  Now any item touched twice, counting the sentinels and snapshots an op adds
  (two puts that swap unique values touch the same sentinels), fails with a
  `ValidationError` naming the entity and both ops, and nothing is sent. So does
  an `additionalItems` op that repeats an event or the idempotency sentinel of the
  append. A transaction whose items pass DynamoDB's 4 MB (4,194,304 bytes) fails
  with a `ValidationError` naming its largest item, instead of DynamoDB's bare
  `ValidationException`. The size is a lower bound by DynamoDB's item-size rules
  (numbers count a byte per two significant digits, plus one, and zero one byte;
  list and map overheads aren't counted; an update counts only its key; a retain
  put counts twice), so a transaction DynamoDB would accept is never refused. Deletes of `unique`, retain and `softDelete` entities
  are still refused (`EDD-9048`), and `Batch.write` still sends versioned puts as
  create-only transactions (below).

  **Error channels.** Compared with 1.22.0, these operations declare new errors.
  Review `catchTag` handlers and exhaustive matches on them:

  | Operation                               | New in its error channel                                                                                                |
  | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
  | `put`, `create`                         | `OptimisticLockError`, `ConcurrentModification`, `TransactionOverflow`                                                  |
  | `upsert`                                | `UniqueConstraintViolation`, `OptimisticLockError`, `ConcurrentModification`, `TransactionOverflow`                     |
  | `update`, `patch`                       | `ConcurrentModification`, `UpdateAppliedButUnreadable`, `TransactionOverflow`                                           |
  | `delete`, `deleteIfExists`              | `OptimisticLockError`, `ConcurrentModification`, `ValidationError`, `TransactionOverflow`, `DeleteAppliedButUnreadable` |
  | `restore`                               | `ItemNotDeleted`, `TransactionOverflow`                                                                                 |
  | `Transaction.transactWrite`             | `OptimisticLockError`, `ConcurrentModification`                                                                         |
  | `EventStore` `append`, `commandHandler` | `UniqueConstraintViolation`, `OptimisticLockError`, `ConcurrentModification`                                            |
  | Aggregate `create`                      | `ConditionalCheckFailed`                                                                                                |

  `TransactionOverflow` could already be raised when an item's own transaction
  would pass 100 items; it is now declared. `GeoIndex.bind`'s `put` declares the
  errors the entity's `put` raises.

  **`Batch.write` of a `versioned` entity.** Its puts are sent first, as
  create-only `TransactWriteItems` of up to 100 items, each conditioned on
  `attribute_not_exists`; a chunk also closes before it would pass DynamoDB's
  4 MB transaction payload. There is no read, so there is no race window. A batch
  that touches a versioned put's item more than once is refused with a
  `ValidationError` before anything is written. A put
  that would replace an existing item cancels its whole chunk, so nothing in that
  chunk is written, and fails with a `ValidationError`. Earlier chunks may
  already have been written, since `Batch.write` was never atomic across chunks,
  and the batch's other requests aren't sent. Each chunk costs twice the write
  capacity of a batch write. Contention cancellations (`TransactionConflict`,
  throttling) are retried with the batch's backoff settings; any other
  cancellation is a `DynamoError` that keeps each reason's message and the SDK
  exception. Puts of other
  entities, and deletes, are still plain `BatchWriteItem` requests.

  ### History of items that share a partition

  An entity whose primary sort key has composites keeps several items in one
  partition. Their version snapshots and soft-delete tombstones used to share one
  key space (`$app#v1#line#v#0000001`, `$app#v1#line#deleted#<timestamp>`), so
  siblings shared one version sequence, a second item's history collided with
  the first's, `deleted.get` and `restore` found the partition's latest
  tombstone rather than the item's, and `purge` removed every sibling. Each item
  now has its own: its history keys carry the composite part of its sort key
  after the marker (`$app#v1#line#v#line_a#0000001`,
  `$app#v1#line#deleted#line_a#<timestamp>`), and `versions`, `getVersion`,
  `deleted.get`, `restore`, the version a re-created item continues from, and
  `purge` all key by the item. `deleted.list` still lists the partition's
  tombstones, every item's. An entity without sort key composites writes and
  reads exactly the keys it did.

  History an earlier release wrote for such an entity keeps its old keys and
  stays readable. A row under the partition-wide keys belongs to the item whose
  key its stored composites compose, and every reader of one item's history
  reads those rows too: `versions` and `getVersion`, `deleted.get` and `restore`
  (which restores from such a tombstone and consumes it), `deleted.list`, the
  version an item created again continues from, and `purge`. A version held both
  ways is read from the item's own row; of two tombstones, the later one wins.
  Reading it costs one keys-only `Limit 1` query of the partition's unsegmented
  range, per `versions`, `deleted.get`, `restore` and `create` (and per
  `getVersion` that misses the item's own key: one more `GetItem`); only when that
  finds a row is the range read in full. `versions` then reads the partition's
  history (and filters it, after one more keys-only query of the item's own
  range); otherwise it reads the item's own range.

  ### Nested sub-aggregates

  A sub-aggregate bound inside another sub-aggregate now works end to end
  (`create`, `get`, `list`, `update`, `delete`). Before, it was written but failed
  to read (`Missing key at ["club"]["squad"]`). The nested level inherits its
  parent's discriminator, its sort keys are prefixed with the parent's
  discriminator values, and it is its own transaction group, so `update` rewrites
  only the inner group that changed. Sub-aggregates bound directly on the root
  keep their keys.

  A nested binding that reuses a discriminator attribute it inherits from its
  parent (`{ clubNo: 9 }` inside `{ clubNo: 1 }`) would overwrite the parent's
  value on the inner rows. `Aggregate.make` now rejects it with `EDD-9056`.

  Fixes [#133](https://github.com/jmenga/effect-dynamodb/issues/133).

### Patch Changes

- Updated dependencies [[`a2904f9`](https://github.com/jmenga/effect-dynamodb/commit/a2904f9678ab71aa095959e520a5a9d529408401)]:
  - @effect-dynamodb/schema@1.23.0

## 1.22.0

### Minor Changes

- [`573d567`](https://github.com/jmenga/effect-dynamodb/commit/573d5674c766c279c482db15e55d057470a4d98d) Thanks [@mixja](https://github.com/mixja)! - `EventStore.makeStream` accepts `casing`, the stream's key casing — the same option, with the same values, that indexes and vector indexes already take. Set, the stream name, stream ids and command ids in the stream's keys all take that casing (the `$<schema>#v<n>` prefix keeps the schema's). Omitted, the stream keeps the layout it has always had: name lower-cased, the rest following the schema. Setting `casing` on a stream that already holds data moves its keys whenever they come out different (e.g. `"preserve"` with a capitalised `streamName`), so its history is no longer read. The `__edd_e__` discriminators stay lower-cased. In the next major, omitting `casing` will mean the schema's casing.

  Index-level `casing` now works on GSIs. `indexes.<name>.casing` was accepted by the types but dropped during normalization, so GSI keys silently used the schema's casing (`primaryKey.casing` always worked). It now overrides the schema's casing for that index on every path — put, query accessors, `.where()` operands, policy-aware updates and collection queries. Collection members must agree on the collection index's casing; a mismatch fails at `DynamoClient.make()` / `Collection.make()` with `EDD-9055`. **If you had set `casing` on an `indexes` entry**, items written by earlier versions carry schema-cased keys for that index and won't be found through it until rewritten. Time-series event SKs keep using the schema's casing for the `#e#` suffix even under a `primaryKey.casing` override, so existing event items stay readable.

  The language-service hover tooltips now show the keys the library actually writes — attribute-name prefixes, cased composite values, padded numbers, GSI `casing`, the `"isolated"` collection default and the `begins_with` delimiter rule — and a parity test pins them to `@effect-dynamodb/schema`. The docs playground now composes keys with `@effect-dynamodb/schema` directly.

  Docs: the `casing` option is described as casing composite values too (it always has), with a warning — now also on the home page, getting-started guide, starter tutorial and both READMEs — that ids differing only by case share a key, that `casing` is part of the storage format, and which fixed key markers (`v1`, `#v#`, `#deleted#`, `_1`) are never cased. Tests pin those markers.

### Patch Changes

- Updated dependencies [[`573d567`](https://github.com/jmenga/effect-dynamodb/commit/573d5674c766c279c482db15e55d057470a4d98d)]:
  - @effect-dynamodb/schema@1.22.0

## 1.21.0

### Minor Changes

- [`e1b5f0d`](https://github.com/jmenga/effect-dynamodb/commit/e1b5f0d311195c200215e2f7d366bd167ac65f5b) Thanks [@mixja](https://github.com/mixja)! - Require Effect 4.0.0 (stable). The `effect` peer dependency moves from `^4.0.0-rc.112` to `^4.0.0`, so pre-release builds of Effect no longer satisfy it. Consumers on an Effect 4 release candidate should upgrade to `effect@4.0.0` and apply the GA renames that affect application code: `Config.string`/`Config.int` → `Config.String`/`Config.Int`, `SchemaGetter.transformOrFail` → `SchemaGetter.transformEffect`, and `effect/unstable/http` / `effect/unstable/httpapi` → `effect/http` / `effect/http-api`. Tests built on `@effect/vitest@4.0.0` need vitest 5. The language-service plugin now compiles with TypeScript 6 and its emitted output is unchanged.

### Patch Changes

- Updated dependencies [[`e1b5f0d`](https://github.com/jmenga/effect-dynamodb/commit/e1b5f0d311195c200215e2f7d366bd167ac65f5b)]:
  - @effect-dynamodb/schema@1.21.0

## 1.20.1

### Patch Changes

- [`66427e2`](https://github.com/jmenga/effect-dynamodb/commit/66427e21a97634988b9e4adc99945b9b3b9008d2) Thanks [@mixja](https://github.com/mixja)! - Fix the lifecycle paths that read a stored row by domain field name ([#127](https://github.com/jmenga/effect-dynamodb/issues/127))

  A row read back from DynamoDB is keyed by its **stored attribute** name, but key
  composition and unique-sentinel composition name **domain** fields. Under a
  `DynamoModel.configure(Model, { id: { field: "widgetId" } })` rename the two
  differ, and two families of bugs followed.

  **Soft-delete reads and `restore`** never applied `renameFromDynamo`:
  `deleted.get` and `deleted.list` decoded an attribute-keyed row against the
  domain-keyed `deletedRecordSchema` and failed with
  `ValidationError` ("Missing key"), while `restore` composed keys from it and died
  with a **defect** out of `KeyComposer.extractComposites`. Any entity with both
  `softDelete` and a renamed field could write a tombstone it could never read back
  or undo.

  **Unique-sentinel composition** read the constraint field off the raw row at five
  call sites — soft delete, hard delete, `restore`, `purge`, and the
  `transactWrite` put-expansion (`Entity._buildPutSideItems`). A renamed field read
  as `undefined`, which the sparse rule treats as "constraint unset", so the
  sentinel was silently skipped: deletes orphaned the sentinel (making the value
  unusable forever), `restore` re-established nothing (allowing duplicates), and a
  put issued through `transactWrite` enforced no uniqueness at all.

  Every one of those sites now composes from a domain-keyed view of the row; the
  item written back stays attribute-keyed.

  Two further bugs on the `restore` path, both independent of any rename, are
  fixed alongside:

  - **`softDelete: { preserveUnique: true }` made `restore` impossible.** The
    delete deliberately keeps the reservation, so the restore-time sentinel Put's
    `attribute_not_exists` guard could never hold: every restore of a constrained
    entity was cancelled and reported as a `UniqueConstraintViolation` against its
    own reservation. Under `preserveUnique` the guard is now
    `attribute_not_exists(#pk) OR (#epk = :epk AND #esk = :esk)`, so the row
    re-claims the sentinel it still owns while a value taken by anybody else is
    still refused. With `preserveUnique` off the plain absence guard is unchanged.
  - **The restore-time retain snapshot inherited `deletedAt` and the soft-delete
    TTL.** It was built straight from the tombstone, and it lands on the same
    `#v#<version>` SK the delete-time snapshot used — so it replaced a clean
    snapshot with one that looked deleted and expired on the `softDelete.ttl`
    clock. The snapshot source is now stripped of both markers; `versioned.ttl`,
    when configured, still applies. `buildSnapshotItem` also strips the
    soft-delete vector stash (`__edd_vs_<index>__`), so a restore-time snapshot
    of a vector-indexed entity no longer carries the whole embedding blob.

- Updated dependencies [[`66427e2`](https://github.com/jmenga/effect-dynamodb/commit/66427e21a97634988b9e4adc99945b9b3b9008d2)]:
  - @effect-dynamodb/schema@1.20.1

## 1.20.0

### Minor Changes

- [`f2871b9`](https://github.com/jmenga/effect-dynamodb/commit/f2871b90dc897ff2de101012292e142b45a4a87b) Thanks [@mixja](https://github.com/mixja)! - Add `.filterBy()` — a client-side predicate that takes part in `limit` and cursor rebuilding

  `limit` is a contract on **results**: the request loop accumulates until `n` items are accepted and rebuilds the cursor from the last accepted item. Only a `FilterExpression` could take part in that, so a predicate DynamoDB cannot express had to be applied after the query returned — which breaks pagination two ways. The page comes back short, and its cursor resumes after the last item _returned_ rather than the last one _kept_, so the next page skips rows. `Page<A>` exposes one page-level cursor and no per-item resume token, so a caller filtering externally could not construct a correct resume point at all.

  The motivating case is case-insensitive matching. DynamoDB has no `lower()`, so a `FilterExpression` compares the stored attribute byte-for-byte:

  ```ts
  // stored: name = "Melbourne Cricket Ground"
  .filter((t, { beginsWith }) => beginsWith(t.name, "melbourne"))   // no match
  .filterBy((v) => v.name.toLowerCase().startsWith("melbourne"))    // matches
  ```

  Composite **keys** never had this problem — `applyCasing` folds both the stored key and the operand — but a sort key is one string, so its members are only usable as a contiguous leading prefix. "As much as the key prefix can take, the rest matched case-insensitively" was not expressible without dropping to the raw SDK and reimplementing the accumulate-and-rebuild loop.

  Added on `Query`, `BoundQuery` and `Aggregate.list`'s `ListOptions` (as `filterBy`), so there is one vocabulary rather than three. On an aggregate it runs on the root item **before assembly**, for the same reason `ListOptions.filter` is worth pushing server-side: a rejected root item never pays for its partition read.

  Two corners are closed rather than left to be discovered:

  - **`.count()`** would have reported the unfiltered count, since `Select: "COUNT"` returns no items to run the predicate against. It now reads the rows and counts the accepted ones — correct, at the cost of the read. Its error channel gains `ValidationError` accordingly, since decoding can now fail during a count.
  - **`.select()` with a predicate** raises **EDD-9054**. A projection returns only the attributes it names, and a predicate is an opaque closure whose attribute reads the library cannot see — so it cannot borrow them into the `ProjectionExpression` the way key attributes are borrowed for cursor rebuilding, and the predicate would be handed items missing the fields it tests.

  Prefer `.filter()` whenever DynamoDB can express the condition: a `FilterExpression` rejects rows before they cross the wire, while this runs after decode, so every examined row is still read and paid for.

  Collection queries (`db.collections.*`) are unchanged — their result is a per-entity grouping rather than a single item stream, so a per-item predicate has no single shape there.

### Patch Changes

- Updated dependencies [[`f2871b9`](https://github.com/jmenga/effect-dynamodb/commit/f2871b90dc897ff2de101012292e142b45a4a87b)]:
  - @effect-dynamodb/schema@1.20.0

## 1.19.1

### Patch Changes

- [`75e562c`](https://github.com/jmenga/effect-dynamodb/commit/75e562ccfd4900ccb4e227935cdfe035bd26fac0) Thanks [@mixja](https://github.com/mixja)! - Accept a caller-supplied generated id in `transactWrite`, `Batch.write` and `EventStore.append`

  `rejectUnsupportedOp` gated the `generatedId` check on the entity's **configuration**, so any entity declaring `generatedId` was barred from every multi-item write path — with the reason "id generation needs the Crypto service, which is not in scope here."

  That reason does not hold when the caller supplies the id. `Entity.put` reaches `Crypto` only for an _absent_ field: `fillGeneratedId` returns the input untouched when the value is present. So the rejected call needed nothing the path lacked, and the workaround the message pointed at — supply the id yourself — was already in effect and did not lift the rejection.

  The gate now reads the op's input. An omitted id is still refused, because this path builds the item straight from the encoded input and never calls `fillGeneratedId`, so the id would stay missing and the primary key would compose around an `undefined`. The message names the field and says which case is unsupported, rather than reading as a ban on the entity.

  This unblocks committing a `generatedId` read model atomically with the events that produced it (`EventStore.append({ additionalItems })`) — the pattern [#100](https://github.com/jmenga/effect-dynamodb/issues/100) exists to enable.

  The neighbouring `refs` and `vectorIndexes` gates are unchanged and stay configuration-gated: `refs` always hydrates at write time and `vectorIndexes` always needs the `Embedder`, so neither dependency is something the caller can remove. The 1.16.0 changelog grouped all three as needing "a read, `Crypto` or an `Embedder`", which was accurate for those two and wrong for this one; that entry is left as the historical record of what shipped.

- Updated dependencies [[`75e562c`](https://github.com/jmenga/effect-dynamodb/commit/75e562ccfd4900ccb4e227935cdfe035bd26fac0)]:
  - @effect-dynamodb/schema@1.19.1

## 1.19.0

### Minor Changes

- [`e82e78c`](https://github.com/jmenga/effect-dynamodb/commit/e82e78c0154b84f89d4a71d50f9b59789ec057c9) Thanks [@mixja](https://github.com/mixja)! - Refuse `.where()` on a sort-key composite the accessor already pinned (EDD-9053)

  DynamoDB allows exactly one sort key condition, and `Query.where` **replaces** the `begins_with` an index accessor installs for its pinned prefix. A condition on a composite the accessor had already pinned therefore discarded that pin instead of narrowing within it, and the query ran against the whole partition.

  Every operator was affected, not only the one-sided ones that the clamping logic guarded: `pinnedKeyForm` is built from composites strictly to the left of the target, so at target index 0 it was empty and `eq` / `beginsWith` / `between` lost the pin too. Under a pinned `label = "ship"`, `eq(t.label, "shine")` composed `#sk = "…#label_shine"` and returned rows whose label was **not** the pinned value.

  The clamping predicate was `targetIndex === 0`, standing in for "the accessor pinned nothing". Those coincided for every shape under test but are different claims — an accessor can pin composite[0] itself. It now asks what the accessor actually pinned, and a condition targeting a pinned composite raises **EDD-9053** rather than silently returning wrong rows.

  `ResolveSkFields` is repaired alongside it. It resolved to `{}` rather than `never` when no composite remained, so `BoundQuery`'s `[SkRemaining] extends [never]` gate never fired and `.where()` was offered on every accessor — including ones that had pinned every composite (the no-cast route to this bug) and ones whose index has no sort key composites at all (the only reason `EDD-9045` needed to exist as a runtime throw). `.where()` now disappears from both, so well-typed code cannot reach either diagnostic.

### Patch Changes

- Updated dependencies [[`e82e78c`](https://github.com/jmenga/effect-dynamodb/commit/e82e78c0154b84f89d4a71d50f9b59789ec057c9)]:
  - @effect-dynamodb/schema@1.19.0

## 1.18.1

### Patch Changes

- [#119](https://github.com/jmenga/effect-dynamodb/pull/119) [`9d6decf`](https://github.com/jmenga/effect-dynamodb/commit/9d6decf94580326c5ccfa40c5ad0bbaad2afe821) Thanks [@jmenga](https://github.com/jmenga)! - Fix `EventStream` variance so the pipeable `commandHandler` form type-checks ([#106](https://github.com/jmenga/effect-dynamodb/issues/106))

  `EventStream`'s operations were declared as function-typed properties, so `strictFunctionTypes` checked their parameters contravariantly. A stream with no `snapshot` config has `TState = never`, and `writeSnapshot(…, state: never, …)` made that stream assignable to no other `EventStream` — not even one instantiated at `any`, since `any` is not assignable to `never`. `append`'s `options?: AppendOptions<TMetadata>` did the same for `TMetadata = undefined`.

  The effect: **every data-last / pipeable `commandHandler` call on a snapshot-less stream failed to compile** — `MatchEvents.pipe(EventStore.commandHandler(decider))`, `pipe(MatchEvents, …)` and the `BoundEventStream` equivalents. `pipe` infers its subject from the callback's parameter, erasing the generic function's type parameters to their constraints, which is where the invariance bites. The data-first form (`commandHandler(decider, MatchEvents)`) always worked, which is why this went unnoticed.

  `writeSnapshot`, `readSnapshot`, `append`, `read`, `readFrom`, `currentVersion` and `query.events` are now **method** declarations on both `EventStream` and `BoundEventStream`, which makes their parameters bivariant. This is a type-only change with no runtime effect, and supplying a `state` still requires `TState`, so the compile-time guarantee that a snapshot-less stream cannot write a snapshot is unchanged.

  One narrowing is lost: `commandHandler`'s `TState extends State` check no longer applies in the _data-last_ form. The data-first overloads still enforce it.

  Found while making every test file type-checked: `tsconfig.test.json` compiled only four files in `effect-dynamodb` and one in `schema`, and the `geo` and `language-service` packages had no test tsconfig at all. All four now compile their whole `test` directory as part of `pnpm check`, which is what surfaced this. No test assertion changed.

- Updated dependencies [[`9d6decf`](https://github.com/jmenga/effect-dynamodb/commit/9d6decf94580326c5ccfa40c5ad0bbaad2afe821)]:
  - @effect-dynamodb/schema@1.18.1

## 1.18.0

### Minor Changes

- [#118](https://github.com/jmenga/effect-dynamodb/pull/118) [`49913c9`](https://github.com/jmenga/effect-dynamodb/commit/49913c9a49ecb65c3922992db70d21f6854b7ea5) Thanks [@jmenga](https://github.com/jmenga)! - Accept bound-client `get` in `Batch.get`, `Transaction.transactGet` and `Transaction.check`

  `db.entities.X.get(key)` now returns a `BoundGet`. It **is** an `Effect<Model, …, never>` exactly as before — `yield*` it, `.pipe(Effect.catchTag("ItemNotFound", …))` it, hand it to `Effect.map` / `Effect.all` — and it is additionally a read descriptor, so it can be passed straight to `Batch.get`, `Transaction.transactGet` and `Transaction.check`.

  This closes the read half of the gap [#100](https://github.com/jmenga/effect-dynamodb/issues/100) closed for writes: an entity authored with the pure, AWS-free `@effect-dynamodb/schema` `Entity.make` carries no operations, so the bound client is the only surface its author holds. Until now that meant such an entity could not take part in batch reads, transactional reads, or condition checks at all — and `Transaction.check` is the sharpest loss, since a condition check on a row you are not writing is the standard way to assert an invariant across entities inside one transaction.

  Bound and unbound get descriptors unwrap through the same protocol (`Entity.extractTransactable`) and may be mixed freely in one array. A value that is not a get descriptor now fails with a `ValidationError` carrying `EDD-9051` on the error channel, where it used to be a thrown defect callers could neither catch nor discriminate.

  No change to the existing `get` surface.

### Patch Changes

- Updated dependencies [[`49913c9`](https://github.com/jmenga/effect-dynamodb/commit/49913c9a49ecb65c3922992db70d21f6854b7ea5)]:
  - @effect-dynamodb/schema@1.18.0

## 1.17.0

### Minor Changes

- [#117](https://github.com/jmenga/effect-dynamodb/pull/117) [`98472f6`](https://github.com/jmenga/effect-dynamodb/commit/98472f6fda2f13b8eb744c9ee6246c2eb5be1d08) Thanks [@jmenga](https://github.com/jmenga)! - Filtered pagination for `Aggregate.list` ([#104](https://github.com/jmenga/effect-dynamodb/issues/104))

  `list` now takes the paging vocabulary the rest of the library settled on in
  1.16.0, plus a server-side predicate:

  - **`filter`** — a `FilterExpression` on the **root-item** query, in the same
    callback and shorthand forms `BoundQuery.filter()` takes. This is a
    performance fix as much as an ergonomic one: `list` assembles each surviving
    root item with its own partition read, so filtering the result afterwards paid
    a full assembly for every aggregate it then discarded.
  - **`limit` means "this many aggregates"** even under a filter — the query
    accumulates across as many requests as it takes, and **`pageSize`** sets
    DynamoDB's `Limit` (rows examined per request). Once a request over-reads, the
    returned cursor is rebuilt from the last item actually returned, so
    `cursor: null` still means genuinely exhausted.
  - **`reverse`** — walk the list index descending (`ScanIndexForward: false`),
    which `list` previously could not express at all.

  The sharded (`list.cardinality`) branch no longer discards paging options in
  silence: `limit` now truncates the merged fan-out, and a `cursor` is rejected
  with a `ValidationError` (**`EDD-9051`**) because a fan-out across N partitions
  has no resumable position — previously it was accepted and the list silently
  restarted from the beginning.

  Also fixes an empty shorthand filter (`.filter({})`) compiling to
  `FilterExpression: ""`, which DynamoDB rejects; it is now a no-op.

### Patch Changes

- Updated dependencies [[`98472f6`](https://github.com/jmenga/effect-dynamodb/commit/98472f6fda2f13b8eb744c9ee6246c2eb5be1d08)]:
  - @effect-dynamodb/schema@1.17.0

## 1.16.0

### Minor Changes

- [#112](https://github.com/jmenga/effect-dynamodb/pull/112) [`e1ecfb4`](https://github.com/jmenga/effect-dynamodb/commit/e1ecfb4def8c7d04faa62cf3fc9d3f66002dd7ff) Thanks [@jmenga](https://github.com/jmenga)! - Aggregates round-trip fields with a schema transformation

  An aggregate whose model carried a transformed field could not round-trip: the write path stored Type-side values, so a `bigint` landed as `{"N":"5"}` and assembly's `Schema.BigIntFromString` decode rejected a number. Aggregate attributes are now encoded to their wire form before marshalling, at the root, sub-aggregate roots, `one` edges, `many` elements and propagated context values.

  Two further causes are fixed with it:

  - **`fieldsOf` did not see through `DynamoModel.configure`**, so any edge whose model is configured — which is most, since `identifier: true` requires it — received **no encoders at all**. Dates on those edges were stored as `{"M":{…}}` or `{"M":{}}`, meaning the date handling added in [#72](https://github.com/jmenga/effect-dynamodb/issues/72) was silently not applying to them.
  - **`aggregate.update` recognised only date transforms** when re-decoding mutated state, so a non-date transform was rejected before any item was built — even when the mutation touched only an untransformed field. The tolerance now covers every leaf transform, and remains scoped to the aggregate decode path: entities pass no such option.

  Only `BigIntFromString` and `NumberFromString` attributes change on the wire, and both were unreadable before, so **no migration is required**. Composed keys are byte-identical.

- [#112](https://github.com/jmenga/effect-dynamodb/pull/112) [`1e20c24`](https://github.com/jmenga/effect-dynamodb/commit/1e20c240ab44533495451c65b279f185b2c04e7a) Thanks [@jmenga](https://github.com/jmenga)! - Accept bound-client CRUD builders in `Batch.write`, `Transaction.transactWrite` and `EventStore.append({ additionalItems })`, and stop silently reinterpreting ops ([#100](https://github.com/jmenga/effect-dynamodb/issues/100))

  `db.entities.X.put(...)` returns a `BoundPut`, which the shared transactable-extraction protocol did
  not recognise — every multi-item write path rejected it with `ValidationError { entityType: "unknown" }`.
  This blocked entities authored with the pure, AWS-free `@effect-dynamodb/schema` `Entity.make`
  entirely: a pure definition carries no CRUD ops, so the bound builder is the only write descriptor
  its author can hold, which made "commit a read model atomically with the events that produced it"
  impossible. `extractTransactable` now unwraps bound builders to the intermediate they wrap.

  **Conditions on transact items are no longer silently dropped.** `.condition(...)`,
  `Entity.condition(...)`, and the implicit guards carried by `create()` (`attribute_not_exists`) and
  `deleteIfExists()` (`attribute_exists`) are compiled onto the `Put` / `Delete`. Previously
  `Transaction.transactWrite([Users.create(x)])` degraded to a blind overwrite.
  `ExpressionAttributeValues` is omitted when a condition carries no values, which also fixes
  `Transaction.check` with a value-free condition.

  **Ops the compile path cannot reproduce faithfully are rejected rather than reinterpreted.**
  `upsert` is an `UpdateItem` using `if_not_exists` for `createdAt`, immutable fields and the version
  counter — compiling it as a `Put` reset all three, including silently resetting the optimistic-lock
  counter. It now fails with a `ValidationError` on all three paths, as do entities configured with
  `refs`, `generatedId` or `vectorIndexes`, whose write contracts need a read, `Crypto` or an
  `Embedder`. `Batch.write` additionally rejects any conditioned op (`BatchWriteItem` has no
  `ConditionExpression`) and now reports unsupported ops on the error channel instead of as an
  untyped defect.

  Known gap, unchanged and now documented: entities with `unique`, `versioned: { retain: true }` or
  `softDelete` still write a single item through these paths, so their sentinel, snapshot or tombstone
  is not written. Prefer the entity's own operation for those. Tracked in [#113](https://github.com/jmenga/effect-dynamodb/issues/113).

  `@effect-dynamodb/schema` is unchanged and remains free of any AWS SDK dependency.

- [#112](https://github.com/jmenga/effect-dynamodb/pull/112) [`1e20c24`](https://github.com/jmenga/effect-dynamodb/commit/1e20c240ab44533495451c65b279f185b2c04e7a) Thanks [@jmenga](https://github.com/jmenga)! - `.condition(...)` now declares `ConditionalCheckFailed` on the error channel ([#102](https://github.com/jmenga/effect-dynamodb/issues/102)).

  A conditional write raised `ConditionalCheckFailed` at runtime but did not declare it, so
  `Effect.catchTag("ConditionalCheckFailed", ...)` — the whole reason to write a conditional put —
  was a type error. Callers had to `catchAll` and re-inspect `_tag`, losing exactly the
  exhaustiveness that makes `catchTags` worth using.

  Applying `.condition(...)` now widens the operation's error channel with `ConditionalCheckFailed`,
  on every surface it is exposed:

  - `BoundPut.condition()` — `db.entities.Users.put(input).condition(...)`
  - `BoundUpdate.condition()` — `db.entities.Users.update(key).set(...).condition(...)`
  - `BoundDelete.condition()` — `db.entities.Users.delete(key).condition(...)`
  - the entity-scoped pipeable — `Users.put(input).pipe(Users.condition(...))`, on unbound
    `EntityPut` / `EntityUpdate` / `EntityDelete`

  The widening is precise: an _unconditional_ `put` / `update` / `delete` keeps its narrow channel,
  and operations that already declare the error (`create`, `patch`, `upsert`, `deleteIfExists`) are
  unchanged — the union collapses. `.asEffect()` and every combinator chained after `.condition()`
  carry the widened channel. An update piped through the combinator stays an `EntityUpdate`; its
  update-payload parameter is not flattened.

  `.expectedVersion(...)` deliberately does **not** widen, and never did: `OptimisticLockError` is
  unconditional on `update` because the `versioned` and unique-constraint write paths CAS whether or
  not an expected version was supplied. The two combinators are now consistent in principle — each
  operation declares exactly the failures reachable on it.

  Also fixed on the unique-constraint transaction path: an update on an **unversioned** entity that
  touched a unique field and was rejected by a user `.condition(...)` reported `OptimisticLockError`
  — naming a version conflict that cannot occur on an entity with no version attribute. It now
  reports `ConditionalCheckFailed`. When a version CAS _is_ present, both predicates ride the same
  `ConditionExpression` and DynamoDB does not say which half failed, so the rejection is still
  attributed to the CAS as `OptimisticLockError`.

  **Semver note.** Released as a minor, not a major. Widening an error channel is technically
  breaking for code that matches the union exhaustively (`Effect.catchTags` over every tag, or a
  hand-written exhaustive `switch` on `_tag`), and the unversioned-unique fix changes which tag such
  code sees. Both are narrow, and neither breaks the common `catchTag` / `catchAll` shapes; staying
  within 1.x is the deliberate call.

  **`.condition()` on `delete` now reaches DynamoDB on every path.** The compiled condition was
  attached only in the simple `DeleteItem` branch. On entities configured with `softDelete` or a
  `unique` constraint — both of which delete via `transactWriteItems` — the guard was built and then
  dropped, and the delete proceeded unconditionally. The condition now rides the current item's own
  `Delete` in both transactions, so a rejection rolls the whole transaction back: no tombstone is
  written, no unique sentinel is released. `deleteIfExists` rode the same drop (it is `delete` plus
  `attribute_exists(pk)`) and is fixed with it, closing a read-then-write window in which a
  concurrently-deleted item could be resurrected as a tombstone.

  **`purge()` now rejects `.condition()` instead of ignoring it** — `ValidationError`, `EDD-9047`.
  `purge` deletes a whole partition across batched writes, so no single `ConditionExpression` can
  guard it atomically. Guard the individual write with `delete(key).condition(...)`.

  Without these, the error-channel widening above would declare a `ConditionalCheckFailed` that those
  entity shapes could never raise.

- [#112](https://github.com/jmenga/effect-dynamodb/pull/112) [`e1ecfb4`](https://github.com/jmenga/effect-dynamodb/commit/e1ecfb4def8c7d04faa62cf3fc9d3f66002dd7ff) Thanks [@jmenga](https://github.com/jmenga)! - Composite keys are composed by one rule everywhere — **storage-format change, migration required for two shapes** ([#101](https://github.com/jmenga/effect-dynamodb/issues/101), [#113](https://github.com/jmenga/effect-dynamodb/issues/113), [#114](https://github.com/jmenga/effect-dynamodb/issues/114), [#115](https://github.com/jmenga/effect-dynamodb/issues/115))

  Key composition ran on the encoded value, so a composite whose domain type is a `number` or `bigint` but whose encoded form is a **string** — `Schema.BigIntFromString`, `Schema.NumberFromString` — was written to the key as text, skipping the zero-padding that makes numbers sort correctly. Values 5 / 42 / 100 stored as `seq_5` / `seq_42` / `seq_100` and sorted `100 < 42 < 5`, so `gte(42n)` returned 42 and 5 instead of 42 and 100.

  Composition now follows one rule at every site: compose from the Encoded form, except when the domain type is numeric and the encoded form is a string, where the numeric Type form is used so it pads. `DynamoModel.DateEpochMs` composites use the padded epoch.

  **⚠️ Migration.** Rows keyed on either shape below were written under the old key and will not be found after upgrading — no error, the partition simply does not resolve. Read them by scan and re-`put` before or during the upgrade.

  - **Entity primary keys and GSI keys** with a `Schema.BigIntFromString` or `Schema.NumberFromString` composite. On 1.15.0 `put` **succeeded** and wrote these rows (only `get` was broken), so this data exists.
  - **Aggregate partition and collection keys** with a `DateEpochMs` / `DateEpochSeconds` composite, which move from their ISO form to the padded epoch.

  Composites of every other shape — plain numbers, bigints, strings, `Schema.Date`, `DateTimeUtc`, literals — are byte-identical and need no migration.

  Eleven modules previously decided independently what to hand the key composer, which is what produced the divergence. `test/KeyFormInvariant.test.ts` now reads each module as source text and fails if a `KeyComposer` call receives a record that did not go through the shared form.

  Fixed by the same change, each previously a silent wrong result:

  - `update()` rewrote GSI keys in a different format than `put()` wrote them, evicting the row from its own GSI.
  - `Transaction.transactWrite` / `Batch.write` composed a different primary key than `Entity.put`, producing unreadable orphan rows; `Batch.get`, `transactGet`, `transactWrite(delete)` and `Transaction.check` used the caller's raw key.
  - `purge()` reported success and deleted nothing.
  - `reembed()` skipped every live item — its guard compared a Type-side recompose against a stored key.
  - `getVersion()`, `deleted.get()` and `restore()` raised `ItemNotFound` for rows that exist, while `versions()` and `deleted.list()` worked.
  - `db.collections.*` and `Collections.make()` returned zero rows for values the equivalent entity accessor found.
  - Vector search composed a different partition than the write path.
  - Aggregate `list()` could not find rows `create` had written.

  Key input on `get` / `update` / `delete` and friends now takes the model's **Type** side — the same value the domain model holds and the query path accepts. Passing the wire form fails with a `ValidationError` naming the attribute rather than returning an empty result.

- [#112](https://github.com/jmenga/effect-dynamodb/pull/112) [`0d67a51`](https://github.com/jmenga/effect-dynamodb/commit/0d67a51b1b27907c746c6fe808f929716d12f504) Thanks [@jmenga](https://github.com/jmenga)! - Separate `limit` (results) from `pageSize` (round trips) on queries and scans

  `limit` and page size were two different ideas sharing one word. They are now two combinators:

  - **`limit(n)`** — return **at most `n` items**. A contract on results. It no longer sets DynamoDB's `Limit`; the query accumulates across as many requests as it takes to reach `n` accepted items or exhaust the key range.
  - **`pageSize(n)`** — fetch in **batches of `n` rows**. This is what sets DynamoDB's `Limit` (rows _examined_ per request). A contract on round trips, not on what comes back.
  - **`maxPages(n)`** — unchanged. Still the hard stop on the number of requests, and the escape hatch when a filter is selective enough that `limit` would otherwise walk a large partition.

  Both compose: `.pageSize(50).limit(120)` fetches in requests of at most 50 examined rows, accumulating until 120 items.

  **This is why they had to split.** DynamoDB's `Limit` bounds rows _examined_, and a `FilterExpression` is applied _after_ it — so `Limit` can never express "give me 3 matching items". Under a filter, `limit` is now satisfied by accumulating across requests; `pageSize` (or an unbounded natural page when unset) is what each request asks for. Every entity query and scan therefore gets correct filtered pagination.

  **Cursors.** Once a request can over-read and discard the surplus, `fetch()`'s cursor can no longer be the raw `LastEvaluatedKey` — that points at the last row _examined_, not the last one returned. It is rebuilt from the last item actually handed back (every item carries the table key and the index key), so the next page resumes after what the caller saw. `cursor: null` still means genuinely exhausted. When a `.select()` projection is active alongside a `limit`, the key attributes are added to the request's `ProjectionExpression` and stripped from the items returned, so a truncated page still carries an accurate cursor.

  **`count()`.** `limit(n)` caps the count: `.limit(n).count()` returns `min(matching, n)` and stops counting once `n` is reached, keeping `count()` equal to `collect().length` for the same query — and making `.limit(1).count()` a cheap existence check. `pageSize(n)` sizes each `Select: "COUNT"` request.

  ## Migration — if you used `limit` as a page-size hint, move to `pageSize`

  `limit` changes meaning on `collect()` and `paginate()`. The same call keeps compiling and quietly means something else, so check every call site:

  | Before                                             | After                                         |
  | -------------------------------------------------- | --------------------------------------------- |
  | `.limit(3).collect()` → every item, in pages of 3  | `.limit(3).collect()` → **3 items**           |
  | `.limit(2).paginate()` → everything, in pages of 2 | `.pageSize(2).paginate()`                     |
  | `.limit(100)` to size a scan's requests            | `.pageSize(100)`                              |
  | `.limit(25).fetch()`                               | unchanged — still up to 25 items and a cursor |

  Callers who wrote what the documentation showed (`.limit(3).collect()` for "the first 3") were getting every matching item; they are now correct without a change. This ships as a minor within 1.x rather than waiting for a 2.0 because the old behaviour is a trap the docs already described incorrectly.

- [#112](https://github.com/jmenga/effect-dynamodb/pull/112) [`1e20c24`](https://github.com/jmenga/effect-dynamodb/commit/1e20c240ab44533495451c65b279f185b2c04e7a) Thanks [@jmenga](https://github.com/jmenga)! - Aggregate: honour `sk.composite` on `many` edges, so one entity can appear more than once in an aggregate (closes [#103](https://github.com/jmenga/effect-dynamodb/issues/103))

  `ManyEdgeConfig.sk.composite` was declared, type-checked and stored on the edge, but never read — the decompose walk composed each element's sort key from the referenced entity's identifier alone. Two elements sharing a ref composed one sort key, and DynamoDB rejected the entire aggregate write with `ValidationException: Transaction request cannot include multiple operations on one item`.

  A declared `sk.composite` is now **authoritative**: it replaces the ref-identifier heuristic rather than extending it, so it decides both uniqueness and the order elements sort in. Entries name attributes on the decomposed element and may use a dotted path to reach a hydrated ref (`"umpire.id"` — hydration replaces the id field with the referenced object, so the bare name no longer exists at the top level).

  Two related fixes for the same defect — a `many` edge whose sort key is not derived from anything distinguishing:

  - **"Element IS the ref" edges now compose a sort key.** `Schema.Array(Player)` hydrates each element to the entity's own flat fields, and the identifier fallback only recognised a field literally named `id`. An entity whose identifier is `playerId` produced _no_ composites, so every element collapsed onto one row. The edge entity's declared `DynamoModel.identifier` field is now used.
  - **Colliding sort keys fail as `AggregateDecompositionError`.** Decomposition detects two items composing the same sort key and fails with the aggregate, the edge and the colliding key — instead of an opaque `DynamoValidationError` naming nothing. This is checked before any write, so nothing is persisted.

  A declared composite must resolve to a **scalar** — string, number, bigint, boolean or date. Naming the hydrated ref object itself (`sk: { composite: ["umpire"] }`) rather than a scalar path (`"umpire.id"`) previously serialised the whole object into the sort key; it now fails with `AggregateDecompositionError` pointing at the dotted form.

  **Migration.** Sort keys change for two shapes, both of which could not previously hold more than one element:

  - edges that already declared `sk.composite` (previously ignored)
  - "element IS the ref" edges whose entity identifier is not named `id` — a single-element edge stored as `$app#v1#matchplayer` now stores as `$app#v1#matchplayer#p-1`

  Existing rows in either shape are orphaned on the next update. Entity-less `many` edges over plain structs now require a declared `sk.composite`; without one, a multi-element edge fails with `AggregateDecompositionError` rather than silently writing one row per aggregate.

### Patch Changes

- [#112](https://github.com/jmenga/effect-dynamodb/pull/112) [`1e20c24`](https://github.com/jmenga/effect-dynamodb/commit/1e20c240ab44533495451c65b279f185b2c04e7a) Thanks [@jmenga](https://github.com/jmenga)! - Compose `.where()` sort key conditions into full sort key values (closes [#101](https://github.com/jmenga/effect-dynamodb/issues/101))

  A sort key condition applied through a named-index or primary-key accessor did
  not narrow the query. Stored sort keys are composed as
  `$schema#v1#entity#<name>_<cased value>`, but `.where()` concatenated the raw
  operand onto the entity prefix — so `gte` matched the whole partition (a raw
  value sorts below every `<name>_`-prefixed segment) while `beginsWith`, `eq`,
  `between`, `lt` and `lte` matched nothing.

  The operand is now placed in the position of the SK composite it targets and run
  through the same composer the write path uses, applying value serialization, the
  `<name>_` prefix and the schema casing. Additional behaviour that follows from
  composing correctly:

  - A condition on a **non-terminal** SK composite covers that value's whole
    subtree — `eq` compiles to a subtree `begins_with`, and inclusive upper bounds
    span the subtree.
  - When the accessor has already pinned leading SK composites, one-sided
    operators are clamped to that prefix (`Query.where` replaces the accessor's
    own `begins_with`, so an unclamped `>=` would leak into neighbouring
    composite values).
  - New `EDD-9045` when `.where()` is used on an index whose sort key has no
    composites, and `EDD-9046` for a strict `lt` on the last SK composite while an
    earlier one is pinned — DynamoDB cannot express `begins_with(prefix) AND sk <
value` in a single key condition, so this is refused rather than silently
    returning the boundary item.

- Updated dependencies [[`e1ecfb4`](https://github.com/jmenga/effect-dynamodb/commit/e1ecfb4def8c7d04faa62cf3fc9d3f66002dd7ff), [`1e20c24`](https://github.com/jmenga/effect-dynamodb/commit/1e20c240ab44533495451c65b279f185b2c04e7a), [`1e20c24`](https://github.com/jmenga/effect-dynamodb/commit/1e20c240ab44533495451c65b279f185b2c04e7a), [`e1ecfb4`](https://github.com/jmenga/effect-dynamodb/commit/e1ecfb4def8c7d04faa62cf3fc9d3f66002dd7ff), [`0d67a51`](https://github.com/jmenga/effect-dynamodb/commit/0d67a51b1b27907c746c6fe808f929716d12f504), [`1e20c24`](https://github.com/jmenga/effect-dynamodb/commit/1e20c240ab44533495451c65b279f185b2c04e7a), [`1e20c24`](https://github.com/jmenga/effect-dynamodb/commit/1e20c240ab44533495451c65b279f185b2c04e7a)]:
  - @effect-dynamodb/schema@1.16.0

## 1.15.0

### Minor Changes

- [`bb71bad`](https://github.com/jmenga/effect-dynamodb/commit/bb71bad064f8f91d8b3a2ff8dffd86baf3454477) Thanks [@mixja](https://github.com/mixja)! - Validate `timestamps` schema overrides, and support `timestamps` on `Aggregate.make`.

  **`timestamps.<slot>.schema` is now validated at make() time ([#97](https://github.com/jmenga/effect-dynamodb/issues/97)).** The `schema` half of a
  `TimestampFieldConfig` is a storage descriptor, not a codec: system timestamps are generated by
  the library, so the only thing readable off the supplied schema is its `DynamoEncoding`
  annotation. A schema without one used to be discarded silently and the field fell back to an ISO
  string — which fails at the service rather than the definition, since a GSI declaring
  `AttributeType: "N"` over that field makes DynamoDB reject the write outright with
  `ValidationException: Type mismatch for Index Key`. `Entity.make` and `Aggregate.make` now throw
  `EDD-9044` naming the offending slot. Pass an annotated `DynamoModel` date schema
  (`DateString`, `DateEpochMs`, `DateEpochSeconds`) or one re-pointed with `storedAs(...)`.

  The bare-schema form (`timestamps: { updated: DynamoModel.DateEpochMs }`) was also inert, for a
  second reason: the internal predicate tested `typeof value === "object"`, but every Effect schema
  has been callable since 4.0.0-rc, so that branch never matched and the config silently fell
  through to the default. It now applies as documented, keeping the default field name.

  **`Aggregate.make({ timestamps })` ([#98](https://github.com/jmenga/effect-dynamodb/issues/98)).** Aggregates compose their DynamoDB rows directly
  rather than routing through Entity write ops, so entity-level `timestamps` never reached them —
  in a single-table design where aggregates hold most of the data, that left the majority of the
  table with no modification timestamp and no way to add one. Aggregates now take the same
  `TimestampsConfig` and stamp every row they write: the root item, `one` and `many` edges, and
  every row inside a sub-aggregate transaction group.

  - `updated` is per row — a diff-based `update` rewrites only the groups whose content changed,
    so rows the mutation leaves alone keep their stored value.
  - `created` is carried forward on rewrite; aggregate writes are `Put`, not `UpdateItem`.
  - Timestamps are stamped downstream of the update diff, so they never widen it.
  - The attributes are stripped on the read path unless the root model declares the field itself.

### Patch Changes

- Updated dependencies [[`bb71bad`](https://github.com/jmenga/effect-dynamodb/commit/bb71bad064f8f91d8b3a2ff8dffd86baf3454477)]:
  - @effect-dynamodb/schema@1.15.0

## 1.14.0

### Minor Changes

- [`cbea2e8`](https://github.com/jmenga/effect-dynamodb/commit/cbea2e816d937ed7c9a7bb8d820c3d7ccdaa26ec) Thanks [@mixja](https://github.com/mixja)! - Make optional properties on definition-time config surfaces strict under `exactOptionalPropertyTypes`.

  `?: T | undefined` and `?: T` mean different things when `exactOptionalPropertyTypes` is on:
  the first permits an explicit `{ field: undefined }`, which is precisely what the flag exists
  to forbid. Declaring `| undefined` on every optional property quietly opts back out of it.

  48 optional properties across the declarative config surfaces — entity config, index
  definitions (`GsiConfig` / `IndexDefinition`), vector index config, aggregate config and edge
  descriptors, and geo index config — are now `?: T`. "Not set" is expressed by omitting the key.

  Construction sites that previously assigned an explicit `undefined` now omit the key instead,
  so absent optionals stay absent rather than becoming present-but-undefined. `VectorIndexDefinition.casing`
  changes from a required `Casing | undefined` to an optional `?: Casing` for the same reason.

  **Possible breaking change for TypeScript consumers.** Code that passes a possibly-undefined
  value into one of these fields — `{ collection: maybeUndefined }` — no longer compiles. Omit the
  key conditionally instead: `...(x !== undefined && { collection: x })`. Runtime behaviour is
  unchanged.

  Types that mirror AWS SDK command inputs (`Query`, `DynamoClient`, vector search emulation),
  runtime plumbing such as `TableConfig.ttlAttributeName`, tagged-error payloads, and the
  incremental builder-state types keep `?: T | undefined` deliberately — those legitimately receive
  computed optional values, and forcing conditional spreads on callers there would cost ergonomics
  for no safety.

### Patch Changes

- Updated dependencies [[`cbea2e8`](https://github.com/jmenga/effect-dynamodb/commit/cbea2e816d937ed7c9a7bb8d820c3d7ccdaa26ec)]:
  - @effect-dynamodb/schema@1.14.0

## 1.13.0

### Minor Changes

- [`19e2305`](https://github.com/jmenga/effect-dynamodb/commit/19e230519975c4f4465403a8e233819906e23961) Thanks [@mixja](https://github.com/mixja)! - Aggregate assembly no longer requires a secondary index, and can opt into strongly consistent reads ([#93](https://github.com/jmenga/effect-dynamodb/issues/93)).

  `collection.index` and `collection.sk` are now optional. Assembly queries the whole partition
  with a bare `pk = :pk` condition and discriminates items by `__edd_e__` in memory — it issues
  no sort-key condition and depends on no ordering, so when the aggregate is keyed on the table's
  primary partition key the query runs against the base table. Omitting the index provisions no
  LSI and stops the collection SK mirror attribute (a verbatim copy of `sk` on every non-root item)
  from being written.

  That removes a 10 GB item-collection cap and a permanent `CreateTable` commitment from the
  structure most likely to grow — LSIs cannot be added or removed after the table exists, so an
  aggregate previously could not be added to an existing table in this shape at all.

  New `consistentRead` option (defaults to `false`, matching DynamoDB and `Entity`). Aggregate
  writes are transactional, so an eventually consistent read taken shortly after a write can
  observe a torn collection: the root may be missing, or an edge may be missing while the root is
  visible — which for optional or empty-able edges assembles into a quietly incomplete aggregate.
  Base-table reads can be strongly consistent; GSI reads cannot.

  Three `make()`-time validations: `EDD-9041` (omitting the index when the aggregate's PK is not
  the table's primary PK), `EDD-9042` (`consistentRead` against a GSI-shaped collection index) and
  `EDD-9043` (`collection.index` and `collection.sk` supplied apart).

  Fully backward compatible — existing index-backed aggregates keep their index, their mirror
  attribute, and their behaviour.

### Patch Changes

- Updated dependencies [[`19e2305`](https://github.com/jmenga/effect-dynamodb/commit/19e230519975c4f4465403a8e233819906e23961)]:
  - @effect-dynamodb/schema@1.13.0

## 1.12.1

### Patch Changes

- [`1f1dd6f`](https://github.com/jmenga/effect-dynamodb/commit/1f1dd6fa911565bff5b006449944ab076d6e7f9d) Thanks [@mixja](https://github.com/mixja)! - Upgrade Effect v4 from `4.0.0-rc.109` to `4.0.0-rc.112` (and `@effect/vitest` to match).

  No source changes were required — rc.110, rc.111 and rc.112 contain no breaking changes
  affecting APIs this library uses. The interface changes in those releases (`Pool.State`,
  `Pool.PoolItem`, `Scope.State.Open`, and the `Matcher` / `ValueMatcher` flavor type
  arguments) touch modules the library does not consume.

  Consumers pick up upstream improvements for free:

  - Faster synchronous `Schema` decode/encode (completed parser exits plus a direct loop for
    common struct parsers) — this library decodes every item it reads.
  - Faster `SchemaError` construction (stack frame capture is now skipped) — validation
    failures are cheaper on hot paths.
  - `Optic` gained dual standalone `get` / `set` / `replace` / `modify` functions, usable
    alongside the cursor API exposed by `Aggregate.update`.

- Updated dependencies [[`1f1dd6f`](https://github.com/jmenga/effect-dynamodb/commit/1f1dd6fa911565bff5b006449944ab076d6e7f9d)]:
  - @effect-dynamodb/schema@1.12.1

## 1.12.0

### Minor Changes

- [#89](https://github.com/jmenga/effect-dynamodb/pull/89) [`63eaed6`](https://github.com/jmenga/effect-dynamodb/commit/63eaed62a5cad7d57c87e9fe0de5ca52f3388071) Thanks [@jmenga](https://github.com/jmenga)! - EventStore: additional transaction items in `append` + command idempotency (closes [#85](https://github.com/jmenga/effect-dynamodb/issues/85))

  **`append(streamId, events, expectedVersion, { additionalItems })`** — commit caller-owned
  transact items atomically with the event puts. `additionalItems` accepts the same op union
  `Transaction.transactWrite` takes (`Entity.put`, `Entity.delete`, `Transaction.check`),
  compiled through a new shared builder so the two APIs cannot drift. `EntityUpdate` remains
  unsupported in both; it will land for both at once.

  **Position-aware cancellation mapping.** Previously _any_ `ConditionalCheckFailed` in
  `CancellationReasons` became `VersionConflict` — which, with caller-owned items in the
  transaction, would misreport a failed user condition and send callers into a
  read-decide-retry loop that could never succeed. Reasons are now matched by transaction
  index, with precedence `DuplicateCommand` > `VersionConflict` >
  `AdditionalItemConditionFailed` > `TransactionCancelled`.

  **Command idempotency.** `commandHandler(decider, stream, { idempotency: { ttl? } })` plus a
  per-call `commandId` writes a dedup sentinel guarded by `attribute_not_exists` into the same
  transaction, co-located in the stream partition and invisible to `read` / `readFrom` /
  `currentVersion`. A replayed `commandId` is rejected with `DuplicateCommand`. Without
  `idempotency`, command processing remains **at-least-once** — now documented in the tutorial.
  Configuring `idempotency` makes `commandId` required at the type level.

  **Transaction size guard.** `events + additionalItems + sentinel + the version-contiguity
ConditionCheck` is checked against the shared `TRANSACT_WRITE_ITEMS_LIMIT` (100) before any
  AWS call, failing with `AppendTooLarge`.

  New tagged errors: `AdditionalItemConditionFailed`, `DuplicateCommand` (both exported from
  `effect-dynamodb` and `@effect-dynamodb/schema`).

  Also fixes the data-last (pipeable) form of `EventStore.commandHandler`, which passed the
  stream and decider to the implementation in swapped order and could never have worked.

  **Type-level note:** `append`'s error channel now unconditionally includes
  `DuplicateCommand | AdditionalItemConditionFailed | AppendTooLarge`, rather than varying
  with the options passed. `Effect.catchTag` / `catchTags` callers are unaffected; a caller
  exhaustively matching on `append`'s error union will need three more cases.

- [#87](https://github.com/jmenga/effect-dynamodb/pull/87) [`f846367`](https://github.com/jmenga/effect-dynamodb/commit/f846367a541fefb9c29f40684f87f89c8333745b) Thanks [@jmenga](https://github.com/jmenga)! - EventStore: guard `append` against TransactWriteItems limits and expectedVersion-ahead version gaps ([#82](https://github.com/jmenga/effect-dynamodb/issues/82))

  - New `AppendTooLarge` tagged error and `TRANSACT_WRITE_ITEMS_LIMIT` constant, exported from both `@effect-dynamodb/schema` and `effect-dynamodb`. `append` now pre-validates the transact-item count and fails with `AppendTooLarge` before issuing any request instead of surfacing a raw AWS validation error. The batch is deliberately never chunked — chunking would break append atomicity.
  - `append` now enforces version contiguity. When `expectedVersion > 0` the transaction carries a `ConditionCheck` requiring the event at exactly `expectedVersion` to exist, so an _ahead_ expected version fails with `VersionConflict` instead of silently writing past the stream head and leaving a permanent gap in the version sequence. The check occupies one transact item, so a single append holds up to 100 events at `expectedVersion === 0` and up to 99 otherwise.

- [#90](https://github.com/jmenga/effect-dynamodb/pull/90) [`5127a60`](https://github.com/jmenga/effect-dynamodb/commit/5127a606049d89da5de92b44f74dc2182e8b4418) Thanks [@jmenga](https://github.com/jmenga)! - EventStore: snapshot support and a `commandHandler` retry option ([#84](https://github.com/jmenga/effect-dynamodb/issues/84)).

  - **Snapshots** — opt in with `makeStream({ ..., snapshot: { schema, every? } })`. One
    snapshot item per stream lives in the stream partition under a distinct sort key
    (`$<schema>#v<n>#<stream>.snapshot`) that can never collide with an event sort key.
    State round-trips through the supplied schema (encode on write, decode on read), so
    transforming schemas work. New primitives: `writeSnapshot` / `readSnapshot` (also on
    `BoundEventStream`). Snapshot writes are monotonic — losing the race is a no-op, never
    a cache regression.
  - **Snapshot-aware `commandHandler`** — when a stream declares `snapshot`, each command
    runs `readSnapshot → readFrom(asOfVersion) → foldFrom → decide → append` instead of a
    full replay. With `every: N`, a fresh snapshot is written (best-effort) after a
    successful append once N events have accumulated since the last one.
  - **Retry** — `commandHandler(decider, stream, { retry })` accepts a max-attempts number
    or an Effect `Schedule`. On `VersionConflict` the **full** read-decide-append cycle
    re-runs, so every attempt decides against fresh state; a blind re-append is impossible
    by construction. Domain and infrastructure errors are never retried. Default: no retry.
  - **Event reads are SK-range hardened** — `read` / `readFrom` / `currentVersion` /
    `query.events` now bound the key condition to the event sort-key range instead of
    relying on the `__edd_e__` filter alone (DynamoDB applies `Limit` before
    `FilterExpression`). `currentVersion` also switched from `Query.collect` to the
    single-page terminal — it previously walked the whole partition one request per item.
  - **Fixed: `commandHandler`'s data-last form.** It was declared with `Function.dual`,
    whose data-last path assumes the data is the _first_ parameter — here the stream is the
    second, so `stream.pipe(commandHandler(decider))` passed the arguments swapped and threw.
    Replaced with a dispatch on the stream's `EventStreamTypeId` brand. `EventStream` and
    `BoundEventStream` are now `Pipeable`.
  - New: `DynamoSchema.composeEventVersionKeyPrefix` and `DynamoSchema.MAX_EVENT_VERSION`.

  Fully backward compatible: streams without `snapshot` and handlers without options behave
  exactly as before.

### Patch Changes

- [#88](https://github.com/jmenga/effect-dynamodb/pull/88) [`f903422`](https://github.com/jmenga/effect-dynamodb/commit/f90342267928cdc0cb50d0d4ef522dc54689de1a) Thanks [@jmenga](https://github.com/jmenga)! - fix(eventstore): codec symmetry — encode on write, decode on read

  `EventStore.append` previously spread the event instance and marshalled it raw,
  so any event schema carrying a transformation (`Schema.DateTimeUtc`,
  `Schema.Date`, branded transforms, fields with defaults) stored its **runtime**
  representation and then failed or drifted when the read path decoded it.
  Events are now run through `Schema.encode` before marshalling, mirroring the
  Entity/Aggregate write path.

  Metadata had the same asymmetry — validated with `Schema.decode` on write and
  returned via a raw cast on read. It is now encoded on write and decoded on
  read, so `StreamEvent.metadata` is the decoded schema type.

  The persisted event envelope (`streamId`, `version`, `eventType`, `timestamp`)
  is decoded through a schema instead of unchecked casts. Encode failures map to
  `ValidationError` with `operation: "EventStore.append"` (or
  `"EventStore.append.metadata"`).

  The injected `_tag` on stored event data keeps working for both `Schema.Class`
  and `Schema.TaggedClass` events.

- Updated dependencies [[`63eaed6`](https://github.com/jmenga/effect-dynamodb/commit/63eaed62a5cad7d57c87e9fe0de5ca52f3388071), [`f846367`](https://github.com/jmenga/effect-dynamodb/commit/f846367a541fefb9c29f40684f87f89c8333745b), [`5127a60`](https://github.com/jmenga/effect-dynamodb/commit/5127a606049d89da5de92b44f74dc2182e8b4418), [`f903422`](https://github.com/jmenga/effect-dynamodb/commit/f90342267928cdc0cb50d0d4ef522dc54689de1a)]:
  - @effect-dynamodb/schema@1.12.0

## 1.11.0

### Minor Changes

- [`c754d7d`](https://github.com/jmenga/effect-dynamodb/commit/c754d7d4a070090c2968a815486796326e4a722b) Thanks [@mixja](https://github.com/mixja)! - Native DynamoDB vector search (closes [#78](https://github.com/jmenga/effect-dynamodb/issues/78)).

  Declare vector indexes on an entity and the library handles the rest — embedding
  generation on write, partition composition, lifecycle stripping, and a fluent
  search builder:

  ```ts
  const Products = Entity.make({
    model: Product,
    entityType: "Product",
    primaryKey: {
      pk: { field: "pk", composite: ["tenantId", "productId"] },
      sk: { field: "sk", composite: [] },
    },
    vectorIndexes: {
      byDescription: {
        name: "vec1",
        dimensions: 1024,
        distance: "cosine",
        source: { fields: ["name", "description"] },
        partition: ["tenantId"],
        filters: ["category"],
      },
    },
  });

  const hits =
    yield *
    db.entities.Products.byDescription("waterproof hiking boots")
      .partition({ tenantId })
      .filter({ category: "footwear" })
      .topK(25)
      .collect();
  ```

  - **Pure models.** The embedding (`__edd_v_<index>__`) and composed HASH
    partition (`__edd_vp_<index>__`) are library-managed and never surface in a
    decoded record.
  - **Automatic entity + tenant scoping.** The partition value is composed by
    `KeyComposer` as `$schema#v1#<entityType>[#composites]`, so a search on a
    shared physical index cannot cross entity types or tenants.
  - **`Embedder` service** (`@effect-dynamodb/schema`, AWS-free) with an in-library
    `Embedder.layerTest`. Dimension agreement is validated at `DynamoClient.make`.
  - **Write gating.** `put`/`create`/`upsert` always embed; `update`/`patch` embed
    only when the write touches a `source.fields` member — by `set()`, `remove()`,
    a null clear, or a path operation. Clearing every source field removes the item
    from the index. `.withVector()` supplies a pre-computed embedding (its name is
    typed and validated); `reembed({ concurrency })` migrates stale vectors.
  - **Declared filters only.** Only attributes listed in an index's `filters: [...]`
    are filterable — enforced by the accessor types, at runtime, and by the
    emulation layer. Entities sharing a physical index union their filters.
  - **Typed errors.** `EmbeddingError` is in the `put`/`create`/`update`/`patch`/
    `upsert` error channel for entities that declare `vectorIndexes`, and absent
    for those that don't.
  - **Lifecycle-aware.** Version snapshots, soft-delete tombstones and time-series
    event items drop out of the index; the tombstone stashes the embedding so
    `restore()` never re-embeds.
  - **Table ops.** `create()` emits merged `VectorIndexes`; `addVectorIndex`,
    `removeVectorIndex` and `waitForVectorIndex` manage them on a live table.
  - **`VectorSearchEmulation.layer`** stands in for DynamoDB Local, which discards
    `VectorIndexes` and rejects `SearchVectors`.

  Requires `@aws-sdk/client-dynamodb` >= 3.1104.0 (bumped). The raw operation needs
  the new `dynamodb:SearchVectors` IAM action, which existing read policies do not
  cover.

  Also fixes an unrelated pre-existing bug found along the way: `upsert` did not
  store the model fields that compose the primary key (it skipped them as "already
  in the Key", but the Key holds the _composed_ `pk`/`sk` strings, not the
  composite source values). Upserted items were stored without e.g. `productId`,
  so every subsequent read — including `upsert`'s own `ReturnValues: ALL_NEW`
  decode — failed with `Missing key`.

### Patch Changes

- Updated dependencies [[`c754d7d`](https://github.com/jmenga/effect-dynamodb/commit/c754d7d4a070090c2968a815486796326e4a722b)]:
  - @effect-dynamodb/schema@1.11.0

## 1.10.0

### Minor Changes

- [`bfec5f2`](https://github.com/jmenga/effect-dynamodb/commit/bfec5f22ca03961b2bcc13dfa62b323a7bab6375) Thanks [@mixja](https://github.com/mixja)! - Upgrade to Effect 4.0.0-rc.109 (release candidate)

  The workspace moves from `effect@4.0.0-beta.85` to `effect@4.0.0-rc.109` (now published from the main Effect-TS/effect repo). The `effect` peer range is raised to `^4.0.0-rc.109` accordingly. Migrations applied:

  - **`Schema.DateValid` removed** — `Schema.Date` now rejects invalid dates itself; all `DateValid` usages (DynamoModel Unsafe date codecs, date-transform substitution) migrate to `Schema.Date` with identical validation semantics.
  - **AST introspection moved to `representation` identities** — the RC removed the `typeConstructor` / `meta` annotation payloads that date/Redacted detection sniffed via `SchemaAST.resolve`. Detection now reads the stable `representation.id` (`effect/schema/Date`, `effect/schema/DateTimeUtc`, `effect/schema/DateTimeZoned`, `effect/schema/Redacted`) through a shared `matchDateRepresentation` helper (deduplicating the former Aggregate.ts matchers).
  - **`Schema.Struct(...)` is now a function** — the `isSchemaClass` detection gains an AST-tag guard (`Declaration` vs `Objects`); without it, Struct-modeled entities were decoded through `new Struct(...)` and silently returned empty objects. A canary test locks the discriminator down.
  - **`SchemaIssue.InvalidType` signature** — the constructor takes the raw rejected input instead of an `Option`; all five custom-getter call sites updated.
  - **`DateTime.toEpochSeconds`** — replaces hand-rolled `Math.floor(toEpochMillis(...) / 1000)` TTL math in Entity and the DateEpochSeconds codec.

### Patch Changes

- Updated dependencies [[`bfec5f2`](https://github.com/jmenga/effect-dynamodb/commit/bfec5f22ca03961b2bcc13dfa62b323a7bab6375)]:
  - @effect-dynamodb/schema@1.10.0

## 1.9.5

### Patch Changes

- fix(aggregate): delete edge items removed by `aggregate.update` (closes [#74](https://github.com/jmenga/effect-dynamodb/issues/74))

  `BoundAggregate.update(key, fn)` previously wrote new/changed edge rows but never **deleted** edge items that the mutation removed. If the returned state dropped an element from a `many` edge (or cleared a `one` edge), the orphaned row was left in the table and re-appeared on the next `get` — adds and updates persisted correctly; only removals were dropped.

  Root cause: the update diff operated at the transaction-**group** level (iterating only the new groups and re-`Put`ting changed ones). But a `many`-edge element — and a cleared `one` edge — lives in its **parent's** transaction group, so removing one element shrinks a group rather than dropping a whole group; a group-level diff therefore never emitted a delete for the orphaned row.

  The diff is now **item-level**: `update` builds the full new and old DynamoDB item sets, and for each changed group applies the surviving `Put`s together with `Delete`s for every old row whose key is absent from the new state — all in one `TransactWriteItems` per group (an add and a sibling removal commit atomically). The orphan check is global across the partition, so an element merely moving between groups is never wrongly deleted, and the root item (constant key) is never removed. Transaction-size validation now spans `Put`s + `Delete`s and runs up-front so an oversized later group fails fast instead of partially committing earlier ones.

- Updated dependencies []:
  - @effect-dynamodb/schema@1.9.5

## 1.9.4

### Patch Changes

- Fix aggregate/ref hydration for pure entity definitions and transform-typed date fields ([#71](https://github.com/jmenga/effect-dynamodb/issues/71), [#72](https://github.com/jmenga/effect-dynamodb/issues/72)), and round-trip self-date fields nested inside refs/edges.

  - **[#71](https://github.com/jmenga/effect-dynamodb/issues/71)** — `BoundAggregate.get`/`create` no longer crash with `TypeError: runtimeEntity.get is not a function` when an aggregate edge is authored from a pure `@effect-dynamodb/schema` `EntityDefinition`. Hydration now promotes such pure edge targets to runtime entities (mirroring `DynamoClient.make`'s entity binding).
  - **[#72](https://github.com/jmenga/effect-dynamodb/issues/72)** — Transform-typed date fields (e.g. `Schema.DateTimeUtcFromString`) on an aggregate root, on a hydrated aggregate edge, and on a plain `Entity` ref target are now decoded exactly once instead of double-decoding (`SchemaError: Expected string, got DateTime.Utc`) — across aggregate `create`, `get`, and `update`. Plain-entity ref hydration re-encodes fetched refs to wire form before splicing; the aggregate reads/validates through a single tolerant decode schema (replacing the previous per-field pre-decode); and decomposed edge date fields are serialized to wire on write.
  - **Nested self-date round-trip (Option A)** — `Schema.DateTimeUtc` / `Schema.Date` (and `Schema.RedactedFromValue`) fields nested inside a `DynamoModel.ref` target or an aggregate edge model now round-trip through DynamoDB, with the nested class instance identity preserved. Covers **required and optional** nesting — `Schema.optional` / `Schema.optionalKey` classes (e.g. optional sub-aggregates), arrays of classes, and leaves.

- Updated dependencies []:
  - @effect-dynamodb/schema@1.9.4

## 1.9.3

### Patch Changes

- fix(client): bind pure `@effect-dynamodb/schema` definitions via `DynamoClient.make` (closes [#69](https://github.com/jmenga/effect-dynamodb/issues/69))

  A pure `EntityDefinition` produced by `@effect-dynamodb/schema`'s `Entity.make` — the AWS-free authoring surface introduced by the schema/runtime split ([#62](https://github.com/jmenga/effect-dynamodb/issues/62)) — was accepted by `DynamoClient.make` but bound to `never`: `db.entities.X` exposed no usable methods, and any call would also have crashed at runtime because pure definitions carry no operations or `_decodeRecord`. The single-source-of-truth goal of the split was unreachable — entities had to be re-authored with the runtime `Entity.make` to get a working client.

  This completes the split's end-to-end path:

  - **Type:** `TypedClient`'s entity mapping now matches a pure `EntityDefinition` (a second conditional branch) in addition to the runtime `Entity`, so `db.entities.X` resolves to the full bound entity (CRUD + index accessors + `scan`) for both authoring styles.
  - **Runtime:** `DynamoClient.make` transparently _promotes_ a pure definition to a full operational entity at bind time via `Entity.fromDefinition` — a thin op-attach over the definition's retained derivation data. This also fixes the silent `db.collections.*` decode crash for pure-authored members. CRUD, index queries, `scan`, collections, and table GSI derivation all work.
  - **Refs:** pure entities with refs are fully supported. Write-time ref hydration calls `.get()` on each ref target, so promotion now promotes ref targets too (one level — a `.get` does not itself hydrate, which also sidesteps cyclic refs). The two packages' `AnyRefValue` are unified onto the shared structural `RefEntity` carrier, so ref-derived id composites survive into the bound client (the pure branch forwards refs instead of dropping them), and a ref target may be authored in either package.
  - **Derivation unified:** the runtime `Entity.make` now delegates to the schema package's shared `buildEntityDefinition` instead of re-implementing the EDD-90xx validation/derivation, eliminating drift between the two layers (the class of bug behind [#54](https://github.com/jmenga/effect-dynamodb/issues/54)). Promotion reuses the derived bundle, so there is no double derivation.
  - **Aggregates:** the runtime `Aggregate` now re-exports the schema package's `TypeId` instead of declaring a nominally-distinct `unique symbol`, closing a dual-package hazard. A pure `AggregateDefinition` remains schema-derivation-only (typed `inputSchema`/`updateSchema` for contracts) and is intentionally not bindable — the decompose/assemble engine is AWS-coupled; author aggregates with `effect-dynamodb`'s `Aggregate.make` to bind them. This is now documented on the type.

  Adds a `pure-authoring` example and type-level + runtime + connected regression tests (including pure entities with refs).

- Updated dependencies []:
  - @effect-dynamodb/schema@1.9.3

## 1.9.2

### Patch Changes

- fix(schema): make the AWS-free pure-authoring path actually usable (closes #66, closes #67).

  Two follow-ups to the #62 schema/runtime split, both blocking its headline use case
  (deriving a typed aggregate input/create payload from `@effect-dynamodb/schema` with
  no AWS SDK):

  - **#66** — the pure edge constructors (`Aggregate.ref` / `one` / `many`) required a
    `RefEntity` with a runtime `get` method, so aggregate edges could not be authored
    from pure `Entity.make` definitions (which have no `get`). `RefEntity` is now the
    minimal structural bound used only for derivation (`_tag`/`entityType`/`model`/
    `indexes`/`schemas`); the runtime ref-hydration narrows back to a `get`-bearing
    entity at its single call site.
  - **#67** — `deriveAggregateSchemas` (the table-free derivation entry point) returned
    `Schema.Top` members, so `typeof result.inputSchema.Type` collapsed to `unknown`.
    It is now generic and returns `Schema.Codec<AggregateInputType<…>>` (plus a
    `createSchema` alias), so the table-free path is as typed as the top-level
    `Aggregate.make` — no stub `table` tag or GSI key config needed.

  Type-checked regression tests for both land in the schema package's `tsconfig.test.json`
  gate (now wired into `pnpm check`).

- Updated dependencies
  - @effect-dynamodb/schema@1.9.2

## 1.9.1

### Patch Changes

- fix(release): resolve `workspace:` protocol at publish time (closes #64).

  `1.9.0` shipped with an unresolved `workspace:` spec (`effect-dynamodb`'s
  `dependencies."@effect-dynamodb/schema": "workspace:^"`, and `@effect-dynamodb/geo`'s
  `peerDependencies.effect-dynamodb`), making `effect-dynamodb@1.9.0` uninstallable for
  consumers. Root cause: `release.yml` published via `npm publish`, which does not
  rewrite the `workspace:` protocol.

  The publish step now packs each package with `pnpm pack` (which rewrites `workspace:`
  in `dependencies` and `peerDependencies` to concrete ranges) and publishes the
  resulting tarball via `npm publish` (preserving OIDC Trusted Publishing + provenance),
  with a guard that refuses to publish if any `workspace:` spec remains in the packed
  manifest. No runtime/API changes — 1.9.1 republishes 1.9.0 with correctly resolved
  dependency ranges.

- Updated dependencies
  - @effect-dynamodb/schema@1.9.1

## 1.9.0

### Minor Changes

- d6bacd7: feat: Clock-backed timestamps/TTL via DateTime.now; wire unique.ttl; accept Duration|string for all TTL configs (closes #56, closes #58).

  - All write-path timestamps and TTLs now derive from the Clock-backed `DateTime.now` instead of `Date.now()`/`new Date()`, making them deterministic under `TestClock` (`R` stays `never` — Clock is an ambient default service). The two duplicate timestamp generators are unified into one shared helper.
  - `unique` constraints now honour `ttl` — when set, the unique sentinel item carries the configured TTL attribute and auto-expires (time-bounded uniqueness reservation). Previously the `ttl` field was typed but never consumed.
  - Every framework TTL config (`versioned.ttl`, `softDelete.ttl`, `timeSeries.ttl`, `unique[].ttl`) now accepts a humanized string (e.g. `"7 days"`) as well as a `Duration`. A bare `number` is rejected at the type level (it would be interpreted as milliseconds), and infinite/unparseable durations fail at `Entity.make()` with **EDD-9005**.

- 6b62366: feat: auto-generated UUID primary keys via Entity.make({ generatedId }) sourced from the Crypto service; R stays never (closes #57)
- 0e56c83: feat: split pure schema/relationship-derivation layer into the new @effect-dynamodb/schema package (importable without @aws-sdk); effect-dynamodb re-exports it, non-breaking (closes #62).
  - New `@effect-dynamodb/schema` package owns the AWS-free core: `DynamoModel`, `DynamoSchema`, `KeyComposer`, the tagged `Errors`, `Projection`, the entity/aggregate derivation internals, and pure `Entity.make` / `Aggregate.make` definition builders carrying the derived `inputSchema` / `updateSchema` / `createSchema`. It has ZERO `@aws-sdk` dependency in both its runtime import graph and its emitted `.d.ts` surface — guarded by an automated test.
  - `effect-dynamodb` depends on and re-exports the entire public surface of `@effect-dynamodb/schema`, then adds the AWS runtime (DynamoClient, CRUD/query operations, Batch/Transaction/Collection, Marshaller). Existing consumers (and `@effect-dynamodb/geo`) are unaffected — every import keeps working unchanged.
  - Consumers who only need an entity/aggregate's derived schemas (e.g. HttpApi payloads, validation) can now `import { Entity, Aggregate, DynamoModel, DynamoSchema } from "@effect-dynamodb/schema"` without pulling `@aws-sdk/*` into their dependency graph or type surface.

### Patch Changes

- ce36573: chore: harden Effect Schema AST access with typed SchemaAST guards in entity/aggregate derivation (closes #55)
- 3d4889a: fix: Aggregate inputSchema/updateSchema preserve branded identifier types on flattened edge refs (closes #61).
- e0cf0ad: chore(deps): upgrade Effect v4 beta.74 → beta.85. No source changes — type-check, unit, lint all green; no breaking upstream changes affect this codebase.
- Updated dependencies [0e56c83]
  - @effect-dynamodb/schema@1.9.0

## 1.8.2

### Patch Changes

- Fix EDD-9002 false positive on ref-derived `<field>Id` composites in the language-service. Closes [#54](https://github.com/jmenga/effect-dynamodb/issues/54).

  Index and primary-key composites that reference a ref's surfaced identifier attribute (e.g. `teamId` for a `team` ref) were incorrectly flagged as unknown attributes. The diagnostic's valid-attribute set was derived only from the model schema's read-side fields and never accounted for the `${field}Id` substitution the runtime applies for `refs`.

  The diagnostic now mirrors the runtime key/input schema: each field listed in the entity's `refs` config is removed from the valid-composite set and replaced by its `<field>Id` form. Referencing the bare ref field name (e.g. `team`) is still reported as an error, matching what `tsc` rejects.

## 1.8.1

### Patch Changes

- Upgrade to Effect v4 beta.74 and fix two breaking changes from the bump.
  - **`Effect.fromYieldable` removal** — `Config<T>` now extends `Effect.Effect<T, ConfigError>` directly, so the `Effect.fromYieldable(config)` wrapper is gone. Config values resolve straight through `Effect.runSync(config)`.
  - **`Schema.Array` element accessor** — beta.71 reintroduced `.value` on `Schema.Array`/`NonEmptyArray`; the element moved off `.schema`. `Aggregate` input-schema derivation read array elements via `.schema`, which silently broke ref→ID field rewriting (many-edge fields were no longer replaced with ID-string arrays). `extractArrayElement` now reads `.value` (with a `.schema` fallback for resilience across beta releases).

## 1.8.0

### Minor Changes

- Add `TableConfig.ttlAttributeName` (default `"_ttl"`) so the library writes TTL values to a configurable attribute name instead of the hardcoded `_ttl`. Closes [#51](https://github.com/jmenga/effect-dynamodb/issues/51).

  A single setting applies to every lifecycle feature on the physical table — `timeSeries: { ttl }`, `softDelete: { ttl }`, and `versioned: { retain, ttl }` write to the same attribute and `Entity.restore()` strips it. This matches DynamoDB's per-table `TimeToLiveSpecification.AttributeName`, which permits exactly one TTL attribute.

  ```ts
  const MainTable = Table.make({ schema, entities: { Users } });

  // Default (unchanged): writes to "_ttl"
  MainTable.layer({ name: "users-prod" });

  // Override: align with a TimeToLiveSpecification.AttributeName = "ttl"
  MainTable.layer({ name: "users-prod", ttlAttributeName: "ttl" });

  // Or from Effect Config
  MainTable.layerConfig({
    name: Config.string("TABLE_NAME"),
    ttlAttributeName: Config.string("TTL_ATTR").pipe(
      Config.withDefault("_ttl")
    ),
  });
  ```

  This is fully backwards compatible — consumers who don't set the field continue to write to `_ttl`. Use it to align the library's writes with a pre-existing or migrated DynamoDB table whose TTL attribute differs, without the destructive table replacement or multi-deploy DDB rename dance.

## 1.7.4

### Patch Changes

- **`.append(input).remove(attrs)` — atomic SET + REMOVE + CAS on time-series entities (closes [#49](https://github.com/jmenga/effect-dynamodb/issues/49)).**

  `BoundAppend` gains a `.remove(attrs)` combinator that emits `REMOVE` clauses on the same `UpdateItem` that carries the scoped `SET` and CAS predicate. Use it when an event needs to clear one or more `appendInput` attributes atomically — e.g. an IoT status event whose absence of an `alert` field means "no alert this cycle; drop the existing alert state on the current item."

  **Motivating problem.** Before v1.7.4, callers wanting to clear an `appendInput` attribute were stuck with three unsatisfactory workarounds:

  - `.append(...)` then `.update().remove([...])` (two writes) — race window: a concurrent writer between the two writes could clobber the cleared state.
  - Sentinel value (e.g. `alertState: "DISABLED"`) — keeps the attribute set, leaves the item in any `'sparse'`-policied GSI half that composes it.
  - `Schema.NullOr` + null payload — writes a literal `NULL` into the item; downstream readers must tolerate it.

  `.append(input).remove(attrs)` closes the race window structurally: a single `UpdateItem` carries `SET + REMOVE + CAS`, atomic with the event `Put`.

  **GSI cascade.** Any GSI half whose composite list intersects `attrs` follows the v1.7.1 cascade-override semantics — the half evaluates with the removed composite treated as absent. Under `'sparse'` the half drops; under `'preserve'` it's a no-op (the stored key field is left as-is unless overridden). The motivating shape is a sparse-PK GSI keyed on the cleared attribute — the item drops out of that GSI in the same write.

  **Validation.** Names listed in `.remove()` are checked at execution time. The Effect fails with `ValidationError(operation: "append.remove")` if any name:

  - is not declared in `appendInput` (enrichment-preservation contract — use `.update().remove([...])` for fields outside `appendInput`);
  - names `orderBy` (would invalidate the CAS anchor);
  - names a primary-key composite (would orphan the item);
  - names a ref field (refs are create-time denormalisations — reassign via `.update()`);
  - also appears in the encoded payload with a non-`undefined` value (DynamoDB rejects `SET`/`REMOVE` overlap).

  Chained `.remove()` calls accumulate. The combinator composes with `.condition()` and `.skipFollowUp()` in any order.

  **API.** New fluent combinator on `BoundAppend`:

  ```ts
  yield *
    db.entities.Telemetry.append({ channel, deviceId, timestamp }).remove([
      "alertState",
    ]);
  ```

  The entity-level unbound `Entity.append()` also gains an optional fourth positional argument (`removeAttrs?: ReadonlyArray<string>`) — used by `BoundAppend`'s `.remove()` wiring; library consumers should prefer the fluent form on `BoundAppend`.

  **No on-disk impact, no backfill required.** Pure feature add — existing time-series entities and existing items are unaffected.

  See `guides/timeseries.mdx` for the documented usage pattern and the issue [#49](https://github.com/jmenga/effect-dynamodb/issues/49) motivating IoT case.

## 1.7.3

### Patch Changes

- **indexPolicy v1.7.3 — reframe per-half evaluation gate as skip-predicate (closes [#46](https://github.com/jmenga/effect-dynamodb/issues/46)).**

  **What broke.** Under v1.7.0 / v1.7.1 / v1.7.2, `Entity.update()` silently skipped composing GSI keys for halves declared with `composite: []` (the standard "bare entity prefix" pattern, common in single-table-design lookup GSIs like `byDeviceBinding: { pk: [deviceBinding], sk: { composite: [] } }`). Items written via `.put()` were correct — but any subsequent `.update()` that touched the OTHER half left the empty-composite half missing → invisible to the GSI. Worse, an `.update()` that bound a previously-sparse GSI for the first time wrote only the PK half, leaving the SK missing.

  **What was wrong.** The per-half evaluation gate was a "touched" predicate — a chain of `||` clauses each enumerating a shape for which to evaluate the half (payload membership in v1.7.0; `removedSet` in v1.7.1; `keyRecord` in v1.7.2). Each missed shape required another tactical patch — `.some(...)` over an empty composite array trivially returns `false`, so empty-composite halves got classified as untouched and skipped.

  **Fix.** Reframe the gate as a **skip-predicate** keyed on the gate's actual purpose (multi-writer protection): skip iff composites exist (otherwise the half value is a constant prefix), no composite was explicitly removed, and every composite is absent from BOTH `updatePayload` AND `keyRecord`. The skip-predicate's negation is observably equivalent to the cumulative `||`-chain plus `composites.length === 0` — same SET/REMOVE outcomes for every existing input. Closes [#46](https://github.com/jmenga/effect-dynamodb/issues/46) directly and the class of degenerate-case bugs that v1.7.0 → v1.7.2 patches were chasing as separate `||` arms.

  **Affected items.** Items written under v1.7.0 / v1.7.1 / v1.7.2 against entities with empty-composite-half GSIs will repair themselves on the next `Entity.update()` against them under v1.7.3. The next write composes the missing half from the constant prefix and the item rejoins the GSI. No data migration is needed; reads via the GSI start returning these items as their next update lands.

  **No API changes.** Purely an internal gate-logic refactor — same observable behavior for every input the previous gate already handled correctly, plus correct behavior for the empty-composite-half shape that was silently broken.

## 1.7.2

### Patch Changes

- **indexPolicy v1.7.2 — fix PK-composites-only GSI regression (closes [#43](https://github.com/jmenga/effect-dynamodb/issues/43)).**

  v1.7.1 introduced a per-half evaluation gate that — by design — skipped GSI evaluation on halves the writer didn't touch. Unfortunately the gate consulted only `updatePayload` and so silently classified entire GSIs as untouched whenever their composites were entirely entity primary-key composites. Those PK composites never appear in `updatePayload` (writers address the row by key, never restate them in `.set({...})`, and `.append()` filtered them out before passing to the composer). The composer never ran, `gsiNpk` and `gsiNsk` were never written, and items were invisible to the GSI for their lifetime.

  Concretely: an entity with `primaryKey: [channel, deviceId]` and a `byChannel: { pk: [channel], sk: [deviceId] }` GSI saw zero items returned from any channel-scoped query under v1.7.0 / v1.7.1, regardless of whether the writes used `.put()`, `.update()`, or `.append()` — the latter two never composed the keys, and `.update()` only re-composed them on calls that explicitly restated `channel` / `deviceId` in the payload (which no realistic writer does).

  **The fix** (two minimal patches working together):

  1. **`KeyComposer.composeGsiKeysForUpdatePolicyAware`** — the per-half gate now also counts `keyRecord` membership (the entity primary-key attributes carried into the composer alongside the payload). PK-composite-only GSI halves are now correctly classified as touched on every write that has a `keyRecord`.
  2. **`Entity.append()`** — no longer filters PK composites out of the payload it passes to the composer. The filter never solved a real problem (the composer doesn't emit redundant SETs for the underlying composite fields, and idempotent recomposition from immutable PK composites is benign) and combined with the v1.7.1 gate to silently break this pattern.

  The change is idempotent for entity-PK composites (re-composing the same value from immutable PK composites produces the same key) and preserves the v1.7.1 multi-writer fix: stamps' GSI composites are not in `updatePayload` AND not in `keyRecord` either (they're enrichment-owned model attrs), so their halves remain untouched as designed.

  **Affected items:** items written to PK-composites-only GSIs under v1.7.0 or v1.7.1 will repair themselves on the next `Entity.update()` against them. The gate now fires correctly, the structural rule composes the immutable PK values, and the missing GSI keys are SET. **No data migration required** — reads via the GSI start returning these items as their next update lands. If you have items that aren't naturally updated, a one-shot bulk `Entity.update(key).set({ otherField: value })` (or even an empty-payload update touching only `updatedAt` + version) is enough to repair them.

  **No API changes** — same `indexPolicy: { pk, sk }` declaration shape, same `Entity.update` / `Entity.append` signatures, same EDD-9025 invariants. Behavior change is strictly more correct than v1.7.1.

  See `DESIGN.md §7` for the updated decision algorithm and `guides/index-policy.mdx` for the updated walkthrough plus the _byChannel GSI returns 0 items_ pitfall.

## 1.7.1

### Patch Changes

- **indexPolicy v1.7.1 — per-half roll-up corrections (closes [#41](https://github.com/jmenga/effect-dynamodb/issues/41)).**

  v1.7.0 shipped the per-key declaration model (`indexPolicy: { pk, sk }`) but had three connected bugs in how outcomes were rolled up. All three were rooted in the same defect: the model was per-half on declaration but not on evaluation, outcome, or cascade. v1.7.1 makes all four uniformly per-half.

  **Bug fixes** (strictly more correct than v1.7.0):

  1. **GSI-wide cascade on can't-compose → per-key REMOVE.** v1.7.0 REMOVE'd both `gsiNpk` AND `gsiNsk` whenever either half couldn't compose. v1.7.1 REMOVEs only the half that couldn't compose; the other half's stored value persists. Closes the multi-writer enrichment-on-pk + telemetry-on-sk scenario that v1.7.0 was designed to enable but didn't actually enable correctly.
  2. **`CompositeKeyHoleError` (EDD-9024) deprecated — no longer thrown.** The v1.7.0 throw under preserve+hole was a defensive runtime safety net for a case the type system already catches (required composites can't be omitted under `exactOptionalPropertyTypes` since v1.7.0 reverted the NullishOr widening). The class export is preserved for back-compat with consumers who type-imported it for `Effect.catchTag` handlers, but no code path raises it anymore. Hole patterns now collapse into the unified per-half can't-compose rule.
  3. **Per-half evaluation gate (NEW in v1.7.1).** v1.7.0 fired the policy on every update of every GSI declaring `indexPolicy`, regardless of whether the writer touched the half. This made stamps and unrelated writers blow away sparse halves they didn't own. v1.7.1 skips untouched halves entirely — a half is touched iff at least one of its composite names appears in the update payload OR in `Entity.remove([...])`.

  **Behavior change:**

  - **`Entity.remove([attr])` is now per-half** (no longer GSI-wide). Removing a composite REMOVEs only the half(s) whose composite list contains it. Other halves follow the per-half evaluation gate (untouched → noop). Combined with the new "cascade override under preserve" rule, the consumer's explicit signal still gets honored — preserve + can't-compose + composite in `removedSet` → REMOVE that half.

  **No API changes:**

  - Same `indexPolicy: { pk, sk }` declaration shape.
  - Same `Entity.remove([...])` API.
  - Same EDD-9025 invariants (composite attributes can't be `Schema.NullOr`).
  - Same `Schema.optional(...)` pattern for sparse composites.

  **v3 model preserved:** the per-key declaration, two-way payload classification, and EDD-9025 footgun gate from v1.7.0 are all preserved unchanged. v1.7.1 only fixes the roll-up.

  See `DESIGN.md §7` for the full v1.7.1 decision algorithm and the `guides/index-policy.mdx` rewrite for the concept-first walkthrough.

## 1.7.0

### Minor Changes

- [`5825d73`](https://github.com/jmenga/effect-dynamodb/commit/5825d73488a255733b965ab2c8e93e1c92c38517) Thanks [@mixja](https://github.com/mixja)! - **indexPolicy v3 — per-half model, structural composition, EDD-9025 invariant.** Closes [#39](https://github.com/jmenga/effect-dynamodb/issues/39) and supersedes [#38](https://github.com/jmenga/effect-dynamodb/issues/38).

  > **Heads up — breaking changes inside a minor bump.** The 1.6.0 → 1.7.0 transition would normally be a major bump on semver grounds, but in-the-wild consumer count is currently ~1 (the author's own consumer migration), so the bump is shipped as minor to accelerate iteration. Future consumers should treat 1.7.0 as if it were 2.0.0 and read this entry before upgrading.

  The v1.6 per-attribute `indexPolicy` callback model proved unwieldy in practice. The standard `update`/`patch` path has only payload-level information (no read-before-write), so per-attribute granularity within a single composed-key half collapsed to per-half outcomes anyway. v3 simplifies to a per-half declaration with a structural composition rule, and closes the `set({composite: null})` footgun at the type level.

  ### What's new in v1.7.0

  - **Per-half `indexPolicy: { pk, sk }`.** Both halves default to `'preserve'` when omitted. Replaces the v1.6 `indexPolicy: (item) => ({ attr: 'sparse' | 'preserve' })` callback API.
  - **Structural composition (longest valid leading prefix).** Symmetric on PK and SK — the v1.6 PK-clear-degrades-to-sparse asymmetry is gone. Hierarchical PK truncation (e.g. `pk.composite = ['accountId', 'fleetId']` → omit `fleetId` → partition key truncates to `account#A`) is **new and additive**.
  - **Two-way payload classification.** `null` = `undefined` = absent. The v1.6 three-way classification (present / explicit-clear / omitted) collapses; `set({attr: null})` no longer separately cascades GSI keys.
  - **Two coherent drop triggers** — both predictable, both tied to clear caller intent:
    - `Entity.remove([attr])` cascade (per-attribute, explicit, per call) — unchanged.
    - `'sparse'` policy + whole-half-empty (per-half, implicit, declared at the index) — narrower than v1.6's per-composite leakage.
  - **Policy-aware hole detection.** Hole pattern (`[A, _, C]`) under `'preserve'` throws `CompositeKeyHoleError` (EDD-9024); under `'sparse'` truncates to the leading prefix (or, if the leading prefix is empty, collapses to whole-half-empty + drop).
  - **EDD-9025 — `CompositeNullableError`.** New `Entity.make()` validation that walks every composite (across `primaryKey`, every entry in `indexes`, every entry in `unique` constraints) and throws if the composite's Schema includes `null` in its type union (`Schema.NullOr`, `Schema.NullishOr`, `Schema.Union` with a Null branch). Composites participate in string composition; null is not a meaningful slot value.
  - **Append unifies with update.** `.append()` now calls the same composer as `.update().set()` — the v1.6 `appendInput`-policy-filter wrapper is gone. Composites outside `appendInput` are simply absent under the structural rule.
  - **`writerScope` (proposal [#38](https://github.com/jmenga/effect-dynamodb/issues/38)) is superseded.** v3's narrower implicit-drop trigger eliminates the cross-writer leakage that motivated `writerScope`.

  ### Breaking changes — migration

  **Per-attribute callback → per-half object literal.** Take the most-restrictive per-attribute policy on each half:

  ```diff
   indexes: {
     byAlert: {
       name: "gsi1",
       pk: { field: "gsi1pk", composite: ["alertState"] },
       sk: { field: "gsi1sk", composite: ["deviceId"] },
  -    indexPolicy: () => ({ alertState: "sparse" }),
  +    indexPolicy: { pk: "sparse", sk: "preserve" },
     },
   }
  ```

  **`set({attr: null})` no longer cascades GSI drop.** Use `Entity.remove([attr])` for atomic remove + GSI cascade:

  ```diff
  - yield* db.entities.Devices.update(key).set({ alertState: null })
  + yield* db.entities.Devices.update(key).remove(["alertState"])
  ```

  **Update-payload type widening reverted.** v1.6 wrapped each update field in `Schema.NullishOr`. v1.7 reverts this; `set({ attr: null })` only compiles when the model declares the attr as nullable. Combined with EDD-9025, this closes the stale-GSI footgun at the type level — `set({composite: null})` no longer compiles.

  **EDD-9025 — composite attribute schemas can't include `null`.** Convert nullable composites to `Schema.optional(...)` (T | undefined; the sparse pattern):

  ```diff
   class Device extends Schema.Class<Device>("Device")({
     channel: Schema.String,
     deviceId: Schema.String,
  -  tenantId: Schema.NullOr(Schema.String),    // ← composite, EDD-9025 rejects
  +  tenantId: Schema.optional(Schema.String),  // ← T | undefined, allowed
   }) {}
  ```

  **Hierarchical PK truncation is now supported (additive).** If you relied on the v1.6 PK-drop behavior on `set({pkComposite: null})`, declare the PK half as `'sparse'` to keep that semantic. Otherwise, the new behavior — truncate to leading prefix — is the right default for multi-tenant fleet / multi-org-project shapes.

  **Hole detection is now policy-aware.** Under `'preserve'`, holes still throw `CompositeKeyHoleError` (EDD-9024). Under `'sparse'`, holes silently truncate (or drop when the leading prefix is empty). If your code relied on the v1.6 strict throw on a sparse half, restructure so holes can't form (e.g. by putting only one composite on each half).

  **Mixed sparse/preserve attrs in same half → not expressible.** Pick one per half. The half is a single concatenated string; per-attribute mixing within a half had no coherent runtime semantic anyway.

  See `DESIGN.md §7 Policy-Aware GSI Composition` and `guides/index-policy.mdx` for the full v3 model + worked examples.

## 1.6.0

### Minor Changes

- indexPolicy v2 — unified-hierarchy attribute model, three-way payload classification, hierarchical SK pruning, hole detection. Plus the SparseMap opt-in is renamed to a typed callable.

  **indexPolicy v2 — behavior changes (closes [#36](https://github.com/jmenga/effect-dynamodb/issues/36))**

  - The runtime now distinguishes three payload states per composite attribute: present-with-value, explicit clear (`null` or `undefined`), and omitted. `null` and `undefined` collapse — both signal "explicit clear, drop this composite from the key now" — and they cascade unconditionally regardless of policy.
  - Omission still defers to `indexPolicy` (`'sparse'` drops the GSI; `'preserve'` is a no-op).
  - Pre-1.6 collapsed omission and explicit `null`/`undefined`, with `'sparse'` firing on every update regardless of whether the caller touched that composite. **Audit any existing `set({ attr: null })` paths** — intent is now unambiguous (always cascades). Switch any `'sparse'` policies that aren't really membership-driving (hybrid-writer GSIs) to `'preserve'`. See the migration table in the [indexPolicy guide](https://github.com/jmenga/effect-dynamodb/blob/main/packages/docs/src/content/docs/guides/index-policy.mdx#migrating-from-150).

  **Hierarchical SK pruning — new opt-in feature**

  - When a _trailing_ SK composite is explicitly cleared with `'preserve'` policy, `gsiNsk` truncates to the leading prefix instead of dropping the GSI entirely. The item stays queryable at the parent (coarser) hierarchy depth — geographic, org, workflow, content classification, permission scope, order grouping. See DESIGN.md §7.6.

  **Hole detection — new write-time validation**

  - An SK composite cleared at position `i` with another SK composite at position `j > i` still present produces a syntactically invalid prefix that no `begins_with` query would match. The library now throws `CompositeKeyHoleError` (EDD-9024) at write time, naming the GSI, the cleared composite, and the offending trailing composite. Pre-existing latent bugs (silent broken keys) become loud failures.

  **SparseMap API rename — breaking change, low blast radius**

  - `storedAs: 'sparse'` (magic string) → `storedAs: DynamoModel.SparseMap()` (typed callable). The `prefix` option moves from a sibling on `ConfigureAttributes` into the `SparseMap({ prefix })` config object — where it semantically belongs.
  - 1.5.0 was the only release that used the magic string; consumer adoption is minimal. No backward-compat shim — mechanical rename.
  - Two motivations: (1) the magic-string `'sparse'` collided with the `indexPolicy` `'sparse'` value (opposite meanings), confusing consumers; (2) the callable form lets options live where they belong rather than as siblings only meaningful when paired with the right `storedAs` value.

  **Type-level changes**

  - `EntityUpdateType` now widens each field to `T | null | undefined` so consumers can express explicit clears through TypeScript without casting. The runtime already accepted them via `Schema.NullishOr` wrap.
  - `ConfigureAttributes.storedAs` becomes `Schema.Schema<A> | SparseMapConfig`. The `| 'sparse'` literal union is dropped.
  - `ConfigureAttributes.prefix` removed from top level.
  - New exports: `DynamoModel.SparseMap`, `DynamoModel.SparseMapConfig`, `DynamoModel.isSparseMapConfig`, `CompositeKeyHoleError`, `makeCompositeKeyHoleError`, `KeyComposer.composeSkPrefixUpTo`.

## 1.5.0

### Minor Changes

- 7a7e72f: Add **SparseMap** storage primitive (`storedAs: 'sparse'`) — flattened storage for logical `Record<K, V>` fields, with each map entry stored as an independently addressable top-level DynamoDB attribute named `<prefix>#<key>`.

  The headline win is per-bucket atomic counters on a fresh item without parent-map ceremony — `ADD totals#2026-01 :1` works as a single op on a row that has never been touched before. Concurrent writers to disjoint buckets never race.

  **API surface:**

  - `DynamoModel.configure(model, { field: { storedAs: 'sparse' } })` — opt in. Optional `prefix` override.
  - Reads transparent — `get` / `query` / `scan` / batch / streams rebuild the domain `Record<K, V>` from flattened attributes.
  - Record-style writes: `.set({ field: { ... } })` decomposes into one SET per bucket (whole-bucket replace; concurrent disjoint-bucket writes safe).
  - Path-style writes: `PathBuilder.entry(key)` plus `.pathAdd` / `.pathSet` for atomic per-leaf updates within a known bucket. Counter case (scalar buckets) needs no bucket ceremony.
  - `.removeEntries(field, keys)` — explicit per-key REMOVE (`null` in record-style is **not** REMOVE — too footgunny).
  - `.clearMap(field)` — Get-then-Update helper that folds REMOVE clauses into the same final UpdateItem as the rest of the builder's combinators. Atomic for `versioned: { retain: true }` entities; best-effort for non-versioned.
  - Conditional ops: `attribute_exists(<prefix>#<key>)` via the path API.

  **Lifecycle interactions:**

  - `versioned: { retain: true }` — snapshots preserve flattened attrs verbatim.
  - `softDelete` — sparse data is preserved across soft-delete and restore.
  - `timeSeries` — sparse fields are aggregate state, not event state. They live on the current item only and are **not** carried on event items (`#e#<orderBy>`).

  **Validation at `Entity.make()` (EDD-9020..9023):** sparse fields must be `Schema.Record`, must not be nested-sparse, must not participate in primary-key/GSI composites or unique constraints, must have distinct prefixes that don't collide with non-sparse field names. User-supplied keys must not contain `#` (validated at write time; no silent escaping).

  See `docs/guides/sparse-maps` for the full guide and `examples/guide-sparse-maps.ts` for a runnable program.

## 1.4.0

### Minor Changes

- fix(entity): correct codec direction so RedactedFromValue and other transform schemas round-trip ([#29](https://github.com/jmenga/effect-dynamodb/issues/29))

  The write paths (`put`, `create`, `update`, `upsert`, `append`, batch/transaction puts) now run `Schema.encode` end-to-end against the entity's input/update schema so any Effect Schema transform (e.g. `Schema.RedactedFromValue`, `Schema.NumberFromString`, `Schema.DateTimeUtcFromString`, custom `decodeTo` chains) round-trips cleanly. Previously the put path validated against the encoded form, which rejected domain instances like `Redacted.make(...)` with `Invalid data <redacted>`.

  Storage-format substitution: at `Entity.make()` time, self date schemas (`Schema.DateTimeUtc`, `Schema.DateTimeZoned`, `Schema.DateValid`) carrying a `DynamoEncoding` annotation are substituted with bidirectional date transforms whose wire format matches the legacy `serializeDateForDynamo` output byte-for-byte. `Schema.RedactedFromValue(...)` fields are substituted with a tolerant Redacted transform (Effect v4's `RedactedFromValue` forbids encoding by default).

  **Breaking change** (narrow): combining a transform schema with a `DynamoEncoding` storage override now raises a clear error at `Entity.make()` time, e.g.

  ```
  [effect-dynamodb] Field "createdAt": cannot apply DynamoEncoding storage override to a transform schema. Either declare a self schema (Schema.DateTimeUtc) and let the annotation drive storage, OR declare a transform and own the wire format — not both.
  ```

  Migrate by either declaring a self schema (`Schema.DateTimeUtc.pipe(DynamoModel.storedAs(...))`) or dropping the override.

  Closes [#29](https://github.com/jmenga/effect-dynamodb/issues/29).

## 1.3.3

### Patch Changes

- [#28](https://github.com/jmenga/effect-dynamodb/pull/28) [`520035b`](https://github.com/jmenga/effect-dynamodb/commit/520035b844e1c49b06bbaefdeba7d99e522b63b5) Thanks [@jmenga](https://github.com/jmenga)! - Fix: unique-constraint sentinels are now sparse — they are only written when every composing field is present on the record (mirrors GSI sparse semantics). Previously, `Entity.put` / `.create` and the related update / delete / restore / purge paths called `KeyComposer.serializeValue(undefined)`, which coerced missing values to the literal string `"undefined"` and synthesized a sentinel keyed on that string. The first record with the field unset succeeded; every subsequent record collided with a false `UniqueConstraintViolation` (issue [#25](https://github.com/jmenga/effect-dynamodb/issues/25)).

  The sparse rule applies symmetrically across all six sentinel sites: `put`/`create`, `update` rotation, hard-delete cleanup, soft-delete cleanup, `restore` re-establish, and `purge` cleanup. The update path now distinguishes four transition states — `undefined → undefined` (no-op), `undefined → defined` (Put only), `defined → undefined` (Delete only), and `defined → defined, changed` (Delete + Put) — instead of unconditionally rotating both sides.

  Migration: any deployment running 1.3.x with a unique constraint on an optional field may have phantom sentinel rows of the form `<entity>._unique.<name>#undefined`. The new code never reads or writes them, so they are harmless; clean them up with a one-time scan if desired.

## 1.3.2

### Patch Changes

- fix(entity): `decodeMarshalledItem` tolerates missing GSI key attributes on sparse-indexed items. `itemSchema` previously required every GSI pk/sk field as `Schema.String`, so decoding a DynamoDB Stream `NewImage` for an item whose GSI composites haven't been stamped yet (e.g. ingest-before-enrichment patterns) failed with `ValidationError: MissingKey`. GSI key fields are now `Schema.optional(Schema.String)` in `itemSchema`; primary pk/sk remain required. Closes [#16](https://github.com/jmenga/effect-dynamodb/issues/16).

## 1.3.1

### Patch Changes

- Fix: `Entity.update` retain path (`versioned: { retain: true }`) marshalled domain `DateTime.Utc` values as DynamoDB Maps, corrupting writes and breaking subsequent reads.

  **Regression introduced in 1.3.0.** The retain path built `newItem` by spreading `currentRaw` (storage primitives from DynamoDB) with `hydratedUpdates` (decoded via the new `fromSelf` variants, so date fields are domain `DateTime.Utc` instances), then called `toAttributeMap(newItem)` without a `serializeDateFields` pass. AWS SDK's `marshall` with `convertClassInstanceToMap: true` then stored the DateTime class as a Map:

  ```json
  "updatedAt": { "M": { "epochMilliseconds": { "N": "..." }, "~effect/time/DateTime": { "S": "..." }, "_tag": { "S": "Utc" } } }
  ```

  Subsequent reads failed with `deserializeDateFromDynamo: expected string for DateTime.Utc/string, got object`.

  **Fix:** Pre-serialize `hydratedUpdates` to storage primitives before merging into `newItem`, mirroring what the non-retain path already does. The system-field block reads from the serialized map, so user-supplied colliding `updatedAt` values also land as storage primitives (not Maps). Affects any entity with `versioned: { retain: true }` that has model-declared date fields or uses the collision-aware timestamp pattern from 1.3.0.

  Put, upsert, append, and non-retain update paths were already correct — only the retain path was missing the serialization step.

## 1.3.0

### Minor Changes

- Domain-value input decode, timestamp collision handling, and adaptive generation.

  **Fixes [#19](https://github.com/jmenga/effect-dynamodb/issues/19)** — `Entity.put/create/update/upsert` (and `Transaction`/`Batch` put paths) now correctly decode domain values. Previously, TypeScript said "pass me a `DateTime.Utc`" but the runtime decoded via a transform schema that expected an ISO string — callers who followed the TS contract hit a `ValidationError`. The runtime decode now uses `fromSelf` variants for date-annotated fields, matching the TS contract.

  **New: declare system-field-colliding timestamps in your model.** If your domain model declares `createdAt` / `updatedAt` with a date-compatible schema (e.g. `Schema.DateTimeUtcFromString`, `Schema.DateFromString`, `Schema.DateTimeUtc`), and `timestamps: true` is set:

  - The input type marks the colliding fields as optional — caller may omit them (library auto-generates) or supply their own value (user value wins, useful for imports/backfill).
  - The library-generated timestamp respects the model field's storage encoding, so declaring `createdAt: Schema.DateTimeUtcFromString.pipe(DynamoModel.storedAs(DynamoModel.DateEpochSeconds))` yields epoch-seconds storage even though the library is generating the value.
  - `createdAt` is treated as immutable in the update schema (stripped entirely).

  **New: user-owned non-date fields that collide with a system field name.** If your model declares e.g. `createdAt: Schema.String` (as a user-managed composite value, not a timestamp), the library detects the non-date collision and yields the field to the user — library timestamp management applies only to non-colliding fields (e.g. `updatedAt`). Preserves existing patterns that use `createdAt` as a plain string SK composite.

  **New: declare `version` in your domain model for read-side ergonomics.** `version: Schema.Number` alongside `versioned: true` is now allowed. The field is stripped from `inputSchema` / `createSchema` / `updateSchema` so callers cannot override via `put` / `create` / `.set()` — the library retains full control of the write path (auto-increment + `.expectedVersion()` optimistic locking). The declaration is purely for type-level visibility: `new MyEntity({ ..., version: 1 })` typechecks, reducers and round-tripping through Schema.Class work as expected. Manual version fixups remain available by dropping to the raw `DynamoClient` service.

  **Type ergonomics.** The exposed `inputSchema` / `createSchema` / `updateSchema` codec types (and the corresponding `Entity.put` / `create` / `update` call signatures) now flatten into plain object literals in hover tooltips instead of showing as wrapped generic aliases.

## 1.2.0

### Minor Changes

- Fluent bound-CRUD builders + per-GSI indexPolicy.

  **Fluent bound-CRUD builders (breaking)**

  `db.entities.X.{put,create,upsert,update,patch,delete,deleteIfExists}` now return fluent builders (`BoundPut`, `BoundUpdate`, `BoundDelete`) instead of accepting variadic `...combinators` rest args. Chain combinators as methods and yield the builder to execute.

  ```ts
  // Before
  yield* db.entities.Tasks.update(key, Entity.set({...}), Entity.expectedVersion(3))
  yield* db.entities.Tasks.put(input, Entity.condition({...}))
  yield* db.entities.Tasks.delete(key, Entity.condition({...}))

  // After
  yield* db.entities.Tasks.update(key).set({...}).expectedVersion(3)
  yield* db.entities.Tasks.put(input).condition({...})
  yield* db.entities.Tasks.delete(key).condition({...})
  ```

  Use `.asEffect()` before piping into Effect combinators (`Effect.catchTag`, `Effect.map`, etc.). Unbound `Entity.make(...)` return values and module-level combinators (`Entity.set`, `Entity.condition`, `Entity.expectedVersion`, `Entity.pathSet`, …) are unchanged — `Transaction.transactWrite(...)` and `Batch.write(...)` consumers keep working without modification. `PutCombinator`, `UpdateCombinator`, `DeleteCombinator` type aliases removed from the public API.

  **Per-GSI `indexPolicy` (breaking)**

  Adds `indexPolicy?: (item) => Partial<Record<attr, "sparse" | "preserve">>` to each GSI definition. Resolves the previously ambiguous "composite missing from update payload" signal into three explicit intents (sparse dropout, enrichment preserve, strict recompose). Default when unspecified: `"preserve"` for every composite.

  - `Entity.update()` and time-series `.append()` no longer throw `PartialGsiCompositeError` on partial composites — that class has been removed. Consumers relying on strict caller-error validation should declare `indexPolicy: () => ({ attr: "sparse" })` or enforce validation in their own update layer.
  - GSIs that declare an `indexPolicy` are always evaluated on every update — "attr not in payload" is treated as "attr not set" per the policy. GSIs without a policy keep the existing touched-gate semantics (only evaluated when a composite appears in the payload).
  - `put()` semantics unchanged (still sparse-by-default for any missing composite; `indexPolicy` not consulted).
  - `Entity.remove([attr])` cascade still drops the GSI entry regardless of policy.

  Closes [#11](https://github.com/jmenga/effect-dynamodb/issues/11).

  **Other**

  Fixes `Marshaller.toAttributeValue` to pass `convertClassInstanceToMap: true` and `removeUndefinedValues: true`, matching `toAttributeMap`. Without this, `update()`/`append()` SET clauses and condition/filter values threw at runtime when the value was a `Schema.Class` instance (or contained one). Fixes [#12](https://github.com/jmenga/effect-dynamodb/issues/12).

## 1.1.0

### Minor Changes

- feat(Entity): `timeSeries` primitive for event-driven workloads

  Adds a new `timeSeries` configuration primitive on `Entity.make()` for IoT-style workloads that need the split-item pattern: one "current" item per partition (latest state, index-visible) plus N immutable "event" items (TTL-bounded, time-queryable).

  - **New API:** `Entity.make({ timeSeries: { orderBy, ttl?, appendInput } })`
  - **`.append(input, condition?)`** — atomic `TransactWriteItems` (UpdateItem current + Put event) with CAS `attribute_not_exists(pk) OR #orderBy < :newOb`. Returns a discriminated union `{ applied: true | false, current }` — stale writes are a success value, not an error.
  - **`.history(key)`** — `BoundQuery` auto-scoped via `begins_with(<currentSk>#e#)`, with `.where()` restricted to the configured `orderBy` attribute.
  - **Enrichment preservation** — `.append()`'s `SET` clause covers only fields declared in `appendInput`. Model fields outside `appendInput` (e.g. background-assigned `accountId`) are never touched.
  - **Mutually exclusive** with `versioned` (`EDD-9012`) and `softDelete` (`EDD-9015`).
  - **`appendInput` is required** (`EDD-9016`) — forces the enrichment-preservation choice to be visible at the entity definition.

  Not source-breaking: the `Entity<...>` generic signature gains a new optional type parameter at the end (`TTimeSeries`, defaults to `undefined`). Existing code compiles unchanged. The semantic change is additive.

## 1.0.0

### Minor Changes

- [#4](https://github.com/jmenga/effect-dynamodb/pull/4) [`76654b7`](https://github.com/jmenga/effect-dynamodb/commit/76654b7a6d35a361fe74a2733bdfb1ce837504bf) Thanks [@jmenga](https://github.com/jmenga)! - Add `.primary()` query accessor on the bound client

  Every entity now exposes a `.primary(...)` accessor on `db.entities.*` alongside the existing GSI accessors. The primary index is treated symmetrically with GSIs: pass required PK composites (and optionally one or more SK composites) to get back a `BoundQuery` with the full combinator surface (`.where()`, `.filter()`, `.select()`, `.limit()`, `.reverse()`, `.startFrom()`, `.consistentRead()`, `.collect()`, `.fetch()`, `.paginate()`, `.count()`).

  Previously the primary index was deliberately excluded from accessor generation, so the shared-PK join-table pattern (many items under one partition key, distinguished by SK) had no first-class typed query path — only `.get(fullKey)` or a raw `Query.make` escape hatch.

  ```ts
  // List every membership in an organization — PK only, SK composites omitted
  const allMembers =
    yield *
    db.entities.Memberships.primary({
      orgId: "org-acme",
    }).collect();

  // Narrow by partial SK composite (begins_with prefix match)
  const bobs =
    yield *
    db.entities.Memberships.primary({
      orgId: "org-acme",
      userId: "u-bob",
    }).collect();
  ```

  `.get(fullKey)` remains the dedicated `GetItem` path for single-item strongly-consistent reads. Resolves [#2](https://github.com/jmenga/effect-dynamodb/issues/2).
