/**
 * @internal Shared helpers for Batch and Transaction operations.
 *
 * Extracts common patterns: table name resolution, primary key composition,
 * and put-item construction (validation + key composition + system fields).
 */

import type { DynamoEncoding } from "@effect-dynamodb/schema/DynamoModel.js"
import { ValidationError } from "@effect-dynamodb/schema/Errors.js"
import { makeDefaultCrypto } from "@effect-dynamodb/schema/internal/DefaultCrypto.js"
import * as KeyComposer from "@effect-dynamodb/schema/KeyComposer.js"
import { DateTime, Effect, Schema } from "effect"
import type { Entity } from "../Entity.js"
import { toAttributeMap } from "../Marshaller.js"
import type { ReturnValuesMode, UpdateState } from "./EntityOps.js"

/**
 * @internal Hidden per-incarnation token of a versioned entity (#133): set
 * when an item is created, it tells an item apart from a deleted-and-recreated
 * one at the same version. Named like
 * `__edd_e__` so it cannot collide with a model field; never decoded.
 */
export const INCARNATION_TOKEN = "__edd_i__"

/**
 * @internal Hidden string set naming the unique-constraint fields an item
 * holds a DEFAULT for (#133): a decoding-default field that is also an index
 * composite is stored when omitted, but a default never creates a unique
 * sentinel — so no sentinel is composed, rotated or deleted for a field
 * listed here until a write supplies a value for it.
 */
export const UNSENTINELED_DEFAULTS = "__edd_d__"

const incarnationCrypto = makeDefaultCrypto()

/** @internal A fresh incarnation token. */
export const freshIncarnationToken: Effect.Effect<string> = Effect.orDie(
  incarnationCrypto.randomUUIDv4,
)

/**
 * Generate a wire-form timestamp value for the configured encoding from a
 * Clock-backed `now: DateTime.Utc` (resolved by the caller via `yield*
 * DateTime.now`). No-encoding default: ISO string. Custom encoding: serialized
 * primitive. Contains no `Date` constructor, so it is deterministic under
 * `TestClock`.
 *
 * Shared by the Entity write path (`Entity.generateTimestamp`) and the
 * Batch/Transaction path so both produce identical timestamps.
 */
export const generateTimestampPrimitive = (
  now: DateTime.Utc,
  encoding: DynamoEncoding | null,
): string | number => {
  if (!encoding) return DateTime.formatIso(now)
  const ms = DateTime.toEpochMillis(now)
  switch (encoding.storage) {
    case "string":
      // System timestamps generated here are UTC; DateTime.Zoned-typed
      // collision fields are rare and were not previously special-cased.
      return DateTime.formatIso(now)
    case "epochMs":
      return ms
    case "epochSeconds":
      return Math.floor(ms / 1000)
  }
}

/**
 * Message for a read path (`Batch.get`, `Transaction.transactGet`,
 * `Transaction.check`) handed something that is not a get descriptor.
 *
 * Both accepted spellings are named, because the bound one is the ONLY
 * descriptor an entity authored with the pure `@effect-dynamodb/schema`
 * `Entity.make` can produce (#108) — a caller who reached here with a pure
 * definition needs to be pointed at the client, not at `Entity.get`.
 */
export const getRejectReason = (operation: string): string =>
  `[EDD-9052] ${operation} requires a get descriptor — pass \`Entity.get(key)\` or the ` +
  "bound `db.entities.X.get(key)`. An already-executed Effect, a query, or a write op " +
  "carries no key to read."

/** Render the configured multi-item features for an error message. */
const describeFeatures = (features: ReadonlyArray<"unique" | "retain" | "softDelete">): string => {
  const labels = features.map((f) =>
    f === "unique" ? "`unique`" : f === "retain" ? "`versioned: { retain: true }`" : "`softDelete`",
  )
  return labels.length === 1
    ? labels[0]!
    : `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}`
}

/**
 * `Batch.write` cannot host the multi-item lifecycle features that apply to the
 * direction being written (EDD-9049). This is a different judgement from the
 * transact path's: there, a `put`'s side items are derivable and get emitted;
 * here `BatchWriteItem` structurally cannot express them at all.
 *
 *   - **No `ConditionExpression`.** A uniqueness sentinel is only a constraint
 *     because of `attribute_not_exists(pk)`. Writing one without the guard would
 *     overwrite another row's reservation and enforce nothing — strictly worse
 *     than writing none, because the table would then *look* guarded.
 *   - **No atomicity.** A sentinel or snapshot that lands without its item (or
 *     an item without them) is a corrupt partition, and `Batch.write` chunks at
 *     25, so related items can even land in different requests.
 *   - **No `UpdateRequest`.** A soft-delete tombstone is a relocation
 *     (delete + put at a new sort key); a batch cannot make that one unit.
 *
 * **Direction matters.** `softDelete` changes only the delete path — a `put` of
 * a soft-deletable entity is an ordinary single-item put and is allowed. Gating
 * it on the put side would reject writes that have always been correct.
 *
 * Returns `undefined` when the entity is safe for `Batch.write` in `opType`.
 */
export const batchRejectReason = (entity: Entity, opType: "put" | "delete"): string | undefined => {
  const features = entity._multiItemWriteFeatures.filter((f) =>
    // `unique` and `retain` add items to BOTH directions (sentinel write /
    // release, snapshot on create / on overwrite-and-delete). `softDelete` only
    // ever changes a delete.
    f === "softDelete" ? opType === "delete" : true,
  )
  if (features.length === 0) return undefined
  return (
    `[EDD-9049] Batch.write cannot ${opType} an entity configured with ${describeFeatures(features)} — ` +
    "BatchWriteItem has no ConditionExpression (which is the whole of a uniqueness " +
    "sentinel's correctness), no UpdateRequest, and no atomicity across its 25-item " +
    "chunks. Use Transaction.transactWrite, or the entity's own operation."
  )
}

/**
 * Reject an op whose semantics the `TransactWriteItems` / `BatchWriteItem`
 * compile path cannot faithfully reproduce.
 *
 * Both paths turn an entity op into a single self-contained `Put` / `Delete`
 * built from the encoded input. That is exactly right for a plain `put` /
 * `delete`, and wrong — silently — for anything whose contract needs a
 * different DynamoDB verb, extra items, or a service the compile step does not
 * have. Loud beats silent (#100).
 *
 * **What this gate covers.** Cases that are silently wrong (`upsert` — see
 * `PutKind`) or that cannot be working for anyone today: a `refs` entity writes
 * an item whose ref attribute is absent, so every later read fails to decode; a
 * `generatedId` entity whose id was NOT supplied dies outright (no `Crypto` in
 * scope) — one supplied on the input is written as given, since that is the case
 * `Entity.put` also handles without `Crypto` (#120); a vector-indexed entity
 * writes an item with no embedding, so it silently drops out of the index.
 *
 * **The multi-item lifecycle features split by direction (#113).** `unique`,
 * `versioned: { retain }` and `softDelete` all need MORE than one item per write:
 *
 * - **put** — expanded. A put of a versioned or unique-constrained entity is
 *   planned exactly as `Entity.put` plans it (#133): the item is read, and the
 *   Put is guarded on what was read, beside its sentinel reservations and
 *   (owned) releases and its retain snapshot (`Entity._planPut`).
 * - **delete** — the sentinel to release is keyed by the *stored* item's unique
 *   values, a retain snapshot copies the *stored* row, and a soft-delete
 *   tombstone IS the stored row relocated to a new sort key. `Transaction.
 *   transactWrite` plans such a delete from a read (`Entity._planDelete`);
 *   a path that builds a delete from its key alone (`EventStore.append`)
 *   rejects it with **EDD-9048**.
 *
 * `Batch.write` rejects BOTH directions (**EDD-9049**) — see `batchRejectReason`.
 *
 * **What a planned update or delete cannot carry** (`Transaction.transactWrite`
 * runs the op with its write recorded, so nothing after the write runs): a
 * cascade (**EDD-9059** — a follow-up write after the update commits), a
 * `returnValues` mode that returns an item (**EDD-9060** — a transaction
 * returns no item attributes), and an update of a vector-indexed entity
 * (**EDD-9061** — the embedding needs the `Embedder` service).
 *
 * `capability` names what the caller would have to give up, so the message can
 * say why rather than just "unsupported".
 */
export const rejectUnsupportedOp = (
  entity: Entity,
  operation: string,
  opType: "put" | "update" | "delete",
  putKind: "put" | "create" | "upsert" | undefined,
  /**
   * The op's own input, for the one gate whose dependency the caller can
   * remove. `undefined` for a delete (which carries a key, not an input) and
   * for any caller with none to offer — both read as "the field is absent".
   */
  input?: unknown,
  details?: {
    /** An update's accumulated state (`.cascade()`, `.returnValues()`, …). */
    readonly updateState?: UpdateState | undefined
    /** A delete's `.returnValues()`. */
    readonly returnValues?: ReturnValuesMode | undefined
    /**
     * The caller reads the stored row before compiling (`transactWrite`'s
     * pre-pass), so a multi-item delete is buildable and EDD-9048 does not apply.
     */
    readonly readsStoredRow?: boolean | undefined
  },
): Effect.Effect<void, ValidationError> => {
  const fail = (capability: string, reason: string) =>
    new ValidationError({
      entityType: entity.entityType,
      operation,
      cause: `${operation}: ${capability} is not supported here — ${reason}`,
    })

  // --- op-kind level -------------------------------------------------------
  if (opType === "put" && putKind === "upsert") {
    return Effect.fail(
      fail(
        "upsert",
        "upsert is an UpdateItem whose SET clause uses if_not_exists for createdAt, " +
          "immutable fields and the version counter. Compiling it as a Put would reset " +
          "them. Use put() or create() here, or run the upsert as its own operation.",
      ),
    )
  }

  // --- what a planned write cannot carry (EDD-9059 – EDD-9061) -------------
  if (opType === "update" && details?.updateState?.cascade !== undefined) {
    return Effect.fail(
      fail(
        "[EDD-9059] cascade",
        "a cascade is a follow-up write to other entities after the update commits, so it " +
          "cannot share the transaction. Run the update as its own operation.",
      ),
    )
  }
  const returnValues =
    opType === "update" ? details?.updateState?.returnValues : details?.returnValues
  if (opType !== "put" && returnValues !== undefined && returnValues !== "none") {
    return Effect.fail(
      fail(
        "[EDD-9060] returnValues",
        `a transaction returns no item attributes. Run the ${opType} as its own operation ` +
          "to read them.",
      ),
    )
  }
  if (opType === "update" && Object.keys(entity._vectorIndexes ?? {}).length > 0) {
    return Effect.fail(
      fail(
        "[EDD-9061] updating an entity with vector indexes",
        "recomputing the embedding calls the Embedder service, which this path does not " +
          "provide. Run the update as its own operation.",
      ),
    )
  }

  // --- multi-item lifecycle, delete direction (EDD-9048) -------------------
  // The put direction is EXPANDED instead — see the doc comment.
  if (
    opType === "delete" &&
    details?.readsStoredRow !== true &&
    entity._multiItemWriteFeatures.length > 0
  ) {
    return Effect.fail(
      fail(
        `[EDD-9048] deleting an entity configured with ${describeFeatures(entity._multiItemWriteFeatures)}`,
        "the extra items a delete must write are derived from the STORED item — the sentinel " +
          "to release is keyed by its unique values, a retain snapshot copies it, and a " +
          "soft-delete tombstone is that row relocated to a new sort key. This path builds a " +
          "delete from its key alone, so it cannot build them. Run the delete as its own operation " +
          "(db.entities.X.delete(...)) or through Transaction.transactWrite, both of which " +
          "read the item first.",
      ),
    )
  }

  // --- entity-configuration level -----------------------------------------
  if (opType === "put" && entity._resolvedRefs.length > 0) {
    return Effect.fail(
      fail(
        "a ref",
        "write-time ref hydration reads the referenced entity, which this compile step " +
          "cannot do; the ref attribute would be written empty.",
      ),
    )
  }
  // Unlike the two gates around it, this one depends on the INPUT rather than
  // the configuration. `Entity.put` only reaches `Crypto` when the caller
  // omitted the field — `fillGeneratedId` returns the input untouched when it
  // is present — so a caller who supplies the id needs nothing this path lacks.
  // Rejecting them named a dependency that did not apply, and pointed at a
  // workaround that was already in effect (#120).
  //
  // The absent case still has to be refused: this path builds the item straight
  // from the encoded input and never calls `fillGeneratedId`, so the id would
  // stay missing and the primary key would compose around an `undefined`.
  if (opType === "put" && entity.generatedId != null) {
    const idField = (entity.generatedId as { readonly field: string }).field
    const supplied =
      typeof input === "object" &&
      input !== null &&
      (input as globalThis.Record<string, unknown>)[idField] != null
    if (!supplied) {
      return Effect.fail(
        fail(
          `an omitted generated id ("${idField}")`,
          "generating one needs the Crypto service, which is not in scope here. Supply " +
            `"${idField}" on the input and this path writes it as given — only the ` +
            "generating case is unsupported.",
        ),
      )
    }
  }
  if (opType === "put" && Object.keys(entity._vectorIndexes ?? {}).length > 0) {
    return Effect.fail(
      fail(
        "a vector index",
        "computing the embedding needs the Embedder service, which is not in scope here; " +
          "the item would be written without its vector and drop out of the index.",
      ),
    )
  }
  return Effect.void
}

/**
 * Resolve table names for a set of entity infos, deduplicating by entity reference.
 */
export const resolveTableNames = (infos: ReadonlyArray<{ readonly entity: Entity }>) =>
  Effect.gen(function* () {
    const tableNames = new Map<Entity, string>()
    for (const info of infos) {
      if (!tableNames.has(info.entity)) {
        const { name } = yield* info.entity._tableTag
        tableNames.set(info.entity, name)
      }
    }
    return tableNames
  })

/**
 * Compose the primary key for an entity item.
 */
export const composePrimaryKey = (
  entity: Entity,
  key: Record<string, unknown>,
): Record<string, unknown> => {
  const primary = entity.indexes.primary!
  const schema = entity._schema
  // The caller's key is a DOMAIN record (`{ takenAt: DateTime }`), and every
  // `KeyComposer` input must first go through the composite key-form rule or it
  // composes a different string than `Entity.put` wrote — a `DateEpochMs` PK
  // composite made `Batch.get` return null and `Batch.write(delete)` report
  // success against a row it never touched (#111).
  const recordKeyForm = entity._keyForm(key)
  return {
    [primary.pk.field]: KeyComposer.composePk(schema, entity.entityType, primary, recordKeyForm),
    [primary.sk.field]: KeyComposer.composeSk(schema, entity.entityType, 1, primary, recordKeyForm),
  }
}

/**
 * Validate input, compose all keys, and build a marshalled put item.
 *
 * Encodes the user-supplied domain payload to wire format via
 * `Schema.encode(inputSchema)`, then assembles the DynamoDB item with system
 * fields and composite keys. Substituted self-date schemas + RedactedFromValue
 * are handled in the encode pass — no per-field serialization needed.
 *
 * Returns the unmarshalled `item` alongside the marshalled one, plus the `now`
 * the timestamps were generated from, and whether the input supplied
 * `createdAt`. `Entity._planPut` needs them to derive uniqueness sentinels and
 * the retain snapshot from the same values that were written (#113), and to
 * keep a replaced item's `createdAt` (#133); re-deriving `now` there would risk
 * a skew between an item's `createdAt` and its snapshot's TTL.
 */
export const validateAndBuildPutItem = (
  entity: Entity,
  input: Record<string, unknown>,
  operation: string,
): Effect.Effect<
  {
    readonly item: Record<string, unknown>
    readonly marshalled: Record<string, import("@aws-sdk/client-dynamodb").AttributeValue>
    readonly now: DateTime.Utc
    /** Whether the input supplied `createdAt` (a replacing put keeps the stored one otherwise). */
    readonly createdAtSupplied: boolean
  },
  ValidationError
> =>
  Effect.gen(function* () {
    // Clock-backed time source resolved once per op; threaded into the sync
    // timestamp builder so the value is deterministic under `TestClock`.
    const now = yield* DateTime.now
    const inputSchema = entity.schemas.inputSchema as Schema.Codec<any>
    // Encode → fall back to decode-then-encode (mirrors Entity.put).
    // Omitted decoding defaults are stored, exactly as `Entity.put` does.
    const filled = yield* entity._fillDecodingDefaults(input)
    const unsentineled = entity._unsentineledDefaults(input)
    const encoded = yield* Schema.encodeUnknownEffect(inputSchema)(filled).pipe(
      Effect.catch(() =>
        Schema.decodeUnknownEffect(inputSchema)(filled).pipe(
          Effect.flatMap((decoded) => Schema.encodeUnknownEffect(inputSchema)(decoded)),
        ),
      ),
      Effect.mapError(
        (cause) =>
          new ValidationError({
            entityType: entity.entityType,
            operation,
            cause,
          }),
      ),
    )

    const item: Record<string, unknown> = { ...(encoded as Record<string, unknown>) }
    item.__edd_e__ = entity.entityType
    if (unsentineled.length > 0) item[UNSENTINELED_DEFAULTS] = new Set(unsentineled)

    // Same normalisation `Entity.put` applies. Without it a `BigIntFromString`
    // composite composed `txn_420` here and `txn_000…0420` there, so the two
    // APIs wrote two different rows for the same logical item and neither
    // accessor could read the transact-written one (#111).
    const keys = KeyComposer.composeAllKeys(
      entity._schema,
      entity.entityType,
      1,
      entity.indexes,
      entity._keyForm(encoded as Record<string, unknown>),
    )
    Object.assign(item, keys)

    // System fields (collision-aware). When a timestamp field collides with a
    // model-declared field, the user may have supplied their own value
    // (already encoded to wire by `Schema.encode`); else generate a wire
    // primitive directly.
    const sf = entity.systemFields
    const createdAtSupplied = sf.createdAt !== null && item[sf.createdAt] !== undefined
    if (sf.createdAt) {
      if (item[sf.createdAt] === undefined) {
        item[sf.createdAt] = generateTimestampPrimitive(now, sf.createdAtEncoding)
      }
    }
    if (sf.updatedAt) {
      if (item[sf.updatedAt] === undefined) {
        item[sf.updatedAt] = generateTimestampPrimitive(now, sf.updatedAtEncoding)
      }
    }
    if (sf.version) item[sf.version] = 1
    if (entity._incarnationToken) item[INCARNATION_TOKEN] = yield* freshIncarnationToken

    // Rename domain fields to their stored attribute names, in the same
    // position `Entity.put` does (after keys + system fields, before sparse
    // flattening). Omitting it gave a `field:`-renamed entity a differently
    // shaped item depending on whether `put` or `transactWrite` wrote it — and
    // `_planPut` derives the retain snapshot from this same item,
    // so the snapshot inherited the wrong shape too (#111).
    entity._renameToDynamo(item)

    // Flatten sparse-map fields into per-entry top-level attributes. Throws
    // on invalid keys; surface as ValidationError at the entity boundary.
    try {
      entity._serializeSparseFields(item)
    } catch (e) {
      return yield* new ValidationError({
        entityType: entity.entityType,
        operation,
        cause: e instanceof Error ? e.message : String(e),
      })
    }

    return { item, marshalled: toAttributeMap(item), now, createdAtSupplied }
  })
