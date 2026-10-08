/**
 * Entity — Binds a domain model to a Table with ElectroDB-style index definitions,
 * system field configuration, unique constraints, and CRUD operations.
 *
 * Entity.make() returns a definition + operations namespace. The Entity object
 * carries derived schemas for 7 type extractors (Model, Record, Input, Update,
 * Key, Item, Marshalled).
 */

import type {
  AttributeValue,
  DeleteItemCommandInput,
  TransactWriteItem,
} from "@aws-sdk/client-dynamodb"
import {
  type ConfiguredModel,
  type DynamoEncoding,
  type ExtractIdentifier,
  getIdentifierField,
  getSparseFields,
  isConfiguredModel,
  isRefField,
} from "@effect-dynamodb/schema/DynamoModel.js"
import * as DynamoSchema from "@effect-dynamodb/schema/DynamoSchema.js"
import type { EmbedderService } from "@effect-dynamodb/schema/Embedder.js"
import { Embedder } from "@effect-dynamodb/schema/Embedder.js"
import {
  CascadePartialFailure,
  ConcurrentModification,
  ConditionalCheckFailed,
  DeleteAppliedButUnreadable,
  EmbeddingError,
  ItemNotDeleted,
  ItemNotFound,
  isAwsConditionalCheckFailed,
  isAwsTransactionCancelled,
  OptimisticLockError,
  RefNotFound,
  StaleAppend,
  TransactionOverflow,
  UniqueConstraintViolation,
  UpdateAppliedButUnreadable,
  ValidationError,
} from "@effect-dynamodb/schema/Errors.js"
import {
  makeCompositeKeyForm,
  toCompositeKeyRecord,
} from "@effect-dynamodb/schema/internal/CompositeCodec.js"
import { makeDefaultCrypto } from "@effect-dynamodb/schema/internal/DefaultCrypto.js"
import type { GsiConfig, IndexDefinition, KeyPart } from "@effect-dynamodb/schema/KeyComposer.js"
import * as KeyComposer from "@effect-dynamodb/schema/KeyComposer.js"
import { normalizeGsiConfig } from "@effect-dynamodb/schema/KeyComposer.js"
import type {
  VectorIndexConfig,
  VectorIndexDefinition,
} from "@effect-dynamodb/schema/VectorIndex.js"
import { deriveSourceText } from "@effect-dynamodb/schema/VectorIndex.js"
import {
  Cause,
  Context,
  Crypto,
  Data,
  DateTime,
  type Duration,
  Effect,
  Equal,
  Option,
  Schema,
  Stream,
} from "effect"
import { DynamoClient, type DynamoClientError } from "./DynamoClient.js"
import type { ConditionInput } from "./Expression.js"
import {
  isBoundOp,
  makeBoundAppend,
  makeBoundDelete,
  makeBoundGet,
  makeBoundPut,
  makeBoundUpdate,
} from "./internal/BoundCrud.js"
import {
  type BoundQueryConfig,
  BoundQueryImpl,
  entityNaming,
  type RawSortKeyCondition,
} from "./internal/BoundQuery.js"
import {
  compileExpr,
  createConditionOps,
  type Expr,
  emptyPartProblem,
  isExpr,
  nonEmptyCondition,
  parseShorthand,
  parseSimpleShorthand,
  toExpr,
} from "./internal/Expr.js"
import { compilePath, createPathBuilder } from "./internal/PathBuilder.js"
import {
  freshIncarnationToken,
  generateTimestampPrimitive,
  INCARNATION_TOKEN,
  UNSENTINELED_DEFAULTS,
} from "./internal/TransactableOps.js"
import { planWrite, type TransactPlan } from "./internal/TransactPlan.js"
import {
  decodeSparseFields,
  encodeSparseFields,
  fromAttributeMap,
  toAttributeMap,
  toAttributeValue,
} from "./Marshaller.js"
import * as Query from "./Query.js"
import { filterExpr, selectPaths } from "./Query.js"
import { resolveTtlAttributeName, type TableConfig } from "./Table.js"

// Internal modules (decomposed from Entity.ts)
export type {
  CascadeIndexConfig,
  GeneratedIdConfig,
  RefValue,
  SoftDeleteConfig,
  TimeSeriesConfig,
  TimestampFieldConfig,
  TimestampsConfig,
  UniqueConfig,
  UniqueConstraintDef,
  UniqueFieldsDef,
  VersionedConfig,
} from "@effect-dynamodb/schema/internal/EntityConfig.js"
export {
  type CascadeConfig,
  type CascadeTarget,
  type DecodeMode,
  type EntityBase,
  type EntityDelete,
  EntityDeleteImpl,
  EntityDeleteTypeId,
  type EntityGet,
  EntityGetImpl,
  type EntityGetOpts,
  type EntityOp,
  EntityOpTypeId,
  type EntityPut,
  EntityPutImpl,
  type EntityPutOpts,
  type EntityUpdate,
  EntityUpdateImpl,
  EntityUpdateTypeId,
  emptyUpdateState,
  type PutKind,
  type ReturnValuesMode,
  returnValuesMap,
  type UpdateReturn,
  type UpdateState,
  type WithVectors,
} from "./internal/EntityOps.js"

import * as Projection from "@effect-dynamodb/schema/Projection.js"
import {
  type CascadeConfig,
  type CascadeTarget,
  type DecodeMode,
  type EntityDelete,
  EntityDeleteImpl,
  EntityDeleteTypeId,
  type EntityGet,
  EntityGetImpl,
  type EntityGetOpts,
  EntityOpTypeId,
  type EntityPut,
  EntityPutImpl,
  type EntityPutOpts,
  type EntityUpdate,
  EntityUpdateImpl,
  emptyUpdateState,
  type PutKind,
  type ReturnValuesMode,
  type UpdateState,
  withGuard,
} from "./internal/EntityOps.js"

export {
  add,
  append,
  asItem,
  asModel,
  asNative,
  asRecord,
  type ConditionPipeable,
  cascade,
  clearMap,
  consistentRead,
  deleteFromSet,
  expectedVersion,
  pathAdd,
  pathAppend,
  pathDelete,
  pathIfNotExists,
  pathPrepend,
  pathRemove,
  pathSet,
  pathSubtract,
  project,
  remove,
  removeEntries,
  returnValues,
  set,
  subtract,
} from "./internal/EntityCombinators.js"

import type {
  GeneratedIdConfig,
  SoftDeleteConfig,
  TimeSeriesConfig,
  TimestampsConfig,
  UniqueConfig,
  UniqueConstraintDef,
  VersionedConfig,
} from "@effect-dynamodb/schema/internal/EntityConfig.js"
import {
  allCompositeAttributes,
  allKeyFieldNames,
  type DerivedSchemas,
  getSchemaFields,
  makePathValueEncoder,
  primaryKeyComposites,
  type ResolvedSystemFields,
  resolveUniqueFields,
} from "@effect-dynamodb/schema/internal/EntitySchemas.js"
import type {
  AppendInputType,
  AppendSuccess as AppendSuccessType,
  EntityInputType,
  EntityKeyType,
  EntityRecordType,
  EntityRefCreateType,
  EntityRefInputType,
  EntityRefUpdateType,
  EntityUpdateType,
  IndexPkInput,
  ModelType,
  PrimaryKeyComposites,
  RefErrors,
  VectorErrors,
  WithGeneratedId,
} from "@effect-dynamodb/schema/internal/EntityTypes.js"
import {
  asModel,
  type ConditionPipeable,
  condition as conditionCombinator,
  expectedVersion,
  set,
} from "./internal/EntityCombinators.js"

// ---------------------------------------------------------------------------
// Re-export KeyComposer types for convenience
// ---------------------------------------------------------------------------

export type { IndexDefinition, KeyPart }

// ---------------------------------------------------------------------------
// Sparse-aware unique sentinel composition
// ---------------------------------------------------------------------------

/**
 * Compose a unique-constraint sentinel key, returning `undefined` when any
 * composing field is missing (`undefined` or `null`) on the source record.
 *
 * Mirrors GSI sparse semantics (`tryComposeIndexKeys`): a sentinel is only
 * written/deleted/checked when every composing field is present, so multiple
 * records can coexist with the field unset and an entity write doesn't synthesize
 * a literal `"undefined"` collision key.
 *
 * `source` is read by **domain** field name — a constraint declares domain
 * fields, so a row that came off the wire (keyed by stored attribute name) MUST
 * be passed through `toDomainView` first. Reading a renamed field off a raw row
 * yields `undefined`, which the sparse rule then reads as "constraint unset" and
 * silently skips the sentinel — the delete/purge/restore leak behind #127.
 */
/**
 * TTL helpers — pure implementations live in `@effect-dynamodb/schema/Entity`,
 * imported here for local use on the write path. `normalizeTtlSeconds` is
 * re-exported below so the public `Entity` namespace surface is unchanged.
 */
import {
  type AnyRefValue,
  buildEntityDefinition,
  type EntityDefinition,
  type EntityDefinitionConfig,
  type EntityDefinitionData,
  normalizeTtlSeconds,
  resolveUniqueTtl,
} from "@effect-dynamodb/schema/Entity.js"

export { normalizeTtlSeconds }

const composeUniqueSentinel = (
  schema: DynamoSchema.DynamoSchema,
  entityType: string,
  constraintName: string,
  constraintDef: UniqueConstraintDef,
  source: globalThis.Record<string, unknown>,
):
  | {
      readonly key: { readonly pk: string; readonly sk: string }
      readonly fieldsRecord: globalThis.Record<string, string>
    }
  | undefined => {
  const fields = resolveUniqueFields(constraintDef)
  // A field holding only a default has no sentinel (#133).
  const defaulted = source[UNSENTINELED_DEFAULTS]
  const defaultedFields =
    defaulted instanceof Set
      ? (defaulted as ReadonlySet<unknown>)
      : new Set(Array.isArray(defaulted) ? defaulted : [])
  if (fields.some((f) => defaultedFields.has(f))) return undefined
  const serialized: Array<string> = []
  const fieldsRecord: globalThis.Record<string, string> = {}
  for (const f of fields) {
    const raw = source[f]
    if (raw === undefined || raw === null) return undefined
    const s = KeyComposer.serializeValue(raw)
    serialized.push(s)
    fieldsRecord[f] = s
  }
  return {
    key: DynamoSchema.composeUniqueKey(schema, entityType, constraintName, serialized),
    fieldsRecord,
  }
}

// ---------------------------------------------------------------------------
// Encode-or-decode-encode helper
// ---------------------------------------------------------------------------

/**
 * Validate user input and produce wire-form output, accepting both Type and
 * Encoded shapes. Strategy:
 *
 *   1. Try `Schema.encode(input)`. This is the canonical Type → Encoded
 *      conversion and works when the user passes Type values for transforms
 *      (e.g. a `DateTime.Utc` for `Schema.DateTimeUtcFromString`, a `number`
 *      for `Schema.NumberFromString`, a `Redacted<string>` for our
 *      RedactedFromValue substitute).
 *
 *   2. On encode failure, fall back to `Schema.decode → Schema.encode`. This
 *      lifts plain inputs to the Type shape first (`Schema.Class` decode is
 *      forgiving — it constructs a class instance from a plain object), then
 *      re-encodes back to wire form. Required for nested `Schema.Class`
 *      fields whose Type is a class instance but whose Encoded is a plain
 *      object — users typically construct the plain object directly.
 *
 * Returns the Encoded shape (a plain object whose values are wire
 * primitives), ready for `toAttributeMap`.
 *
 * @internal
 */
/**
 * @internal Build a key-form normaliser for an arbitrary entity-like value.
 *
 * `makeImpl` closes over its own `keyForm`; this is the escape hatch for the
 * two places that compose against a DIFFERENT entity's index (`cascade`) or run
 * outside `makeImpl` entirely (`bind`'s `reembed` / `history`). Same rule, same
 * `CompositeCodec` — just resolved per target.
 */
const keyFormFor = (
  target: unknown,
  record: globalThis.Record<string, unknown>,
): globalThis.Record<string, unknown> => {
  const t = target as {
    readonly model?: Schema.Top | undefined
    readonly schemas?: { readonly inputSchema?: Schema.Top | undefined } | undefined
  }
  const source = t.schemas?.inputSchema ?? t.model
  if (source === undefined) return record
  const form = makeCompositeKeyForm(source, (attr, value) => {
    throw new Error(
      `[EDD-9050] Composite "${attr}" could not be put into its key form: ` +
        `${JSON.stringify(String(value))} resolves under neither encode nor decode->encode.`,
    )
  })
  return toCompositeKeyRecord(form, record)
}

/**
 * @internal A plain update of a complete item found no item: the caller
 * creates it instead (`updateOrCreate`). Never escapes the update operation.
 */
class MissingForCreate extends Data.TaggedError("MissingForCreate")<{
  readonly input: unknown
}> {}

/** DynamoDB's limits on one expression: its length, and its operators + functions. */
const EXPRESSION_LIMIT = 4096
const OPERATOR_LIMIT = 300

/**
 * The tokens of a compiled expression. Names and values are placeholders
 * (`#…`, `:…`), so no operator can hide inside one.
 */
const EXPRESSION_TOKEN = /#\w+|:\w+|<>|<=|>=|[=<>()+\-,.[\]]|[A-Za-z_]\w*|\d+/g

/**
 * The operators DynamoDB counts against an expression's 300 (#133), measured
 * against DynamoDB: in a condition, each comparison (`=`, `<>`, `<`, `<=`,
 * `>`, `>=`), `AND` / `OR` / `NOT`, `BETWEEN` (whose own `AND` is part of it,
 * not another operator), `IN`, and each function; in an update, each `+` /
 * `-` and each function (`if_not_exists`, `list_append`) — a `SET` clause's
 * `=` is not an operator.
 */
/** @internal */
export const countOperators = (expression: string, kind: "condition" | "update"): number => {
  const tokens = expression.match(EXPRESSION_TOKEN) ?? []
  let count = 0
  let betweens = 0
  for (const [i, token] of tokens.entries()) {
    const word = token.toUpperCase()
    const isFunction = /^[A-Za-z_]\w*$/.test(token) && tokens[i + 1] === "("
    if (kind === "update") {
      if (token === "+" || token === "-" || isFunction) count++
      continue
    }
    if (isFunction) count++
    else if (["=", "<>", "<", "<=", ">", ">="].includes(token)) count++
    else if (word === "BETWEEN") {
      count++
      betweens++
    } else if (word === "AND" && betweens > 0) betweens--
    else if (word === "AND" || word === "OR" || word === "NOT" || word === "IN") count++
  }
  return count
}

/** What DynamoDB counts against an expression's limits. */
const expressionCost = (
  expression: string | undefined,
  kind: "condition" | "update" = "condition",
): { readonly length: number; readonly operators: number } =>
  expression === undefined
    ? { length: 0, operators: 0 }
    : { length: expression.length, operators: countOperators(expression, kind) }
const expressionFits = (
  expression: string,
  kind: "condition" | "update" = "condition",
): boolean => {
  const cost = expressionCost(expression, kind)
  return cost.length <= EXPRESSION_LIMIT && cost.operators <= OPERATOR_LIMIT
}

const bytesKey = (bytes: Uint8Array): string => Array.from(bytes).join(",")

/** Structural equality of two marshalled values (sets compare as sets). */
const attributeValueEquals = (
  a: AttributeValue | undefined,
  b: AttributeValue | undefined,
): boolean => {
  if (a === undefined || b === undefined) return a === b
  const [kind] = Object.keys(a)
  if (kind === undefined || Object.keys(b)[0] !== kind) return false
  const x = (a as unknown as globalThis.Record<string, unknown>)[kind]
  const y = (b as unknown as globalThis.Record<string, unknown>)[kind]
  switch (kind) {
    case "L": {
      const xs = x as ReadonlyArray<AttributeValue>
      const ys = y as ReadonlyArray<AttributeValue>
      return xs.length === ys.length && xs.every((v, i) => attributeValueEquals(v, ys[i]))
    }
    case "M": {
      const xm = x as globalThis.Record<string, AttributeValue>
      const ym = y as globalThis.Record<string, AttributeValue>
      const keys = Object.keys(xm)
      return (
        keys.length === Object.keys(ym).length &&
        keys.every((k) => attributeValueEquals(xm[k], ym[k]))
      )
    }
    case "SS":
    case "NS": {
      const xs = new Set(x as ReadonlyArray<string>)
      const ys = y as ReadonlyArray<string>
      return xs.size === new Set(ys).size && ys.every((v) => xs.has(v))
    }
    case "B":
      return bytesKey(x as Uint8Array) === bytesKey(y as Uint8Array)
    case "BS": {
      const xs = new Set((x as ReadonlyArray<Uint8Array>).map(bytesKey))
      const ys = (y as ReadonlyArray<Uint8Array>).map(bytesKey)
      return xs.size === new Set(ys).size && ys.every((v) => xs.has(v))
    }
    default:
      return x === y
  }
}

/**
 * The top-level attributes an UpdateExpression writes (SET / REMOVE / ADD /
 * DELETE targets), resolved through its attribute names.
 */
const expressionTargets = (
  expression: string,
  names: globalThis.Record<string, string>,
): ReadonlySet<string> => {
  const targets = new Set<string>()
  const sections = expression.split(/\b(SET|REMOVE|ADD|DELETE)\b/).slice(1)
  for (let i = 0; i + 1 < sections.length; i += 2) {
    const action = sections[i]!
    const body = sections[i + 1]!
    // Split on commas outside parentheses (`list_append(#a, :b)`).
    const items: Array<string> = []
    let depth = 0
    let current = ""
    for (const ch of body) {
      if (ch === "(") depth++
      if (ch === ")") depth--
      if (ch === "," && depth === 0) {
        items.push(current)
        current = ""
      } else current += ch
    }
    items.push(current)
    for (const item of items) {
      const trimmed = item.trim()
      if (trimmed === "") continue
      const path = action === "SET" ? trimmed.split("=")[0]!.trim() : trimmed.split(/\s+/)[0]!
      const head = path.split(/[.[]/)[0]!
      targets.add(head.startsWith("#") ? (names[head] ?? head) : head)
    }
  }
  return targets
}

const encodeOrDecodeEncode = (
  schema: Schema.Codec<any>,
  input: unknown,
  entityType: string,
  operation: string,
): Effect.Effect<unknown, ValidationError> =>
  Schema.encodeUnknownEffect(schema)(input).pipe(
    Effect.catch((primaryCause) =>
      Schema.decodeUnknownEffect(schema)(input).pipe(
        Effect.flatMap((decoded) => Schema.encodeUnknownEffect(schema)(decoded)),
        // On fallback failure, surface the original encode error — its
        // message is keyed on the caller's input shape, which is what the
        // user expects to see.
        Effect.catch(() =>
          Effect.fail(
            new ValidationError({
              entityType,
              operation: `${operation}.decode`,
              cause: primaryCause,
            }),
          ),
        ),
      ),
    ),
  )

// ---------------------------------------------------------------------------
// Entity interface — the return type of Entity.make()
// ---------------------------------------------------------------------------

/**
 * An Entity binds a domain model to a DynamoDB Table with index definitions,
 * system field configuration, unique constraints, and CRUD operations.
 *
 * Created via {@link make}. Returns a definition with descriptor builders.
 * Table association and binding happen via `DynamoClient.make()`.
 *
 * @typeParam TModel - Effect Schema defining the domain model
 * @typeParam TEntityType - Literal string discriminator stored as `__edd_e__`
 * @typeParam TIndexes - Index definitions (primary + GSIs)
 * @typeParam TTimestamps - Timestamp configuration
 * @typeParam TVersioned - Optimistic locking configuration
 * @typeParam TSoftDelete - Soft-delete configuration
 * @typeParam TUnique - Unique constraint definitions
 * @typeParam TIdentifier - Identifier field name (auto-generated, omitted from Create type)
 */
export interface Entity<
  TModel extends Schema.Top = Schema.Top,
  TEntityType extends string = string,
  TIndexes extends globalThis.Record<string, IndexDefinition> = globalThis.Record<
    string,
    IndexDefinition
  >,
  TTimestamps extends TimestampsConfig | undefined = TimestampsConfig | undefined,
  TVersioned extends VersionedConfig | undefined = VersionedConfig | undefined,
  TSoftDelete extends SoftDeleteConfig | undefined = SoftDeleteConfig | undefined,
  TUnique extends UniqueConfig | undefined = UniqueConfig | undefined,
  TRefs extends globalThis.Record<string, AnyRefValue> | undefined = undefined,
  TIdentifier extends string | undefined = undefined,
  TTimeSeries extends TimeSeriesConfig<any> | undefined = undefined,
  TGeneratedId extends GeneratedIdConfig | undefined = undefined,
  TVectorIndexes extends globalThis.Record<string, VectorIndexConfig> | undefined = undefined,
> {
  readonly _tag: "Entity"
  readonly model: TModel
  readonly entityType: TEntityType
  readonly indexes: TIndexes
  readonly timestamps: TTimestamps
  readonly versioned: TVersioned
  readonly softDelete: TSoftDelete
  readonly unique: TUnique
  readonly identifier: TIdentifier
  readonly timeSeries: TTimeSeries
  readonly generatedId: TGeneratedId
  /** Vector index declarations as authored (see `DESIGN.md §14`). */
  readonly vectorIndexes: TVectorIndexes

  /** @internal Normalized vector index definitions keyed by logical name. */
  readonly _vectorIndexes: globalThis.Record<string, VectorIndexDefinition>

  /** @internal Resolved ref metadata — used by cascade to inspect target entities */
  readonly _resolvedRefs: ReadonlyArray<{
    readonly fieldName: string
    readonly idFieldName: string
    readonly identifierField: string
    readonly refEntityType: string
  }>

  /** @internal Full decode pipeline: rename + schema decode. Used by Batch/Aggregate. */
  readonly _decodeRecord: (
    raw: globalThis.Record<string, unknown>,
  ) => Effect.Effect<any, ValidationError>

  /** @internal Domain field name → stored DynamoDB attribute name (`storedAs` renames). */
  readonly _resolveDbName: (domainName: string) => string

  /** @internal Entity schema version baked into composed keys. */
  readonly _entityVersion: number

  /**
   * @internal Flatten sparse-map fields into per-entry top-level attributes.
   * Mutates the item in place. Used by Transaction/Batch put builders.
   * Throws on invalid keys.
   */
  readonly _serializeSparseFields: (item: globalThis.Record<string, unknown>) => void

  /**
   * @internal Put a record into the composite key form (`internal/CompositeCodec.ts`)
   * before it reaches `KeyComposer`.
   *
   * Exposed because the multi-item write paths (`internal/TransactableOps.ts`,
   * `internal/TransactWriteOps.ts`, `Batch.ts`) and the aggregate/geo composers
   * compose keys OUTSIDE `makeImpl`, and a second implementation of the rule is
   * a second chance to disagree with `Entity.put`. Every such site must call
   * this rather than hand `KeyComposer` a raw record — `test/KeyFormInvariant.test.ts`
   * enforces it across all of them.
   */
  readonly _keyForm: (
    record: globalThis.Record<string, unknown>,
  ) => globalThis.Record<string, unknown>

  /**
   * @internal Rename domain field names to their stored DynamoDB attribute
   * names (`storedAs`), in place. `Entity.put` applies this; the transact/batch
   * put builder must apply the SAME one or a `storedAs` entity gets a
   * differently-shaped item depending on which API wrote it.
   */
  readonly _renameToDynamo: (item: globalThis.Record<string, unknown>) => void

  /**
   * @internal Whether a created item carries the hidden incarnation token
   * (`__edd_i__`) — every versioned entity.
   */
  readonly _incarnationToken: boolean

  /** @internal An item with an incarnation token but no version (see `versionCorruption`). */
  readonly _versionCorruption: (
    item: Readonly<globalThis.Record<string, unknown>> | undefined,
    operation: string,
  ) => ValidationError | undefined

  /** @internal Fill omitted decoding-default fields of a put's input (see `put`). */
  readonly _fillDecodingDefaults: (input: unknown) => Effect.Effect<unknown, ValidationError>

  /** @internal Unique fields a put's input leaves to an index-stored default. */
  readonly _unsentineledDefaults: (input: unknown) => ReadonlyArray<string>

  /**
   * @internal Plan a guarded put — of a versioned or unique-constrained entity —
   * from a fresh read of the item (#133): the same items `put` writes (the
   * item continued or created, its sentinels rotated, its snapshot), for the
   * transaction paths (`Transaction.transactWrite`, `EventStore.append`'s
   * `additionalItems`) to write in their own transaction. `item` is the output
   * of `validateAndBuildPutItem`. A `Retry` verdict means plan it again.
   */
  readonly _planPut: (args: {
    readonly tableName: string
    readonly ttlAttrName: string
    readonly now: DateTime.Utc
    readonly item: globalThis.Record<string, unknown>
    readonly createdAtSupplied: boolean
    readonly userCondition:
      | {
          readonly expression: string
          readonly names: globalThis.Record<string, string>
          readonly values: globalThis.Record<string, AttributeValue>
        }
      | undefined
    readonly create: boolean
    readonly operation: string
    readonly key: globalThis.Record<string, unknown>
  }) => Effect.Effect<PutPlan, ValidationError | DynamoClientError, DynamoClient>

  /**
   * @internal Whether this entity's write contract needs items beyond the one
   * the transact/batch compile path emits, and which config asks for it. Used to
   * decide between expanding (put) and rejecting (delete, and all of Batch).
   */
  readonly _multiItemWriteFeatures: ReadonlyArray<"unique" | "retain" | "softDelete">

  /**
   * @internal Compile an `update` into the transact items it would issue,
   * without issuing them — for `Transaction.transactWrite`.
   *
   * Runs the standalone update itself (`updateOrCreate`, so `patch` and an
   * update of a missing row behave exactly as standalone) with its write
   * recorded instead of sent (`internal/TransactPlan.planWrite`). The plan is
   * therefore the standalone write, guards included. It reads first wherever
   * the standalone update does, which is why the read's outcomes
   * (`ItemNotFound`, `OptimisticLockError`, …) can surface here.
   */
  readonly _planUpdate: (
    key: unknown,
    state: UpdateState,
  ) => Effect.Effect<TransactPlan, PlanUpdateError, DynamoClient | TableConfig>

  /**
   * @internal Compile a `delete` into the transact items it would issue — the
   * `delete` counterpart of `_planUpdate`. For an entity with `unique`,
   * `versioned: { retain: true }` or `softDelete` this includes the read of the
   * stored row the sentinel releases, snapshot and tombstone are built from:
   * the read whose absence is why the plain compile path refuses these
   * deletes (EDD-9048).
   */
  readonly _planDelete: (
    key: unknown,
    opts: {
      readonly condition: Expr | ConditionInput | undefined
      readonly mustExist: boolean
    },
  ) => Effect.Effect<TransactPlan, PlanDeleteError, DynamoClient | TableConfig>

  /**
   * @internal Which of this entity's rows are items: a row is live iff the
   * sort key composed from its own stored composites is its stored sort key.
   * Version snapshots, soft-delete tombstones and time-series events keep the
   * entity's `__edd_e__` but never satisfy it, so a query of the primary key
   * or a scan leaves them out (#133). `undefined` without `retain`,
   * `softDelete` or `timeSeries` — every row is then an item.
   */
  readonly _liveRows: () => Query.LiveRows | undefined

  /** @internal Attach model class prototype to a decoded plain object (no-op for Schema.Struct models). */
  readonly _attachPrototype: (decoded: any) => any

  /**
   * @internal Configure the entity with table schema and tag.
   * Called by DynamoClient.make() when binding entities to a table.
   */
  readonly _configure: (
    schema: DynamoSchema.DynamoSchema,
    tableTag: import("effect").Context.Service<TableConfig, TableConfig>,
  ) => void

  /**
   * @internal Inject a GSI index definition into this entity.
   * Called by Collections binding or DynamoClient.make() to add collection-owned indexes.
   * The injected index becomes available to all entity operations (put, update, etc.)
   * for key composition.
   */
  readonly _injectIndex: (name: string, def: IndexDefinition) => void

  /** @internal Injected DynamoSchema — available after _configure(). Used by cascade. */
  readonly _schema: DynamoSchema.DynamoSchema
  /** @internal Injected TableConfig tag — available after _configure(). Used by cascade. */
  readonly _tableTag: import("effect").Context.Service<TableConfig, TableConfig>

  /** Resolved system field names */
  readonly systemFields: ResolvedSystemFields

  /** Derived schemas for type extraction */
  readonly schemas: DerivedSchemas

  /**
   * Typed input schema. Ref-aware: ref fields are replaced with branded ID fields.
   * Use in HttpApiEndpoint payloads or for validation.
   *
   * @example
   * ```ts
   * HttpApiEndpoint.post("create", "/squads", {
   *   payload: SquadSelections.inputSchema,
   * })
   * ```
   */
  readonly inputSchema: Schema.Codec<
    EntityRefInputType<TModel, TRefs, TTimestamps, TVersioned, TTimeSeries>
  >

  /**
   * Typed create schema. Input fields minus primary key composites — the common
   * "create" payload where IDs are auto-generated.
   *
   * @example
   * ```ts
   * HttpApiEndpoint.post("create", "/teams", {
   *   payload: Teams.createSchema,
   *   success: Team,
   * })
   * ```
   */
  readonly createSchema: Schema.Codec<
    EntityRefCreateType<TModel, TIndexes, TRefs, TIdentifier, TTimestamps, TVersioned, TTimeSeries>
  >

  /**
   * Typed update schema. Partial fields minus primary key composites and immutable fields.
   * Ref fields are replaced with optional branded ID fields.
   */
  readonly updateSchema: Schema.Codec<
    EntityRefUpdateType<TModel, TIndexes, TRefs, TTimestamps, TVersioned>
  >

  // --- CRUD Operations ---

  /** Fetch an item by primary key. Returns a lazy {@link EntityGet} intermediate. */
  readonly get: (
    key: EntityKeyType<TModel, TIndexes>,
  ) => EntityGet<
    ModelType<TModel>,
    EntityRecordType<TModel, TTimestamps, TVersioned>,
    ItemNotFound | DynamoClientError | ValidationError,
    DynamoClient | TableConfig
  >

  /**
   * Create or replace an item. Returns a lazy {@link EntityPut} intermediate.
   *
   * On a versioned or unique-constrained entity the put reads the item first:
   * a replacing put continues its version, incarnation and `createdAt`,
   * snapshots it (`retain`) and rotates its unique sentinels — releasing only
   * those the item owns; a created one continues after any history retained
   * for its key. A concurrent write between the read and the put is retried,
   * so the last writer wins; a race lost on every attempt is an
   * `OptimisticLockError` (versioned) or `ConcurrentModification`.
   */
  readonly put: (
    input: WithGeneratedId<
      EntityRefInputType<TModel, TRefs, TTimestamps, TVersioned, TTimeSeries>,
      TGeneratedId
    >,
  ) => EntityPut<
    ModelType<TModel>,
    EntityRecordType<TModel, TTimestamps, TVersioned>,
    | DynamoClientError
    | TransactionOverflow
    | ValidationError
    | UniqueConstraintViolation
    | OptimisticLockError
    | ConcurrentModification
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    DynamoClient | TableConfig
  >

  /**
   * Begin an update operation on an existing item. Returns a lazy {@link EntityUpdate} intermediate.
   * Pipe to {@link set} to provide update fields and {@link expectedVersion} for optimistic locking.
   */
  readonly update: (
    key: EntityKeyType<TModel, TIndexes>,
  ) => EntityUpdate<
    ModelType<TModel>,
    EntityRecordType<TModel, TTimestamps, TVersioned>,
    EntityRefUpdateType<TModel, TIndexes, TRefs, TTimestamps, TVersioned>,
    | DynamoClientError
    | TransactionOverflow
    | ItemNotFound
    | OptimisticLockError
    | ConcurrentModification
    | UpdateAppliedButUnreadable
    | UniqueConstraintViolation
    | ValidationError
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    DynamoClient | TableConfig
  >

  /**
   * Delete an item by primary key. Returns a lazy {@link EntityDelete} intermediate.
   * A unique-constrained entity releases only the sentinels the item owns.
   */
  readonly delete: (
    key: EntityKeyType<TModel, TIndexes>,
  ) => EntityDelete<
    | DynamoClientError
    | TransactionOverflow
    | ItemNotFound
    | OptimisticLockError
    | ConcurrentModification
    | ValidationError
    | DeleteAppliedButUnreadable,
    DynamoClient | TableConfig,
    void,
    ModelType<TModel>
  >

  /**
   * Create a new item. Fails with `ConditionalCheckFailed` if an item with the same
   * primary key already exists. Equivalent to `put(input)` with an `attribute_not_exists`
   * condition — it never reads the item (a `retain` entity reads only the highest
   * version retained for the key, to continue after it).
   */
  readonly create: (
    input: WithGeneratedId<
      EntityRefInputType<TModel, TRefs, TTimestamps, TVersioned, TTimeSeries>,
      TGeneratedId
    >,
  ) => EntityPut<
    ModelType<TModel>,
    EntityRecordType<TModel, TTimestamps, TVersioned>,
    | DynamoClientError
    | TransactionOverflow
    | ValidationError
    | UniqueConstraintViolation
    | OptimisticLockError
    | ConcurrentModification
    | ConditionalCheckFailed
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    DynamoClient | TableConfig
  >

  /**
   * Update an existing item. Fails with `ConditionalCheckFailed` if the item doesn't exist.
   * Equivalent to `update(key)` with an automatic `attribute_exists` condition on the PK field.
   */
  readonly patch: (
    key: EntityKeyType<TModel, TIndexes>,
  ) => EntityUpdate<
    ModelType<TModel>,
    EntityRecordType<TModel, TTimestamps, TVersioned>,
    EntityRefUpdateType<TModel, TIndexes, TRefs, TTimestamps, TVersioned>,
    | DynamoClientError
    | TransactionOverflow
    | ItemNotFound
    | OptimisticLockError
    | ConcurrentModification
    | UpdateAppliedButUnreadable
    | UniqueConstraintViolation
    | ValidationError
    | ConditionalCheckFailed
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    DynamoClient | TableConfig
  >

  /**
   * Delete an existing item. Fails with `ConditionalCheckFailed` if the item doesn't exist.
   * Equivalent to `delete(key)` with an automatic `attribute_exists` condition on the PK field.
   */
  readonly deleteIfExists: (
    key: EntityKeyType<TModel, TIndexes>,
  ) => EntityDelete<
    | DynamoClientError
    | TransactionOverflow
    | ItemNotFound
    | OptimisticLockError
    | ConcurrentModification
    | ValidationError
    | DeleteAppliedButUnreadable
    | ConditionalCheckFailed,
    DynamoClient | TableConfig,
    void,
    ModelType<TModel>
  >

  /**
   * Create an item, or update the fields the input supplies on an existing one —
   * immutable fields, `createdAt` and fields the input omits keep their stored
   * values; the version is incremented. The whole input is validated either way.
   * Returns the full record.
   *
   * Plain entities: one UpdateItem, `if_not_exists()` for immutable fields,
   * `createdAt` and the version (`if_not_exists(version, 0) + 1`). An entity with
   * `unique` constraints or `versioned: { retain: true }` — or an input omitting a
   * defaulted index composite — reads the item once first: missing, it is
   * `create`d (sentinels reserved, snapshot written, defaults stored); present, it
   * is updated (sentinels rotated, the replaced item snapshotted). A concurrent
   * create or delete in between is retried the other way; a race lost on every
   * attempt is an `OptimisticLockError` (versioned) or `ConcurrentModification`.
   */
  readonly upsert: (
    input: WithGeneratedId<
      EntityRefInputType<TModel, TRefs, TTimestamps, TVersioned, TTimeSeries>,
      TGeneratedId
    >,
  ) => EntityPut<
    ModelType<TModel>,
    EntityRecordType<TModel, TTimestamps, TVersioned>,
    | DynamoClientError
    | TransactionOverflow
    | ValidationError
    | ItemNotFound
    | UniqueConstraintViolation
    | OptimisticLockError
    | ConcurrentModification
    | ConditionalCheckFailed
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    DynamoClient | TableConfig
  >

  // --- Lifecycle Operations ---

  /** Fetch a specific version snapshot by version number. */
  readonly getVersion: (
    key: EntityKeyType<TModel, TIndexes>,
    version: number,
  ) => EntityGet<
    ModelType<TModel>,
    EntityRecordType<TModel, TTimestamps, TVersioned>,
    ItemNotFound | DynamoClientError | ValidationError,
    DynamoClient | TableConfig
  >

  /** Query version history for an item. Returns a Query for piping with limit, reverse, collect. */
  readonly versions: (
    key: EntityKeyType<TModel, TIndexes>,
  ) => Query.Query<EntityRecordType<TModel, TTimestamps, TVersioned>>

  /** Restore a soft-deleted item. Recomposes index keys and re-establishes unique sentinels. */
  readonly restore: (
    key: EntityKeyType<TModel, TIndexes>,
  ) => EntityGet<
    ModelType<TModel>,
    EntityRecordType<TModel, TTimestamps, TVersioned>,
    | ItemNotFound
    | ItemNotDeleted
    | DynamoClientError
    | TransactionOverflow
    | ValidationError
    | UniqueConstraintViolation,
    DynamoClient | TableConfig
  >

  /** Permanently remove an item plus all version history and sentinels. */
  readonly purge: (
    key: EntityKeyType<TModel, TIndexes>,
  ) => EntityDelete<DynamoClientError | ValidationError, DynamoClient | TableConfig>

  /** Soft-deleted item accessors. */
  readonly deleted: {
    /** Get a specific soft-deleted item. */
    readonly get: (
      key: EntityKeyType<TModel, TIndexes>,
    ) => EntityGet<
      ModelType<TModel>,
      EntityRecordType<TModel, TTimestamps, TVersioned>,
      ItemNotFound | DynamoClientError | ValidationError,
      DynamoClient | TableConfig
    >
    /** List soft-deleted items within a partition. */
    readonly list: (
      key: EntityKeyType<TModel, TIndexes>,
    ) => Query.Query<EntityRecordType<TModel, TTimestamps, TVersioned>>
  }

  // --- Time-series Operations (only when `timeSeries` is configured) ---

  /**
   * Append an event to a time-series entity. Atomically updates the "current"
   * item (scoped SET on `appendInput` fields only + CAS on `orderBy`) and
   * writes an immutable event item under the same partition.
   *
   * **Stale-as-error contract:** on CAS rejection the Effect fails with
   * {@link StaleAppend} (carrying `current: Option.some(...)` from the
   * follow-up GetItem). When a user-supplied `condition` rejected the write
   * but the CAS held, the Effect fails with {@link ConditionalCheckFailed}
   * (also carrying `current`). This supersedes the v1 stale-as-value design.
   *
   * The optional `condition` argument is ANDed onto the CAS ConditionExpression.
   * The optional `skipFollowUp` flag suppresses the post-transaction GetItem;
   * success becomes `void` and CAS / user-condition failures collapse into
   * `StaleAppend(current: Option.none())` (cannot disambiguate without the
   * GetItem). See `guides/timeseries.mdx`.
   *
   * Only available when the entity was built with `timeSeries: { ... }`.
   */
  readonly append: [TTimeSeries] extends [TimeSeriesConfig<infer TAI extends Schema.Top>]
    ? {
        (
          input: AppendInputType<TAI>,
          condition?: Expr | ConditionInput,
          skipFollowUp?: false,
          removeAttrs?: ReadonlyArray<string>,
        ): Effect.Effect<
          { readonly current: ModelType<TModel> },
          DynamoClientError | ValidationError | StaleAppend | ConditionalCheckFailed,
          DynamoClient | TableConfig
        >
        (
          input: AppendInputType<TAI>,
          condition: Expr | ConditionInput | undefined,
          skipFollowUp: true,
          removeAttrs?: ReadonlyArray<string>,
        ): Effect.Effect<
          void,
          DynamoClientError | ValidationError | StaleAppend,
          DynamoClient | TableConfig
        >
      }
    : never

  /**
   * Query the event history of a time-series entity partition. Returns a
   * `Query.Query` auto-scoped via `begins_with(<currentSk>#e#)`.
   *
   * Only available when the entity was built with `timeSeries: { ... }`.
   */
  readonly history: [TTimeSeries] extends [TimeSeriesConfig<any>]
    ? (key: EntityKeyType<TModel, TIndexes>) => Query.Query<ModelType<TModel>>
    : never

  // --- Update Combinators (entity-scoped, type-safe in both paths) ---

  /**
   * Set combinator pre-bound to this entity's update type.
   *
   * GSI all-or-none composites are enforced at runtime via `ValidationError`.
   *
   * @example
   * ```typescript
   * // data-last
   * yield* Users.update({ userId: "u-1" }).pipe(
   *   Users.set({ tenantId: "t-3", region: "us-east-1" }),
   * )
   *
   * // data-first
   * Entity.set(Users.update({ userId: "u-1" }), { tenantId: "t-3", region: "us-east-1" })
   * ```
   */
  readonly set: {
    (
      updates: EntityRefUpdateType<TModel, TIndexes, TRefs, TTimestamps, TVersioned>,
    ): <A, Rec, U, E, R>(self: EntityUpdate<A, Rec, U, E, R>) => EntityUpdate<A, Rec, U, E, R>
    <A, Rec, U, E, R>(
      self: EntityUpdate<A, Rec, U, E, R>,
      updates: EntityRefUpdateType<TModel, TIndexes, TRefs, TTimestamps, TVersioned>,
    ): EntityUpdate<A, Rec, U, E, R>
  }

  /**
   * Set expected version for optimistic locking.
   * Entity-scoped for consistent pipe chains with {@link Entity.set}.
   */
  readonly expectedVersion: {
    (
      version: number,
    ): <A, Rec, U, E, R>(self: EntityUpdate<A, Rec, U, E, R>) => EntityUpdate<A, Rec, U, E, R>
    <A, Rec, U, E, R>(
      self: EntityUpdate<A, Rec, U, E, R>,
      version: number,
    ): EntityUpdate<A, Rec, U, E, R>
  }

  /** Index query accessors. Each non-primary index becomes a method that accepts PK composites and returns a {@link Query.Query}. */
  readonly query: {
    readonly [K in Exclude<keyof TIndexes, "primary">]: (
      pk: IndexPkInput<TModel, TIndexes, K, TRefs>,
    ) => Query.Query<ModelType<TModel>>
  }

  /** Scan all items of this entity type. Returns a {@link Query.Query} that uses DynamoDB Scan. */
  readonly scan: () => Query.Query<ModelType<TModel>>

  // --- Expression Combinators (callback + shorthand) ---

  /**
   * Build a condition expression combinator.
   * Callback receives `(t, ops)` where `t` is a PathBuilder and `ops` provides comparison functions.
   * Shorthand accepts a simple object for AND-equality conditions.
   *
   * @example
   * ```typescript
   * // Callback
   * Teams.condition((t, { eq }) => eq(t.status, "active"))
   * // Shorthand
   * Teams.condition({ status: "active" })
   * ```
   *
   * Applying the returned pipeable widens the operation's error channel with
   * {@link ConditionalCheckFailed}, so `Effect.catchTag("ConditionalCheckFailed", ...)`
   * type-checks on the resulting Effect.
   */
  readonly condition: {
    (
      cb: (
        t: import("./internal/PathBuilder.js").PathBuilder<ModelType<TModel>, ModelType<TModel>>,
        ops: import("./internal/Expr.js").ConditionOps<ModelType<TModel>>,
      ) => import("./internal/Expr.js").Expr,
    ): ConditionPipeable
    (shorthand: globalThis.Record<string, unknown>): ConditionPipeable
  }

  /**
   * Build a filter expression for query/scan operations.
   * Same API as `condition()` — callback or shorthand.
   *
   * @example
   * ```typescript
   * teams.collect(Teams.query.byAll(pk), Teams.filter((t, { gt }) => gt(t.wins, 10)))
   * ```
   */
  readonly filter: {
    (
      cb: (
        t: import("./internal/PathBuilder.js").PathBuilder<ModelType<TModel>, ModelType<TModel>>,
        ops: import("./internal/Expr.js").ConditionOps<ModelType<TModel>>,
      ) => import("./internal/Expr.js").Expr,
    ): <A>(self: Query.Query<A>) => Query.Query<A>
    (shorthand: globalThis.Record<string, unknown>): <A>(self: Query.Query<A>) => Query.Query<A>
  }

  /**
   * Build a select (projection) combinator for get/query/scan operations.
   * Callback receives `t` — a PathBuilder. Returns an array of paths to project.
   * String array shorthand for top-level only projections.
   *
   * @example
   * ```typescript
   * Teams.select((t) => [t.name, t.address.city])
   * Teams.select(["name", "status"])
   * ```
   */
  readonly select: {
    (
      cb: (
        t: import("./internal/PathBuilder.js").PathBuilder<ModelType<TModel>, ModelType<TModel>>,
      ) => ReadonlyArray<import("./internal/PathBuilder.js").Path<ModelType<TModel>, any>>,
    ): (self: Query.Query<any>) => Query.Query<any>
    (attributes: ReadonlyArray<string>): (self: Query.Query<any>) => Query.Query<any>
  }
}

// ---------------------------------------------------------------------------
// BoundEntity — Entity operations with services pre-resolved (R = never)
// ---------------------------------------------------------------------------

/**
 * An Entity whose CRUD operations have `DynamoClient` and `TableConfig` already
 * resolved, so all methods return `Effect<A, E, never>`.
 *
 * Created via {@link bind}. Use in service layers to avoid leaking infrastructure
 * requirements through service method signatures.
 *
 * @example
 * ```typescript
 * export class TeamService extends Context.Service<TeamService>()("TeamService", {
 *   make: Effect.gen(function* () {
 *     const db = yield* DynamoClient.make({ entities: { Teams }, tables: { MainTable } })
 *     const teams = db.entities.Teams
 *     return {
 *       get: (id: TeamId) => teams.get({ id }),           // R = never
 *       create: (input) => teams.create({ ...input }),     // R = never
 *       list: (filter) => teams.byAll(filter).collect(),  // R = never
 *     }
 *   }),
 * }) {}
 * ```
 */
/**
 * The logical vector index names an entity declares, as a string union.
 *
 * Resolves to `never` for entities without `vectorIndexes`, which makes
 * `.withVector()` uncallable on them — an unusable method beats a method that
 * silently does nothing.
 */
export type VectorIndexNames<TVectorIndexes> =
  TVectorIndexes extends globalThis.Record<string, VectorIndexConfig>
    ? keyof TVectorIndexes & string
    : never

export interface BoundEntity<
  TModel extends Schema.Top,
  TIndexes extends globalThis.Record<string, IndexDefinition>,
  TRefs extends globalThis.Record<string, AnyRefValue> | undefined,
  TKey = EntityKeyType<TModel, TIndexes>,
  TTimeSeries extends TimeSeriesConfig<any> | undefined = undefined,
  TTimestamps extends TimestampsConfig | undefined = undefined,
  TVersioned extends VersionedConfig | undefined = undefined,
  TGeneratedId extends GeneratedIdConfig | undefined = undefined,
  TVectorIndexes extends globalThis.Record<string, VectorIndexConfig> | undefined = undefined,
> {
  // --- CRUD Operations ---

  /**
   * Fetch an item by primary key.
   *
   * Returns a {@link BoundGet}, which **is** an `Effect<Model, …, never>` —
   * `yield*` it, `.pipe(Effect.catchTag("ItemNotFound", …))` it, hand it to any
   * Effect combinator, exactly as before. It additionally carries the read
   * descriptor, so it can be passed straight to `Batch.get`,
   * `Transaction.transactGet` and `Transaction.check` (#108).
   *
   * ```ts
   * const user = yield* db.entities.Users.get({ userId })
   * const [a, b] = yield* Batch.get([
   *   db.entities.Users.get({ userId: "u-1" }),
   *   db.entities.Users.get({ userId: "u-2" }),
   * ])
   * ```
   */
  readonly get: (
    key: TKey,
  ) => import("./internal/BoundCrud.js").BoundGet<
    ModelType<TModel>,
    ItemNotFound | DynamoClientError | ValidationError
  >

  /**
   * Create or replace an item. Returns a fluent {@link BoundPut} — yield to execute,
   * chain `.condition(...)` to add a condition expression.
   *
   * ```ts
   * yield* db.entities.Users.put(input)
   * yield* db.entities.Users.put(input).condition({ status: "active" })
   * ```
   */
  readonly put: (
    input: WithGeneratedId<
      EntityRefInputType<TModel, TRefs, TTimestamps, TVersioned, TTimeSeries>,
      TGeneratedId
    >,
  ) => import("./internal/BoundCrud.js").BoundPut<
    ModelType<TModel>,
    ModelType<TModel>,
    | DynamoClientError
    | TransactionOverflow
    | ValidationError
    | UniqueConstraintViolation
    | OptimisticLockError
    | ConcurrentModification
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    VectorIndexNames<TVectorIndexes>
  >

  /**
   * Create a new item. Fails with {@link ConditionalCheckFailed} if an item with the same
   * primary key already exists. Returns a fluent {@link BoundPut}.
   */
  readonly create: (
    input: WithGeneratedId<
      EntityRefInputType<TModel, TRefs, TTimestamps, TVersioned, TTimeSeries>,
      TGeneratedId
    >,
  ) => import("./internal/BoundCrud.js").BoundPut<
    ModelType<TModel>,
    ModelType<TModel>,
    | DynamoClientError
    | TransactionOverflow
    | ValidationError
    | UniqueConstraintViolation
    | OptimisticLockError
    | ConcurrentModification
    | ConditionalCheckFailed
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    VectorIndexNames<TVectorIndexes>
  >

  /**
   * Begin an update on an existing item. Returns a fluent {@link BoundUpdate} — chain
   * `.set(...)`, `.remove(...)`, `.add(...)`, `.condition(...)`, `.expectedVersion(...)`,
   * then `yield*`.
   *
   * ```ts
   * yield* db.entities.Tasks.update({ id }).set(updates).expectedVersion(3)
   * ```
   */
  readonly update: (
    key: TKey,
  ) => import("./internal/BoundCrud.js").BoundUpdate<
    ModelType<TModel>,
    ModelType<TModel>,
    EntityRefUpdateType<TModel, TIndexes, TRefs, TTimestamps, TVersioned>,
    | DynamoClientError
    | TransactionOverflow
    | ItemNotFound
    | OptimisticLockError
    | ConcurrentModification
    | UpdateAppliedButUnreadable
    | UniqueConstraintViolation
    | ValidationError
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    VectorIndexNames<TVectorIndexes>
  >

  /**
   * Delete an item by primary key. Returns a fluent {@link BoundDelete} — yield to execute,
   * chain `.condition(...)` and/or `.returnValues(...)`.
   *
   * ```ts
   * yield* db.entities.Tasks.delete(key)
   * yield* db.entities.Tasks.delete(key).condition({ status: "archived" })
   * ```
   */
  readonly delete: (
    key: TKey,
  ) => import("./internal/BoundCrud.js").BoundDelete<
    ModelType<TModel>,
    | DynamoClientError
    | TransactionOverflow
    | ItemNotFound
    | OptimisticLockError
    | ConcurrentModification
    | ValidationError
    | DeleteAppliedButUnreadable
  >

  /**
   * Create an item, or update the fields the input supplies on an existing one.
   * Returns a fluent {@link BoundPut}. See `Entity.upsert`: a plain entity is one
   * UpdateItem with `if_not_exists()` for immutable fields, `createdAt` and the
   * version; a `unique` or `retain` entity (or an input omitting a defaulted index
   * composite) reads the item once and creates or updates it.
   */
  readonly upsert: (
    input: WithGeneratedId<
      EntityRefInputType<TModel, TRefs, TTimestamps, TVersioned, TTimeSeries>,
      TGeneratedId
    >,
  ) => import("./internal/BoundCrud.js").BoundPut<
    ModelType<TModel>,
    ModelType<TModel>,
    | DynamoClientError
    | TransactionOverflow
    | ValidationError
    | ItemNotFound
    | UniqueConstraintViolation
    | OptimisticLockError
    | ConcurrentModification
    | ConditionalCheckFailed
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    VectorIndexNames<TVectorIndexes>
  >

  /**
   * Update an existing item. Fails with {@link ConditionalCheckFailed} if the item doesn't exist.
   * Returns a fluent {@link BoundUpdate}.
   *
   * ```ts
   * yield* db.entities.Tasks.patch({ id }).set(updates)
   * ```
   */
  readonly patch: (
    key: TKey,
  ) => import("./internal/BoundCrud.js").BoundUpdate<
    ModelType<TModel>,
    ModelType<TModel>,
    EntityRefUpdateType<TModel, TIndexes, TRefs, TTimestamps, TVersioned>,
    | DynamoClientError
    | TransactionOverflow
    | ItemNotFound
    | OptimisticLockError
    | ConcurrentModification
    | UpdateAppliedButUnreadable
    | UniqueConstraintViolation
    | ValidationError
    | ConditionalCheckFailed
    | RefErrors<TRefs>
    | VectorErrors<TVectorIndexes>,
    VectorIndexNames<TVectorIndexes>
  >

  /** Delete an existing item, fails if not found. Returns a fluent {@link BoundDelete}. */
  readonly deleteIfExists: (
    key: TKey,
  ) => import("./internal/BoundCrud.js").BoundDelete<
    ModelType<TModel>,
    | DynamoClientError
    | TransactionOverflow
    | ItemNotFound
    | OptimisticLockError
    | ConcurrentModification
    | ValidationError
    | DeleteAppliedButUnreadable
    | ConditionalCheckFailed
  >

  // --- Lifecycle Operations ---

  /** Fetch a specific version snapshot by version number. */
  readonly getVersion: (
    key: TKey,
    version: number,
  ) => Effect.Effect<ModelType<TModel>, ItemNotFound | DynamoClientError | ValidationError, never>

  /**
   * List all version snapshots for an item as a fluent BoundQuery.
   * Requires `versioned: { retain: true }` on the entity definition.
   *
   * ```ts
   * const all = yield* db.entities.Users.versions({ userId: "u-1" }).collect()
   * const last5 = yield* db.entities.Users
   *   .versions({ userId: "u-1" })
   *   .reverse()
   *   .limit(5)
   *   .collect()
   * ```
   */
  readonly versions: (
    key: TKey,
  ) => import("./internal/BoundQuery.js").BoundQuery<ModelType<TModel>, never, ModelType<TModel>>

  /** Restore a soft-deleted item. */
  readonly restore: (
    key: TKey,
  ) => Effect.Effect<
    ModelType<TModel>,
    | ItemNotFound
    | ItemNotDeleted
    | DynamoClientError
    | TransactionOverflow
    | ValidationError
    | UniqueConstraintViolation,
    never
  >

  /** Permanently remove an item plus all version history and sentinels. */
  readonly purge: (key: TKey) => Effect.Effect<void, DynamoClientError | ValidationError, never>

  /** Soft-deleted item accessors. */
  readonly deleted: {
    /** Get a specific soft-deleted item. */
    readonly get: (
      key: TKey,
    ) => Effect.Effect<ModelType<TModel>, ItemNotFound | DynamoClientError | ValidationError, never>
    /**
     * List all soft-deleted items in this partition as a fluent BoundQuery.
     * Requires `softDelete` on the entity definition.
     *
     * ```ts
     * const tombstones = yield* db.entities.Employees.deleted
     *   .list({ employeeId: "e-1" })
     *   .collect()
     * ```
     */
    readonly list: (
      key: TKey,
    ) => import("./internal/BoundQuery.js").BoundQuery<ModelType<TModel>, never, ModelType<TModel>>
  }

  // --- Time-series Operations (only when `timeSeries` is configured) ---

  /**
   * Append an event to a time-series entity. Atomically updates the "current"
   * item (scoped SET on `appendInput` fields only + CAS on `orderBy`) and
   * writes an immutable event item under the same partition.
   *
   * Returns a fluent {@link BoundAppend} — yield to execute, or chain
   * `.condition(...)` and/or `.skipFollowUp()`.
   *
   * **Stale-as-error contract:** on CAS rejection the Effect fails with
   * {@link StaleAppend}; user-condition rejection (when CAS held) fails with
   * {@link ConditionalCheckFailed}. `.skipFollowUp()` collapses both into
   * `StaleAppend(current: Option.none())` because no follow-up GetItem
   * runs.
   *
   * ```ts
   * const { current } = yield* db.entities.Telemetry.append(input)
   * yield* db.entities.Telemetry.append(input).condition({ status: "active" })
   * yield* db.entities.Telemetry.append(input).skipFollowUp() // → void
   * ```
   *
   * Only available when the entity was built with `timeSeries: { ... }`.
   */
  readonly append: [TTimeSeries] extends [TimeSeriesConfig<infer TAI extends Schema.Top>]
    ? (
        input: AppendInputType<TAI>,
      ) => import("./internal/BoundCrud.js").BoundAppend<
        ModelType<TModel>,
        { readonly current: ModelType<TModel> },
        DynamoClientError | ValidationError | StaleAppend | ConditionalCheckFailed,
        DynamoClientError | ValidationError | StaleAppend
      >
    : never

  /**
   * Query the event history of a time-series entity partition. Returns a
   * {@link import("./internal/BoundQuery.js").BoundQuery} auto-scoped to event
   * items via `begins_with(<currentSk>#e#)`. `.where()` is typed to the
   * configured `orderBy` attribute only; `.filter()` works on any model
   * attribute.
   *
   * Only available when the entity was built with `timeSeries: { ... }`.
   */
  readonly history: [TTimeSeries] extends [TimeSeriesConfig<any>]
    ? (
        key: TKey,
      ) => import("./internal/BoundQuery.js").BoundQuery<
        ModelType<TModel>,
        { readonly [K in NonNullable<TTimeSeries>["orderBy"] & string]: string },
        ModelType<TModel>
      >
    : never

  // --- Query Execution ---

  /**
   * Execute a query and return a lazy Stream of items. Automatically paginates through all pages.
   * Accepts optional query combinators (e.g. `Query.limit(...)`, `Query.reverse`).
   *
   * ```ts
   * const stream = teams.paginate(Teams.query.byRole({ role: "admin" }), Query.limit(10))
   * ```
   */
  readonly paginate: <A>(
    query: Query.Query<A>,
    ...combinators: ReadonlyArray<(q: Query.Query<A>) => Query.Query<A>>
  ) => Stream.Stream<A, DynamoClientError | ValidationError, never>

  /**
   * Execute a query and collect all pages into a single array.
   * Accepts optional query combinators (e.g. `Query.limit(...)`, `Query.reverse`).
   *
   * ```ts
   * const items = yield* teams.collect(Teams.query.byRole({ role: "admin" }), Query.limit(10))
   * ```
   */
  readonly collect: <A>(
    query: Query.Query<A>,
    ...combinators: ReadonlyArray<(q: Query.Query<A>) => Query.Query<A>>
  ) => Effect.Effect<Array<A>, DynamoClientError | ValidationError, never>

  /**
   * Execute a single DynamoDB page and return a {@link Query.Page} with an opaque cursor.
   * Use `Query.startFrom(cursor)` to iterate through subsequent pages.
   * Accepts optional query combinators (e.g. `Query.limit(...)`, `Query.reverse`).
   *
   * ```ts
   * const page = yield* teams.fetch(Teams.query.byRole({ role: "admin" }), Query.limit(25))
   * if (page.cursor) {
   *   const next = yield* teams.fetch(Teams.query.byRole({ role: "admin" }), Query.limit(25), Query.startFrom(page.cursor))
   * }
   * ```
   */
  readonly fetch: <A>(
    query: Query.Query<A>,
    ...combinators: ReadonlyArray<(q: Query.Query<A>) => Query.Query<A>>
  ) => Effect.Effect<Query.Page<A>, DynamoClientError | ValidationError, never>

  /**
   * Execute a single DynamoDB scan page and return a {@link Query.Page} with an opaque cursor.
   * Convenience for `fetch(Entity.scan(), ...)`. Use `Query.startFrom(cursor)` for subsequent pages.
   * Accepts optional query combinators (e.g. `Query.limit(...)`, `Query.filter(...)`).
   *
   * ```ts
   * const page = yield* teams.scanFetch(Query.limit(25))
   * if (page.cursor) {
   *   const next = yield* teams.scanFetch(Query.limit(25), Query.startFrom(page.cursor))
   * }
   * ```
   */
  readonly scanFetch: (
    ...combinators: ReadonlyArray<
      (q: Query.Query<ModelType<TModel>>) => Query.Query<ModelType<TModel>>
    >
  ) => Effect.Effect<Query.Page<ModelType<TModel>>, DynamoClientError | ValidationError, never>
}

// ---------------------------------------------------------------------------
// Transaction limit pre-check (100-item DynamoDB limit)
// ---------------------------------------------------------------------------

/**
 * Compile a condition that may be either an Expr ADT node or a ConditionInput object.
 * Routes both paths through the Expr ADT compiler for a single compilation backend.
 * Returns undefined if the condition is undefined.
 */
const compileCondition = (
  cond: Expr | ConditionInput | undefined,
  resolveDbNameFn?: (name: string) => string,
):
  | {
      expression: string
      names: globalThis.Record<string, string>
      values: globalThis.Record<string, import("@aws-sdk/client-dynamodb").AttributeValue>
    }
  | undefined => {
  if (cond === undefined) return undefined
  const expr = isExpr(cond) ? cond : parseShorthand(cond)
  return compileExpr(expr, resolveDbNameFn)
}

const TRANSACTION_LIMIT = 100

const checkTransactionLimit = (
  entityType: string,
  operation: string,
  items: ReadonlyArray<unknown>,
): Effect.Effect<void, TransactionOverflow> =>
  items.length > TRANSACTION_LIMIT
    ? Effect.fail(
        new TransactionOverflow({
          entityType,
          operation,
          itemCount: items.length,
          limit: TRANSACTION_LIMIT,
        }),
      )
    : Effect.void

/** Attempts a guarded put makes before it reports a lost race (#133). */
const GUARDED_PUT_ATTEMPTS = 3

/**
 * @internal A delete's item was deleted between its read and its write, with
 * nothing asserted about it: read again, never reported (#133).
 */
class DeletedConcurrently extends Data.TaggedError("DeletedConcurrently")<{}> {}

/**
 * The `ConcurrentModification`s that report a sentinel release whose
 * reservation changed hands between the ownership read and the write (#133),
 * as opposed to a change of the item itself. Nothing was written, and the
 * write is still valid against a fresh read — so an `upsert` plans it again,
 * as a put does. Carried across the copies that fill in `current`.
 */
const releaseRaces = new WeakSet<ConcurrentModification>()
/** `next`, a copy of `previous`, marked as a release race if `previous` was. */
const keepReleaseRace = (
  previous: ConcurrentModification,
  next: ConcurrentModification,
): ConcurrentModification => {
  if (releaseRaces.has(previous)) releaseRaces.add(next)
  return next
}

/** @internal One positional reason of a cancelled (or conditional) write. */
export interface WriteCancellationReason {
  readonly Code?: string | undefined
  readonly Item?: Readonly<globalThis.Record<string, unknown>> | undefined
}

/** @internal What a cancelled guarded put means (#133) — see `planPut`. */
export type PutVerdict =
  | {
      /** A lost race: plan the put again from a fresh read. */
      readonly _tag: "Retry"
      readonly stored: globalThis.Record<string, AttributeValue> | undefined
      /** Reported when every attempt loses. */
      readonly error: OptimisticLockError | ConcurrentModification
    }
  /** The caller's own condition rejected it (or `create` found the item). */
  | { readonly _tag: "Condition" }
  | { readonly _tag: "Fail"; readonly error: UniqueConstraintViolation | ValidationError }

/**
 * @internal A guarded put — of a versioned or unique-constrained entity —
 * compiled against the item as read (#133): the items to write (the main Put
 * first), the item written, and how to read a cancellation of them.
 */
export interface PutPlan {
  readonly items: ReadonlyArray<TransactWriteItem>
  readonly item: globalThis.Record<string, unknown>
  readonly marshalled: globalThis.Record<string, AttributeValue>
  /** `reasons` positional with `items`; `undefined` when none of them failed. */
  readonly verdict: (
    reasons: ReadonlyArray<WriteCancellationReason | undefined>,
  ) => PutVerdict | undefined
}

// ---------------------------------------------------------------------------
// Entity.make()
// ---------------------------------------------------------------------------

/**
 * Create a new Entity — binding a domain model to a Table with index definitions
 * and optional system field configuration.
 *
 * @param config - Entity configuration
 * @param config.model - Effect Schema class or struct defining the domain model
 * @param config.entityType - Literal string discriminator stored as `__edd_e__`
 * @param config.indexes - ElectroDB-style composite key definitions (must include `primary`)
 * @param config.timestamps - Automatic `createdAt` / `updatedAt` fields
 * @param config.versioned - Optimistic locking via auto-incrementing version field
 * @param config.softDelete - Soft-delete behavior
 * @param config.unique - Named unique constraints enforced via transaction sentinels
 * @returns An {@link Entity} with typed CRUD operations and index query accessors
 *
 * @example
 * ```typescript
 * const Users = Entity.make({
 *   model: User,
 *   table: MainTable,
 *   entityType: "User",
 *   indexes: {
 *     primary: { pk: { field: "pk", composite: ["userId"] }, sk: { field: "sk", composite: [] } },
 *     byEmail: { index: "gsi1", pk: { field: "gsi1pk", composite: ["email"] }, sk: { field: "gsi1sk", composite: [] } },
 *   },
 *   timestamps: true,
 *   versioned: true,
 * })
 * ```
 */
/** Compute the normalized indexes type from primaryKey + optional GSI configs. */
type NormalizedIndexes<
  TPrimaryKey extends PrimaryKeyDef,
  TGsiIndexes extends globalThis.Record<string, GsiConfig>,
> = keyof TGsiIndexes extends never
  ? { readonly primary: TPrimaryKey }
  : { readonly primary: TPrimaryKey } & {
      readonly [K in keyof TGsiIndexes & string]: IndexDefinition & {
        readonly collection: TGsiIndexes[K]["collection"]
        readonly pk: { readonly composite: TGsiIndexes[K]["pk"]["composite"] }
        readonly sk: { readonly composite: TGsiIndexes[K]["sk"]["composite"] }
      }
    }

/** Primary key definition — used in the new `primaryKey` config form. */
type PrimaryKeyDef = IndexDefinition &
  (
    | { readonly pk: { readonly composite: readonly [string, ...string[]] } }
    | { readonly sk: { readonly composite: readonly [string, ...string[]] } }
  )

/**
 * Create an Entity definition.
 *
 * `primaryKey` defines the table's primary key. `indexes` optionally defines GSI
 * access patterns. GSIs with a `collection` property are auto-discovered as
 * cross-entity collections by `DynamoClient.make()`.
 *
 * @example
 * ```typescript
 * const Tasks = Entity.make({
 *   model: Task,
 *   entityType: "Task",
 *   primaryKey: {
 *     pk: { field: "pk", composite: ["taskId"] },
 *     sk: { field: "sk", composite: [] },
 *   },
 *   indexes: {
 *     byProject: {
 *       name: "gsi1",
 *       pk: { field: "gsi1pk", composite: ["projectId"] },
 *       sk: { field: "gsi1sk", composite: ["status"] },
 *     },
 *     assigned: {
 *       collection: "assignments",
 *       name: "gsi2",
 *       pk: { field: "gsi2pk", composite: ["employee"] },
 *       sk: { field: "gsi2sk", composite: ["project"] },
 *     },
 *   },
 * })
 * ```
 */
export const make = <
  TModel extends Schema.Top,
  const TEntityType extends string,
  const TPrimaryKey extends PrimaryKeyDef,
  const TGsiIndexes extends globalThis.Record<string, GsiConfig> = {},
  const TTimestamps extends TimestampsConfig | undefined = undefined,
  const TVersioned extends VersionedConfig | undefined = undefined,
  const TSoftDelete extends SoftDeleteConfig | undefined = undefined,
  const TUnique extends UniqueConfig | undefined = undefined,
  const TRefs extends globalThis.Record<string, AnyRefValue> | undefined = undefined,
  const TTimeSeries extends TimeSeriesConfig<any> | undefined = undefined,
  const TGeneratedId extends GeneratedIdConfig | undefined = undefined,
  const TVectorIndexes extends
    | globalThis.Record<string, VectorIndexConfig<any>>
    | undefined = undefined,
  const TAttrs extends {} = {},
>(config: {
  readonly model: TModel | ConfiguredModel<TModel, TAttrs>
  readonly entityType: TEntityType
  readonly primaryKey: TPrimaryKey
  readonly indexes?: TGsiIndexes
  readonly timestamps?: TTimestamps
  readonly versioned?: TVersioned
  readonly softDelete?: TSoftDelete
  readonly unique?: TUnique
  readonly refs?: TRefs
  readonly timeSeries?: TTimeSeries
  readonly generatedId?: TGeneratedId
  readonly vectorIndexes?: TVectorIndexes
}): Entity<
  TModel,
  TEntityType,
  NormalizedIndexes<TPrimaryKey, TGsiIndexes>,
  TTimestamps,
  TVersioned,
  TSoftDelete,
  TUnique,
  TRefs,
  ExtractIdentifier<ConfiguredModel<TModel, TAttrs>>,
  TTimeSeries,
  TGeneratedId,
  TVectorIndexes
> => {
  // Normalize GSI configs to internal IndexDefinition format
  const gsiIndexes: globalThis.Record<string, IndexDefinition> = {}
  if (config.indexes) {
    for (const [name, gsi] of Object.entries(config.indexes)) {
      gsiIndexes[name] = normalizeGsiConfig(gsi)
    }
  }

  const indexes = { primary: config.primaryKey, ...gsiIndexes } as any
  // Delegate to the internal implementation with normalized indexes
  return makeImpl({ ...config, indexes }) as any
}

/**
 * @internal Resolved ref metadata + the operational target entity. Mirrors the
 * pure-side `ResolvedRef` but types `refEntity` as the runtime {@link Entity}, so
 * write-time hydration can call its CRUD operations.
 */
interface ResolvedRef {
  readonly fieldName: string
  readonly idFieldName: string
  readonly identifierField: string
  readonly identifierSchema: Schema.Top
  readonly refEntity: Entity
  readonly refEntityType: string
}

/** @internal A definition that has had its operations attached (has `.get`). */
const isOperational = (e: { readonly get?: unknown }): boolean => typeof e.get === "function"

/** @internal Copy the schema + table tag a definition received via `_configure`
 * (Table.make injects them at table-definition time) onto a promoted entity. */
const carryConfigure = (from: EntityDefinition, to: Entity): void => {
  if (from._schema !== undefined && from._tableTag !== undefined) {
    to._configure(
      from._schema,
      from._tableTag as import("effect").Context.Service<TableConfig, TableConfig>,
    )
  }
}

/**
 * Promote a pure `@effect-dynamodb/schema` {@link EntityDefinition} into a full
 * operational runtime {@link Entity}, attaching CRUD/query operations. The
 * already-derived data bundle (`def._data`) is reused, so this is a thin
 * op-attach — no re-validation or re-derivation. The table binding the pure
 * definition already received (via `Table.make`) is preserved.
 *
 * **Ref targets are promoted too.** Write-time ref hydration calls `.get()` on
 * each ref target (to denormalise it), so a pure ref target must be operational
 * or the write crashes with `ref.refEntity.get is not a function`. Each ref
 * target is promoted one level: `.get` (a read) does not itself hydrate refs, so
 * the target's *own* ref targets need not be promoted — which also sidesteps
 * cyclic refs (A→B→A) without recursion. An entity that is itself bound and
 * written goes through `fromDefinition` independently and gets its own refs
 * promoted.
 *
 * Used by `DynamoClient.make()` to bind pure definitions; idempotent on an
 * entity that is already operational.
 *
 * @internal
 */
export const fromDefinition = (def: EntityDefinition): Entity => {
  const resolvedRefs = def._data.resolvedRefs
  // Replace any pure ref target with an operational one so hydration's `.get`
  // works. Already-operational targets (runtime-authored, or mixed refs once the
  // shared AnyRefValue lands) pass through unchanged.
  // Cast: a promoted ref target is a runtime `Entity`, not the pure
  // `EntityDefinition` that `EntityDefinitionData.resolvedRefs` is typed against.
  // Hydration only reads `.model`/`.entityType`/`.get` off it (the latter via the
  // runtime `ResolvedRef` cast inside makeImpl), so this is sound.
  const data =
    resolvedRefs.length > 0
      ? ({
          ...def._data,
          resolvedRefs: resolvedRefs.map((r) =>
            isOperational(r.refEntity as { readonly get?: unknown })
              ? r
              : { ...r, refEntity: promoteRefTarget(r.refEntity as unknown as EntityDefinition) },
          ),
        } as unknown as EntityDefinitionData)
      : def._data

  const runtime = makeImpl(
    def._config as unknown as Parameters<typeof makeImpl>[0],
    data,
  ) as unknown as Entity
  carryConfigure(def, runtime)
  return runtime
}

/** @internal Promote a ref target to an operational entity WITHOUT promoting its
 * own ref targets (one level — see {@link fromDefinition}). The target's table
 * binding is carried over so `.get` can compose keys. */
const promoteRefTarget = (def: EntityDefinition): Entity => {
  const runtime = makeImpl(
    def._config as unknown as Parameters<typeof makeImpl>[0],
    def._data,
  ) as unknown as Entity
  carryConfigure(def, runtime)
  return runtime
}

const makeImpl = <
  TModel extends Schema.Top,
  const TEntityType extends string,
  const TIndexes extends globalThis.Record<string, IndexDefinition> & {
    readonly primary: IndexDefinition &
      (
        | { readonly pk: { readonly composite: readonly [string, ...string[]] } }
        | { readonly sk: { readonly composite: readonly [string, ...string[]] } }
      )
  },
  const TTimestamps extends TimestampsConfig | undefined = undefined,
  const TVersioned extends VersionedConfig | undefined = undefined,
  const TSoftDelete extends SoftDeleteConfig | undefined = undefined,
  const TUnique extends UniqueConfig | undefined = undefined,
  const TRefs extends globalThis.Record<string, AnyRefValue> | undefined = undefined,
  const TTimeSeries extends TimeSeriesConfig<any> | undefined = undefined,
  const TGeneratedId extends GeneratedIdConfig | undefined = undefined,
  const TVectorIndexes extends
    | globalThis.Record<string, VectorIndexConfig<any>>
    | undefined = undefined,
  const TAttrs extends {} = {},
>(
  config: {
    readonly model: TModel | ConfiguredModel<TModel, TAttrs>
    readonly entityType: TEntityType
    readonly indexes: typeof undefined extends never ? never : TIndexes
    readonly timestamps?: TTimestamps
    readonly versioned?: TVersioned
    readonly softDelete?: TSoftDelete
    readonly unique?: TUnique
    readonly refs?: TRefs
    readonly timeSeries?: TTimeSeries
    readonly generatedId?: TGeneratedId
    readonly vectorIndexes?: TVectorIndexes
  },
  precomputedData?: EntityDefinitionData,
): Entity<
  TModel,
  TEntityType,
  TIndexes,
  TTimestamps,
  TVersioned,
  TSoftDelete,
  TUnique,
  TRefs,
  ExtractIdentifier<ConfiguredModel<TModel, TAttrs>>,
  TTimeSeries,
  TGeneratedId,
  TVectorIndexes
> => {
  // Derivation (validation + schema/ref/sparse/rename resolution) is shared with
  // the pure `@effect-dynamodb/schema` Entity.make via `buildEntityDefinition`,
  // giving a single source of truth for the EDD-90xx rules. Promotion of a pure
  // definition (see `fromDefinition`) passes the already-computed bundle as
  // `precomputedData`, so derivation runs exactly once per entity.
  // Cast: the runtime config's `refs` uses the runtime `AnyRefValue` (whose
  // `entity` is the operational `Entity`), which is nominally distinct from the
  // schema package's `AnyRefValue` (`entity: EntityDefinition`). buildEntityDefinition
  // only reads `.model`/`.entityType`/identifier off ref targets, both of which
  // the runtime Entity carries, so the cast is sound.
  const data = precomputedData ?? buildEntityDefinition(config as unknown as EntityDefinitionConfig)
  const {
    systemFields,
    schemas,
    hasRefs,
    immutableFields,
    resolvedIdentifier,
    generatedIdField,
    generatedIdVersion,
    entityType,
    entityVersion,
    sparseFields,
    hasSparseFields,
    renameToDynamo,
    renameFromDynamo,
    resolveDbName,
    rawModel,
    isSchemaClass,
    hasHiddenFields,
  } = data
  // Encoders for path-addressed update values (`pathSet`, `pathAppend`, …) and
  // record-based `append`, which bypass the update schema (#133).
  const pathValues = makePathValueEncoder(
    data.modelFields,
    (
      schemas.writeModelSchema as unknown as {
        readonly fields: globalThis.Record<string, Schema.Top>
      }
    ).fields,
    // A `DynamoModel.ref` field's own schema is opaque; paths under it follow
    // the model it is denormalised from.
    Object.fromEntries(
      (
        data.resolvedRefs as ReadonlyArray<{
          readonly fieldName: string
          readonly refEntity?: { readonly model?: unknown }
        }>
      )
        .filter((ref) => ref.refEntity?.model !== undefined)
        .map((ref) => {
          const model = ref.refEntity!.model as Schema.Top
          return [
            ref.fieldName,
            isConfiguredModel(model) ? (model.model as Schema.Top) : model,
          ] as const
        }),
    ),
  )
  /**
   * Encode a path-addressed update value and check it against the write schema
   * at its path — container checks included (#133). `undefined` issue = valid.
   */
  const encodePathValue = (
    segments: ReadonlyArray<string | number>,
    value: unknown,
    kind: "value" | "elements",
  ): { readonly encoded: unknown; readonly issue: unknown } => {
    if (kind === "value") {
      const encoded = pathValues.value(segments, value)
      return { encoded, issue: pathValues.validate(segments, encoded) }
    }
    const encoded = pathValues.elements(segments, value)
    return { encoded, issue: pathValues.validateElements(segments, encoded) }
  }
  const hasPathOps = (uState: UpdateState): boolean =>
    [
      uState.pathSets,
      uState.pathRemoves,
      uState.pathAdds,
      uState.pathSubtracts,
      uState.pathAppends,
      uState.pathPrepends,
      uState.pathIfNotExists,
      uState.pathDeletes,
    ].some((ops) => ops !== undefined && ops.length > 0)

  // resolvedRefs carries the actual ref-target entity objects; at runtime they
  // are operational Entities (for runtime-authored refs) so write-time hydration
  // can call their CRUD ops. The pure bundle widens refEntity to EntityDefinition.
  const resolvedRefs = data.resolvedRefs as unknown as ReadonlyArray<ResolvedRef>
  // allIndexes is mutable: _injectIndex adds collection-owned GSIs after make().
  let allIndexes: globalThis.Record<string, IndexDefinition> = { ...data.initialIndexes }

  /** Composites of the secondary indexes (`allIndexes` grows after make()). */
  const indexCompositeFields = (): ReadonlySet<string> =>
    new Set(
      Object.entries(allIndexes)
        .filter(([name]) => name !== "primary")
        .flatMap(([, def]) => [...def.pk.composite, ...def.sk.composite]),
    )

  /**
   * Why `field` feeds a key or a unique sentinel — values the library derives
   * from the field at write time — or `undefined` when it feeds neither.
   */
  const derivedFrom = (field: string): string | undefined => {
    const primary = allIndexes.primary
    if (primary && [...primary.pk.composite, ...primary.sk.composite].includes(field)) {
      return "a primary-key composite"
    }
    for (const [name, def] of Object.entries(allIndexes)) {
      if (name === "primary") continue
      if (def.pk.composite.includes(field) || def.sk.composite.includes(field)) {
        return `a composite of index "${name}"`
      }
    }
    for (const [name, def] of Object.entries(
      (config.unique ?? {}) as globalThis.Record<string, UniqueConstraintDef>,
    )) {
      if (resolveUniqueFields(def).includes(field)) return `a field of unique constraint "${name}"`
    }
    return undefined
  }

  /**
   * Path operations on a field that feeds a key or a unique sentinel (#133).
   *
   * DynamoDB evaluates a path operation, so compiling one on such a field would
   * change the attribute while its index keys and sentinel stay as they were.
   * A top-level `pathSet` of a value / `pathRemove` IS `.set()` / `.remove()`
   * of that field, and `pathAdd` / `pathSubtract` of a number IS `.add()` /
   * `.subtract()`: they are rewritten into those record operations, which go
   * through the key composer and the sentinel rotation. Anything whose result
   * only DynamoDB knows (a copy from another attribute, `if_not_exists`, list
   * and set operations) is refused, as is a path below such a field, a
   * primary-key composite (it identifies the item), an immutable field, and a
   * field targeted twice. Returns the rewritten state or the refusal.
   */
  const normalizeDerivedPathOps = (uState: UpdateState): UpdateState | string => {
    if (!hasPathOps(uState)) return uState
    const updates: globalThis.Record<string, unknown> = {
      ...((uState.updates as globalThis.Record<string, unknown> | undefined) ?? {}),
    }
    const remove = [...(uState.remove ?? [])]
    const add: globalThis.Record<string, number> = { ...uState.add }
    const subtract: globalThis.Record<string, number> = { ...uState.subtract }
    const targeted = new Set<string>([
      ...Object.keys(updates),
      ...remove,
      ...Object.keys(add),
      ...Object.keys(subtract),
      ...Object.keys(uState.append ?? {}),
      ...Object.keys(uState.deleteFromSet ?? {}),
    ])
    let refusal: string | undefined
    let rewritten = false
    const shown = (segments: ReadonlyArray<string | number>) =>
      JSON.stringify(
        segments.map((s, i) => (typeof s === "number" ? `[${s}]` : i === 0 ? s : `.${s}`)).join(""),
      )
    // The field a path operation addresses when it feeds a key or sentinel,
    // "free" when it feeds neither, or "refused" (recorded) for the shapes
    // that can never be rewritten.
    const claim = (
      op: string,
      segments: ReadonlyArray<string | number>,
    ): { readonly field: string; readonly reason: string } | "free" | "refused" => {
      const first = segments[0]
      if (typeof first !== "string") return "free"
      const head: string = first
      const reason = derivedFrom(head)
      if (reason === undefined) return "free"
      if (segments.length > 1) {
        refusal ??=
          `${op} on ${shown(segments)}: "${head}" is ${reason}, so a path below it cannot be ` +
          `updated. Write the whole value with .set({ ${head} }).`
      } else if (reason === "a primary-key composite") {
        refusal ??=
          `${op} on "${head}": "${head}" is a primary-key composite — it identifies the item ` +
          "and cannot be changed by an update."
      } else if (immutableFields.has(head)) {
        refusal ??= `${op} on "${head}": "${head}" is immutable and cannot be changed by an update.`
      } else if (targeted.has(head)) {
        refusal ??=
          `${op} on "${head}": "${head}" is ${reason} and is already targeted by another ` +
          "operation in this update. Give it one operation."
      } else {
        targeted.add(head)
        return { field: head, reason }
      }
      return "refused"
    }
    const serverSide = (op: string, field: string, reason: string) => {
      refusal ??=
        `${op} on "${field}": "${field}" is ${reason}. DynamoDB computes this operation's ` +
        `result at write time, so the key or sentinel derived from "${field}" could not be ` +
        `recomposed to match. Read the item and write the new value with .set({ ${field} }).`
    }
    const keep = <Op>(
      ops: ReadonlyArray<Op> | undefined,
      segmentsOf: (op: Op) => ReadonlyArray<string | number>,
      name: string,
      rewrite: (op: Op, field: string, reason: string) => void,
    ): ReadonlyArray<Op> | undefined => {
      if (ops === undefined) return undefined
      const kept: Array<Op> = []
      for (const op of ops) {
        const claimed = claim(name, segmentsOf(op))
        if (claimed === "free") kept.push(op)
        if (typeof claimed === "string") continue
        rewritten = true
        rewrite(op, claimed.field, claimed.reason)
      }
      return kept
    }
    const pathSets = keep(
      uState.pathSets,
      (op) => op.segments,
      "pathSet",
      (op, field, reason) => {
        if (op.isPath) serverSide("pathSet (a copy of another attribute)", field, reason)
        else updates[field] = op.value
      },
    )
    const pathRemoves = keep(
      uState.pathRemoves,
      (segments) => segments,
      "pathRemove",
      (_, field) => {
        remove.push(field)
      },
    )
    const pathAdds = keep(
      uState.pathAdds,
      (op) => op.segments,
      "pathAdd",
      (op, field, reason) => {
        if (typeof op.value === "number") add[field] = op.value
        else serverSide("pathAdd (to a set)", field, reason)
      },
    )
    const pathSubtracts = keep(
      uState.pathSubtracts,
      (op) => op.segments,
      "pathSubtract",
      (op, field, reason) => {
        if (!op.isPath && typeof op.value === "number") subtract[field] = op.value
        else serverSide("pathSubtract (of another attribute)", field, reason)
      },
    )
    const refuseAll =
      (name: string) =>
      (_: unknown, field: string, reason: string): void =>
        serverSide(name, field, reason)
    const pathAppends = keep(
      uState.pathAppends,
      (op) => op.segments,
      "pathAppend",
      refuseAll("pathAppend"),
    )
    const pathPrepends = keep(
      uState.pathPrepends,
      (op) => op.segments,
      "pathPrepend",
      refuseAll("pathPrepend"),
    )
    const pathIfNotExists = keep(
      uState.pathIfNotExists,
      (op) => op.segments,
      "pathIfNotExists",
      refuseAll("pathIfNotExists"),
    )
    const pathDeletes = keep(
      uState.pathDeletes,
      (op) => op.segments,
      "pathDelete",
      refuseAll("pathDelete"),
    )
    if (refusal !== undefined) return refusal
    if (!rewritten) return uState
    return {
      ...uState,
      updates: Object.keys(updates).length > 0 ? updates : uState.updates,
      remove: remove.length > 0 ? remove : uState.remove,
      add: Object.keys(add).length > 0 ? add : uState.add,
      subtract: Object.keys(subtract).length > 0 ? subtract : uState.subtract,
      pathSets,
      pathRemoves,
      pathAdds,
      pathSubtracts,
      pathAppends,
      pathPrepends,
      pathIfNotExists,
      pathDeletes,
    }
  }

  /**
   * A `.set()` of a primary-key composite with a value other than the key's
   * (#133): it identifies the item and cannot change. It was silently dropped;
   * the update types already exclude it. The key's own value is a no-op, so a
   * spread record keeps working.
   */
  const refusedRecordSet = (updates: unknown, key: unknown): string | undefined => {
    if (typeof updates !== "object" || updates === null) return undefined
    const record = updates as globalThis.Record<string, unknown>
    const keyRecord = (key ?? {}) as globalThis.Record<string, unknown>
    const primary = allIndexes.primary
    for (const field of primary ? [...primary.pk.composite, ...primary.sk.composite] : []) {
      if (record[field] === undefined || Equal.equals(record[field], keyRecord[field])) continue
      return (
        `.set() of "${field}": "${field}" is a primary-key composite — it identifies the item ` +
        "and cannot be changed by an update."
      )
    }
    return undefined
  }

  type AssertedImmutable = {
    readonly field: string
    readonly attr: string
    readonly value: AttributeValue
  }

  /**
   * The immutable fields a `.set()` names, in stored form (#133). An update may
   * restate an immutable field's value — a spread record does — but never
   * change it: each one is checked against the item (read, or a condition on
   * the write) and a different value is refused.
   */
  const assertedImmutables = (updates: unknown): ReadonlyArray<AssertedImmutable> | string => {
    if (typeof updates !== "object" || updates === null) return []
    const record = updates as globalThis.Record<string, unknown>
    const out: Array<AssertedImmutable> = []
    for (const field of immutableFields) {
      if (record[field] === undefined) continue
      const encoded = encodePathValue([field], record[field], "value")
      if (encoded.issue !== undefined) {
        return `.set() of immutable "${field}": ${String(encoded.issue)}`
      }
      out.push({ field, attr: resolveDbName(field), value: toAttributeValue(encoded.encoded) })
    }
    return out
  }
  const immutableMismatch = (
    asserted: ReadonlyArray<AssertedImmutable>,
    stored: Readonly<globalThis.Record<string, unknown>>,
  ): string | undefined => {
    const changed = asserted.find(
      ({ attr, value }) => !attributeValueEquals(stored[attr] as AttributeValue | undefined, value),
    )
    return changed === undefined
      ? undefined
      : `.set() of "${changed.field}": "${changed.field}" is immutable and the update's value ` +
          "differs from the stored one."
  }

  /**
   * Whether an update can safely create the item it names (#133): its payload
   * supplies every required model field and every primary-key composite, so
   * the item it writes decodes. Anything less on a missing item would leave a
   * partial row behind — those updates require the item to exist.
   */
  const completeUpsertPayload = (updates: unknown): boolean => {
    if (typeof updates !== "object" || updates === null) return false
    const record = updates as globalThis.Record<string, unknown>
    const present = (field: string) => record[field] !== undefined && record[field] !== null
    const primary = allIndexes.primary
    if (primary && ![...primary.pk.composite, ...primary.sk.composite].every(present)) return false
    for (const [field, fieldSchema] of Object.entries(
      data.modelFields as globalThis.Record<string, Schema.Top>,
    )) {
      const ast = fieldSchema.ast as {
        readonly context?: { readonly isOptional?: boolean }
        readonly encoding?: ReadonlyArray<{
          readonly to: { readonly context?: { readonly isOptional?: boolean } }
        }>
      }
      if (ast.context?.isOptional === true) continue
      // A decoding default (optional on the stored side): `put` may omit it too.
      if (ast.encoding?.[ast.encoding.length - 1]?.to.context?.isOptional === true) continue
      const ref = resolvedRefs.find((r) => r.fieldName === field)
      if (present(field) || (ref !== undefined && present(ref.idFieldName))) continue
      return false
    }
    return true
  }

  // schema + tableTag are injected via _configure() when the entity is registered
  // on a Table and bound through DynamoClient.make(); captured by operation closures.
  let schema!: DynamoSchema.DynamoSchema
  let tableTag!: import("effect").Context.Service<TableConfig, TableConfig>

  /**
   * Flatten sparse Map fields into per-entry top-level attributes.
   * Called on the write path after rename + date serialization, before
   * `toAttributeMap`. No-op when the entity has no sparse fields.
   *
   * Throws on key validation errors (caught by callers and translated into
   * tagged `ValidationError` at the entity boundary).
   */
  const serializeSparseFields = (item: globalThis.Record<string, unknown>): void => {
    if (!hasSparseFields) return
    encodeSparseFields(item, sparseFields)
  }

  /**
   * Rebuild sparse Map fields from flattened top-level attributes.
   * Called on the read path after `fromAttributeMap`, before rename + date
   * deserialization. No-op when the entity has no sparse fields.
   */
  const deserializeSparseFields = (raw: globalThis.Record<string, unknown>): void => {
    if (!hasSparseFields) return
    decodeSparseFields(raw, sparseFields)
  }

  /**
   * A DOMAIN-keyed VIEW of a row that came off the wire.
   *
   * Stored rows are keyed by ATTRIBUTE name, but everything that reads a row by
   * field name — key composition (`composeAllKeys`, `keyForm`), vector-partition
   * recomposition, unique-sentinel composition — is written against DOMAIN
   * names. Under a `DynamoModel.configure(model, { id: { field: "widgetId" } })`
   * rename the two differ, and reading the wrong one yields `undefined`: key
   * composition throws (`Missing composite attribute "id" in record`) and
   * sentinel composition silently treats the constraint as unset (#127).
   *
   * Returns a shallow COPY — the caller's row stays attribute-keyed, so the item
   * actually written back (tombstone, restored item, snapshot) is unaffected and
   * `decodeAs` can still do its own in-place rename on it. Callers that decode
   * the row and discard it rename in place instead (see `decodeRecord`).
   */
  const toDomainView = (
    raw: globalThis.Record<string, unknown>,
  ): globalThis.Record<string, unknown> => {
    const view = { ...raw }
    renameFromDynamo(view)
    return view
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  // Read-path composite encoder — see `internal/CompositeCodec.ts`. The write
  // path composes keys from the encoded record (`put` encodes, THEN composes);
  // key and query inputs arrive as decoded model values, so they are encoded
  // through the model's own field codecs before composition. Encoding is
  // idempotent via the `decode -> encode` fallback, so callers that already
  // hold an encoded record are unaffected.
  // Source: `inputSchema`, the EXACT schema `put` encodes through before
  // `composeAllKeys`. The raw model is not equivalent — entity derivation
  // substitutes date/Redacted fields with their wire transforms, so reading
  // fields off the model would miss encodings the write path applies.
  const compositeKeyForm = makeCompositeKeyForm(
    schemas.inputSchema as unknown as Schema.Top,
    (attr, value) => {
      throw new Error(
        `[EDD-9050] Composite "${attr}" on entity "${entityType}" could not be encoded to its ` +
          `stored form. The attribute's schema carries an encoding transformation, so the ` +
          `stored key holds the ENCODED value, but ${JSON.stringify(String(value))} encodes ` +
          `under neither encode nor decode->encode. Supply a value of the attribute's own type.`,
      )
    },
  )

  /**
   * PUBLIC key boundary. Converts a caller-supplied key record from the model's
   * **Type** side — the one convention the whole API takes — into the ENCODED
   * form key composition expects.
   *
   * `put` composes keys from the encoded record, so the stored key holds the
   * wire value. Key-taking operations used to run a plain `decode`, which reads
   * the Encoded side and then handed the *decoded* result to the composer. On a
   * transformed composite neither spelling worked: `get({ txn: 420n })` failed
   * validation and `get({ txn: "420" })` silently returned `ItemNotFound` for a
   * row that exists. Encoding the Type side fixes the one spelling that should
   * have worked all along, and matches what the query path takes.
   *
   * Encode-ONLY, deliberately: no `decode -> encode` fallback. The Encoded side
   * is not a public input, so `{ txn: "420" }` fails here — loudly, with the
   * attribute named by the schema error — rather than composing a key by a
   * second convention. Internal callers that legitimately hold encoded records
   * do not come through here; see `composePrimaryKey`.
   */
  const encodeKey = (
    key: unknown,
    operation: string,
  ): Effect.Effect<globalThis.Record<string, unknown>, ValidationError> =>
    Schema.encodeUnknownEffect(schemas.keySchema as Schema.Codec<any>)(key).pipe(
      Effect.map((encoded) => encoded as globalThis.Record<string, unknown>),
      Effect.mapError((cause) => new ValidationError({ entityType, operation, cause })),
    )

  /**
   * Synchronous `encodeKey`, for the query builders (`history`, `versions`,
   * `deleted.list`) that compose a partition key outside an Effect. Same
   * Type-side-only rule; throws the schema's own error, which names the
   * offending attribute — these builders have always thrown on a bad key.
   */
  const encodeKeySync = (key: unknown): globalThis.Record<string, unknown> =>
    Schema.encodeUnknownSync(schemas.keySchema as Schema.Codec<any>)(key) as globalThis.Record<
      string,
      unknown
    >

  /**
   * INTERNAL key composition. Takes an **already-encoded** record — either the
   * output of `encodeKey` on the public path, or an item read back from
   * DynamoDB on the retain / restore / soft-delete paths, which are wire-shaped
   * by construction.
   *
   * The `encodeCompositeRecord` pass is a normalisation, not a second input
   * convention: composite encoding is idempotent, so an already-encoded value
   * round-trips to itself. It exists so that `keySchema` (built from raw model
   * fields) and `inputSchema` (built from derivation-substituted fields) cannot
   * disagree for a composite whose wire transform is added by derivation —
   * `Schema.Date` being the case that matters.
   */
  /**
   * THE key-form normaliser. Every record handed to `KeyComposer` in this
   * module goes through it, so the write path, the update path, the lifecycle
   * paths and the read path cannot disagree about how a composite is spelled
   * in a key.
   *
   * Skipping it on ONE site is enough to corrupt data: `composeAllKeys` (put)
   * normalised while `composeGsiKeysForUpdatePolicyAware` (update) did not, so
   * an `update()` rewrote a padded `gsi1pk` to its unpadded form and evicted
   * the row from its own GSI. `test/KeyFormInvariant.test.ts` scans this file
   * and fails if a `KeyComposer.compose*` call takes a record that did not come
   * through `keyForm(...)`.
   */
  const keyForm = (record: globalThis.Record<string, unknown>) =>
    toCompositeKeyRecord(compositeKeyForm, record)

  const composePrimaryKey = (record: globalThis.Record<string, unknown>) => {
    const primary = config.indexes.primary
    const recordKeyForm = keyForm(record)
    return {
      [primary.pk.field]: KeyComposer.composePk(schema, entityType, primary, recordKeyForm),
      [primary.sk.field]: KeyComposer.composeSk(
        schema,
        entityType,
        entityVersion,
        primary,
        recordKeyForm,
      ),
    }
  }

  const composeAllKeys = (record: globalThis.Record<string, unknown>) =>
    KeyComposer.composeAllKeys(schema, entityType, entityVersion, allIndexes, keyForm(record))

  /**
   * Fill the auto-generated id field on the raw `put` input when configured and
   * absent. Sources a UUID from the bundled `Crypto` service. Caller-supplied
   * values are respected (never overwritten). Returns the input unchanged when
   * `generatedId` is not configured — entities without it never touch Crypto.
   *
   * `Crypto.Crypto` is provided from the context `DynamoClient.make` bundles,
   * so the bound `put` stays `R = never` despite this yield.
   */
  const fillGeneratedId = (input: unknown): Effect.Effect<unknown, never, Crypto.Crypto> => {
    if (!generatedIdField) return Effect.succeed(input)
    if (typeof input !== "object" || input === null) return Effect.succeed(input)
    const rec = input as globalThis.Record<string, unknown>
    if (rec[generatedIdField] !== undefined && rec[generatedIdField] !== null) {
      return Effect.succeed(input)
    }
    return Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto
      const id =
        generatedIdVersion === "v7" ? yield* crypto.randomUUIDv7 : yield* crypto.randomUUIDv4
      return { ...rec, [generatedIdField]: id }
    }).pipe(
      // randomUUIDv4/v7 surface a PlatformError; the global Crypto wrapper never
      // fails in practice, so collapse it to a defect rather than widening the
      // put error channel.
      Effect.orDie,
    )
  }

  /**
   * The model fields with a decoding default (`Schema.withDecodingDefault` and
   * kin: optional on the stored side, required on the domain side).
   */
  const defaultedFields: ReadonlyArray<readonly [string, Schema.Top]> = Object.entries(
    data.modelFields as globalThis.Record<string, Schema.Top>,
  ).filter(([, field]) => {
    const ast = field.ast as {
      readonly context?: { readonly isOptional?: boolean }
      readonly encoding?: ReadonlyArray<{
        readonly to: { readonly context?: { readonly isOptional?: boolean } }
      }>
    }
    return (
      ast.context?.isOptional !== true &&
      ast.encoding?.[ast.encoding.length - 1]?.to.context?.isOptional === true
    )
  })

  /**
   * Fill each omitted decoding-default field that an INDEX key derives from —
   * a primary-key, GSI or LSI composite — with its default (#133), so the item
   * stores it, its keys compose from it, and the item, its keys and its decoded
   * read all agree. Left out, the item had no index keys while its decoded
   * record named a value the index is keyed by. A default never creates a
   * unique sentinel (`unsentineledDefaults`); other defaulted fields keep
   * their contract: not stored, defaulted on read.
   */
  /** The defaulted fields an index key derives from — materialised when omitted. */
  const indexedDefaultFields = (): ReadonlySet<string> => {
    const composites = new Set(
      Object.values(allIndexes).flatMap((def) => [...def.pk.composite, ...def.sk.composite]),
    )
    return new Set(defaultedFields.map(([name]) => name).filter((name) => composites.has(name)))
  }
  /**
   * The unique-constraint fields a write's input omits but stores a default
   * for (an index composite): the item lists them in `__edd_d__`, and no
   * sentinel is composed from them — a default never creates a sentinel.
   */
  const unsentineledDefaults = (input: unknown): ReadonlyArray<string> => {
    if (config.unique == null || typeof input !== "object" || input === null) return []
    const record = input as globalThis.Record<string, unknown>
    const unique = new Set(
      Object.values(config.unique as globalThis.Record<string, UniqueConstraintDef>).flatMap(
        (def) => [...resolveUniqueFields(def)],
      ),
    )
    return [...indexedDefaultFields()].filter(
      (name) => unique.has(name) && record[name] === undefined,
    )
  }

  /**
   * An update's `.remove()` of a defaulted index composite: the field would
   * read back as its default while its index keys were dropped. It becomes a
   * `.set()` of the default instead (#133) — stored, keys recomposed from it —
   * and the fields so defaulted are returned (a unique one then holds only a
   * default: `__edd_d__`).
   */
  const rematerializeRemovedDefaults = (
    uState: UpdateState,
  ): Effect.Effect<
    { readonly state: UpdateState; readonly defaulted: ReadonlyArray<string> },
    ValidationError
  > => {
    const indexed = indexedDefaultFields()
    const removed = (uState.remove ?? []).filter((field) => indexed.has(field))
    if (removed.length === 0) return Effect.succeed({ state: uState, defaulted: [] })
    const fields = defaultedFields.filter(([name]) => removed.includes(name))
    return Schema.decodeUnknownEffect(
      Schema.Struct(Object.fromEntries(fields)) as unknown as Schema.Codec<any>,
    )({}).pipe(
      Effect.map((defaults) => ({
        state: {
          ...uState,
          remove: (uState.remove ?? []).filter((field) => !removed.includes(field)),
          updates: {
            ...((uState.updates as globalThis.Record<string, unknown> | undefined) ?? {}),
            ...(defaults as object),
          },
        },
        defaulted: removed,
      })),
      Effect.mapError(
        (cause) => new ValidationError({ entityType, operation: "update.default", cause }),
      ),
    ) as Effect.Effect<
      { readonly state: UpdateState; readonly defaulted: ReadonlyArray<string> },
      ValidationError
    >
  }

  const fillDecodingDefaults = (input: unknown): Effect.Effect<unknown, ValidationError> => {
    if (defaultedFields.length === 0 || typeof input !== "object" || input === null) {
      return Effect.succeed(input)
    }
    const record = input as globalThis.Record<string, unknown>
    const keyed = indexedDefaultFields()
    const missing = defaultedFields.filter(
      ([name]) => keyed.has(name) && record[name] === undefined,
    )
    if (missing.length === 0) return Effect.succeed(input)
    return Schema.decodeUnknownEffect(
      Schema.Struct(Object.fromEntries(missing)) as unknown as Schema.Codec<any>,
    )({}).pipe(
      Effect.map((defaults) => ({ ...record, ...(defaults as object) })),
      Effect.mapError(
        (cause) => new ValidationError({ entityType, operation: "put.default", cause }),
      ),
    ) as Effect.Effect<unknown, ValidationError>
  }

  /** Whether `input` omits a defaulted field an index key derives from (see `fillDecodingDefaults`). */
  const omitsIndexedDefault = (input: unknown): boolean => {
    if (defaultedFields.length === 0 || typeof input !== "object" || input === null) return false
    const record = input as globalThis.Record<string, unknown>
    return [...indexedDefaultFields()].some((name) => record[name] === undefined)
  }

  /** Attach the model class prototype to a decoded plain object (when model is Schema.Class). */
  const attachPrototype = (decoded: any) =>
    isSchemaClass ? Object.assign(Object.create((rawModel as any).prototype), decoded) : decoded

  /**
   * An item of a versioned entity with an incarnation token but no version
   * (#133). A versioned entity stamps the token when it creates an item, so
   * only an item written before the entity was `versioned` may lack a version
   * — and it lacks the token too. Token without version: the version was
   * removed outside the library. Reading it as version 0 would let the next
   * update rewrite the item's history, so every read and write refuses it.
   */
  const versionCorruption = (
    item: Readonly<globalThis.Record<string, unknown>> | undefined,
    operation: string,
  ): ValidationError | undefined =>
    systemFields.version &&
    item !== undefined &&
    item[systemFields.version] === undefined &&
    item[INCARNATION_TOKEN] !== undefined
      ? new ValidationError({
          entityType,
          operation,
          cause:
            `The ${entityType} item has an incarnation token (${INCARNATION_TOKEN}) but no ` +
            `"${systemFields.version}" attribute: its version was removed outside the library. ` +
            "It is refused rather than read as version 0, which would let an update overwrite " +
            "its version history. Restore the version attribute.",
        })
      : undefined
  const checkVersion = (
    item: Readonly<globalThis.Record<string, unknown>> | undefined,
    operation: string,
  ): Effect.Effect<void, ValidationError> => {
    const corruption = versionCorruption(item, operation)
    return corruption === undefined ? Effect.void : Effect.fail(corruption)
  }

  const decodeRecord = (raw: globalThis.Record<string, unknown>) => {
    const corruption = versionCorruption(raw, "decode")
    if (corruption !== undefined) return Effect.fail(corruption)
    deserializeSparseFields(raw)
    renameFromDynamo(raw)
    return Schema.decodeUnknownEffect(schemas.recordSchema as Schema.Codec<any>)(raw).pipe(
      Effect.map(attachPrototype),
      Effect.mapError(
        (cause) =>
          new ValidationError({
            entityType,
            operation: "decode",
            cause,
          }),
      ),
    )
  }

  /** Decode a raw item using the schema selected by mode */
  const decodeAs = (
    raw: globalThis.Record<string, unknown>,
    marshalled: globalThis.Record<string, AttributeValue>,
    mode: DecodeMode,
  ) => {
    const corruption = versionCorruption(raw, "decode")
    if (corruption !== undefined) return Effect.fail(corruption)
    if (mode === "native") return Effect.succeed(marshalled)
    // Collect flattened sparse-map attributes back into the domain Record.
    // Must happen before rename so the rebuilt Record sits at the domain field
    // name (sparse field renaming is not supported).
    deserializeSparseFields(raw)
    // Rename DynamoDB attributes back to domain field names. Substituted
    // schemas handle wire→domain conversion (date primitives, Redacted, etc.).
    renameFromDynamo(raw)
    const targetSchema =
      mode === "model"
        ? schemas.modelSchema
        : mode === "record"
          ? schemas.recordSchema
          : schemas.itemSchema
    return Schema.decodeUnknownEffect(targetSchema as Schema.Codec<any>)(raw).pipe(
      Effect.map((decoded) =>
        mode === "model" && isSchemaClass && !hasHiddenFields
          ? new (rawModel as any)(decoded)
          : mode !== "item"
            ? attachPrototype(decoded)
            : decoded,
      ),
      Effect.mapError(
        (cause) =>
          new ValidationError({
            entityType,
            operation: "decode",
            cause,
          }),
      ),
    )
  }

  const nowIso = (now: DateTime.Utc) => DateTime.formatIso(now)

  /**
   * Generate a timestamp value as a wire primitive ready for marshalling.
   * Default (no encoding): ISO string. Custom encoding: serialized primitive.
   *
   * Used for system fields whether colliding or not — the value is always a
   * wire primitive (string or number) ready for `toAttributeMap`.
   *
   * Time source is the Clock-backed `now: DateTime.Utc` resolved at the write
   * op's entry (`yield* DateTime.now`) and threaded in — no `Date` constructor,
   * so timestamps are deterministic under `TestClock`. Delegates to the shared
   * {@link generateTimestampPrimitive} so the Batch/Transaction path produces
   * identical output.
   */
  const generateTimestamp = (encoding: DynamoEncoding | null, now: DateTime.Utc): string | number =>
    generateTimestampPrimitive(now, encoding)

  // ---------------------------------------------------------------------------
  // Lifecycle config helpers
  // ---------------------------------------------------------------------------

  const isRetainEnabled = (): boolean =>
    typeof config.versioned === "object" &&
    config.versioned !== null &&
    (config.versioned as { retain?: boolean }).retain === true

  const isSoftDeleteEnabled = (): boolean =>
    config.softDelete === true ||
    (typeof config.softDelete === "object" && config.softDelete !== null)

  const retainTtl = (): Duration.Duration | string | undefined => {
    if (typeof config.versioned !== "object" || config.versioned === null) return undefined
    return (config.versioned as { ttl?: Duration.Duration | string }).ttl
  }

  const softDeleteTtl = (): Duration.Duration | string | undefined => {
    if (typeof config.softDelete !== "object" || config.softDelete === null) return undefined
    return (config.softDelete as { ttl?: Duration.Duration | string }).ttl
  }

  const preserveUnique = (): boolean => {
    if (typeof config.softDelete !== "object" || config.softDelete === null) return false
    return (config.softDelete as { preserveUnique?: boolean }).preserveUnique === true
  }

  /**
   * The uniqueness sentinel's guard. `attribute_not_exists` must name the
   * entity's CONFIGURED partition-key attribute — a literal `pk` is simply
   * absent from an entity declaring `pk: { field: "PK" }`, so the condition is
   * vacuously true and the constraint is silently unenforced (#111). Routed
   * through `ExpressionAttributeNames` because the field name is user-supplied
   * and may be a reserved word.
   */
  const sentinelGuard = (): {
    readonly ConditionExpression: string
    readonly ExpressionAttributeNames: globalThis.Record<string, string>
  } => ({
    ConditionExpression: "attribute_not_exists(#sentinel_pk)",
    ExpressionAttributeNames: { "#sentinel_pk": config.indexes.primary.pk.field },
  })

  /**
   * The RESTORE-time sentinel guard.
   *
   * `restore` re-establishes one sentinel per satisfiable constraint. With
   * `preserveUnique` off the delete released them, so `attribute_not_exists` is
   * exactly right: the row is gone and anyone may have taken the value since.
   *
   * With `softDelete: { preserveUnique: true }` the delete deliberately KEPT the
   * reservation, so `attribute_not_exists` can never hold and every restore of a
   * constrained entity was cancelled and reported as a `UniqueConstraintViolation`
   * against its own reservation — `preserveUnique` made `restore` impossible.
   * The guard must instead let the row re-claim the sentinel it still owns while
   * still refusing one somebody else holds, which is what the stored
   * `_entity_pk`/`_entity_sk` back-pointer is for: they name the item the
   * reservation belongs to, and they are the same values this Put writes.
   */
  const restoreSentinelGuard = (
    entityPk: unknown,
    entitySk: unknown,
  ): {
    readonly ConditionExpression: string
    readonly ExpressionAttributeNames: globalThis.Record<string, string>
    readonly ExpressionAttributeValues?: globalThis.Record<string, AttributeValue>
  } => {
    const pkField = config.indexes.primary.pk.field
    if (!preserveUnique()) {
      return {
        ConditionExpression: "attribute_not_exists(#pk)",
        ExpressionAttributeNames: { "#pk": pkField },
      }
    }
    return {
      ConditionExpression: "attribute_not_exists(#pk) OR (#epk = :epk AND #esk = :esk)",
      ExpressionAttributeNames: {
        "#pk": pkField,
        "#epk": "_entity_pk",
        "#esk": "_entity_sk",
      },
      ExpressionAttributeValues: {
        ":epk": toAttributeValue(entityPk),
        ":esk": toAttributeValue(entitySk),
      },
    }
  }

  /** Collect all key field names (pk, sk, gsi*pk, gsi*sk) */
  const gsiKeyFields = (): ReadonlyArray<string> => {
    const fields: Array<string> = []
    for (const [indexName, indexDef] of Object.entries(allIndexes)) {
      if (indexName === "primary") continue
      fields.push(indexDef.pk.field, indexDef.sk.field)
    }
    return fields
  }

  // ---------------------------------------------------------------------------
  // Vector search — write-path helpers (see `DESIGN.md §14`)
  // ---------------------------------------------------------------------------

  const vectorIndexes = data.vectorIndexes
  const hasVectorIndexes = data.hasVectorIndexes
  const vectorIndexEntries: ReadonlyArray<readonly [string, VectorIndexDefinition]> =
    Object.entries(vectorIndexes)

  /**
   * Vector attributes a write should SET, and those it should REMOVE.
   *
   * REMOVE is how an item leaves a vector index — DynamoDB's sparse semantics
   * mean an item without the embedding (or without the composed HASH value) is
   * simply not indexed. There is no "delete from index" call.
   */
  interface VectorWriteResult {
    readonly sets: globalThis.Record<string, unknown>
    readonly removes: ReadonlyArray<string>
  }

  /**
   * Library-managed vector attribute names (embedding + composed partition).
   *
   * These join `gsiKeyFields()` in every strip set — version snapshots,
   * soft-delete tombstones, and time-series event items. DynamoDB's sparse
   * vector index semantics do the rest: an item without the vector attribute
   * (or without the HASH attribute) is simply not in the index, so stripping IS
   * the delete. See `DESIGN.md §14 Lifecycle integration`.
   */
  const vectorKeyFields = (): ReadonlyArray<string> => {
    const fields: Array<string> = []
    for (const [, definition] of vectorIndexEntries) {
      fields.push(definition.vectorField, definition.partitionField)
    }
    return fields
  }

  /**
   * Resolve the `Embedder` service from context.
   *
   * Deliberately resolved with `Effect.serviceOption` rather than a plain
   * `yield* Embedder`: only entities that declare `vectorIndexes` need one, and
   * widening every entity operation's `R` to include `Embedder` would force the
   * service on consumers who will never call it. A missing embedder surfaces as
   * a tagged {@link EmbeddingError} naming the index, not a type error at the
   * far end of the program.
   */
  const resolveEmbedder = (
    indexName: string,
  ): Effect.Effect<EmbedderService, EmbeddingError, never> =>
    Effect.flatMap(Effect.serviceOption(Embedder), (maybe) =>
      Option.isSome(maybe)
        ? Effect.succeed(maybe.value)
        : Effect.fail(
            new EmbeddingError({
              entityType,
              index: indexName,
              reason:
                `No Embedder service is available. Provide one via ` +
                `DynamoClient.make({ embedder }) or Effect.provide(Embedder.layerTest(...)), ` +
                `or supply a pre-computed vector with .withVector("${indexName}", [...]).`,
            }),
          ),
    )

  /**
   * Reject `.withVector("typo", ...)` before it silently does nothing.
   *
   * The combinator takes a plain string at runtime, and an unknown logical name
   * would otherwise be dropped by the declared-name lookup — the Embedder would
   * run anyway and the caller's supplied vector would vanish without a trace.
   * Fails fast with the list of declared names instead.
   */
  const checkWithVectorNames = (
    withVectors: globalThis.Record<string, ReadonlyArray<number>> | undefined,
    operation: string,
  ): Effect.Effect<void, ValidationError, never> => {
    if (withVectors === undefined) return Effect.void
    for (const name of Object.keys(withVectors)) {
      if (name in vectorIndexes) continue
      const declared = Object.keys(vectorIndexes).sort().join(", ")
      return Effect.fail(
        new ValidationError({
          entityType,
          operation: `${operation}.withVector`,
          cause:
            `Unknown vector index "${name}". Declared vector indexes: ` +
            `${declared.length > 0 ? declared : "(none)"}. ` +
            `\`.withVector()\` takes the LOGICAL name from Entity.make({ vectorIndexes }).`,
        }),
      )
    }
    return Effect.void
  }

  /** @internal Validate an embedding against the index's declared dimensionality. */
  const checkDimensions = (
    indexName: string,
    definition: VectorIndexDefinition,
    vector: ReadonlyArray<number>,
  ): Effect.Effect<ReadonlyArray<number>, EmbeddingError, never> =>
    vector.length === definition.dimensions
      ? Effect.succeed(vector)
      : Effect.fail(
          new EmbeddingError({
            entityType,
            index: indexName,
            reason:
              `Embedding has ${vector.length} dimensions but vector index "${definition.index}" ` +
              `declares ${definition.dimensions}. Dimensions are immutable on a DynamoDB vector index.`,
          }),
        )

  /**
   * Compute the vector + partition attributes for a full (put-style) record.
   *
   * Each index is independent and sparse: an index whose partition composites
   * or source fields are absent from the record contributes nothing, and the
   * item is simply not in that index.
   *
   * `embedFor` restricts which indexes are (re-)embedded — `"all"` on the put
   * path, a gated subset on the update path. The partition value is always
   * recomposed when it can be: it is idempotent, and writing it is what keeps
   * an item in the index.
   */
  const computeVectorAttributes = (
    record: globalThis.Record<string, unknown>,
    withVectors: globalThis.Record<string, ReadonlyArray<number>> | undefined,
    embedFor: ReadonlySet<string> | "all",
  ): Effect.Effect<VectorWriteResult, EmbeddingError, never> =>
    Effect.gen(function* () {
      const sets: globalThis.Record<string, unknown> = {}
      const removes: Array<string> = []
      for (const [logicalName, definition] of vectorIndexEntries) {
        const shouldEmbed = embedFor === "all" || embedFor.has(logicalName)
        const partition = KeyComposer.tryComposeVectorPartition(
          schema,
          entityType,
          definition,
          keyForm(record),
        )
        if (partition === undefined) {
          // No partition value ⇒ the item cannot be in the index at all. Drop
          // both attributes so a previously-indexed item leaves the index
          // rather than lingering under a stale partition.
          removes.push(definition.partitionField, definition.vectorField)
          continue
        }
        sets[definition.partitionField] = partition

        if (!shouldEmbed) continue

        const explicit = withVectors?.[logicalName]
        if (explicit !== undefined) {
          sets[definition.vectorField] = yield* checkDimensions(logicalName, definition, explicit)
          continue
        }
        const text = deriveSourceText(definition, record)
        if (text === undefined) {
          // Every source field is gone. Sparse semantics say the item should
          // leave the index — keeping the previous embedding would make it
          // findable by a description it no longer has.
          removes.push(definition.vectorField, definition.partitionField)
          delete sets[definition.partitionField]
          continue
        }
        const embedder = yield* resolveEmbedder(logicalName)
        const embedded = yield* embedder.embed(text)
        sets[definition.vectorField] = yield* checkDimensions(logicalName, definition, embedded)
      }
      return { sets, removes }
    })

  /**
   * Which vector indexes does this update require work for?
   *
   * Re-embedding fires when the write touches a `source.fields` member by ANY
   * channel — a `set()` payload entry, an `Entity.remove([...])` clearing it, or
   * a path-based operation whose root segment names it — or when the caller
   * supplied an explicit vector.
   *
   * Enumerating every channel (rather than only the SET payload) is the same
   * discipline the policy-aware GSI composer applies to `removedSet` (§7): a
   * clear is a change, and a writer that clears the source must not leave the
   * old embedding behind. Writers that touch none of it neither pay for an
   * embedding call nor clobber a vector another writer owns.
   */
  const vectorIndexesNeedingUpdate = (
    touchedFields: ReadonlySet<string>,
    withVectors: globalThis.Record<string, ReadonlyArray<number>> | undefined,
  ): ReadonlyArray<readonly [string, VectorIndexDefinition]> =>
    vectorIndexEntries.filter(
      ([logicalName, definition]) =>
        withVectors?.[logicalName] !== undefined ||
        definition.sourceFields.some((field) => touchedFields.has(field)),
    )

  /**
   * Every top-level attribute this update touches, by any channel.
   *
   * `touchesSource` alone only sees the SET payload; `remove([...])`, null
   * clears and path-based operations are just as capable of invalidating an
   * embedding, so they all feed the same set.
   */
  const collectTouchedFields = (
    updatePayload: globalThis.Record<string, unknown>,
    uState: UpdateState,
  ): ReadonlySet<string> => {
    const touched = new Set<string>(Object.keys(updatePayload))
    for (const attr of uState.remove ?? []) touched.add(attr)
    const pathOps: ReadonlyArray<ReadonlyArray<{ segments: ReadonlyArray<string | number> }>> = [
      uState.pathSets ?? [],
      uState.pathAdds ?? [],
      uState.pathSubtracts ?? [],
      uState.pathAppends ?? [],
      uState.pathPrepends ?? [],
      uState.pathIfNotExists ?? [],
      uState.pathDeletes ?? [],
    ]
    for (const ops of pathOps) {
      for (const op of ops) {
        const root = op.segments[0]
        if (typeof root === "string") touched.add(root)
      }
    }
    for (const segments of uState.pathRemoves ?? []) {
      const root = segments[0]
      if (typeof root === "string") touched.add(root)
    }
    return touched
  }

  /**
   * Compute vector-related SET / REMOVE attributes for the standard
   * (UpdateItem) path.
   *
   * When re-embedding is required and the payload does not carry every source
   * field, the current item is read once so the embedding is derived from the
   * complete post-update source text rather than the fragment that happened to
   * be in this payload. Attributes the update clears (via `remove([...])` or a
   * null payload entry) are subtracted from that merged record, so "clear the
   * description" produces the embedding of what actually remains — or drops the
   * item out of the index when nothing does.
   */
  const computeVectorUpdateAttributes = (
    encodedKey: globalThis.Record<string, unknown>,
    marshalledKey: globalThis.Record<string, AttributeValue>,
    tableName: string,
    updatePayload: globalThis.Record<string, unknown>,
    uState: UpdateState,
  ): Effect.Effect<VectorWriteResult, EmbeddingError | DynamoClientError, DynamoClient> =>
    Effect.gen(function* () {
      if (!hasVectorIndexes) return { sets: {}, removes: [] }
      const withVectors = uState.withVectors
      const touchedFields = collectTouchedFields(updatePayload, uState)
      const needing = vectorIndexesNeedingUpdate(touchedFields, withVectors)
      const clearedFields = new Set<string>(uState.remove ?? [])
      for (const [attr, value] of Object.entries(updatePayload)) {
        if (value === null || value === undefined) clearedFields.add(attr)
      }

      // Base record: key composites plus whatever the payload supplies. Enough
      // to compose partition values (their composites are normally primary-key
      // members) without any extra read.
      let merged: globalThis.Record<string, unknown> = { ...encodedKey, ...updatePayload }

      const needsFullSource = needing.some(
        ([logicalName, definition]) =>
          withVectors?.[logicalName] === undefined &&
          definition.sourceFields.some((field) => !(field in merged)),
      )
      if (needsFullSource) {
        const client = yield* DynamoClient
        const current = yield* client.getItem({ TableName: tableName, Key: marshalledKey })
        if (current.Item) {
          const currentDomain = fromAttributeMap(current.Item) as globalThis.Record<string, unknown>
          renameFromDynamo(currentDomain)
          merged = { ...currentDomain, ...merged }
        }
      }
      // Cleared attributes are absent post-update, whichever channel cleared
      // them — the same two-way classification the GSI composer uses (§7).
      for (const attr of clearedFields) delete merged[attr]

      const sets: globalThis.Record<string, unknown> = {}
      const removes: Array<string> = []
      for (const [logicalName, definition] of vectorIndexEntries) {
        const isNeeded = needing.some(([name]) => name === logicalName)
        // Partition value: idempotent and cheap, so recompose whenever the
        // merged record can supply it (same reasoning as the PK-composites-only
        // GSI shape in §7).
        const partition = KeyComposer.tryComposeVectorPartition(
          schema,
          entityType,
          definition,
          keyForm(merged),
        )
        if (partition === undefined) {
          // Only drop the partition when THIS writer invalidated it. Otherwise
          // the composite simply wasn't in scope for this call and dropping it
          // would be the multi-writer clobber the §7 gate exists to prevent.
          if (definition.partition.some((attr) => clearedFields.has(attr))) {
            removes.push(definition.partitionField, definition.vectorField)
          }
          continue
        }
        sets[definition.partitionField] = partition

        if (!isNeeded) continue
        const explicit = withVectors?.[logicalName]
        if (explicit !== undefined) {
          sets[definition.vectorField] = yield* checkDimensions(logicalName, definition, explicit)
          continue
        }
        const text = deriveSourceText(definition, merged)
        if (text === undefined) {
          removes.push(definition.vectorField, definition.partitionField)
          delete sets[definition.partitionField]
          continue
        }
        const embedder = yield* resolveEmbedder(logicalName)
        const embedded = yield* embedder.embed(text)
        sets[definition.vectorField] = yield* checkDimensions(logicalName, definition, embedded)
      }
      return { sets, removes }
    })

  // ---------------------------------------------------------------------------
  // Update concurrency and return values (#133)
  // ---------------------------------------------------------------------------

  /**
   * What proves an item is the same INCARNATION as the one read — not a
   * deleted-and-recreated item at the same version: a hidden per-incarnation
   * token, `__edd_i__`, a random UUID set when the item is created (put,
   * create, upsert, batch / transaction put). `createdAt` would not do: two
   * creates in the same millisecond, or a caller-supplied value, repeat it.
   * Only versioned entities need it — their version CAS is what it completes.
   */
  const incarnationAttr: string | undefined = systemFields.version ? INCARNATION_TOKEN : undefined
  const stampsIncarnation = incarnationAttr === INCARNATION_TOKEN
  const freshIncarnation = freshIncarnationToken

  /**
   * The condition (and its names / values) proving the stored item is still
   * the incarnation `read` — `undefined` when the entity needs no proof.
   */
  const incarnationGuard = (
    read: globalThis.Record<string, AttributeValue>,
    names: globalThis.Record<string, string>,
    values: globalThis.Record<string, AttributeValue>,
  ): string | undefined => {
    if (incarnationAttr === undefined) return undefined
    names["#inc"] = incarnationAttr
    const value = read[incarnationAttr]
    if (value === undefined) return "attribute_not_exists(#inc)"
    values[":inc"] = value
    return "#inc = :inc"
  }

  /**
   * The version an item is stored at: `undefined` for no item, 0 for an item
   * written before the entity was `versioned` (it has no version attribute;
   * real versions start at 1).
   */
  const storedVersionOf = (
    item: Readonly<globalThis.Record<string, unknown>> | undefined,
  ): number | undefined => {
    if (item === undefined || !systemFields.version) return undefined
    const attr = item[systemFields.version]
    return attr !== undefined ? Number((attr as { readonly N?: string }).N) : 0
  }

  /**
   * A version-snapshot Put that never overwrites history (#133). The
   * `v#<version>` row may already exist only as a snapshot of the SAME state —
   * a retain `put` writes `v#0000001` and the first update snapshots that same
   * version 1 state again; a restore re-writes the delete-time snapshot — so
   * the row is replaced only when it holds the same version, the same
   * incarnation and (with timestamps) the same `updatedAt`. Any other row is a
   * different history, and the write is refused.
   */
  const snapshotPut = (tableName: string, snapshotItem: globalThis.Record<string, unknown>) => {
    const names: globalThis.Record<string, string> = { "#snap": config.indexes.primary.pk.field }
    const values: globalThis.Record<string, AttributeValue> = {}
    const same: Array<string> = []
    if (systemFields.version) {
      const version = snapshotItem[systemFields.version]
      same.push(
        versionIs(typeof version === "number" ? version : 0, names, values, "#snapVer", ":snapVer"),
      )
    }
    const sameAs = (attr: string, tag: string) => {
      names[`#${tag}`] = attr
      const value = snapshotItem[attr]
      if (value === undefined) {
        same.push(`attribute_not_exists(#${tag})`)
      } else {
        values[`:${tag}`] = toAttributeValue(value)
        same.push(`#${tag} = :${tag}`)
      }
    }
    sameAs(INCARNATION_TOKEN, "snapInc")
    if (systemFields.updatedAt) sameAs(systemFields.updatedAt, "snapUpd")
    return {
      TableName: tableName,
      Item: toAttributeMap(snapshotItem),
      ConditionExpression: `attribute_not_exists(#snap) OR (${same.join(" AND ")})`,
      ExpressionAttributeNames: names,
      ...(Object.keys(values).length > 0 && { ExpressionAttributeValues: values }),
    }
  }
  /** A version snapshot Put refused: that version's history row already exists. */
  const historyConflict = (version: number, operation: string): ValidationError =>
    new ValidationError({
      entityType,
      operation,
      cause:
        `The ${entityType} version ${version} snapshot already exists: the item's version is ` +
        "inconsistent with its history, and history is never overwritten. Nothing was written.",
    })

  /**
   * The condition that the stored version is `version` — for version 0 (an
   * item written before the entity was `versioned`), that it has none (#133).
   */
  const versionIs = (
    version: number,
    names: globalThis.Record<string, string>,
    values: globalThis.Record<string, AttributeValue>,
    name: string,
    value: string,
  ): string => {
    names[name] = systemFields.version!
    if (version === 0) return `attribute_not_exists(${name})`
    values[value] = toAttributeValue(version)
    return `${name} = ${value}`
  }

  /**
   * Which predicate rejected a version-conditioned (or user-conditioned)
   * write. The write asked for `ReturnValuesOnConditionCheckFailure: "ALL_OLD"`,
   * so DynamoDB hands back the item as stored: a version other than the one
   * the write was conditioned on — or another incarnation at that version — is
   * a lost race; otherwise the user's `.condition()` rejected it. No item back
   * means the item is gone — a race when the write was version-conditioned.
   */
  const conditionRejection = (
    key: globalThis.Record<string, unknown>,
    stored: Readonly<globalThis.Record<string, unknown>> | undefined,
    cas:
      | {
          /** `undefined`: the item read had no version (written before `versioned`). */
          readonly version: number | undefined
          /** The item read, when the write also proved its incarnation. */
          readonly read?: globalThis.Record<string, AttributeValue> | undefined
        }
      | undefined,
    userCondition: boolean,
  ): OptimisticLockError | ConditionalCheckFailed => {
    const storedVersion = storedVersionOf(stored)
    const otherIncarnation =
      cas?.read !== undefined &&
      incarnationAttr !== undefined &&
      !attributeValueEquals(
        stored?.[incarnationAttr] as AttributeValue | undefined,
        cas.read[incarnationAttr],
      )
    if (
      (cas !== undefined && (storedVersion !== cas.version || otherIncarnation)) ||
      !userCondition
    ) {
      return new OptimisticLockError({
        entityType,
        key,
        expectedVersion: cas?.version ?? -1,
        actualVersion: storedVersion ?? -1,
      })
    }
    return new ConditionalCheckFailed({ entityType, key })
  }

  /**
   * A condition the library sends — its own guard plus the user's
   * `.condition()` — that exceeds DynamoDB's limits on one expression (4 KB,
   * 300 operators and functions): refused BEFORE writing (#133).
   */
  const oversizedCondition = (
    operation: string,
    condition: string | undefined,
    user: string | undefined,
  ): ValidationError | undefined => {
    if (condition === undefined || expressionFits(condition)) return undefined
    const own = expressionCost(user)
    return new ValidationError({
      entityType,
      operation,
      cause:
        `The ${operation}'s condition is ${expressionCost(condition).length} characters / ` +
        `${expressionCost(condition).operators} operators — beyond DynamoDB's limits on one ` +
        `expression (${EXPRESSION_LIMIT} characters, ${OPERATOR_LIMIT} operators). The ` +
        `.condition() alone is ${own.length} characters / ${own.operators} operators, and ` +
        "the library's own guard needs room beside it: simplify the condition.",
    })
  }

  /** The domain field a stored attribute holds (`storedAs` renames reversed). */
  const domainNameOf = (attr: string): string =>
    Object.keys(data.modelFields as object).find((field) => resolveDbName(field) === attr) ?? attr

  /**
   * The condition a read-then-write DELETE carries (#133): the item is still
   * what the tombstone, snapshot and sentinel deletes were derived from.
   * Versioned: its version and incarnation. Otherwise: it exists and every
   * attribute in `dependsOn` still holds the value read (absent ones still
   * absent). `"item"` means the whole item — a tombstone copies all of it —
   * so every stored attribute is guarded, plus every attribute the entity
   * knows of that was absent; an item too wide for that falls back to the
   * strongest guard within DynamoDB's expression limit (never a refusal).
   */
  const deleteGuard = (
    read: globalThis.Record<string, AttributeValue>,
    dependsOn: "item" | ReadonlyArray<string>,
    /** The rest of the condition, ANDed with the guard (the user's, …). */
    others: string | undefined,
  ):
    | {
        readonly expression: string
        readonly names: globalThis.Record<string, string>
        readonly values: globalThis.Record<string, AttributeValue>
        readonly inputs: ReadonlyArray<string>
      }
    | ValidationError => {
    // Whether a guard fits beside `others` within DynamoDB's real limits.
    const fits = (guard: string) =>
      expressionFits(others === undefined || others === "" ? guard : `${guard} AND (${others})`)
    const names: globalThis.Record<string, string> = {}
    const values: globalThis.Record<string, AttributeValue> = {}
    const parts: Array<string> = []
    const pkField = config.indexes.primary.pk.field
    const skField = config.indexes.primary.sk.field
    if (systemFields.version) {
      names["#dver"] = systemFields.version
      // An item written before the entity was `versioned` has no version.
      const readVersion = read[systemFields.version]
      if (readVersion === undefined) {
        parts.push("attribute_not_exists(#dver)")
      } else {
        values[":dver"] = readVersion
        parts.push("#dver = :dver")
      }
      const incarnation = incarnationGuard(read, names, values)
      if (incarnation !== undefined) parts.push(incarnation)
      const expression = parts.join(" AND ")
      if (!fits(expression)) return tooLargeBeside(expression)
      return { expression, names, values, inputs: [] }
    }
    names["#dpk"] = pkField
    parts.push("attribute_exists(#dpk)")
    if (!fits(parts.join(" AND "))) return tooLargeBeside(parts.join(" AND "))
    const attrs =
      dependsOn === "item"
        ? [
            ...new Set([
              ...Object.keys(read),
              ...Object.keys(data.modelFields as object).map(resolveDbName),
              ...[systemFields.createdAt, systemFields.updatedAt].filter(
                (f): f is string => typeof f === "string",
              ),
              ...gsiKeyFields(),
              ...vectorKeyFields(),
            ]),
          ].filter((attr) => attr !== pkField && attr !== skField)
        : dependsOn
    const guardOf = (attr: string, i: number): string => {
      names[`#dg${i}`] = attr
      if (read[attr] === undefined) return `attribute_not_exists(#dg${i})`
      values[`:dg${i}`] = read[attr]!
      return `#dg${i} = :dg${i}`
    }
    const full = [...parts, ...attrs.map(guardOf)].join(" AND ")
    if (fits(full)) return { expression: full, names, values, inputs: attrs }
    // Too wide to guard attribute by attribute (a sparse map with hundreds of
    // entries): the strongest guard that fits. Every library write sets
    // `updatedAt`, so with timestamps it alone detects any library update;
    // then as many attributes as fit, those the sentinels derive from first,
    // then the model's own fields, then the rest.
    for (const key of Object.keys(names)) if (key.startsWith("#dg")) delete names[key]
    for (const key of Object.keys(values)) if (key.startsWith(":dg")) delete values[key]
    const uniqueAttrs = Object.values(config.unique ?? {}).flatMap((def) =>
      resolveUniqueFields(def as UniqueConstraintDef).map(resolveDbName),
    )
    const modelAttrs = Object.keys(data.modelFields as object).map(resolveDbName)
    const ordered = [
      ...new Set([
        ...(systemFields.updatedAt ? [systemFields.updatedAt] : []),
        ...uniqueAttrs,
        ...modelAttrs,
        ...attrs.slice().sort(),
      ]),
    ].filter((attr) => attrs.includes(attr))
    const inputs: Array<string> = []
    let expression = parts.join(" AND ")
    for (const attr of ordered) {
      const clause = guardOf(attr, inputs.length)
      if (!fits(`${expression} AND ${clause}`)) {
        delete names[`#dg${inputs.length}`]
        delete values[`:dg${inputs.length}`]
        continue
      }
      expression = `${expression} AND ${clause}`
      inputs.push(attr)
    }
    return { expression, names, values, inputs }

    function tooLargeBeside(guard: string): ValidationError {
      return (
        oversizedCondition(
          "write",
          others === undefined || others === "" ? guard : `${guard} AND (${others})`,
          others,
        ) ?? new ValidationError({ entityType, operation: "write", cause: "condition too large" })
      )
    }
  }

  /** Which predicate rejected a guarded delete — from the stored item (ALL_OLD). */
  const deleteRejection = (
    key: globalThis.Record<string, unknown>,
    read: globalThis.Record<string, AttributeValue>,
    inputs: ReadonlyArray<string>,
    stored: Readonly<globalThis.Record<string, unknown>> | undefined,
    userCondition: boolean,
  ): ItemNotFound | OptimisticLockError | ConcurrentModification | ConditionalCheckFailed => {
    if (stored === undefined) return new ItemNotFound({ entityType, key })
    if (systemFields.version) {
      return conditionRejection(
        key,
        stored,
        { version: storedVersionOf(read), read },
        userCondition,
      )
    }
    const changed = inputs.filter(
      (attr) => !attributeValueEquals(stored[attr] as AttributeValue | undefined, read[attr]),
    )
    if (changed.length === 0 && userCondition)
      return new ConditionalCheckFailed({ entityType, key })
    return new ConcurrentModification({
      entityType,
      key,
      attributes: changed.map(domainNameOf),
      current: Option.none(),
    })
  }

  /** Fill a `ConcurrentModification`'s `current` with the stored item, decoded. */
  const withCurrentItem = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    stored: () => globalThis.Record<string, AttributeValue> | undefined,
  ) =>
    effect.pipe(
      Effect.catchIf(
        (e): e is E & ConcurrentModification => e instanceof ConcurrentModification,
        (e) =>
          Effect.gen(function* () {
            const item = stored()
            const current =
              item === undefined
                ? Option.none()
                : yield* Effect.option(
                    decodeAs(
                      fromAttributeMap(item) as globalThis.Record<string, unknown>,
                      item,
                      "model",
                    ),
                  )
            return yield* keepReleaseRace(e, new ConcurrentModification({ ...e, current }))
          }),
      ),
    )

  /**
   * The item a retain path update wrote, read back after its transaction. The
   * write is proven by version AND incarnation: within one incarnation a
   * version is written exactly once, so the live item is ours while it holds
   * our version and incarnation; once a later write replaced it, the
   * `v#<ourVersion>` snapshot that write took is ours — if it carries our
   * incarnation (a snapshot left by an earlier incarnation of the item does
   * not). It is restored to item shape: what a snapshot strips (index and
   * vector attributes) or adds (its sort key and TTL) comes from the item we
   * replaced plus this update's own writes. Without a proven post-image the
   * update still WAS applied: {@link UpdateAppliedButUnreadable}.
   */
  const readRetainPostImage = (
    tableName: string,
    marshalledKey: globalThis.Record<string, AttributeValue>,
    replaced: globalThis.Record<string, AttributeValue>,
    ourVersion: number,
    ourIncarnation: AttributeValue | undefined,
    derivedSets: globalThis.Record<string, AttributeValue>,
    derivedRemoves: ReadonlySet<string>,
    ttlAttrName: string,
    encodedKey: globalThis.Record<string, unknown>,
  ) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const ours = (item: globalThis.Record<string, AttributeValue> | undefined) =>
        item !== undefined &&
        (incarnationAttr === undefined ||
          attributeValueEquals(item[incarnationAttr], ourIncarnation))
      const current = (yield* client.getItem({
        TableName: tableName,
        Key: marshalledKey,
        ConsistentRead: true,
      })).Item
      if (storedVersionOf(current) === ourVersion && ours(current)) return current!
      const skField = config.indexes.primary.sk.field
      const snapshot = (yield* client.getItem({
        TableName: tableName,
        Key: {
          ...marshalledKey,
          [skField]: toAttributeValue(
            DynamoSchema.composeVersionKey(
              schema,
              entityType,
              ourVersion,
              historyKeyOptions(marshalledKey[skField]?.S),
            ),
          ),
        },
        ConsistentRead: true,
      })).Item
      if (!ours(snapshot)) {
        return yield* new UpdateAppliedButUnreadable({
          entityType,
          key: encodedKey,
          version: ourVersion,
          reason:
            current === undefined
              ? "the item was deleted before it could be read back"
              : `the item moved past version ${ourVersion}, and no snapshot of this write ` +
                "remains to read it from",
        })
      }
      const post: globalThis.Record<string, AttributeValue> = { ...snapshot! }
      post[skField] = marshalledKey[skField]!
      const restored = [
        ...gsiKeyFields(),
        ...vectorKeyFields(),
        ...vectorIndexEntries.map(([, definition]) => definition.stashField),
        ...(retainTtl() ? [ttlAttrName] : []),
      ]
      for (const field of restored) {
        delete post[field]
        if (replaced[field] !== undefined) post[field] = replaced[field]
      }
      Object.assign(post, derivedSets)
      for (const field of derivedRemoves) delete post[field]
      return post
    })

  /**
   * Decode only `attributes` of a stored image (`updatedOld` / `updatedNew`):
   * a partial — the fields present, each through its own schema.
   */
  const decodePartial = (
    image: globalThis.Record<string, AttributeValue> | undefined,
    attributes: ReadonlySet<string>,
    mode: DecodeMode,
  ) => {
    const corruption = versionCorruption(image, "decode")
    if (corruption !== undefined) return Effect.fail(corruption)
    const projected: globalThis.Record<string, AttributeValue> = {}
    for (const [attr, value] of Object.entries(image ?? {})) {
      if (attributes.has(attr)) projected[attr] = value
    }
    if (mode === "native") return Effect.succeed(projected)
    const raw = fromAttributeMap(projected) as globalThis.Record<string, unknown>
    deserializeSparseFields(raw)
    renameFromDynamo(raw)
    const target =
      mode === "model"
        ? schemas.modelSchema
        : mode === "record"
          ? schemas.recordSchema
          : schemas.itemSchema
    const fields = (target as unknown as { readonly fields: Schema.Struct.Fields }).fields
    const present = Object.fromEntries(Object.entries(fields).filter(([name]) => name in raw))
    return Schema.decodeUnknownEffect(Schema.Struct(present) as unknown as Schema.Codec<any>)(
      raw,
    ).pipe(
      Effect.mapError((cause) => new ValidationError({ entityType, operation: "decode", cause })),
    )
  }
  /**
   * The value an update returns for its `returnValues` mode, from the item
   * before (`old`) and after (`next`) it, and the attributes it wrote:
   * `none` → `undefined`; `allOld` / `allNew` → the whole item; `updatedOld` /
   * `updatedNew` → just the written attributes, decoded as a partial.
   */
  const updateResult = (
    rv: ReturnValuesMode,
    old: globalThis.Record<string, AttributeValue> | undefined,
    next: globalThis.Record<string, AttributeValue> | undefined,
    written: ReadonlySet<string>,
    mode: DecodeMode,
    encodedKey: globalThis.Record<string, unknown>,
  ): Effect.Effect<unknown, ValidationError | ItemNotFound> => {
    switch (rv) {
      case "none":
        return Effect.succeed(undefined)
      case "updatedOld":
        return decodePartial(old, written, mode)
      case "updatedNew":
        return decodePartial(next, written, mode)
      default: {
        const image = rv === "allOld" ? old : next
        if (image === undefined)
          return Effect.fail(new ItemNotFound({ entityType, key: encodedKey }))
        return decodeAs(fromAttributeMap(image) as globalThis.Record<string, unknown>, image, mode)
      }
    }
  }

  /**
   * The history keys (version snapshots, soft-delete tombstones) of the item
   * whose live sort key is `liveSk` (#133). An entity whose primary sort key
   * has composites holds several items per partition, so each item's history
   * keys carry its segment — the composite part of `liveSk` — and items never
   * share a version sequence, a snapshot or a tombstone. Without sort key
   * composites there is one item per partition and no segment: the keys are
   * byte-identical to the partition-wide ones every earlier release wrote.
   */
  const historyKeyOptions = (liveSk: unknown): DynamoSchema.HistoryKeyOptions | undefined => {
    if (typeof liveSk !== "string") return undefined
    const item = KeyComposer.composeHistoryItemSegment(
      schema,
      entityType,
      entityVersion,
      config.indexes.primary,
      liveSk,
    )
    return item === undefined ? undefined : { item }
  }
  /**
   * See {@link Entity._liveRows}. A row is dropped only when it is positively
   * history: its sort key is not the one its own composites compose (or they
   * don't compose) AND it has the layout of a snapshot (`#v#…<version>`), a
   * tombstone (`#deleted#…<timestamp>`) or a time-series event (`#e#` under
   * the live key). Any other row is kept and decoded as before — including
   * rows an earlier release keyed differently (an unpadded number composite),
   * which the current composer can't reproduce.
   */
  const liveRows = (): Query.LiveRows | undefined => {
    if (!isRetainEnabled() && !isSoftDeleteEnabled() && config.timeSeries === undefined) {
      return undefined
    }
    const primary = config.indexes.primary
    const skField = primary.sk.field
    const compositeAttrs = primary.sk.composite.map(resolveDbName)
    const versions = DynamoSchema.composeVersionKeyPrefix(schema, entityType)
    const tombstones = DynamoSchema.composeDeletedKeyPrefix(schema, entityType)
    const bare = KeyComposer.composeSk(
      schema,
      entityType,
      entityVersion,
      { ...primary, sk: { ...primary.sk, composite: [] } },
      keyForm({}),
    )
    const eventMarker = KeyComposer.composeEventSkPrefix("", schema.casing)
    const historyShaped = (sk: string): boolean =>
      (isRetainEnabled() &&
        sk.startsWith(versions) &&
        /^(?:.*#)?\d{7,}$/.test(sk.slice(versions.length))) ||
      (isSoftDeleteEnabled() &&
        sk.startsWith(tombstones) &&
        /^(?:.*#)?\d{4}-\d\d-\d\dT[^#]*$/.test(sk.slice(tombstones.length))) ||
      (config.timeSeries !== undefined &&
        sk.startsWith(bare) &&
        sk.slice(bare.length).includes(eventMarker))
    return {
      isLive: (row) => {
        const sk = row[skField]?.S
        if (sk === undefined) return true
        // Only the composites are unmarshalled: the row may be large.
        const composites: globalThis.Record<string, AttributeValue> = {}
        for (const attr of compositeAttrs) {
          const value = row[attr]
          if (value !== undefined) composites[attr] = value
        }
        try {
          if (liveSkOf(toDomainView(fromAttributeMap(composites))) === sk) return true
        } catch {
          // Not composable now: judged by its layout alone.
        }
        return !historyShaped(sk)
      },
      reads: [skField, ...compositeAttrs],
    }
  }
  // ---------------------------------------------------------------------------
  // History an earlier release wrote without an item segment (#133)
  // ---------------------------------------------------------------------------

  /**
   * Whether the entity's history keys carry an item segment — its primary sort
   * key has composites. Only then can history an earlier release wrote, under
   * the partition-wide keys (`#v#0000003`, `#deleted#<ts>`), be in the
   * partition: every reader of one item's history also reads those rows, and
   * takes the ones whose stored composites compose the item's live key. A
   * version held both ways is read from the segmented row.
   */
  const hasItemHistory = config.indexes.primary.sk.composite.length > 0
  /** Whether a stored (attribute-keyed) row is the history of the item at `liveSk`. */
  const isItemsRow = (row: globalThis.Record<string, AttributeValue>, liveSk: string): boolean => {
    try {
      return liveSkOf(toDomainView(fromAttributeMap(row))) === liveSk
    } catch {
      return false
    }
  }
  /**
   * The item's unsegmented history rows of one kind, in sort key order (latest
   * first with `descending`), stopping at the first with `first`. Unsegmented
   * rows sort before every segmented one — a version or timestamp starts with
   * a digit, a segment with a composite name — so a key range bounds them.
   */
  const legacyHistory = (args: {
    readonly tableName: string
    readonly pk: unknown
    readonly liveSk: string
    readonly kind: "version" | "deleted"
    readonly descending?: boolean
    readonly first?: boolean
  }) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const primary = config.indexes.primary
      const prefix =
        args.kind === "version"
          ? DynamoSchema.composeVersionKeyPrefix(schema, entityType)
          : DynamoSchema.composeDeletedKeyPrefix(schema, entityType)
      const range = {
        TableName: args.tableName,
        KeyConditionExpression: "#pk = :pk AND #sk BETWEEN :lo AND :hi",
        ExpressionAttributeNames: { "#pk": primary.pk.field, "#sk": primary.sk.field },
        ExpressionAttributeValues: {
          ":pk": toAttributeValue(args.pk),
          ":lo": toAttributeValue(`${prefix}0`),
          ":hi": toAttributeValue(`${prefix}:`),
        },
        ConsistentRead: true,
      }
      // The common case — a partition no earlier release wrote history into —
      // costs one `Limit 1` keys-only read. (A row it finds may still be
      // segmented, if a segment starts with a digit; the read below filters.)
      const probe = yield* client.query({ ...range, ProjectionExpression: "#sk", Limit: 1 })
      if ((probe.Items ?? []).length === 0) return []
      const rows: Array<globalThis.Record<string, AttributeValue>> = []
      let start: globalThis.Record<string, AttributeValue> | undefined
      do {
        const result = yield* client.query({
          ...range,
          ScanIndexForward: !args.descending,
          ExclusiveStartKey: start,
        })
        for (const row of result.Items ?? []) {
          const sk = row[primary.sk.field]?.S ?? ""
          if (sk.slice(prefix.length).includes("#") || !isItemsRow(row, args.liveSk)) continue
          rows.push(row)
          if (args.first) return rows
        }
        start = result.LastEvaluatedKey as globalThis.Record<string, AttributeValue> | undefined
      } while (start !== undefined)
      return rows
    })
  /** The version a history sort key ends with. */
  const versionOfSk = (sk: string | undefined): number => {
    if (sk === undefined) return 0
    const version = Number(sk.slice(sk.lastIndexOf("#") + 1))
    return Number.isInteger(version) ? version : 0
  }
  /**
   * The item's latest tombstone — its own, or one an earlier release wrote
   * without a segment, whichever is later (its own on a tie).
   */
  const latestTombstone = (tableName: string, pk: unknown, liveSk: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const primary = config.indexes.primary
      const own = (yield* client.query({
        TableName: tableName,
        KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :skPrefix)",
        ExpressionAttributeNames: { "#pk": primary.pk.field, "#sk": primary.sk.field },
        ExpressionAttributeValues: {
          ":pk": toAttributeValue(pk),
          ":skPrefix": toAttributeValue(
            DynamoSchema.composeDeletedKeyPrefix(schema, entityType, historyKeyOptions(liveSk)),
          ),
        },
        Limit: 1,
        ScanIndexForward: false,
      })).Items?.[0]
      if (!hasItemHistory) return own
      const [legacy] = yield* legacyHistory({
        tableName,
        pk,
        liveSk,
        kind: "deleted",
        descending: true,
        first: true,
      })
      if (legacy === undefined) return own
      if (own === undefined) return legacy
      const stamp = (row: globalThis.Record<string, AttributeValue>) => {
        const sk = row[primary.sk.field]?.S ?? ""
        return sk.slice(sk.lastIndexOf("#") + 1)
      }
      return stamp(legacy) > stamp(own) ? legacy : own
    })
  /** The live sort key of the item at `encodedKey` (the caller's key, encoded). */
  const liveSkOf = (encodedKey: unknown): string =>
    KeyComposer.composeSk(
      schema,
      entityType,
      entityVersion,
      config.indexes.primary,
      keyForm(encodedKey as globalThis.Record<string, unknown>),
    )

  /**
   * Build a version snapshot item: same PK, version SK, stripped GSI keys,
   * keeps __edd_e__ for entity type filtering.
   *
   * The TTL attribute name is resolved from the caller's TableConfig and
   * passed in — the snapshot belongs to the same physical table, so it must
   * align with that table's `TimeToLiveSpecification.AttributeName`.
   */
  const buildSnapshotItem = (
    item: globalThis.Record<string, unknown>,
    version: number,
    _tablePkField: string,
    tableSkField: string,
    ttlAttrName: string,
    now: DateTime.Utc,
    /** The item's live sort key, when `item` is not the live item (a tombstone). */
    liveSk: unknown = item[tableSkField],
  ): globalThis.Record<string, unknown> => {
    const snapshot: globalThis.Record<string, unknown> = { ...item }

    // Strip GSI key fields — snapshots must not appear in index queries
    for (const field of gsiKeyFields()) {
      delete snapshot[field]
    }
    // Same for vector + partition attributes — a snapshot is never an ANN hit.
    for (const field of vectorKeyFields()) {
      delete snapshot[field]
    }
    // And the soft-delete STASH. It is not an indexed attribute, so nothing
    // above touches it, but `restore` builds its snapshot from the tombstone —
    // which is exactly where the stash lives — so without this the `#v#N` row
    // ends up carrying the full embedding blob that the delete-time snapshot at
    // the same SK never had. A snapshot never re-enters the index, so it has no
    // use for a stashed embedding either.
    for (const [, definition] of vectorIndexEntries) {
      delete snapshot[definition.stashField]
    }

    // Replace SK with version SK
    snapshot[tableSkField] = DynamoSchema.composeVersionKey(
      schema,
      entityType,
      version,
      historyKeyOptions(liveSk),
    )

    // Add optional TTL
    const ttl = retainTtl()
    if (ttl) {
      snapshot[ttlAttrName] = DateTime.toEpochSeconds(now) + normalizeTtlSeconds(ttl)
    }

    return snapshot
  }

  // ---------------------------------------------------------------------------
  // Multi-item write support for the transact compile path (#113)
  // ---------------------------------------------------------------------------

  /**
   * Which parts of this entity's write contract need more than one item.
   * Drives both the `put` expansion and the `delete` / `Batch.write` rejections.
   */
  const multiItemWriteFeatures = (): ReadonlyArray<"unique" | "retain" | "softDelete"> => {
    const features: Array<"unique" | "retain" | "softDelete"> = []
    if (config.unique != null && Object.keys(config.unique).length > 0) features.push("unique")
    if (isRetainEnabled()) features.push("retain")
    if (isSoftDeleteEnabled()) features.push("softDelete")
    return features
  }

  // ---------------------------------------------------------------------------
  // Unique sentinels: reservation and release (#133)
  // ---------------------------------------------------------------------------

  /**
   * The sentinel item reserving `key` for the item at `owner`. `_entity_pk` /
   * `_entity_sk` name the reserving item — the back-pointer a release checks
   * (see {@link ownedSentinels}).
   */
  const sentinelItemFor = (
    constraintName: string,
    constraintDef: UniqueConstraintDef,
    key: { readonly pk: string; readonly sk: string },
    owner: { readonly pk: unknown; readonly sk: unknown },
    now: DateTime.Utc,
    ttlAttrName: string,
  ): globalThis.Record<string, unknown> => {
    const sentinel: globalThis.Record<string, unknown> = {
      [config.indexes.primary.pk.field]: key.pk,
      [config.indexes.primary.sk.field]: key.sk,
      __edd_e__: `${entityType}._unique.${constraintName}`,
      _entity_pk: owner.pk,
      _entity_sk: owner.sk,
    }
    // Optional TTL — when the constraint declares one, the sentinel
    // auto-expires (time-bounded uniqueness reservation/hold).
    const uniqueTtl = resolveUniqueTtl(constraintDef)
    if (uniqueTtl !== undefined) {
      sentinel[ttlAttrName] = DateTime.toEpochSeconds(now) + normalizeTtlSeconds(uniqueTtl)
    }
    return sentinel
  }

  /**
   * The sentinels among `candidates` that the item at `owner` holds, read
   * consistently (#133). An item can hold a unique value WITHOUT owning its
   * sentinel: the constraint was added after the item was written, the item
   * was written outside the library, or a `ttl`'d reservation expired and
   * another item then claimed the value. That other item owns the sentinel
   * now — its `_entity_pk` / `_entity_sk` name it — and releasing the
   * sentinel by key alone would delete its reservation and let the value be
   * taken twice. So a write releases only the sentinels this returns, each
   * under {@link sentinelRelease}.
   */
  const ownedSentinels = <K extends { readonly pk: string; readonly sk: string }>(
    tableName: string,
    candidates: ReadonlyArray<K>,
    owner: { readonly pk: unknown; readonly sk: unknown },
  ): Effect.Effect<ReadonlyArray<K>, DynamoClientError, DynamoClient> =>
    Effect.gen(function* () {
      if (candidates.length === 0) return []
      const client = yield* DynamoClient
      const pkField = config.indexes.primary.pk.field
      const skField = config.indexes.primary.sk.field
      const ownerPk = toAttributeValue(owner.pk)
      const ownerSk = toAttributeValue(owner.sk)
      const reads = yield* Effect.forEach(
        candidates,
        (key) =>
          client.getItem({
            TableName: tableName,
            Key: toAttributeMap({ [pkField]: key.pk, [skField]: key.sk }),
            ConsistentRead: true,
            ProjectionExpression: "#epk, #esk",
            ExpressionAttributeNames: { "#epk": "_entity_pk", "#esk": "_entity_sk" },
          }),
        { concurrency: "unbounded" },
      )
      return candidates.filter((_, i) => {
        const stored = reads[i]?.Item
        return (
          stored !== undefined &&
          attributeValueEquals(stored._entity_pk, ownerPk) &&
          attributeValueEquals(stored._entity_sk, ownerSk)
        )
      })
    })

  /**
   * The Delete releasing a sentinel {@link ownedSentinels} found owned by
   * `owner` — conditioned on that ownership still holding, so a reservation
   * that changed hands between the read and the write cancels the write
   * instead of being deleted.
   */
  const sentinelRelease = (
    tableName: string,
    key: { readonly pk: string; readonly sk: string },
    owner: { readonly pk: unknown; readonly sk: unknown },
  ) => ({
    Delete: {
      TableName: tableName,
      Key: toAttributeMap({
        [config.indexes.primary.pk.field]: key.pk,
        [config.indexes.primary.sk.field]: key.sk,
      }),
      ConditionExpression: "#epk = :epk AND #esk = :esk",
      ExpressionAttributeNames: { "#epk": "_entity_pk", "#esk": "_entity_sk" },
      ExpressionAttributeValues: {
        ":epk": toAttributeValue(owner.pk),
        ":esk": toAttributeValue(owner.sk),
      },
    },
  })

  /**
   * A sentinel release cancelled: the reservation changed hands between the
   * read and the write. Nothing was written; the item still holds the value.
   */
  const releaseRaced = (
    key: globalThis.Record<string, unknown>,
    constraintName: string,
  ): ConcurrentModification => {
    const error = new ConcurrentModification({
      entityType,
      key,
      attributes: [...resolveUniqueFields(config.unique![constraintName]!)],
      current: Option.none(),
    })
    releaseRaces.add(error)
    return error
  }

  // ---------------------------------------------------------------------------
  // Guarded puts (#133)
  // ---------------------------------------------------------------------------

  /**
   * The highest version retained for the item at `pk` — 0 with none. A
   * deleted retain item's version snapshots outlive it, so the item created
   * again at its key continues the sequence after them rather than
   * overwriting its earlier incarnation's history.
   */
  const highestRetainedVersion = (tableName: string, pk: unknown, liveSk: unknown) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const primary = config.indexes.primary
      const { Items } = yield* client.query({
        TableName: tableName,
        KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :prefix)",
        ExpressionAttributeNames: { "#pk": primary.pk.field, "#sk": primary.sk.field },
        ExpressionAttributeValues: {
          ":pk": toAttributeValue(pk),
          ":prefix": toAttributeValue(
            DynamoSchema.composeVersionKeyPrefix(schema, entityType, historyKeyOptions(liveSk)),
          ),
        },
        ScanIndexForward: false,
        Limit: 1,
        ConsistentRead: true,
      })
      const own = versionOfSk(Items?.[0]?.[primary.sk.field]?.S)
      if (!hasItemHistory || typeof liveSk !== "string") return own
      // History an earlier release wrote for this item counts too: a version it
      // holds is never written again.
      const [legacy] = yield* legacyHistory({
        tableName,
        pk,
        liveSk,
        kind: "version",
        descending: true,
        first: true,
      })
      return Math.max(own, versionOfSk(legacy?.[primary.sk.field]?.S))
    })

  /**
   * A guarded put — of a versioned or unique-constrained entity — compiled
   * against the item as read (#133). Shared by `put` / `create` / `upsert`
   * and the transaction paths (`Transaction.transactWrite`,
   * `EventStore.append`'s `additionalItems`), so they write the same items:
   *
   * - **The item exists:** its next version, its incarnation and its
   *   `createdAt` (unless the caller supplies one); the Put is guarded on the
   *   item read — version and incarnation, or the unique values its sentinels
   *   derive from. Retain: the replaced item is snapshotted at its version.
   * - **The item is missing** (or not read: `create`): the Put requires it
   *   still missing. A retain item continues after the highest version
   *   retained for its key — a deleted item's history outlives it — with a new
   *   incarnation, and is snapshotted at that version.
   * - **Sentinels:** a changed value takes the new reservation and releases the
   *   old one — only if this item owns it ({@link ownedSentinels}).
   *
   * `verdict` reads a cancellation: a lost race or retained history the read
   * did not see is retried by the caller (re-planned from a fresh read); the
   * caller's own condition is `Condition`; a taken value or a history conflict
   * is final.
   */
  const planPut = (args: {
    readonly tableName: string
    readonly ttlAttrName: string
    readonly now: DateTime.Utc
    /** The item as built: stored attribute names, version 1, a fresh incarnation. */
    readonly item: globalThis.Record<string, unknown>
    /** Whether the caller supplied `createdAt` (a replacing put keeps the stored one). */
    readonly createdAtSupplied: boolean
    readonly userCondition:
      | {
          readonly expression: string
          readonly names: globalThis.Record<string, string>
          readonly values: globalThis.Record<string, AttributeValue>
        }
      | undefined
    /** `create`: the item must be missing — it is not read. */
    readonly create: boolean
    readonly operation: string
    /** The key errors name. */
    readonly key: globalThis.Record<string, unknown>
  }): Effect.Effect<PutPlan, ValidationError | DynamoClientError, DynamoClient> =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const pkField = config.indexes.primary.pk.field
      const skField = config.indexes.primary.sk.field
      const item: globalThis.Record<string, unknown> = { ...args.item }
      const owner = { pk: item[pkField], sk: item[skField] }
      const current = args.create
        ? undefined
        : (yield* client.getItem({
            TableName: args.tableName,
            Key: toAttributeMap({ [pkField]: owner.pk, [skField]: owner.sk }),
            ConsistentRead: true,
          })).Item
      yield* checkVersion(current, args.operation)
      const currentRaw =
        current !== undefined
          ? (fromAttributeMap(current) as globalThis.Record<string, unknown>)
          : undefined
      const storedVersion = storedVersionOf(current)
      if (currentRaw !== undefined) {
        // The same item, continued: its next version, its incarnation, its
        // creation time (unless the caller supplies one).
        if (systemFields.version) {
          item[systemFields.version] = (storedVersion ?? 0) + 1
          if (currentRaw[INCARNATION_TOKEN] !== undefined) {
            item[INCARNATION_TOKEN] = currentRaw[INCARNATION_TOKEN]
          }
        }
        if (systemFields.createdAt && !args.createdAtSupplied) {
          const createdAttr = resolveDbName(systemFields.createdAt)
          if (currentRaw[createdAttr] !== undefined) item[createdAttr] = currentRaw[createdAttr]
        }
      } else if (systemFields.version && isRetainEnabled()) {
        item[systemFields.version] =
          (yield* highestRetainedVersion(args.tableName, owner.pk, owner.sk)) + 1
      }
      const marshalled = toAttributeMap(item)

      // The main item. Missing: it must still be missing. Present: still the
      // item read — its version and incarnation, or (unversioned) the unique
      // values its sentinels are keyed by.
      const uniqueAttrs = [
        ...new Set(
          Object.values((config.unique ?? {}) as globalThis.Record<string, UniqueConstraintDef>)
            .flatMap((def) => [...resolveUniqueFields(def)])
            .map(resolveDbName),
        ),
      ]
      const userCondition = args.userCondition
      const guard =
        current === undefined
          ? {
              expression: "attribute_not_exists(#pk)",
              names: { "#pk": pkField } as globalThis.Record<string, string>,
              values: {} as globalThis.Record<string, AttributeValue>,
              inputs: [] as ReadonlyArray<string>,
            }
          : deleteGuard(current, uniqueAttrs, userCondition?.expression)
      if (guard instanceof ValidationError) return yield* guard
      const mainValues = { ...guard.values, ...userCondition?.values }
      const mainCondition = userCondition
        ? `${guard.expression} AND (${userCondition.expression})`
        : guard.expression
      const tooLarge = oversizedCondition(args.operation, mainCondition, userCondition?.expression)
      if (tooLarge !== undefined) return yield* tooLarge

      const items: Array<TransactWriteItem> = [
        {
          Put: {
            TableName: args.tableName,
            Item: marshalled,
            ConditionExpression: mainCondition,
            ExpressionAttributeNames: { ...guard.names, ...userCondition?.names },
            ...(Object.keys(mainValues).length > 0 && { ExpressionAttributeValues: mainValues }),
            ReturnValuesOnConditionCheckFailure: "ALL_OLD",
          },
        },
      ]
      const roles: Array<
        | { readonly kind: "main" }
        | {
            readonly kind: "sentinel"
            readonly constraintName: string
            readonly fields: globalThis.Record<string, string>
          }
        | { readonly kind: "release"; readonly constraintName: string }
        | { readonly kind: "snapshot"; readonly version: number }
      > = [{ kind: "main" }]

      // Sentinels: a changed value takes the new reservation and releases the
      // old one this item owns; an unchanged value is left alone. A field
      // holding only a default has none (`__edd_d__`).
      if (config.unique != null) {
        const newSource = toDomainView(item)
        const oldSource = currentRaw !== undefined ? toDomainView(currentRaw) : undefined
        const changes: Array<{
          readonly constraintName: string
          readonly constraintDef: UniqueConstraintDef
          readonly next:
            | {
                readonly key: { readonly pk: string; readonly sk: string }
                readonly fieldsRecord: globalThis.Record<string, string>
              }
            | undefined
          readonly prior: { readonly pk: string; readonly sk: string } | undefined
        }> = []
        for (const [constraintName, constraintDef] of Object.entries(
          config.unique as globalThis.Record<string, UniqueConstraintDef>,
        )) {
          const next = composeUniqueSentinel(
            schema,
            entityType,
            constraintName,
            constraintDef,
            newSource,
          )
          const prior =
            oldSource !== undefined
              ? composeUniqueSentinel(schema, entityType, constraintName, constraintDef, oldSource)
              : undefined
          if (
            next !== undefined &&
            prior !== undefined &&
            next.key.pk === prior.key.pk &&
            next.key.sk === prior.key.sk
          ) {
            continue
          }
          changes.push({ constraintName, constraintDef, next, prior: prior?.key })
        }
        const owned = new Set(
          yield* ownedSentinels(
            args.tableName,
            changes.flatMap((change) => (change.prior !== undefined ? [change.prior] : [])),
            owner,
          ),
        )
        for (const change of changes) {
          if (change.prior !== undefined && owned.has(change.prior)) {
            items.push(sentinelRelease(args.tableName, change.prior, owner))
            roles.push({ kind: "release", constraintName: change.constraintName })
          }
          if (change.next === undefined) continue
          items.push({
            Put: {
              TableName: args.tableName,
              Item: toAttributeMap(
                sentinelItemFor(
                  change.constraintName,
                  change.constraintDef,
                  change.next.key,
                  owner,
                  args.now,
                  args.ttlAttrName,
                ),
              ),
              ...sentinelGuard(),
            },
          })
          roles.push({
            kind: "sentinel",
            constraintName: change.constraintName,
            fields: change.next.fieldsRecord,
          })
        }
      }

      // Retain: the item replaced (at its version), or the new item at its own.
      if (isRetainEnabled()) {
        const version =
          currentRaw !== undefined ? (storedVersion ?? 0) : (item[systemFields.version!] as number)
        items.push({
          Put: snapshotPut(
            args.tableName,
            buildSnapshotItem(
              currentRaw ?? item,
              version,
              pkField,
              skField,
              args.ttlAttrName,
              args.now,
            ),
          ),
        })
        roles.push({ kind: "snapshot", version })
      }

      /** The stored attributes that differ from the item read. */
      const changedFrom = (stored: globalThis.Record<string, AttributeValue>) =>
        guard.inputs.filter(
          (attr) => !attributeValueEquals(stored[attr], current?.[attr] as AttributeValue),
        )
      /** A lost race on the main item: the error once retries run out. */
      const lostRace = (
        stored: globalThis.Record<string, AttributeValue> | undefined,
      ): PutVerdict => ({
        _tag: "Retry",
        stored,
        error: systemFields.version
          ? new OptimisticLockError({
              entityType,
              key: args.key,
              expectedVersion: storedVersion ?? 0,
              actualVersion: storedVersionOf(stored) ?? -1,
            })
          : new ConcurrentModification({
              entityType,
              key: args.key,
              attributes:
                stored !== undefined && current !== undefined
                  ? changedFrom(stored).map(domainNameOf)
                  : [],
              current: Option.none(),
            }),
      })

      const verdict = (
        reasons: ReadonlyArray<WriteCancellationReason | undefined>,
      ): PutVerdict | undefined => {
        const main = reasons[0]
        if (main?.Code === "ConditionalCheckFailed") {
          const stored = main.Item as globalThis.Record<string, AttributeValue> | undefined
          // `create`: the item exists, or the caller's condition rejected it.
          if (args.create) return { _tag: "Condition" }
          if (current === undefined) {
            // Still missing: the caller's condition. Created since: a race.
            return stored === undefined && userCondition !== undefined
              ? { _tag: "Condition" }
              : lostRace(stored)
          }
          if (stored === undefined) return lostRace(stored)
          const raced = systemFields.version
            ? storedVersionOf(stored) !== storedVersion ||
              (incarnationAttr !== undefined &&
                !attributeValueEquals(stored[incarnationAttr], current[incarnationAttr]))
            : changedFrom(stored).length > 0
          return raced || userCondition === undefined ? lostRace(stored) : { _tag: "Condition" }
        }
        for (let i = 1; i < roles.length; i++) {
          if (reasons[i]?.Code !== "ConditionalCheckFailed") continue
          const role = roles[i]!
          switch (role.kind) {
            case "sentinel":
              return {
                _tag: "Fail",
                error: new UniqueConstraintViolation({
                  entityType,
                  constraint: role.constraintName,
                  fields: role.fields,
                }),
              }
            case "snapshot":
              // A missing item's snapshot: history the read did not see — retried
              // past it. A present item's: its version disagrees with its history.
              return current === undefined
                ? lostRace(undefined)
                : { _tag: "Fail", error: historyConflict(role.version, args.operation) }
            case "release":
              // The reservation changed hands since it was read.
              return {
                _tag: "Retry",
                stored: current,
                error: releaseRaced(args.key, role.constraintName),
              }
          }
        }
        return undefined
      }

      return { items, item, marshalled, verdict }
    })

  /**
   * Run a guarded put to completion (#133): plan it from a fresh read, write
   * it, and on a lost race — a concurrent create, replace or delete of the
   * item, or retained history the read did not see — plan and write it again.
   * A put replaces the whole item, so the last writer wins, as a plain
   * `PutItem` would. Only the caller's own condition, a taken unique value or
   * a history conflict fail it; a race lost on every attempt fails with the
   * concurrency error.
   */
  const runGuardedPut = (args: Parameters<typeof planPut>[0]) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      let lost: Extract<PutVerdict, { readonly _tag: "Retry" }> | undefined
      for (let attempt = 0; attempt < GUARDED_PUT_ATTEMPTS; attempt++) {
        const plan = yield* planPut(args)
        yield* checkTransactionLimit(entityType, args.operation, plan.items)
        const write: Effect.Effect<unknown, DynamoClientError> =
          plan.items.length === 1
            ? client.putItem(plan.items[0]!.Put!)
            : client.transactWriteItems({ TransactItems: [...plan.items] })
        const verdict = yield* write.pipe(
          Effect.as(undefined as PutVerdict | undefined),
          Effect.catch((err: DynamoClientError) => {
            const reasons = isAwsTransactionCancelled(err.cause)
              ? (err.cause.CancellationReasons ?? [])
              : isAwsConditionalCheckFailed(err.cause)
                ? [{ Code: "ConditionalCheckFailed", Item: err.cause.Item }]
                : undefined
            const read = reasons !== undefined ? plan.verdict(reasons) : undefined
            return read !== undefined ? Effect.succeed(read) : Effect.fail(err)
          }),
        )
        if (verdict === undefined) return plan
        switch (verdict._tag) {
          case "Fail":
            return yield* verdict.error
          case "Condition":
            return yield* new ConditionalCheckFailed({ entityType, key: args.key })
          case "Retry":
            lost = verdict
        }
      }
      return yield* withCurrentItem(Effect.fail(lost!.error), () => lost!.stored)
    })

  // ---------------------------------------------------------------------------
  // Ref hydration
  // ---------------------------------------------------------------------------

  /**
   * Hydrate ref fields by fetching referenced entities by ID and embedding
   * their core domain data. Returns a new object with ref fields populated.
   *
   * @param decoded - Decoded input containing `${field}Id` values
   * @returns Object with `${field}Id` removed and `${field}` populated with domain data
   */
  const hydrateRefs = (
    decoded: globalThis.Record<string, unknown>,
    refs?: ReadonlyArray<ResolvedRef>,
  ): Effect.Effect<
    globalThis.Record<string, unknown>,
    RefNotFound | DynamoClientError | ItemNotFound | ValidationError,
    DynamoClient | TableConfig
  > =>
    Effect.gen(function* () {
      const refsToProcess = refs ?? resolvedRefs
      if (refsToProcess.length === 0) return decoded

      // Fetch all refs in parallel
      const hydrated = { ...decoded }
      const fetchEffects = refsToProcess.map((ref) => {
        const id = decoded[ref.idFieldName] as string
        return Effect.gen(function* () {
          const keyInput = { [ref.identifierField]: id }
          // Use asModel to convert EntityGet to Effect<ModelType>
          const getEffect = asModel(ref.refEntity.get(keyInput) as EntityGet<any, any, any, any>)
          const model = yield* getEffect.pipe(
            Effect.catchTag("ItemNotFound", () =>
              Effect.fail(
                new RefNotFound({
                  entity: entityType,
                  field: ref.fieldName,
                  refEntity: ref.refEntityType,
                  refId: id,
                }),
              ),
            ),
          )
          // `asModel` returns the DECODED domain value (transform fields lifted to
          // their domain form, e.g. `Schema.DateTimeUtcFromString` → `DateTime`).
          // The parent decodes this nested ref field via the ref target's ORIGINAL
          // `Schema.Class` (`substituteSchemas` does not recurse into nested
          // classes), whose Encoded side is the wire primitive. Re-encode through
          // the ref's substituted `modelSchema` so the spliced value is wire-form:
          // marshall-safe for `toAttributeMap` AND decoded exactly once by the
          // parent (issue #72 — without this, a put marshalls a `DateTime` and the
          // return decode fails "Expected string, got DateTime.Utc").
          const wire = yield* Schema.encodeUnknownEffect(
            ref.refEntity.schemas.modelSchema as Schema.Codec<any>,
          )(model).pipe(
            Effect.mapError(
              (cause) =>
                new ValidationError({
                  entityType,
                  operation: "hydrateRefs.encode",
                  cause,
                }),
            ),
          )
          return { fieldName: ref.fieldName, idFieldName: ref.idFieldName, data: wire }
        })
      })

      const results = yield* Effect.all(fetchEffects, { concurrency: "unbounded" })
      for (const r of results) {
        // `data` is already a wire-form plain object from the ref's modelSchema.
        hydrated[r.fieldName] = r.data as object
        delete hydrated[r.idFieldName]
      }

      return hydrated
    })

  // ---------------------------------------------------------------------------
  // Cascade update execution
  // ---------------------------------------------------------------------------

  /**
   * Find a GSI on a target entity whose PK composite includes the source entity's
   * identifier field name (e.g., "playerId"). This GSI is used to query all target
   * items that reference the source entity.
   *
   * Selection priority:
   *   1. A GSI whose PK composite is exactly `[idFieldName]` (single attribute).
   *      This is the only shape that can be recomposed from the source identifier
   *      alone, so it's always safe for cascade.
   *   2. Otherwise, the first GSI whose PK composite contains `idFieldName`.
   *      This is a fallback for backwards compatibility — `executeCascade`
   *      validates that all extra composites can be supplied at runtime and
   *      raises a clear error if they cannot.
   */
  const findCascadeIndex = (
    target: CascadeTarget,
    idFieldName: string,
  ): { readonly indexName: string; readonly indexDef: IndexDefinition } | undefined => {
    let fallback: { readonly indexName: string; readonly indexDef: IndexDefinition } | undefined
    for (const [indexName, indexDef] of Object.entries(target.indexes)) {
      if (indexName === "primary") continue
      const composite = indexDef.pk.composite
      if (composite.length === 1 && composite[0] === idFieldName) {
        // Exact match — preferred. PK is fully determined by the source id.
        return { indexName, indexDef }
      }
      if (fallback === undefined && composite.includes(idFieldName)) {
        fallback = { indexName, indexDef }
      }
    }
    return fallback
  }

  /** Cached source entity identifier field name (e.g., "playerId") */
  const sourceIdentifierField = (() => {
    const idField = getIdentifierField(config.model as Schema.Top)
    return idField?.name
  })()

  /**
   * Execute cascade updates after a source entity update completes.
   * For each target entity: query its cascade GSI, then update all matching items.
   */
  const executeCascade = (
    cascadeConfig: CascadeConfig,
    sourceDomainData: globalThis.Record<string, unknown>,
    sourceIdValue: string,
  ): Effect.Effect<void, CascadePartialFailure | DynamoClientError, DynamoClient | TableConfig> =>
    Effect.gen(function* () {
      const { targets, filter, mode } = cascadeConfig
      const sourceEntityType = entityType
      const client = yield* DynamoClient

      if (!sourceIdentifierField) return undefined as undefined

      const allUpdateOps: Array<{
        readonly tableName: string
        readonly key: globalThis.Record<string, AttributeValue>
        readonly refFieldName: string
        readonly refData: AttributeValue
      }> = []

      for (const target of targets) {
        // Find the ref on the target that points to our source entity
        const matchingRef = target._resolvedRefs.find((r) => r.refEntityType === sourceEntityType)
        if (!matchingRef) continue

        // Find GSI on target whose PK composite includes the ref's ID field
        const cascadeIdx = findCascadeIndex(target, matchingRef.idFieldName)
        if (!cascadeIdx) continue

        // The cascade can only recompose a GSI PK from the source identifier
        // if every composite attribute is satisfied. The source entity (e.g. a
        // Team) only carries its own identifier (e.g. teamId), so any GSI whose
        // PK includes additional composites (e.g. season, series) is unusable
        // for cascade. findCascadeIndex prefers the safe shape, but the
        // fallback may still hand back an over-composed GSI when no exact
        // match exists. Detect that here and fail with a clear, tagged error
        // rather than throwing deep inside KeyComposer.composePk.
        const cascadePkComposite = cascadeIdx.indexDef.pk.composite
        if (cascadePkComposite.length !== 1 || cascadePkComposite[0] !== matchingRef.idFieldName) {
          return yield* new CascadePartialFailure({
            sourceEntity: sourceEntityType,
            sourceId: sourceIdValue,
            succeeded: 0,
            failed: 0,
            errors: [
              `Cascade target "${target.entityType}" has no GSI usable for cascade from "${sourceEntityType}". ` +
                `The chosen index "${cascadeIdx.indexName}" (${cascadeIdx.indexDef.index}) has PK composite ` +
                `[${cascadePkComposite.join(", ")}], but cascade requires a GSI whose PK composite is exactly ` +
                `["${matchingRef.idFieldName}"]. Add a single-attribute GSI keyed on "${matchingRef.idFieldName}" ` +
                `(or use the refs.${matchingRef.fieldName}.cascade config to auto-generate one).`,
            ],
          })
        }

        // Resolve target table name
        const { name: targetTableName } = yield* target._tableTag

        // Compose GSI PK for the source entity's ID
        const targetSchema = target._schema
        const gsiPkValue = KeyComposer.composePk(
          targetSchema,
          target.entityType,
          cascadeIdx.indexDef,
          keyFormFor(target, { [matchingRef.idFieldName]: sourceIdValue }),
        )

        // Build query parameters
        const gsiPkField = cascadeIdx.indexDef.pk.field
        const queryNames: globalThis.Record<string, string> = {
          "#pk": gsiPkField,
          "#et": "__edd_e__",
        }
        const queryValues: globalThis.Record<string, AttributeValue> = {
          ":pk": { S: gsiPkValue },
          ":et0": { S: target.entityType },
        }

        let filterExpression = "#et = :et0"

        // Add user-provided filter
        if (filter) {
          let filterIdx = 0
          for (const [key, value] of Object.entries(filter)) {
            const nameKey = `#cf${filterIdx}`
            const valKey = `:cf${filterIdx}`
            queryNames[nameKey] = key
            queryValues[valKey] = toAttributeValue(value)
            filterExpression += ` AND ${nameKey} = ${valKey}`
            filterIdx++
          }
        }

        // Paginated query
        let exclusiveStartKey: globalThis.Record<string, AttributeValue> | undefined
        do {
          const queryResult = yield* client.query({
            TableName: targetTableName,
            IndexName: cascadeIdx.indexDef.index,
            KeyConditionExpression: "#pk = :pk",
            FilterExpression: filterExpression,
            ExpressionAttributeNames: queryNames,
            ExpressionAttributeValues: queryValues,
            ExclusiveStartKey: exclusiveStartKey,
          })

          for (const item of queryResult.Items ?? []) {
            allUpdateOps.push({
              tableName: targetTableName,
              key: { pk: item.pk!, sk: item.sk! },
              refFieldName: matchingRef.fieldName,
              refData: toAttributeValue(sourceDomainData),
            })
          }

          exclusiveStartKey = queryResult.LastEvaluatedKey as
            | globalThis.Record<string, AttributeValue>
            | undefined
        } while (exclusiveStartKey !== undefined)
      }

      if (allUpdateOps.length === 0) return

      if (mode === "transactional") {
        // Transactional mode — fail if >100 items
        if (allUpdateOps.length > 100) {
          return yield* new CascadePartialFailure({
            sourceEntity: sourceEntityType,
            sourceId: sourceIdValue,
            succeeded: 0,
            failed: allUpdateOps.length,
            errors: [`Transactional cascade exceeded 100-item limit: ${allUpdateOps.length} items`],
          })
        }

        yield* client.transactWriteItems({
          TransactItems: allUpdateOps.map((op) => ({
            Update: {
              TableName: op.tableName,
              Key: op.key,
              UpdateExpression: "SET #ref = :refData",
              ExpressionAttributeNames: { "#ref": op.refFieldName },
              ExpressionAttributeValues: { ":refData": op.refData },
            },
          })),
        })
      } else {
        // Eventual mode — concurrent updates with partial failure tracking
        let succeeded = 0
        const errors: Array<unknown> = []

        const updateEffects = allUpdateOps.map((op) =>
          client
            .updateItem({
              TableName: op.tableName,
              Key: op.key,
              UpdateExpression: "SET #ref = :refData",
              ExpressionAttributeNames: { "#ref": op.refFieldName },
              ExpressionAttributeValues: { ":refData": op.refData },
            })
            .pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  succeeded++
                }),
              ),
              Effect.catch((err: DynamoClientError) =>
                Effect.sync(() => {
                  errors.push(err)
                }),
              ),
            ),
        )

        yield* Effect.all(updateEffects, { concurrency: 10 })

        if (errors.length > 0) {
          return yield* new CascadePartialFailure({
            sourceEntity: sourceEntityType,
            sourceId: sourceIdValue,
            succeeded,
            failed: errors.length,
            errors,
          })
        }
      }
    })

  // ---------------------------------------------------------------------------
  // put operation
  // ---------------------------------------------------------------------------

  // Mutable self — safe because operations are closures that only
  // dereference `self` when called by users, after make() has returned
  let self: Entity

  const put = (input: unknown) =>
    new EntityPutImpl(
      (mode: DecodeMode, opts: EntityPutOpts) =>
        Effect.gen(function* () {
          const client = yield* DynamoClient
          const tc = yield* tableTag
          const tableName = tc.name
          const ttlAttrName = resolveTtlAttributeName(tc)

          // `upsert` creates through this path: its errors name it.
          const operation = opts.operation ?? "put"
          yield* checkWithVectorNames(opts.withVectors, operation)

          // Auto-generated id: fill the configured primary-key field with a
          // fresh UUID BEFORE the encode, but only when the caller omitted it —
          // a caller-supplied value is always respected. The id is sourced from
          // the bundled Crypto service (`DynamoClient.make` provides a default),
          // so this keeps the bound `put` at `R = never`. The filled id then
          // flows through input validation → key composition → stored item →
          // returned record in this same pass.
          const withGeneratedId = yield* fillGeneratedId(input)
          const unsentineled = unsentineledDefaults(withGeneratedId)
          const inputWithGeneratedId = yield* fillDecodingDefaults(withGeneratedId)

          // Encode user input → wire form. Users typically construct domain
          // values for transforms (DateTime, Redacted, Number) and plain
          // objects for Schema.Class fields. To handle both:
          //   1. Try `encode` directly — the canonical Type → Encoded
          //      conversion. Works when the input is fully Type-shaped
          //      (which is the case for the transforms that issue #29
          //      surfaced: RedactedFromValue, NumberFromString, etc.).
          //   2. On failure, fall back to `decode → encode` — `decode` is
          //      forgiving for Schema.Class (lifts plain objects to
          //      instances) and the substituted date/Redacted transforms
          //      tolerate either form. The subsequent `encode` produces
          //      the canonical wire shape.
          const encodedInput = yield* encodeOrDecodeEncode(
            schemas.inputSchema as Schema.Codec<any>,
            inputWithGeneratedId,
            entityType,
            operation,
          )

          // Compose all keys from the encoded input (ID fields still present
          // and key composites are wire-format strings/numbers).
          const keys = composeAllKeys(encodedInput as globalThis.Record<string, unknown>)

          // Hydrate refs: replace ID fields with full entity domain data
          const encoded: globalThis.Record<string, unknown> = hasRefs
            ? yield* hydrateRefs(encodedInput as globalThis.Record<string, unknown>)
            : (encodedInput as globalThis.Record<string, unknown>)

          // Build the DynamoDB item — already in wire-format after encode
          const item: globalThis.Record<string, unknown> = { ...encoded }

          // Clock-backed time source resolved once for this write op and
          // threaded into every timestamp/TTL computation below — deterministic
          // under `TestClock`, wall-clock in production.
          const now = yield* DateTime.now

          // Add system fields. When a timestamp field collides with a
          // model-declared field, the user may have supplied their own value
          // (optional on collision in `inputSchema`). Respect their value if
          // present (already encoded to wire); otherwise generate a wire
          // primitive directly.
          if (systemFields.createdAt) {
            if (item[systemFields.createdAt] === undefined) {
              item[systemFields.createdAt] = generateTimestamp(systemFields.createdAtEncoding, now)
            }
          }
          if (systemFields.updatedAt) {
            if (item[systemFields.updatedAt] === undefined) {
              item[systemFields.updatedAt] = generateTimestamp(systemFields.updatedAtEncoding, now)
            }
          }
          if (systemFields.version) item[systemFields.version] = 1
          if (stampsIncarnation) item[INCARNATION_TOKEN] = yield* freshIncarnation

          // Add entity type discriminator
          item.__edd_e__ = entityType
          // Unique fields holding only a default: no sentinel for them (#133).
          if (unsentineled.length > 0) item[UNSENTINELED_DEFAULTS] = new Set(unsentineled)

          // Apply composed keys
          Object.assign(item, keys)

          // Vector search: embed the source text and compose the partition
          // value. Sparse — an index whose source or partition composites are
          // absent contributes nothing. See `DESIGN.md §14`.
          if (hasVectorIndexes) {
            const vectorWrite = yield* computeVectorAttributes(
              encoded as globalThis.Record<string, unknown>,
              opts.withVectors,
              "all",
            )
            Object.assign(item, vectorWrite.sets)
            // `put` writes a whole item, so a REMOVE is just an omission.
            for (const field of vectorWrite.removes) delete item[field]
          }

          const hasUniqueConstraints =
            config.unique != null && Object.keys(config.unique).length > 0
          // A versioned or unique-constrained entity's put is guarded (#133):
          // a put over an existing item continues its version sequence,
          // incarnation and `createdAt`, snapshots the replaced item (retain)
          // and rotates its unique sentinels — never resetting history or
          // orphaning one. See `runGuardedPut`.
          const guarded = hasUniqueConstraints || Boolean(systemFields.version)
          const createdAtSupplied =
            systemFields.createdAt !== null &&
            (encoded as globalThis.Record<string, unknown>)[systemFields.createdAt] !== undefined

          // Rename domain fields to DynamoDB attribute names
          renameToDynamo(item)
          // Flatten sparse Map fields into per-entry top-level attributes.
          // Wrapped to surface key-validation errors as ValidationError.
          try {
            serializeSparseFields(item)
          } catch (e) {
            return yield* new ValidationError({
              entityType,
              operation: `${operation}.sparse`,
              cause: e instanceof Error ? e.message : String(e),
            })
          }

          // The caller's condition, ANDed onto `create`'s not-exists guard —
          // a guard no `.condition()` can replace (#133). `upsert`'s create
          // attempt brings its own (`runGuardedPut` requires the item missing).
          const condition = withGuard(
            opts.putKind === "create" && operation !== "upsert"
              ? {
                  attributeNotExists: [
                    config.indexes.primary.pk.field,
                    config.indexes.primary.sk.field,
                  ],
                }
              : undefined,
            opts.condition,
          )
          const userCondition = condition ? compileCondition(condition, resolveDbName) : undefined

          if (guarded) {
            const plan = yield* runGuardedPut({
              tableName,
              ttlAttrName,
              now,
              item,
              createdAtSupplied,
              userCondition,
              create: opts.putKind === "create",
              operation,
              key: encoded as globalThis.Record<string, unknown>,
            })
            return yield* decodeAs(plan.item, plan.marshalled, mode)
          }

          const marshalledItem = toAttributeMap(item)
          {
            // Simple put without unique constraints or versioning
            const putInput: {
              TableName: string
              Item: globalThis.Record<string, AttributeValue>
              ConditionExpression?: string
              ExpressionAttributeNames?: globalThis.Record<string, string>
              ExpressionAttributeValues?: globalThis.Record<string, AttributeValue>
            } = {
              TableName: tableName,
              Item: marshalledItem,
            }
            if (userCondition) {
              putInput.ConditionExpression = userCondition.expression
              putInput.ExpressionAttributeNames = userCondition.names
              if (Object.keys(userCondition.values).length > 0) {
                putInput.ExpressionAttributeValues = userCondition.values
              }
            }
            yield* client.putItem(putInput).pipe(
              Effect.mapError((err): DynamoClientError | ConditionalCheckFailed => {
                if (condition && isAwsConditionalCheckFailed(err.cause)) {
                  return new ConditionalCheckFailed({
                    entityType,
                    key: encoded as globalThis.Record<string, unknown>,
                  })
                }
                return err
              }),
            )
          }

          // Decode and return using selected mode
          return yield* decodeAs(item, marshalledItem, mode)
        }),
      self,
      input as globalThis.Record<string, unknown>,
    )

  // ---------------------------------------------------------------------------
  // create operation — put + attribute_not_exists condition
  // ---------------------------------------------------------------------------

  // The not-exists guard is the put builder's, from `putKind: "create"` —
  // never the op's condition, which `.condition()` replaces (#133).
  const create = (input: unknown) => {
    const op = put(input)
    return new EntityPutImpl(
      op._builder,
      op._entity,
      op._input,
      op._condition,
      op._withVectors,
      "create",
    )
  }

  // ---------------------------------------------------------------------------
  // patch operation — update + attribute_exists condition
  // ---------------------------------------------------------------------------

  // The exists guard is `runUpdate`'s, from `patch: true` — never the op's
  // condition, which `.condition()` replaces (#133).
  const patch = (key: unknown) => {
    const op = update(key)
    return new EntityUpdateImpl(
      op._builder,
      { ...op._updateState, patch: true },
      op._entity,
      op._key,
    )
  }

  // ---------------------------------------------------------------------------
  // get operation
  // ---------------------------------------------------------------------------

  const get = (key: unknown) =>
    new EntityGetImpl(
      (mode: DecodeMode, opts: EntityGetOpts) =>
        Effect.gen(function* () {
          const client = yield* DynamoClient
          const { name: tableName } = yield* tableTag

          // Caller key: Type side in, ENCODED out (see `encodeKey`).
          const encodedKey = yield* encodeKey(key, "get.decode")

          // Compose primary key
          const primaryKey = composePrimaryKey(encodedKey)
          const marshalledKey = toAttributeMap(primaryKey)

          // Build ProjectionExpression if projection attributes provided
          let projectionExpression: string | undefined
          let projectionNames: globalThis.Record<string, string> | undefined
          if (opts.projection && opts.projection.length > 0) {
            const proj = Projection.projection(opts.projection)
            projectionExpression = proj.expression
            projectionNames = proj.names
          }

          const result = yield* client.getItem({
            TableName: tableName,
            Key: marshalledKey,
            ConsistentRead: opts.consistentRead || undefined,
            ProjectionExpression: projectionExpression,
            ExpressionAttributeNames:
              projectionNames && Object.keys(projectionNames).length > 0
                ? projectionNames
                : undefined,
          })

          if (!result.Item) {
            return yield* new ItemNotFound({ entityType, key: encodedKey })
          }

          // Raw mode: return unmarshalled record without schema decode
          if (mode === "raw") {
            return fromAttributeMap(result.Item)
          }

          // Unmarshall and decode using selected mode
          const raw = fromAttributeMap(result.Item)
          return yield* decodeAs(raw, result.Item, mode)
        }),
      self,
      key as globalThis.Record<string, unknown>,
    )

  // ---------------------------------------------------------------------------
  // update operation
  // ---------------------------------------------------------------------------

  const runUpdate = (
    key: unknown,
    mode: DecodeMode,
    requested: UpdateState,
    /**
     * The item as already read — consistently — by the caller (`upsert`).
     * Used instead of reading it again: the write is guarded on it either way.
     */
    preRead?: globalThis.Record<string, AttributeValue>,
  ) =>
    Effect.gen(function* () {
      // Path operations on a field that feeds a key or a unique sentinel
      // become the record operations they are equivalent to, or are
      // refused (#133) — before anything reads the state.
      const normalized = normalizeDerivedPathOps(requested)
      if (typeof normalized === "string") {
        return yield* new ValidationError({
          entityType,
          operation: "update",
          cause: normalized,
        })
      }
      // Removing a defaulted index composite re-materialises its default —
      // stored, keys recomposed from it — as a put that omits it does (#133).
      const { state: uState, defaulted: removedToDefault } =
        yield* rematerializeRemovedDefaults(normalized)
      const refusedSet = refusedRecordSet(uState.updates, key)
      if (refusedSet !== undefined) {
        return yield* new ValidationError({
          entityType,
          operation: "update",
          cause: refusedSet,
        })
      }
      const immutables = assertedImmutables(uState.updates)
      if (typeof immutables === "string") {
        return yield* new ValidationError({
          entityType,
          operation: "update",
          cause: immutables,
        })
      }
      const upsertable = completeUpsertPayload(uState.updates)
      const requestedRv = uState.returnValues ?? "allNew"
      // A cascade propagates the NEW item; a mode returning the OLD one then
      // needs both images exactly — the read-then-write branch has both.
      const needsBothImages =
        uState.cascade !== undefined && (requestedRv === "allOld" || requestedRv === "updatedOld")
      const updates = uState.updates
      const evExpected = uState.expectedVersion
      // An entity without `versioned` has no version to check: an expected
      // version would be silently skipped — a concurrency check that never
      // runs — so it is refused before anything is read or sent (#134).
      if (evExpected !== undefined && !systemFields.version) {
        return yield* new ValidationError({
          entityType,
          operation: "update.expectedVersion",
          cause:
            `expectedVersion(${evExpected}): entity "${entityType}" is not versioned, so there ` +
            "is no version to check. Add `versioned: true`, or use a `.condition()`. Nothing was sent.",
        })
      }
      // The caller's condition, ANDed onto `patch`'s exists guard — a guard
      // no `.condition()` can replace (#133).
      const userCond = withGuard(
        uState.patch ? { attributeExists: [config.indexes.primary.pk.field] } : undefined,
        uState.condition,
      )
      const client = yield* DynamoClient
      const tc = yield* tableTag
      const tableName = tc.name
      const ttlAttrName = resolveTtlAttributeName(tc)
      // Clock-backed time source for the updatedAt timestamp + retain snapshot TTL.
      const now = yield* DateTime.now

      yield* checkWithVectorNames(uState.withVectors, "update")

      // Caller key: Type side in, ENCODED out (see `encodeKey`).
      const encodedKey = yield* encodeKey(key, "update.decodeKey")
      // A missing item: `patch`'s exists guard failed — `ConditionalCheckFailed`,
      // on every path, including those that read first (#134); for `update`,
      // `ItemNotFound`.
      const missingItem = () =>
        uState.patch
          ? new ConditionalCheckFailed({
              entityType,
              key: encodedKey as globalThis.Record<string, unknown>,
            })
          : new ItemNotFound({ entityType, key: encodedKey })

      // Encode update payload → wire form (see `put` for the strategy).
      const encodedUpdates = yield* encodeOrDecodeEncode(
        schemas.updateSchema as Schema.Codec<any>,
        updates ?? {},
        entityType,
        "update",
      )

      // Hydrate refs in updates: for any ${field}Id present, fetch and embed
      let hydratedUpdates = encodedUpdates as globalThis.Record<string, unknown>
      if (hasRefs) {
        const refsToHydrate = resolvedRefs.filter(
          (ref) =>
            ref.idFieldName in (encodedUpdates as globalThis.Record<string, unknown>) &&
            (encodedUpdates as globalThis.Record<string, unknown>)[ref.idFieldName] !== undefined,
        )
        if (refsToHydrate.length > 0) {
          hydratedUpdates = yield* hydrateRefs(hydratedUpdates, refsToHydrate)
        }
      }

      // Compose primary key
      const primaryKey = composePrimaryKey(encodedKey)
      const marshalledKey = toAttributeMap(primaryKey)

      // Detect whether the update touches any unique constraint fields
      const hasUniqueConstraints = config.unique != null && Object.keys(config.unique).length > 0
      let touchesUniqueFields = false
      if (hasUniqueConstraints) {
        const uniqueFieldSet = new Set(
          Object.values(config.unique!).flatMap((def) => [...resolveUniqueFields(def)]),
        )
        const allUpdatedFields = new Set([
          ...Object.keys(hydratedUpdates as globalThis.Record<string, unknown>).filter(
            (k) => (hydratedUpdates as globalThis.Record<string, unknown>)[k] !== undefined,
          ),
          ...(uState.remove ?? []),
          ...Object.keys(uState.add ?? {}),
          ...Object.keys(uState.subtract ?? {}),
          ...Object.keys(uState.append ?? {}),
          ...Object.keys(uState.deleteFromSet ?? {}),
        ])
        touchesUniqueFields = [...uniqueFieldSet].some((f) => allUpdatedFields.has(f))
      }

      // `.add()` / `.subtract()` / `.append()` / `.deleteFromSet()` on an
      // index composite: DynamoDB would compute the new value, but the
      // index key is composed from it here — so the item is read, the
      // value computed and the keys recomposed by the read-then-write
      // branch below, exactly as for a retain entity (#133).
      const indexComposites = indexCompositeFields()
      const computedComposites = [
        ...Object.keys(uState.add ?? {}),
        ...Object.keys(uState.subtract ?? {}),
        ...Object.keys(uState.append ?? {}),
        ...Object.keys(uState.deleteFromSet ?? {}),
      ].filter((field) => indexComposites.has(field))

      // Path operations are never emulated in memory: DynamoDB applies them
      // (#133). A retain update carrying them takes the standard branch
      // below, transacted with its version snapshot. An update that also
      // rotates a unique sentinel or computes an index composite needs this
      // branch's read-then-put, which cannot carry them — refused rather
      // than silently dropping them.
      const pathOpsPresent = hasPathOps(uState)
      if (pathOpsPresent && (touchesUniqueFields || computedComposites.length > 0)) {
        return yield* new ValidationError({
          entityType,
          operation: "update",
          cause: touchesUniqueFields
            ? "Path operations (pathSet, pathAppend, …) cannot be combined with a change to a " +
              "unique-constraint field in one update: the unique sentinels are rotated by a " +
              "read-then-put that DynamoDB path expressions cannot join. Split the update in two."
            : `Path operations (pathSet, pathAppend, …) cannot be combined with a computed ` +
              `change to index composite "${computedComposites[0]}" in one update: its new ` +
              "value is computed by a read-then-put that DynamoDB path expressions cannot " +
              "join. Split the update in two.",
        })
      }
      if (needsBothImages && pathOpsPresent && !isRetainEnabled() && !systemFields.version) {
        return yield* new ValidationError({
          entityType,
          operation: "update",
          cause:
            'A cascade with returnValues("allOld" | "updatedOld") and path operations ' +
            "needs the item before AND after the update exactly, which an entity without " +
            "`versioned` cannot prove. Drop the path operations, the cascade, or the mode.",
        })
      }
      if (
        (isRetainEnabled() ||
          touchesUniqueFields ||
          computedComposites.length > 0 ||
          needsBothImages) &&
        !pathOpsPresent
      ) {
        // --- Retain path: read-then-transact ---
        // Read current item (needed to create snapshot of pre-update state)
        const currentResult =
          preRead !== undefined
            ? { Item: preRead }
            : yield* client.getItem({
                TableName: tableName,
                Key: marshalledKey,
                ConsistentRead: true,
              })

        if (!currentResult.Item) {
          return yield* missingItem()
        }

        const currentRaw = fromAttributeMap(currentResult.Item)
        // 0: written before the entity was `versioned` (#133).
        const currentVersion = systemFields.version
          ? (((currentRaw as globalThis.Record<string, unknown>)[systemFields.version] as
              | number
              | undefined) ?? 0)
          : 0

        // Validate optimistic lock if requested
        if (evExpected !== undefined && currentVersion !== evExpected) {
          return yield* new OptimisticLockError({
            entityType,
            key: encodedKey,
            expectedVersion: evExpected,
            actualVersion: currentVersion,
          })
        }

        // Build new item: merge current + updates + rich ops + recompose keys.
        // `currentRaw` uses DynamoDB attribute names (e.g. `dn`, `hd`);
        // `hydratedUpdates` uses domain names. To avoid the merge leaving
        // both names (which would later have the rename clobber the user's
        // value), translate the current item's keys to domain names first
        // so the spread + assign overlay cleanly.
        const currentDomainItem = {
          ...(currentRaw as globalThis.Record<string, unknown>),
        }
        renameFromDynamo(currentDomainItem)
        // Rebuild sparse Map fields from flattened attrs into domain Records.
        // Done in-place on the renamed domain item so subsequent merge logic
        // operates on domain shape and re-flattening happens once at the end.
        deserializeSparseFields(currentDomainItem)
        const newItem: globalThis.Record<string, unknown> = { ...currentDomainItem }
        // Apply user updates. For sparse fields, merge bucket-by-bucket
        // rather than whole-field replace — concurrent writers to disjoint
        // buckets must coexist (the version CAS protects against same-bucket
        // races). Non-sparse fields use the existing replace semantics.
        //
        // null and undefined collapse: both mean "absent" under v3's
        // two-way classification (DESIGN.md §7). For the in-memory item
        // we DELETE the attribute (mirrors a DynamoDB REMOVE), so
        // subsequent decode sees the field as absent rather than as a
        // typed null that the model schema may not permit. The composer
        // below sees the attr as absent in `newItem` and applies the
        // structural rule.
        for (const [attr, val] of Object.entries(
          hydratedUpdates as globalThis.Record<string, unknown>,
        )) {
          if (val === null || val === undefined) {
            delete newItem[attr]
            continue
          }
          if (hasSparseFields && attr in sparseFields && typeof val === "object") {
            const existing = (newItem[attr] as globalThis.Record<string, unknown>) ?? {}
            newItem[attr] = { ...existing, ...(val as globalThis.Record<string, unknown>) }
          } else {
            newItem[attr] = val
          }
        }

        // Apply rich operations to in-memory item
        if (uState.remove) {
          for (const attr of uState.remove) {
            delete newItem[attr]
          }
        }
        // Sparse-map removes: drop named entries from the in-memory Record.
        if (uState.sparseRemoveEntries) {
          for (const op of uState.sparseRemoveEntries) {
            if (!hasSparseFields || !(op.field in sparseFields)) continue
            const bucket = newItem[op.field] as globalThis.Record<string, unknown> | undefined
            if (!bucket) continue
            for (const k of op.keys) delete bucket[k]
          }
        }
        // Sparse-map clear: blow away the entire bucket Record. The retain
        // path always reads-then-writes the full item, so the version CAS
        // already gives clearMap atomic semantics for free — no per-bucket
        // GET dance needed here.
        if (uState.sparseClearFields) {
          for (const field of uState.sparseClearFields) {
            if (!hasSparseFields || !(field in sparseFields)) continue
            newItem[field] = {}
          }
        }
        if (uState.add) {
          for (const [attr, val] of Object.entries(uState.add)) {
            newItem[attr] = ((newItem[attr] as number) ?? 0) + val
          }
        }
        if (uState.subtract) {
          for (const [attr, val] of Object.entries(uState.subtract)) {
            newItem[attr] = ((newItem[attr] as number) ?? 0) - val
          }
        }
        if (uState.append) {
          for (const [attr, val] of Object.entries(uState.append)) {
            const existing = (newItem[attr] as Array<unknown>) ?? []
            const appended = encodePathValue([attr], val, "elements")
            if (appended.issue !== undefined) {
              return yield* new ValidationError({
                entityType,
                operation: "update.append",
                cause: appended.issue,
              })
            }
            newItem[attr] = [...existing, ...(appended.encoded as ReadonlyArray<unknown>)]
          }
        }
        if (uState.deleteFromSet) {
          for (const [attr, val] of Object.entries(uState.deleteFromSet)) {
            if (newItem[attr] instanceof Set && val instanceof Set) {
              const current = newItem[attr] as Set<unknown>
              for (const elem of val as Set<unknown>) {
                current.delete(elem)
              }
            }
          }
        }

        // A unique field that held only a default gets its sentinel once a
        // write supplies (or removes) its value (#133).
        const defaultedMarker = newItem[UNSENTINELED_DEFAULTS]
        if (defaultedMarker instanceof Set) {
          const supplied = new Set<string>([
            ...Object.keys(hydratedUpdates as globalThis.Record<string, unknown>),
            ...(uState.remove ?? []),
            ...Object.keys(uState.add ?? {}),
            ...Object.keys(uState.subtract ?? {}),
          ])
          const still = [...(defaultedMarker as ReadonlySet<string>)].filter(
            (f) => !supplied.has(f),
          )
          if (still.length === 0) delete newItem[UNSENTINELED_DEFAULTS]
          else newItem[UNSENTINELED_DEFAULTS] = new Set(still)
        }
        // …and a unique field removed back to its default holds only a default.
        const uniqueRemovedToDefault = removedToDefault.filter((field) =>
          Object.values(config.unique ?? {}).some((def) =>
            resolveUniqueFields(def as UniqueConstraintDef).includes(field),
          ),
        )
        if (uniqueRemovedToDefault.length > 0) {
          const marker = newItem[UNSENTINELED_DEFAULTS]
          newItem[UNSENTINELED_DEFAULTS] = new Set([
            ...(marker instanceof Set ? (marker as ReadonlySet<string>) : []),
            ...uniqueRemovedToDefault,
          ])
        }

        // Increment version and update timestamp. If the caller supplied a
        // value for `updatedAt` (allowed when the field collides with a
        // model-declared field), respect it (already wire-form via encode);
        // else generate a fresh wire primitive.
        const newVersion = currentVersion + 1
        if (systemFields.version) newItem[systemFields.version] = newVersion
        if (systemFields.updatedAt) {
          const userSupplied = (hydratedUpdates as globalThis.Record<string, unknown>)[
            systemFields.updatedAt
          ]
          newItem[systemFields.updatedAt] =
            userSupplied !== undefined
              ? userSupplied
              : generateTimestamp(systemFields.updatedAtEncoding, now)
        }

        // `newItem` is already in domain names (the merge built it from
        // `currentDomainItem` + domain-keyed updates) — no rename needed
        // here. Recompose all keys with the updated attributes.
        //
        // Primary key always recomposes from `newItem` (Put-style).
        const primaryKeyMap = composePrimaryKey(newItem)
        Object.assign(newItem, primaryKeyMap)

        // GSI keys: route through the policy-aware composer so the
        // v1.7.1 per-half evaluation gate, structural rule, and per-half
        // cascade match the standard update path. `newItem` carries
        // stored values for any composite the user did not touch — used
        // as the merged record for the structural rule. The composer
        // emits per-half SETs/REMOVEs/noops that we apply directly to
        // `newItem` (which becomes the put-style item written back).
        //
        // No try/catch needed — EDD-9024 was deprecated in v1.7.1 and
        // the composer no longer throws.
        const retainRemovedSet = uState.remove ? new Set(uState.remove) : undefined
        const gsiUpdate = KeyComposer.composeGsiKeysForUpdatePolicyAware(
          schema,
          entityType,
          entityVersion,
          allIndexes,
          keyForm(hydratedUpdates as globalThis.Record<string, unknown>),
          keyForm(newItem),
          retainRemovedSet === undefined ? {} : { removedSet: retainRemovedSet },
        )
        for (const [field, value] of Object.entries(gsiUpdate.sets)) {
          newItem[field] = value
        }
        for (const field of gsiUpdate.removes) {
          delete newItem[field]
        }
        // GSIs where neither half was touched (per the v1.7.1 evaluation
        // gate): retain path semantics are Put-style — recompose from
        // `newItem` and drop both keys when any composite is missing.
        // For touched halves, trust the composer's per-half decision
        // (SET / REMOVE / leave-stored-value-on-newItem-alone for noop).
        const addressed = new Set<string>([...Object.keys(gsiUpdate.sets), ...gsiUpdate.removes])
        for (const [indexName, indexDef] of Object.entries(allIndexes)) {
          if (indexName === "primary") continue
          // If either half was addressed, the composer made the per-half
          // decision; the other half's stored value already lives on
          // `newItem` from the read-then-write merge above.
          if (addressed.has(indexDef.pk.field) || addressed.has(indexDef.sk.field)) continue
          const keys = KeyComposer.tryComposeIndexKeys(
            schema,
            entityType,
            entityVersion,
            indexDef,
            keyForm(newItem),
          )
          if (keys) {
            Object.assign(newItem, keys)
          } else {
            delete newItem[indexDef.pk.field]
            delete newItem[indexDef.sk.field]
          }
        }

        // Compute sentinel rotation values while newItem is in domain names.
        // currentRaw uses DynamoDB names, so read it through `toDomainView`
        // for fields that may have been renamed (e.g. id → teamId).
        // Sparse-aware rotation: each constraint may transition through one of four
        // states between old and new — both-missing (no-op), missing→present (Put only),
        // present→missing (Delete only), present→present (Delete + Put if changed).
        type SentinelRotation = {
          constraintName: string
          oldUniqueKey: { readonly pk: string; readonly sk: string } | undefined
          newUniqueKey: { readonly pk: string; readonly sk: string } | undefined
          newFieldsRecord: globalThis.Record<string, string> | undefined
        }
        const sentinelRotations: Array<SentinelRotation> = []
        if (touchesUniqueFields) {
          const currentRawDomain = toDomainView(currentRaw as globalThis.Record<string, unknown>)
          for (const [constraintName, constraintDef] of Object.entries(config.unique!)) {
            const oldSentinel = composeUniqueSentinel(
              schema,
              entityType,
              constraintName,
              constraintDef,
              currentRawDomain,
            )
            const newSentinel = composeUniqueSentinel(
              schema,
              entityType,
              constraintName,
              constraintDef,
              newItem,
            )

            if (!oldSentinel && !newSentinel) continue
            if (
              oldSentinel &&
              newSentinel &&
              oldSentinel.key.pk === newSentinel.key.pk &&
              oldSentinel.key.sk === newSentinel.key.sk
            ) {
              continue
            }

            sentinelRotations.push({
              constraintName,
              oldUniqueKey: oldSentinel?.key,
              newUniqueKey: newSentinel?.key,
              newFieldsRecord: newSentinel?.fieldsRecord,
            })
          }
        }

        // Vector search: the retain path already holds the full merged
        // item, so a put-style recompute is exact — no extra read needed.
        // Re-embedding is still gated on the payload touching a source
        // field (`DESIGN.md §14`); untouched indexes keep their stored
        // vector, which the merge above already carried onto `newItem`.
        if (hasVectorIndexes) {
          const embedFor = new Set(
            vectorIndexesNeedingUpdate(
              collectTouchedFields(hydratedUpdates as globalThis.Record<string, unknown>, uState),
              uState.withVectors,
            ).map(([logicalName]) => logicalName),
          )
          // `newItem` is the fully merged post-update item (removals already
          // applied above), so a put-style recompute is exact.
          const vectorWrite = yield* computeVectorAttributes(newItem, uState.withVectors, embedFor)
          Object.assign(newItem, vectorWrite.sets)
          for (const field of vectorWrite.removes) delete newItem[field]
        }

        // Convert back to DynamoDB attribute names for storage
        renameToDynamo(newItem)
        // Flatten sparse Map fields into per-entry top-level attributes.
        try {
          serializeSparseFields(newItem)
        } catch (e) {
          return yield* new ValidationError({
            entityType,
            operation: "update.sparse",
            cause: e instanceof Error ? e.message : String(e),
          })
        }

        const currentItem = currentResult.Item
        yield* checkVersion(currentItem, "update")
        const immutableChange = immutableMismatch(immutables, currentItem)
        if (immutableChange !== undefined) {
          return yield* new ValidationError({
            entityType,
            operation: "update",
            cause: immutableChange,
          })
        }
        const pkField = config.indexes.primary.pk.field
        const skField = config.indexes.primary.sk.field
        // A legacy item of an entity whose incarnation is proven by a
        // hidden token gets one now, so the write is provable from here on.
        if (stampsIncarnation && currentItem[INCARNATION_TOKEN] === undefined) {
          newItem[INCARNATION_TOKEN] = yield* freshIncarnation
        }
        const marshalledNewItem = toAttributeMap(newItem)

        // The write is a guarded Update of what changed — never a Put of
        // the whole item read, which would overwrite whatever another
        // writer changed in between (#133). Attributes the update names
        // are written even when unchanged; the rest only when they differ.
        const named = new Set<string>(
          [
            ...Object.keys(hydratedUpdates as globalThis.Record<string, unknown>),
            ...(uState.remove ?? []),
            ...Object.keys(uState.add ?? {}),
            ...Object.keys(uState.subtract ?? {}),
            ...Object.keys(uState.append ?? {}),
            ...Object.keys(uState.deleteFromSet ?? {}),
          ].map(resolveDbName),
        )
        if (systemFields.version) named.add(systemFields.version)
        if (systemFields.updatedAt) named.add(systemFields.updatedAt)
        const setAttrs: globalThis.Record<string, AttributeValue> = {}
        for (const [attr, value] of Object.entries(marshalledNewItem)) {
          if (attr === pkField || attr === skField) continue
          if (named.has(attr) || !attributeValueEquals(currentItem[attr], value)) {
            setAttrs[attr] = value
          }
        }
        const removeAttrs = Object.keys(currentItem).filter((attr) => !(attr in marshalledNewItem))
        const written = new Set<string>([...Object.keys(setAttrs), ...removeAttrs])

        const writeNames: globalThis.Record<string, string> = {}
        const writeValues: globalThis.Record<string, AttributeValue> = {}
        const setClauses = Object.entries(setAttrs).map(([attr, value], i) => {
          writeNames[`#w${i}`] = attr
          writeValues[`:w${i}`] = value
          return `#w${i} = :w${i}`
        })
        const removeClauses = removeAttrs.map((attr, i) => {
          writeNames[`#x${i}`] = attr
          return `#x${i}`
        })
        if (setClauses.length === 0 && removeClauses.length === 0) {
          // Nothing changed: an idempotent write keeps the condition (and
          // the transaction's other items) meaningful.
          writeNames["#we"] = "__edd_e__"
          writeValues[":we"] = currentItem.__edd_e__ ?? toAttributeValue(entityType)
          setClauses.push("#we = :we")
        }
        const updateExpression = [
          setClauses.length > 0 ? `SET ${setClauses.join(", ")}` : "",
          removeClauses.length > 0 ? `REMOVE ${removeClauses.join(", ")}` : "",
        ]
          .filter((part) => part !== "")
          .join(" ")

        // The condition proves the item is still what the new values were
        // computed from. Versioned: its version and incarnation. Otherwise:
        // it exists, and every attribute whose read value fed a computed
        // value is unchanged — so an unrelated concurrent write survives
        // and a concurrent change to an input is refused.
        const condParts: Array<string> = []
        const inputs: Array<string> = []
        if (systemFields.version) {
          condParts.push(versionIs(currentVersion, writeNames, writeValues, "#ver", ":expectedVer"))
          const incarnation = incarnationGuard(currentItem, writeNames, writeValues)
          if (incarnation !== undefined) condParts.push(incarnation)
        } else {
          writeNames["#pk"] = pkField
          condParts.push("attribute_exists(#pk)")
          const fromPayload = new Set<string>([
            ...Object.entries(hydratedUpdates as globalThis.Record<string, unknown>)
              .filter(([, value]) => value !== undefined && value !== null)
              .map(([field]) => field),
          ])
          const add = (fields: ReadonlyArray<string>, always = false) => {
            for (const field of fields) {
              if ((always || !fromPayload.has(field)) && !inputs.includes(field)) {
                inputs.push(field)
              }
            }
          }
          add(
            [
              ...Object.keys(uState.add ?? {}),
              ...Object.keys(uState.subtract ?? {}),
              ...Object.keys(uState.append ?? {}),
              ...Object.keys(uState.deleteFromSet ?? {}),
            ],
            true,
          )
          for (const [indexName, indexDef] of Object.entries(allIndexes)) {
            if (indexName === "primary") continue
            if (written.has(indexDef.pk.field)) add(indexDef.pk.composite)
            if (written.has(indexDef.sk.field)) add(indexDef.sk.composite)
          }
          for (const rotation of sentinelRotations) {
            add(resolveUniqueFields(config.unique![rotation.constraintName]!), true)
          }
          for (const [, definition] of vectorIndexEntries) {
            if (written.has(definition.vectorField)) add(definition.sourceFields)
            if (written.has(definition.partitionField)) add(definition.partition)
          }
          for (const [i, field] of inputs.entries()) {
            const attr = resolveDbName(field)
            writeNames[`#g${i}`] = attr
            const read = currentItem[attr]
            if (read === undefined) {
              condParts.push(`attribute_not_exists(#g${i})`)
            } else {
              writeValues[`:g${i}`] = read
              condParts.push(`#g${i} = :g${i}`)
            }
          }
        }
        const uc = userCond ? compileCondition(userCond, resolveDbName) : undefined
        if (uc) {
          condParts.push(`(${uc.expression})`)
          Object.assign(writeNames, uc.names)
          Object.assign(writeValues, uc.values)
        }
        const mainUpdate = {
          TableName: tableName,
          Key: marshalledKey,
          UpdateExpression: updateExpression,
          ConditionExpression: condParts.join(" AND "),
          ExpressionAttributeNames: writeNames,
          ...(Object.keys(writeValues).length > 0 && {
            ExpressionAttributeValues: writeValues,
          }),
          ReturnValuesOnConditionCheckFailure: "ALL_OLD" as const,
        }

        // An update too wide for one expression (DynamoDB: 4 KB, 300
        // operators — e.g. hundreds of sparse-map entries) writes the whole
        // item instead, as before #133: under the version condition when
        // versioned (exact), otherwise under the strongest guard that fits —
        // the same fallback a wide soft delete uses.
        const wide = !expressionFits(updateExpression, "update")
        const wideGuardOrRefusal =
          wide && !systemFields.version
            ? deleteGuard(currentItem, "item", condParts.slice(1).join(" AND "))
            : undefined
        if (wideGuardOrRefusal instanceof ValidationError) return yield* wideGuardOrRefusal
        const wideGuard = wideGuardOrRefusal
        const putCondition =
          wideGuard === undefined
            ? condParts.join(" AND ")
            : [wideGuard.expression, ...condParts.slice(1)].join(" AND ")
        // Only the placeholders the condition uses (the Put has no update expression).
        const putTokens = new Set(putCondition.match(/[#:][A-Za-z0-9_]+/g) ?? [])
        const used = <V>(entries: globalThis.Record<string, V>) =>
          Object.fromEntries(Object.entries(entries).filter(([token]) => putTokens.has(token)))
        const putValues = used({ ...writeValues, ...wideGuard?.values })
        const mainPut = {
          TableName: tableName,
          Item: marshalledNewItem,
          ConditionExpression: putCondition,
          ExpressionAttributeNames: used({ ...writeNames, ...wideGuard?.names }),
          ...(Object.keys(putValues).length > 0 && { ExpressionAttributeValues: putValues }),
          ReturnValuesOnConditionCheckFailure: "ALL_OLD" as const,
        }
        const tooLarge = oversizedCondition(
          "update",
          wide ? putCondition : mainUpdate.ConditionExpression,
          userCond !== undefined
            ? compileCondition(userCond, resolveDbName)?.expression
            : undefined,
        )
        if (tooLarge !== undefined) return yield* tooLarge
        // The stored attributes the rejection compares: the guarded inputs,
        // and on the wide fallback whatever else its guard covered.
        const guardedAttrs = [
          ...new Set([...inputs.map(resolveDbName), ...(wideGuard?.inputs ?? [])]),
        ]

        // A rejected main write: which predicate, from the stored item.
        const mainRejection = (
          stored: Readonly<globalThis.Record<string, unknown>> | undefined,
        ): OptimisticLockError | ConditionalCheckFailed | ConcurrentModification | ItemNotFound => {
          if (systemFields.version) {
            return conditionRejection(
              encodedKey as globalThis.Record<string, unknown>,
              stored,
              { version: currentVersion, read: currentItem },
              userCond !== undefined,
            )
          }
          if (stored === undefined) return missingItem()
          const changed = guardedAttrs.filter(
            (attr) =>
              !attributeValueEquals(stored[attr] as AttributeValue | undefined, currentItem[attr]),
          )
          if (changed.length === 0 && userCond) {
            return new ConditionalCheckFailed({ entityType, key: encodedKey })
          }
          return new ConcurrentModification({
            entityType,
            key: encodedKey,
            attributes: changed.map(domainNameOf),
            current: Option.none(),
          })
        }
        let rejectedItem: globalThis.Record<string, AttributeValue> | undefined
        const recordRejected = (
          stored: Readonly<globalThis.Record<string, unknown>> | undefined,
        ) => {
          rejectedItem = stored as globalThis.Record<string, AttributeValue> | undefined
          return mainRejection(stored)
        }
        // `current` of a ConcurrentModification: the stored item, decoded.
        const withCurrent = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          effect.pipe(
            Effect.catchIf(
              (e): e is E & ConcurrentModification => e instanceof ConcurrentModification,
              (e) =>
                Effect.gen(function* () {
                  const current =
                    rejectedItem === undefined
                      ? Option.none()
                      : yield* Effect.option(
                          decodeAs(
                            fromAttributeMap(rejectedItem) as globalThis.Record<string, unknown>,
                            rejectedItem,
                            "model",
                          ),
                        )
                  return yield* keepReleaseRace(e, new ConcurrentModification({ ...e, current }))
                }),
            ),
          )

        // Build snapshot of the pre-update state (only when retain is enabled)
        const snapshotItem = isRetainEnabled()
          ? buildSnapshotItem(
              currentRaw as globalThis.Record<string, unknown>,
              currentVersion,
              config.indexes.primary.pk.field,
              config.indexes.primary.sk.field,
              ttlAttrName,
              now,
            )
          : undefined

        const rv = uState.returnValues ?? "allNew"
        // The item before and after the write. Exact: a versioned write is
        // CAS-guarded on what was read; an unguarded-for-unrelated-attribute
        // single UpdateItem returns its own image.
        let oldImage: globalThis.Record<string, AttributeValue> | undefined = currentItem
        let newImage: globalThis.Record<string, AttributeValue> | undefined = marshalledNewItem

        if (snapshotItem === undefined && sentinelRotations.length === 0) {
          // One item: a plain UpdateItem, whose ALL_OLD image plus this
          // write's own SET / REMOVE is the exact new image as well.
          const result = yield* withCurrent(
            (wide
              ? client.putItem({ ...mainPut, ReturnValues: "ALL_OLD" })
              : client.updateItem({ ...mainUpdate, ReturnValues: "ALL_OLD" })
            ).pipe(
              Effect.mapError((err) =>
                isAwsConditionalCheckFailed(err.cause) ? recordRejected(err.cause.Item) : err,
              ),
            ),
          )
          oldImage = result.Attributes ?? currentItem
          const applied: globalThis.Record<string, AttributeValue> = {
            ...oldImage,
            ...setAttrs,
          }
          for (const attr of removeAttrs) delete applied[attr]
          newImage = wide ? marshalledNewItem : applied
        } else {
          type TransactPut = {
            Put: {
              TableName: string
              Item: globalThis.Record<string, AttributeValue>
              ConditionExpression?: string
              ExpressionAttributeNames?: globalThis.Record<string, string>
              ExpressionAttributeValues?: globalThis.Record<string, AttributeValue>
            }
          }
          type TransactDelete = ReturnType<typeof sentinelRelease>
          type TransactUpdate = { Update: typeof mainUpdate }
          type TransactMainPut = { Put: typeof mainPut }
          const transactItems: Array<
            TransactPut | TransactDelete | TransactUpdate | TransactMainPut
          > = [wide ? { Put: mainPut } : { Update: mainUpdate }]

          // Snapshot of pre-update state (only when retain is enabled)
          if (snapshotItem) {
            transactItems.push({ Put: snapshotPut(tableName, snapshotItem) })
          }

          // Apply sentinel rotations computed earlier. Delete and Put are emitted
          // independently — a sparse field that becomes set emits Put only; a sparse
          // field that becomes unset emits Delete only. A Delete releases only a
          // sentinel this item owns (#133): the item may hold the old value
          // without its reservation, which another item may hold.
          const owner = { pk: primaryKey[pkField], sk: primaryKey[skField] }
          const owned = new Set(
            yield* ownedSentinels(
              tableName,
              sentinelRotations.flatMap((rotation) =>
                rotation.oldUniqueKey !== undefined ? [rotation.oldUniqueKey] : [],
              ),
              owner,
            ),
          )
          const sentinelPutIndices: Array<{
            index: number
            constraintName: string
            newFieldsRecord: globalThis.Record<string, string>
          }> = []
          const releaseIndices: Array<{ index: number; constraintName: string }> = []
          for (const rotation of sentinelRotations) {
            if (rotation.oldUniqueKey && owned.has(rotation.oldUniqueKey)) {
              releaseIndices.push({
                index: transactItems.length,
                constraintName: rotation.constraintName,
              })
              transactItems.push(sentinelRelease(tableName, rotation.oldUniqueKey, owner))
            }

            if (rotation.newUniqueKey && rotation.newFieldsRecord) {
              sentinelPutIndices.push({
                index: transactItems.length,
                constraintName: rotation.constraintName,
                newFieldsRecord: rotation.newFieldsRecord,
              })
              transactItems.push({
                Put: {
                  TableName: tableName,
                  Item: toAttributeMap(
                    sentinelItemFor(
                      rotation.constraintName,
                      config.unique![rotation.constraintName]!,
                      rotation.newUniqueKey,
                      owner,
                      now,
                      ttlAttrName,
                    ),
                  ),
                  ...sentinelGuard(),
                },
              })
            }
          }

          yield* checkTransactionLimit(entityType, "update", transactItems)
          yield* withCurrent(
            client.transactWriteItems({ TransactItems: transactItems }).pipe(
              Effect.mapError(
                (
                  err,
                ):
                  | DynamoClientError
                  | OptimisticLockError
                  | ConditionalCheckFailed
                  | ConcurrentModification
                  | ItemNotFound
                  | ValidationError
                  | UniqueConstraintViolation => {
                  if (isAwsTransactionCancelled(err.cause)) {
                    const reasons = err.cause.CancellationReasons
                    if (reasons) {
                      // The main item first: a sentinel Put rejected because
                      // the main write was cancelled reports nothing useful.
                      if (reasons[0]?.Code === "ConditionalCheckFailed") {
                        return recordRejected(reasons[0].Item)
                      }
                      if (snapshotItem && reasons[1]?.Code === "ConditionalCheckFailed") {
                        return historyConflict(currentVersion, "update")
                      }
                      for (const { index, constraintName, newFieldsRecord } of sentinelPutIndices) {
                        if (reasons[index]?.Code === "ConditionalCheckFailed") {
                          return new UniqueConstraintViolation({
                            entityType,
                            constraint: constraintName,
                            fields: newFieldsRecord,
                          })
                        }
                      }
                      for (const { index, constraintName } of releaseIndices) {
                        if (reasons[index]?.Code === "ConditionalCheckFailed") {
                          return releaseRaced(
                            encodedKey as globalThis.Record<string, unknown>,
                            constraintName,
                          )
                        }
                      }
                    }
                  }
                  if (isAwsConditionalCheckFailed(err.cause)) {
                    return recordRejected(err.cause.Item)
                  }
                  return err
                },
              ),
            ),
          )
        }

        // Execute cascade if configured (retain path) — from the new item.
        if (uState.cascade) {
          const domainData = {
            ...((yield* decodeAs(
              fromAttributeMap(newImage) as globalThis.Record<string, unknown>,
              newImage,
              "model",
            )) as object),
          }
          const sourceId =
            sourceIdentifierField != null
              ? (encodedKey as globalThis.Record<string, unknown>)[sourceIdentifierField]
              : undefined
          if (sourceId != null) {
            yield* executeCascade(uState.cascade, domainData, String(sourceId))
          }
        }

        return yield* updateResult(
          rv,
          oldImage,
          newImage,
          written,
          mode,
          encodedKey as globalThis.Record<string, unknown>,
        )
      }

      // --- Standard path: updateItem ---
      // A retain entity reaching this branch carries path operations. Read
      // the item first: its version snapshot is built from it, and the
      // update below is conditioned on that version, so the snapshot is
      // exactly the item the update replaces (#133).
      let retainSnapshot:
        | {
            readonly item: globalThis.Record<string, unknown>
            readonly raw: globalThis.Record<string, AttributeValue>
            readonly version: number
            /** The incarnation the post-image carries (proof of this write). */
            readonly incarnation: AttributeValue | undefined
            /** A fresh incarnation token this write stamps on a legacy item. */
            readonly stamp: string | undefined
          }
        | undefined
      // A cascade that also returns the OLD item, with path operations on a
      // versioned entity: read first, condition the write on that version
      // and incarnation, so the read IS the old item and ALL_NEW the new.
      let casRead:
        | { readonly raw: globalThis.Record<string, AttributeValue>; readonly version: number }
        | undefined
      if (isRetainEnabled() || needsBothImages) {
        const current =
          preRead !== undefined
            ? { Item: preRead }
            : yield* client.getItem({
                TableName: tableName,
                Key: marshalledKey,
                ConsistentRead: true,
              })
        if (!current.Item) {
          return yield* missingItem()
        }
        yield* checkVersion(current.Item, "update")
        const immutableChange = immutableMismatch(immutables, current.Item)
        if (immutableChange !== undefined) {
          return yield* new ValidationError({
            entityType,
            operation: "update",
            cause: immutableChange,
          })
        }
        const currentRaw = fromAttributeMap(current.Item) as globalThis.Record<string, unknown>
        const version = systemFields.version
          ? ((currentRaw[systemFields.version] as number | undefined) ?? 0)
          : 0
        if (evExpected !== undefined && version !== evExpected) {
          return yield* new OptimisticLockError({
            entityType,
            key: encodedKey,
            expectedVersion: evExpected,
            actualVersion: version,
          })
        }
        if (!isRetainEnabled()) {
          casRead = { raw: current.Item, version }
        } else {
          retainSnapshot = {
            item: buildSnapshotItem(
              currentRaw,
              version,
              config.indexes.primary.pk.field,
              config.indexes.primary.sk.field,
              ttlAttrName,
              now,
            ),
            raw: current.Item,
            version,
            ...(stampsIncarnation && current.Item[INCARNATION_TOKEN] === undefined
              ? yield* freshIncarnation.pipe(
                  Effect.map((stamp) => ({ stamp, incarnation: toAttributeValue(stamp) })),
                )
              : {
                  stamp: undefined,
                  incarnation:
                    incarnationAttr !== undefined ? current.Item[incarnationAttr] : undefined,
                }),
          }
        }
      }
      // Plain (unread) updates require the item to exist (#133). One that
      // could create a complete item — a plain `.set()` of every required
      // field and key composite — does so on a missing item through `put`
      // (`createOnMissing`), so the item is exactly the one `put` writes.
      const plainWrite = retainSnapshot === undefined && casRead === undefined
      const createsOnMissing =
        plainWrite &&
        upsertable &&
        !uState.patch &&
        evExpected === undefined &&
        userCond === undefined &&
        uState.cascade === undefined &&
        uState.withVectors === undefined &&
        (requestedRv === "allNew" || requestedRv === "none" || requestedRv === "updatedNew") &&
        [uState.remove, uState.sparseRemoveEntries, uState.sparseClearFields].every(
          (ops) => ops === undefined || ops.length === 0,
        ) &&
        [uState.add, uState.subtract, uState.append, uState.deleteFromSet].every(
          (ops) => ops === undefined || Object.keys(ops).length === 0,
        ) &&
        !hasPathOps(uState)
      // Build UpdateExpression
      const setClauses: Array<string> = []
      const names: globalThis.Record<string, string> = {}
      const values: globalThis.Record<string, AttributeValue> = {}
      let counter = 0

      // `hydratedUpdates` was produced by `Schema.encode(updateSchema)` and
      // is already in wire-format (ISO string / epoch number / etc.). Use
      // it directly — no `serializeDateFields` pass required.
      const encodedUpdatesMap = hydratedUpdates as globalThis.Record<string, unknown>

      // System-colliding updatedAt is handled by the system-field block below.
      // createdAt and version are already excluded by `updateSchema`.
      const updateSystemColliders = new Set<string>()
      if (systemFields.updatedAtCollision && systemFields.updatedAt)
        updateSystemColliders.add(systemFields.updatedAt)

      // Add user-provided updates. Buffered REMOVE clauses for null
      // payload entries land in `removeClauses` below — under v3,
      // `set({ attr: null | undefined })` REMOVEs the attribute from the
      // item but does NOT separately cascade-drop GSI keys. The structural
      // composer treats the cleared attribute as absent and recomposes
      // (or truncates / drops / no-ops) per the half's policy.
      // EDD-9025 guarantees no composite is nullable, so `set` of `null`
      // is only reachable for non-composite, model-declared-nullable
      // attributes. See DESIGN.md §7.
      const nullClears: Array<string> = []
      for (const [attr, val] of Object.entries(encodedUpdatesMap)) {
        if (updateSystemColliders.has(attr)) continue

        if (val === null || val === undefined) {
          // Sparse fields: clearing a sparse field with null is a no-op
          // here — the `.removeEntries` API is the explicit per-key
          // remove. Skip to avoid REMOVE'ing the whole prefix erroneously.
          if (hasSparseFields && attr in sparseFields) continue
          nullClears.push(attr)
          continue
        }

        // Sparse fields: emit one SET per bucket. Whole-bucket replace
        // semantics — concurrent writers to disjoint buckets are safe.
        if (hasSparseFields && attr in sparseFields) {
          if (typeof val !== "object") continue
          const sparse = sparseFields[attr]!
          for (const [k, v] of Object.entries(val as globalThis.Record<string, unknown>)) {
            if (typeof k !== "string" || k.length === 0 || k.includes("#")) {
              return yield* new ValidationError({
                entityType,
                operation: "update.sparse",
                cause: `Sparse map "${attr}": invalid key ${JSON.stringify(k)}`,
              })
            }
            const nameKey = `#u${counter}`
            const valKey = `:u${counter}`
            names[nameKey] = `${sparse.prefix}#${k}`
            values[valKey] = toAttributeValue(v)
            setClauses.push(`${nameKey} = ${valKey}`)
            counter++
          }
          continue
        }

        const nameKey = `#u${counter}`
        const valKey = `:u${counter}`
        names[nameKey] = resolveDbName(attr)
        values[valKey] = toAttributeValue(val)
        setClauses.push(`${nameKey} = ${valKey}`)
        counter++
      }

      // Add updatedAt timestamp. User-supplied value wins (already in wire
      // form via encode); else fall back to a freshly generated wire
      // primitive.
      if (systemFields.updatedAt) {
        const userSupplied = encodedUpdatesMap[systemFields.updatedAt]
        const nameKey = `#u${counter}`
        const valKey = `:u${counter}`
        names[nameKey] = systemFields.updatedAt
        values[valKey] = toAttributeValue(
          userSupplied !== undefined
            ? userSupplied
            : generateTimestamp(systemFields.updatedAtEncoding, now),
        )
        setClauses.push(`${nameKey} = ${valKey}`)
        counter++
      }

      // Add version increment
      if (systemFields.version) {
        const nameKey = `#u${counter}`
        names[nameKey] = systemFields.version
        // An item written before the entity was `versioned` has none: its
        // first versioned write makes it version 1 (#133).
        values[":vzero"] = toAttributeValue(0)
        setClauses.push(`${nameKey} = if_not_exists(${nameKey}, :vzero) + :vinc`)
        values[":vinc"] = toAttributeValue(1)
        counter++
      }

      // Rich update operations from state
      const removeClauses: Array<string> = []
      const addClauses: Array<string> = []
      const deleteClauses: Array<string> = []

      // SUBTRACT → synthesize SET #field = #field - :val
      if (uState.subtract) {
        for (const [attr, val] of Object.entries(uState.subtract)) {
          const nameKey = `#u${counter}`
          const valKey = `:u${counter}`
          names[nameKey] = resolveDbName(attr)
          values[valKey] = toAttributeValue(val)
          setClauses.push(`${nameKey} = ${nameKey} - ${valKey}`)
          counter++
        }
      }

      // APPEND → synthesize SET #field = list_append(#field, :val)
      if (uState.append) {
        for (const [attr, val] of Object.entries(uState.append)) {
          const nameKey = `#u${counter}`
          const valKey = `:u${counter}`
          names[nameKey] = resolveDbName(attr)
          const appended = encodePathValue([attr], val, "elements")
          if (appended.issue !== undefined) {
            return yield* new ValidationError({
              entityType,
              operation: "update.append",
              cause: appended.issue,
            })
          }
          values[valKey] = toAttributeValue(appended.encoded)
          setClauses.push(`${nameKey} = list_append(${nameKey}, ${valKey})`)
          counter++
        }
      }

      // REMOVE
      // Two channels feed REMOVE clauses:
      //  1. `Entity.remove([attr])` — explicit per-attribute drop. Cascades
      //     to drop the GSI keys for any GSI containing the attr as a
      //     composite (one of v3's two drop triggers — see DESIGN.md §7).
      //     Tracked in `removedSet` so the composer can apply the cascade
      //     rule.
      //  2. Null payload entries (`set({ attr: null })`). These REMOVE the
      //     attribute from the item only. They do NOT cascade GSI drops —
      //     under v3 two-way classification, the structural composer just
      //     treats the attribute as absent. (EDD-9025 guarantees these
      //     are never composites.)
      const removedSet = uState.remove ? new Set(uState.remove) : undefined
      if (uState.remove) {
        for (const attr of uState.remove) {
          const nameKey = `#r${removeClauses.length}`
          names[nameKey] = resolveDbName(attr)
          removeClauses.push(nameKey)
        }
      }
      for (const attr of nullClears) {
        const nameKey = `#r${removeClauses.length}`
        names[nameKey] = resolveDbName(attr)
        removeClauses.push(nameKey)
      }

      // Policy-aware GSI key composition (v1.7.1 — per-half evaluation
      // gate, structural rule, and per-half cascade). One call covers
      // SETs (full recompose, truncated leading prefix), per-half REMOVEs
      // (sparse can't-compose, or preserve + cascade override), and
      // per-half noops (preserve + can't-compose without cascade). See
      // DESIGN.md §7 Policy-Aware GSI Composition.
      //
      // No try/catch — EDD-9024 was deprecated in v1.7.1 and the
      // composer no longer throws. Hole patterns collapse into the
      // unified can't-compose rule.
      const gsiUpdate = KeyComposer.composeGsiKeysForUpdatePolicyAware(
        schema,
        entityType,
        entityVersion,
        allIndexes,
        keyForm(hydratedUpdates as globalThis.Record<string, unknown>),
        keyForm(encodedKey as globalThis.Record<string, unknown>),
        removedSet === undefined ? {} : { removedSet },
      )
      // The key and vector attributes this update writes — a retain
      // snapshot strips them, so the post-image is rebuilt from these.
      const derivedSets: globalThis.Record<string, AttributeValue> = {}
      const derivedRemoves = new Set<string>()
      for (const [field, value] of Object.entries(gsiUpdate.sets)) {
        const nameKey = `#u${counter}`
        const valKey = `:u${counter}`
        names[nameKey] = field
        values[valKey] = toAttributeValue(value)
        derivedSets[field] = values[valKey]
        setClauses.push(`${nameKey} = ${valKey}`)
        counter++
      }
      for (const keyField of gsiUpdate.removes) {
        const nameKey = `#r${removeClauses.length}`
        names[nameKey] = keyField
        derivedRemoves.add(keyField)
        removeClauses.push(nameKey)
      }

      // Vector search: partition value is recomposed whenever it can be
      // (idempotent); the embedding is regenerated only when the payload
      // touched a `source.fields` member or supplied `.withVector(...)`.
      // See `DESIGN.md §14 Write path`.
      if (hasVectorIndexes) {
        const vectorWrite = yield* computeVectorUpdateAttributes(
          encodedKey as globalThis.Record<string, unknown>,
          marshalledKey,
          tableName,
          hydratedUpdates as globalThis.Record<string, unknown>,
          uState,
        )
        for (const [field, value] of Object.entries(vectorWrite.sets)) {
          const nameKey = `#u${counter}`
          const valKey = `:u${counter}`
          names[nameKey] = field
          values[valKey] = toAttributeValue(value)
          derivedSets[field] = values[valKey]
          setClauses.push(`${nameKey} = ${valKey}`)
          counter++
        }
        // Clearing every source field takes the item out of the index —
        // there is no other way to delete a vector index entry.
        for (const field of new Set(vectorWrite.removes)) {
          const nameKey = `#r${removeClauses.length}`
          names[nameKey] = field
          derivedRemoves.add(field)
          removeClauses.push(nameKey)
        }
      }

      // ADD (atomic numeric increment / set addition)
      if (uState.add) {
        for (const [attr, val] of Object.entries(uState.add)) {
          const nameKey = `#a${addClauses.length}`
          const valKey = `:a${addClauses.length}`
          names[nameKey] = resolveDbName(attr)
          values[valKey] = toAttributeValue(val)
          addClauses.push(`${nameKey} ${valKey}`)
        }
      }

      // DELETE (remove elements from set)
      if (uState.deleteFromSet) {
        for (const [attr, val] of Object.entries(uState.deleteFromSet)) {
          const nameKey = `#d${deleteClauses.length}`
          const valKey = `:d${deleteClauses.length}`
          names[nameKey] = resolveDbName(attr)
          values[valKey] = toAttributeValue(val)
          deleteClauses.push(`${nameKey} ${valKey}`)
        }
      }

      // Path-based operations (from typed callback API)
      const pathCounter = { value: 0 }

      // Path SET operations
      if (uState.pathSets) {
        for (const op of uState.pathSets) {
          const pathExpr = compilePath(op.segments, names, "ps", pathCounter, resolveDbName)
          if (op.isPath && op.valueSegments) {
            const srcExpr = compilePath(op.valueSegments, names, "ps", pathCounter, resolveDbName)
            setClauses.push(`${pathExpr} = ${srcExpr}`)
          } else {
            const valKey = `:ps${pathCounter.value++}`
            const set = encodePathValue(op.segments, op.value, "value")
            if (set.issue !== undefined) {
              return yield* new ValidationError({
                entityType,
                operation: "update.pathSet",
                cause: set.issue,
              })
            }
            values[valKey] = toAttributeValue(set.encoded)
            setClauses.push(`${pathExpr} = ${valKey}`)
          }
        }
      }

      // Path SUBTRACT operations
      if (uState.pathSubtracts) {
        for (const op of uState.pathSubtracts) {
          const pathExpr = compilePath(op.segments, names, "psb", pathCounter, resolveDbName)
          if (op.isPath && op.valueSegments) {
            const srcExpr = compilePath(op.valueSegments, names, "psb", pathCounter, resolveDbName)
            setClauses.push(`${pathExpr} = ${pathExpr} - ${srcExpr}`)
          } else {
            const valKey = `:psb${pathCounter.value++}`
            values[valKey] = toAttributeValue(op.value)
            setClauses.push(`${pathExpr} = ${pathExpr} - ${valKey}`)
          }
        }
      }

      // Path APPEND operations
      if (uState.pathAppends) {
        for (const op of uState.pathAppends) {
          const pathExpr = compilePath(op.segments, names, "pa", pathCounter, resolveDbName)
          const valKey = `:pa${pathCounter.value++}`
          const appended = encodePathValue(op.segments, op.value, "elements")
          if (appended.issue !== undefined) {
            return yield* new ValidationError({
              entityType,
              operation: "update.pathAppend",
              cause: appended.issue,
            })
          }
          values[valKey] = toAttributeValue(appended.encoded)
          setClauses.push(`${pathExpr} = list_append(${pathExpr}, ${valKey})`)
        }
      }

      // Path PREPEND operations
      if (uState.pathPrepends) {
        for (const op of uState.pathPrepends) {
          const pathExpr = compilePath(op.segments, names, "pp", pathCounter, resolveDbName)
          const valKey = `:pp${pathCounter.value++}`
          const prepended = encodePathValue(op.segments, op.value, "elements")
          if (prepended.issue !== undefined) {
            return yield* new ValidationError({
              entityType,
              operation: "update.pathPrepend",
              cause: prepended.issue,
            })
          }
          values[valKey] = toAttributeValue(prepended.encoded)
          setClauses.push(`${pathExpr} = list_append(${valKey}, ${pathExpr})`)
        }
      }

      // Path if_not_exists operations
      if (uState.pathIfNotExists) {
        for (const op of uState.pathIfNotExists) {
          const pathExpr = compilePath(op.segments, names, "pi", pathCounter, resolveDbName)
          const valKey = `:pi${pathCounter.value++}`
          const ifAbsent = encodePathValue(op.segments, op.value, "value")
          if (ifAbsent.issue !== undefined) {
            return yield* new ValidationError({
              entityType,
              operation: "update.pathIfNotExists",
              cause: ifAbsent.issue,
            })
          }
          values[valKey] = toAttributeValue(ifAbsent.encoded)
          setClauses.push(`${pathExpr} = if_not_exists(${pathExpr}, ${valKey})`)
        }
      }

      // Path REMOVE operations
      if (uState.pathRemoves) {
        for (const segments of uState.pathRemoves) {
          const pathExpr = compilePath(segments, names, "pr", pathCounter, resolveDbName)
          removeClauses.push(pathExpr)
        }
      }

      // Path ADD operations
      if (uState.pathAdds) {
        for (const op of uState.pathAdds) {
          const pathExpr = compilePath(op.segments, names, "pad", pathCounter, resolveDbName)
          const valKey = `:pad${pathCounter.value++}`
          values[valKey] = toAttributeValue(op.value)
          addClauses.push(`${pathExpr} ${valKey}`)
        }
      }

      // Path DELETE operations
      if (uState.pathDeletes) {
        for (const op of uState.pathDeletes) {
          const pathExpr = compilePath(op.segments, names, "pd", pathCounter, resolveDbName)
          const valKey = `:pd${pathCounter.value++}`
          values[valKey] = toAttributeValue(op.value)
          deleteClauses.push(`${pathExpr} ${valKey}`)
        }
      }

      // Sparse-map .removeEntries — explicit REMOVE on `<prefix>#<key>`.
      // Each key emits one REMOVE clause through ExpressionAttributeNames
      // (the literal "#" survives via compilePath's raw-segment escape).
      if (uState.sparseRemoveEntries) {
        for (const op of uState.sparseRemoveEntries) {
          if (!hasSparseFields || !(op.field in sparseFields)) {
            return yield* new ValidationError({
              entityType,
              operation: "update.removeEntries",
              cause: `field "${op.field}" is not configured as a sparse map`,
            })
          }
          const sparse = sparseFields[op.field]!
          for (const k of op.keys) {
            if (typeof k !== "string" || k.length === 0 || k.includes("#")) {
              return yield* new ValidationError({
                entityType,
                operation: "update.removeEntries",
                cause: `Sparse map "${op.field}": invalid key ${JSON.stringify(k)}`,
              })
            }
            const nameKey = `#sr${removeClauses.length}`
            names[nameKey] = `${sparse.prefix}#${k}`
            removeClauses.push(nameKey)
          }
        }
      }

      // Sparse-map .clearMap — Get-then-Update helper. Reads the current
      // item with a consistent read to discover which `<prefix>#*` attrs
      // exist, then folds the resulting REMOVEs into this same UpdateItem.
      // On a versioned entity the update is conditioned on the version that
      // read found (`casRead`), so a writer adding a bucket in between makes
      // it fail rather than survive the clear; a stale `expectedVersion` is
      // refused before anything is sent. Non-versioned entities are
      // best-effort (a concurrent writer can add a new bucket between the read
      // and the update — that bucket survives).
      if (uState.sparseClearFields && uState.sparseClearFields.length > 0) {
        for (const field of uState.sparseClearFields) {
          if (!hasSparseFields || !(field in sparseFields)) {
            return yield* new ValidationError({
              entityType,
              operation: "update.clearMap",
              cause: `field "${field}" is not configured as a sparse map`,
            })
          }
        }
        // Read once for all clearMap fields combined.
        const clearGetResult = yield* client.getItem({
          TableName: tableName,
          Key: marshalledKey,
          ConsistentRead: true,
        })
        if (clearGetResult.Item) {
          if (systemFields.version && casRead === undefined && retainSnapshot === undefined) {
            yield* checkVersion(clearGetResult.Item, "update")
            const clearedRaw = fromAttributeMap(clearGetResult.Item) as globalThis.Record<
              string,
              unknown
            >
            const version = (clearedRaw[systemFields.version] as number | undefined) ?? 0
            if (evExpected !== undefined && version !== evExpected) {
              return yield* new OptimisticLockError({
                entityType,
                key: encodedKey,
                expectedVersion: evExpected,
                actualVersion: version,
              })
            }
            casRead = { raw: clearGetResult.Item, version }
          }
          const clearItemKeys = Object.keys(clearGetResult.Item)
          for (const field of uState.sparseClearFields) {
            const sparse = sparseFields[field]!
            const prefixWithDelim = `${sparse.prefix}#`
            for (const attrName of clearItemKeys) {
              if (attrName.startsWith(prefixWithDelim)) {
                const nameKey = `#sc${removeClauses.length}`
                names[nameKey] = attrName
                removeClauses.push(nameKey)
              }
            }
          }
        }
        // If the item doesn't exist, clearMap is a no-op — and the
        // subsequent UpdateItem's existence condition reports it.
      }

      // A legacy retain item gets its incarnation token with this write.
      if (retainSnapshot?.stamp !== undefined) {
        names["#eddInc"] = INCARNATION_TOKEN
        values[":eddInc"] = toAttributeValue(retainSnapshot.stamp)
        setClauses.push("#eddInc = :eddInc")
      }

      const hasAnyUpdate =
        setClauses.length > 0 ||
        removeClauses.length > 0 ||
        addClauses.length > 0 ||
        deleteClauses.length > 0

      const rv = uState.returnValues ?? "allNew"
      if (!hasAnyUpdate) {
        // Nothing to update: nothing written, so nothing "updated"; the
        // whole-item modes are the current item.
        if (rv === "none") return undefined
        if (rv === "updatedOld" || rv === "updatedNew") {
          return yield* decodePartial(undefined, new Set(), mode)
        }
        return yield* get(key)._run(mode)
      }

      // Compose UpdateExpression from all clause types
      const expressionParts: Array<string> = []
      if (setClauses.length > 0) expressionParts.push(`SET ${setClauses.join(", ")}`)
      if (removeClauses.length > 0) expressionParts.push(`REMOVE ${removeClauses.join(", ")}`)
      if (addClauses.length > 0) expressionParts.push(`ADD ${addClauses.join(", ")}`)
      if (deleteClauses.length > 0) expressionParts.push(`DELETE ${deleteClauses.join(", ")}`)
      const updateExpression = expressionParts.join(" ")

      // Build condition expression — combine optimistic lock + user condition
      const condParts: Array<string> = []
      if (evExpected !== undefined && systemFields.version) {
        condParts.push(versionIs(evExpected, names, values, "#condVer", ":expectedVer"))
      }
      if (plainWrite && systemFields.version) {
        // Never version an item whose version was removed outside the library
        // (incarnation token, no version) as if it predated versioning.
        names["#intVer"] = systemFields.version
        names["#intInc"] = INCARNATION_TOKEN
        condParts.push("(attribute_exists(#intVer) OR attribute_not_exists(#intInc))")
      }
      // A plain write already requires the item (`attribute_exists(#exists)`
      // below), so `patch`'s own exists guard would only repeat it.
      const sentCond = plainWrite ? uState.condition : userCond
      const uc = sentCond ? compileCondition(sentCond, resolveDbName) : undefined
      if (uc) {
        condParts.push(`(${uc.expression})`)
        Object.assign(names, uc.names)
        Object.assign(values, uc.values)
      }
      if (plainWrite) {
        // Restated immutable values must match the item's.
        for (const [i, { attr, value }] of immutables.entries()) {
          names[`#imm${i}`] = attr
          values[`:imm${i}`] = value
          condParts.push(`#imm${i} = :imm${i}`)
        }
        names["#exists"] = config.indexes.primary.pk.field
        condParts.push("attribute_exists(#exists)")
      }
      if (casRead !== undefined && systemFields.version) {
        condParts.push(versionIs(casRead.version, names, values, "#casVer", ":casVer"))
        const incarnation = incarnationGuard(casRead.raw, names, values)
        if (incarnation !== undefined) condParts.push(incarnation)
      }
      const conditionExpression = condParts.length > 0 ? condParts.join(" AND ") : undefined
      {
        const tooLarge = oversizedCondition(
          "update",
          conditionExpression,
          sentCond !== undefined
            ? compileCondition(sentCond, resolveDbName)?.expression
            : undefined,
        )
        if (tooLarge !== undefined) return yield* tooLarge
      }

      // The top-level attributes this update writes — what `updatedOld` /
      // `updatedNew` return.
      const written = expressionTargets(updateExpression, names)
      const cascadeFrom = (image: globalThis.Record<string, AttributeValue>) =>
        Effect.gen(function* () {
          if (!uState.cascade) return
          const sourceId =
            sourceIdentifierField != null
              ? (encodedKey as globalThis.Record<string, unknown>)[sourceIdentifierField]
              : undefined
          if (sourceId == null) return
          const domainData = yield* decodeAs(
            fromAttributeMap(image) as globalThis.Record<string, unknown>,
            image,
            "model",
          )
          yield* executeCascade(uState.cascade, { ...(domainData as object) }, String(sourceId))
        })

      if (retainSnapshot !== undefined) {
        // The SAME UpdateExpression, transacted with the version snapshot of
        // the item it replaces — DynamoDB applies the path operations, so
        // their semantics are DynamoDB's own (#133). The version AND the
        // incarnation prove the snapshot is of the item this replaces.
        const snapshot = retainSnapshot
        const guardParts = [...condParts]
        if (systemFields.version) {
          guardParts.push(versionIs(snapshot.version, names, values, "#retainVer", ":retainVer"))
          const incarnation = incarnationGuard(snapshot.raw, names, values)
          if (incarnation !== undefined) guardParts.push(incarnation)
        }
        const tooLarge = oversizedCondition(
          "update",
          guardParts.join(" AND "),
          sentCond !== undefined
            ? compileCondition(sentCond, resolveDbName)?.expression
            : undefined,
        )
        if (tooLarge !== undefined) return yield* tooLarge
        const transactItems = [
          {
            Update: {
              TableName: tableName,
              Key: marshalledKey,
              UpdateExpression: updateExpression,
              ExpressionAttributeNames: names,
              ExpressionAttributeValues: Object.keys(values).length > 0 ? values : undefined,
              ...(guardParts.length > 0 && {
                ConditionExpression: guardParts.join(" AND "),
                ReturnValuesOnConditionCheckFailure: "ALL_OLD" as const,
              }),
            },
          },
          { Put: snapshotPut(tableName, snapshot.item) },
        ]
        yield* checkTransactionLimit(entityType, "update", transactItems)
        yield* client.transactWriteItems({ TransactItems: transactItems }).pipe(
          Effect.mapError(
            (
              err,
            ):
              | DynamoClientError
              | OptimisticLockError
              | ConditionalCheckFailed
              | ValidationError => {
              // The update is guarded by the snapshot version AND any user
              // condition; the stored item (ALL_OLD) says which rejected it.
              const rejection = (
                stored: Readonly<globalThis.Record<string, unknown>> | undefined,
              ) =>
                conditionRejection(
                  encodedKey as globalThis.Record<string, unknown>,
                  stored,
                  systemFields.version
                    ? { version: snapshot.version, read: snapshot.raw }
                    : undefined,
                  userCond !== undefined,
                )
              if (isAwsTransactionCancelled(err.cause)) {
                const main = err.cause.CancellationReasons?.[0]
                if (main?.Code === "ConditionalCheckFailed") return rejection(main.Item)
                if (err.cause.CancellationReasons?.[1]?.Code === "ConditionalCheckFailed") {
                  return historyConflict(snapshot.version, "update")
                }
              }
              if (isAwsConditionalCheckFailed(err.cause)) return rejection(err.cause.Item)
              return err
            },
          ),
        )
        // A transacted Update returns no attributes. The old item is exact:
        // the version condition proves `snapshot.raw` is what it replaced.
        // The new item is read back only when needed, and proven ours.
        const needsNew = rv === "allNew" || rv === "updatedNew" || uState.cascade !== undefined
        const after = needsNew
          ? yield* readRetainPostImage(
              tableName,
              marshalledKey,
              snapshot.raw,
              snapshot.version + 1,
              snapshot.incarnation,
              derivedSets,
              derivedRemoves,
              ttlAttrName,
              encodedKey as globalThis.Record<string, unknown>,
            )
          : undefined
        if (after !== undefined) yield* cascadeFrom(after)
        return yield* updateResult(
          rv,
          snapshot.raw,
          after,
          written,
          mode,
          encodedKey as globalThis.Record<string, unknown>,
        )
      }

      // The image the mode returns; `none` asks for nothing unless a
      // cascade needs the new item.
      const wantsOld = casRead === undefined && (rv === "allOld" || rv === "updatedOld")
      const returned = wantsOld
        ? "ALL_OLD"
        : rv === "none" && !uState.cascade && casRead === undefined
          ? "NONE"
          : "ALL_NEW"
      // DynamoDB rejects an empty `ExpressionAttributeValues` map. When
      // the UpdateExpression is REMOVE-only (e.g. clearMap with no other
      // combinators), `values` may be empty — omit the property entirely.
      const result = yield* client
        .updateItem({
          TableName: tableName,
          Key: marshalledKey,
          UpdateExpression: updateExpression,
          ExpressionAttributeNames: names,
          ExpressionAttributeValues: Object.keys(values).length > 0 ? values : undefined,
          ConditionExpression: conditionExpression,
          ...(conditionExpression !== undefined && {
            ReturnValuesOnConditionCheckFailure: "ALL_OLD" as const,
          }),
          ReturnValues: returned,
        })
        .pipe(
          Effect.mapError(
            (
              err,
            ):
              | DynamoClientError
              | OptimisticLockError
              | ConditionalCheckFailed
              | ItemNotFound
              | MissingForCreate
              | ValidationError => {
              // The stored item (ALL_OLD) says what rejected the update: no
              // item (it must exist), a restated immutable value, the
              // version CAS, or the user's condition.
              if (isAwsConditionalCheckFailed(err.cause)) {
                const stored = err.cause.Item
                if (stored === undefined && !uState.patch) {
                  return createsOnMissing
                    ? new MissingForCreate({ input: uState.updates })
                    : new ItemNotFound({ entityType, key: encodedKey })
                }
                const corrupt = versionCorruption(stored, "update")
                if (corrupt !== undefined) return corrupt
                const immutableChange =
                  stored !== undefined ? immutableMismatch(immutables, stored) : undefined
                if (immutableChange !== undefined) {
                  return new ValidationError({
                    entityType,
                    operation: "update",
                    cause: immutableChange,
                  })
                }
                return conditionRejection(
                  encodedKey as globalThis.Record<string, unknown>,
                  stored,
                  casRead !== undefined
                    ? { version: casRead.version, read: casRead.raw }
                    : systemFields.version && evExpected !== undefined
                      ? { version: evExpected }
                      : undefined,
                  userCond !== undefined,
                )
              }
              return err
            },
          ),
        )

      // A cascade never meets an old-image mode here: that combination is
      // routed to the read-then-write branch, or read first (`casRead`).
      const newImage = wantsOld ? undefined : result.Attributes
      if (uState.cascade && newImage !== undefined) yield* cascadeFrom(newImage)

      return yield* updateResult(
        rv,
        casRead !== undefined ? casRead.raw : wantsOld ? result.Attributes : undefined,
        newImage,
        written,
        mode,
        encodedKey as globalThis.Record<string, unknown>,
      )
    })

  /**
   * An update; on a missing item, a plain `.set()` of a complete item creates
   * it through `create` — exactly the item `put` writes (#133). If another
   * writer creates it first, the update runs once more against that item.
   */
  const updateOrCreate = (key: unknown, mode: DecodeMode, requested: UpdateState) =>
    runUpdate(key, mode, requested).pipe(
      Effect.catchIf(
        (e): e is MissingForCreate => e instanceof MissingForCreate,
        (missing) =>
          Effect.gen(function* () {
            const created = yield* Effect.exit(
              (create(missing.input) as unknown as EntityPutImpl<any, any, any, any>)._run(
                "native",
              ) as Effect.Effect<globalThis.Record<string, AttributeValue>, unknown, any>,
            )
            if (created._tag === "Failure") {
              const raced = Cause.findErrorOption(created.cause).pipe(
                Option.filter((e) => e instanceof ConditionalCheckFailed),
              )
              if (Option.isNone(raced)) return yield* created
              // Created concurrently: now an ordinary update of that item.
              return yield* runUpdate(key, mode, requested).pipe(
                Effect.catchIf(
                  (e): e is MissingForCreate => e instanceof MissingForCreate,
                  () =>
                    Effect.fail(
                      new ItemNotFound({
                        entityType,
                        key: key as globalThis.Record<string, unknown>,
                      }),
                    ),
                ),
              )
            }
            const item = created.value
            return yield* updateResult(
              requested.returnValues ?? "allNew",
              undefined,
              item,
              new Set(Object.keys(item)),
              mode,
              key as globalThis.Record<string, unknown>,
            )
          }),
      ),
    )

  const update = (key: unknown) =>
    new EntityUpdateImpl(
      (mode: DecodeMode, requested: UpdateState) => updateOrCreate(key, mode, requested),
      emptyUpdateState,
      self,
      key as globalThis.Record<string, unknown>,
    )

  /**
   * Run one of this entity's write ops with its write recorded instead of
   * sent (`planWrite`), and say how to read a cancellation of what it would
   * have written. The op is the standalone op itself, so the plan carries
   * every read it makes, every item it writes and every guard it puts on them
   * — see `internal/TransactPlan.ts`.
   *
   * The verdict follows a guarded put's (`_planPut`): a cancelled sentinel
   * reservation is the `UniqueConstraintViolation` the standalone op reports;
   * a cancelled main item is the caller's condition when there is one
   * (`TransactionCancelled`); anything else is a lost race — the row changed
   * between the read and the transaction — and `transactWrite` plans it again
   * from a fresh read.
   *
   * `payload` is what the caller asked to write. With the op's own read of
   * the row, it lets a reservation say which values it reserves.
   */
  const planned = <E, R>(
    operation: string,
    key: unknown,
    /** The op itself — a factory, because a missing row means running it again. */
    op: () => Effect.Effect<unknown, E, R>,
    request: {
      readonly payload: globalThis.Record<string, unknown>
      /** Values `.add()` / `.subtract()` change by, for the reported reservation. */
      readonly deltas?: globalThis.Record<string, number> | undefined
      /** The caller's own `.condition()` — not `patch`'s or `deleteIfExists`'s guard. */
      readonly callerCondition: boolean
      readonly expectedVersion?: number | undefined
    },
  ) =>
    Effect.gen(function* () {
      const encodedKey = yield* encodeKey(key, `${operation}.decodeKey`)
      const primaryKey = composePrimaryKey(encodedKey)
      const ownKey = toAttributeMap(primaryKey)
      const recorded = yield* planWrite(operation, op(), ownKey)
      let { items, ownRow } = recorded
      // An op that wrote without reading its row relies on its write's own
      // condition to find the row missing — a standalone `update` then
      // creates it (or fails `ItemNotFound`), `patch` fails
      // `ConditionalCheckFailed`. A transaction cannot follow that fallback,
      // so the row is read here, and a missing row is answered to the op as
      // its write would have been answered: it takes the same path.
      if (!recorded.readOwnRow && items.length > 0) {
        const client = yield* DynamoClient
        const { name: tableName } = yield* tableTag
        const read = yield* client.getItem({
          TableName: tableName,
          Key: ownKey,
          ConsistentRead: true,
        })
        if (read.Item === undefined) {
          ;({ items, ownRow } = yield* planWrite(operation, op(), ownKey, { rowMissing: true }))
        } else {
          ownRow = read.Item
          // A pinned version the row no longer has is refused before sending,
          // as the read-first paths do — not retried as if it were a race.
          if (request.expectedVersion !== undefined && systemFields.version) {
            const stored = fromAttributeMap(read.Item)[systemFields.version]
            const actual = typeof stored === "number" ? stored : 0
            if (actual !== request.expectedVersion) {
              return yield* new OptimisticLockError({
                entityType,
                key: encodedKey,
                expectedVersion: request.expectedVersion,
                actualVersion: actual,
              })
            }
          }
        }
      }
      const pkField = config.indexes.primary.pk.field
      const skField = config.indexes.primary.sk.field
      const isOwn = (row: globalThis.Record<string, AttributeValue> | undefined) => {
        if (row === undefined) return false
        const plain = fromAttributeMap(row)
        return plain[pkField] === primaryKey[pkField] && plain[skField] === primaryKey[skField]
      }
      // What the row looks like after the write, in domain names: the main
      // Put's item when the op writes the whole row, else the row it read
      // overlaid with the payload.
      const mainPut = items.find((i) => i.Put !== undefined && isOwn(i.Put.Item))?.Put?.Item
      const read = ownRow === undefined ? {} : toDomainView(fromAttributeMap(ownRow))
      const after: globalThis.Record<string, unknown> =
        mainPut !== undefined
          ? toDomainView(fromAttributeMap(mainPut))
          : { ...read, ...request.payload }
      if (mainPut === undefined) {
        for (const [field, delta] of Object.entries(request.deltas ?? {})) {
          const base = read[field]
          after[field] = (typeof base === "number" ? base : 0) + delta
        }
      }
      const sentinelPrefix = `${entityType}._unique.`
      type Role =
        | { readonly _tag: "main" }
        | { readonly _tag: "side" }
        | { readonly _tag: "reserve"; readonly error: UniqueConstraintViolation }
      const roles = items.map((item): Role => {
        if (isOwn(item.Put?.Item) || isOwn(item.Update?.Key) || isOwn(item.Delete?.Key)) {
          return { _tag: "main" }
        }
        const tag = item.Put?.Item?.__edd_e__?.S
        if (
          item.Put === undefined ||
          item.Put.ConditionExpression === undefined ||
          tag === undefined ||
          !tag.startsWith(sentinelPrefix)
        ) {
          return { _tag: "side" }
        }
        const constraintName = tag.slice(sentinelPrefix.length)
        const constraintDef = config.unique?.[constraintName]
        const reserved =
          constraintDef === undefined
            ? undefined
            : composeUniqueSentinel(schema, entityType, constraintName, constraintDef, after)
        const putKey = fromAttributeMap(item.Put.Item ?? {})
        // Only values that recompose THIS sentinel are reported — never a guess.
        const fields =
          reserved !== undefined &&
          reserved.key.pk === putKey[pkField] &&
          reserved.key.sk === putKey[skField]
            ? reserved.fieldsRecord
            : {}
        return {
          _tag: "reserve",
          error: new UniqueConstraintViolation({ entityType, constraint: constraintName, fields }),
        }
      })
      const lostRace = new ConcurrentModification({
        entityType,
        key: encodedKey as globalThis.Record<string, unknown>,
        attributes: [],
        current: Option.none(),
      })
      return {
        items,
        // Everything but a taken unique value and the caller's own condition
        // is retried, and every attempt plans again from fresh reads — so a
        // row now missing takes the op's missing-row path, and a pinned
        // version the row no longer has fails `OptimisticLockError`, exactly
        // as standalone.
        verdict: (reasons) => {
          let failed = false
          let callerRejected = false
          for (const [i, role] of roles.entries()) {
            const reason = reasons[i]
            if (reason?.Code !== "ConditionalCheckFailed") continue
            if (role._tag === "reserve") return { _tag: "Fail", error: role.error }
            failed = true
            // The row is there, so the existence guards held: only the
            // caller's own condition can have rejected it — or a race.
            if (role._tag === "main" && request.callerCondition && reason.Item !== undefined) {
              callerRejected = true
            }
          }
          if (!failed) return undefined
          if (callerRejected) return { _tag: "Condition" }
          return { _tag: "Retry", stored: undefined, error: lostRace }
        },
      } satisfies TransactPlan
    })

  /** Compile an update for `Transaction.transactWrite` — see `_planUpdate`. */
  const planUpdate = (key: unknown, uState: UpdateState) =>
    planned("transactWrite.update", key, () => updateOrCreate(key, "native", uState), {
      payload: (uState.updates ?? {}) as globalThis.Record<string, unknown>,
      deltas: {
        ...uState.add,
        ...Object.fromEntries(
          Object.entries(uState.subtract ?? {}).map(([field, by]) => [field, -by]),
        ),
      },
      callerCondition: uState.condition !== undefined,
      expectedVersion: uState.expectedVersion,
    })

  // ---------------------------------------------------------------------------
  // delete operation
  // ---------------------------------------------------------------------------

  const del = (key: unknown) =>
    new EntityDeleteImpl(
      (opts: {
        readonly condition: Expr | ConditionInput | undefined
        readonly returnValues: ReturnValuesMode | undefined
        readonly mustExist: boolean
      }) =>
        Effect.gen(function* () {
          const client = yield* DynamoClient
          const tc = yield* tableTag
          const tableName = tc.name
          const ttlAttrName = resolveTtlAttributeName(tc)

          // Caller key: Type side in, ENCODED out (see `encodeKey`).
          const encodedKey = yield* encodeKey(key, "delete.decode")

          // Compose primary key
          const primaryKey = composePrimaryKey(encodedKey)
          const marshalledKey = toAttributeMap(primaryKey)
          const primary = config.indexes.primary
          const hasUniqueConstraints =
            config.unique != null && Object.keys(config.unique).length > 0

          // A delete that reads the item first already requires it to exist,
          // so a condition asserting only that (`deleteIfExists`) adds nothing
          // to its guard: it is judged against the read instead — a missing
          // item is `ConditionalCheckFailed` — and the delete is retried after
          // a race like an unconditioned one.
          const readsFirst = isSoftDeleteEnabled() || hasUniqueConstraints || isRetainEnabled()
          // `deleteIfExists` (alone, or ANDed with a `.condition()`): a missing
          // item fails its condition — a guard no `.condition()` replaces.
          const mustExist = opts.mustExist
          const existenceOnly = readsFirst && mustExist && opts.condition === undefined
          if (
            opts.returnValues !== undefined &&
            opts.returnValues !== "none" &&
            opts.returnValues !== "allOld"
          ) {
            return yield* new ValidationError({
              entityType,
              operation: "delete",
              cause:
                `delete: returnValues("${opts.returnValues}") is not a mode DeleteItem ` +
                'supports — only "none" and "allOld". Nothing was sent.',
            })
          }
          // The caller's condition, ANDed onto `deleteIfExists`'s exists guard.
          const condition = withGuard(
            mustExist ? { attributeExists: [primary.pk.field] } : undefined,
            opts.condition,
          )
          const userCondition =
            condition && !existenceOnly ? compileCondition(condition, resolveDbName) : undefined
          /**
           * What the delete returns: with `returnValues("allOld")`, the item it
           * removed — the one read (every read-first delete is guarded on it) or
           * DynamoDB's `ALL_OLD` — as a model, `undefined` when there was none.
           */
          const deletedResult = (old: globalThis.Record<string, AttributeValue> | undefined) => {
            if (opts.returnValues !== "allOld" || old === undefined)
              return Effect.succeed(undefined)
            const raw = fromAttributeMap(old) as globalThis.Record<string, unknown>
            // The delete is applied: a decode failure must not read as a refusal.
            return decodeAs({ ...raw }, old, "model").pipe(
              Effect.mapError(
                (cause) =>
                  new DeleteAppliedButUnreadable({
                    entityType,
                    key: encodedKey as globalThis.Record<string, unknown>,
                    item: raw,
                    cause,
                  }),
              ),
            )
          }
          /** The item is missing: what a delete that read it reports. */
          const missing = (
            otherwise: ItemNotFound | undefined,
          ): Effect.Effect<undefined, ItemNotFound | ConditionalCheckFailed> =>
            mustExist
              ? Effect.fail(new ConditionalCheckFailed({ entityType, key: encodedKey }))
              : otherwise === undefined
                ? Effect.succeed(undefined)
                : Effect.fail(otherwise)

          /**
           * Map a rejected user condition on either transaction delete path
           * (soft-delete, unique-constraint) to `ConditionalCheckFailed`.
           *
           * On both paths the current-item Delete sits at index 0 and is the
           * only item carrying a ConditionExpression — the sentinel Deletes and
           * the tombstone/snapshot Puts are unconditional — so an index-0
           * cancellation can only be the user's condition. Every other reason
           * falls through as the raw `DynamoClientError`.
           */
          const mapDeleteConditionFailure = (
            err: DynamoClientError,
          ): DynamoClientError | ConditionalCheckFailed => {
            if (!userCondition) return err
            const cancelledAtMainItem =
              isAwsTransactionCancelled(err.cause) &&
              err.cause.CancellationReasons?.[0]?.Code === "ConditionalCheckFailed"
            if (cancelledAtMainItem || isAwsConditionalCheckFailed(err.cause)) {
              return new ConditionalCheckFailed({ entityType, key: encodedKey })
            }
            return err
          }

          /** The guard ANDed with the user's condition, for the main Delete. */
          const guardedDeleteCondition = (guard: {
            readonly expression: string
            readonly names: globalThis.Record<string, string>
            readonly values: globalThis.Record<string, AttributeValue>
          }) => {
            const values = { ...guard.values, ...userCondition?.values }
            return {
              ConditionExpression: userCondition
                ? `${guard.expression} AND (${userCondition.expression})`
                : guard.expression,
              ExpressionAttributeNames: { ...guard.names, ...userCondition?.names },
              ...(Object.keys(values).length > 0 && { ExpressionAttributeValues: values }),
              ReturnValuesOnConditionCheckFailure: "ALL_OLD" as const,
            }
          }
          /**
           * Run a guarded delete transaction: index 0 is the main Delete, the
           * only conditioned item, so its rejection is read from the stored
           * item (ALL_OLD) — gone, raced, or the user's condition.
           */
          const guardedDelete = <A>(
            write: Effect.Effect<A, DynamoClientError>,
            read: globalThis.Record<string, AttributeValue>,
            inputs: ReadonlyArray<string>,
            snapshotAt: { readonly index: number; readonly version: number } | undefined,
            releases: ReadonlyArray<{ readonly index: number; readonly constraintName: string }>,
          ) => {
            let rejected: globalThis.Record<string, AttributeValue> | undefined
            const reject = (stored: Readonly<globalThis.Record<string, unknown>> | undefined) => {
              rejected = stored as globalThis.Record<string, AttributeValue> | undefined
              // Deleted since the read: with nothing asserted, read it again —
              // a delete of a missing item reports what such a delete does.
              if (stored === undefined && userCondition === undefined) {
                return new DeletedConcurrently()
              }
              return deleteRejection(
                encodedKey as globalThis.Record<string, unknown>,
                read,
                inputs,
                stored,
                userCondition !== undefined,
              )
            }
            return withCurrentItem(
              write.pipe(
                Effect.mapError((err) => {
                  if (isAwsTransactionCancelled(err.cause)) {
                    const main = err.cause.CancellationReasons?.[0]
                    if (main?.Code === "ConditionalCheckFailed") return reject(main.Item)
                    if (
                      snapshotAt !== undefined &&
                      err.cause.CancellationReasons?.[snapshotAt.index]?.Code ===
                        "ConditionalCheckFailed"
                    ) {
                      return historyConflict(snapshotAt.version, "delete")
                    }
                    for (const { index, constraintName } of releases) {
                      if (
                        err.cause.CancellationReasons?.[index]?.Code === "ConditionalCheckFailed"
                      ) {
                        return releaseRaced(
                          encodedKey as globalThis.Record<string, unknown>,
                          constraintName,
                        )
                      }
                    }
                  }
                  if (isAwsConditionalCheckFailed(err.cause)) return reject(err.cause.Item)
                  return err
                }),
              ),
              () => rejected,
            )
          }

          /**
           * Append a release for each sentinel the stored item `raw` holds AND
           * owns (#133) — composed from its domain view, since the stored row is
           * attribute-keyed and the constraint is not (#127) — returning where
           * each landed, so a release that lost a race can be reported.
           */
          const ownedReleases = (
            table: string,
            raw: globalThis.Record<string, unknown>,
            into: Array<unknown>,
          ) =>
            Effect.gen(function* () {
              const rawDomain = toDomainView(raw)
              const held: Array<{
                readonly pk: string
                readonly sk: string
                readonly constraintName: string
              }> = []
              for (const [constraintName, constraintDef] of Object.entries(config.unique!)) {
                const sentinel = composeUniqueSentinel(
                  schema,
                  entityType,
                  constraintName,
                  constraintDef,
                  rawDomain,
                )
                if (sentinel) held.push({ ...sentinel.key, constraintName })
              }
              const owner = { pk: primaryKey[primary.pk.field], sk: primaryKey[primary.sk.field] }
              const releases: Array<{ index: number; constraintName: string }> = []
              for (const sentinel of yield* ownedSentinels(table, held, owner)) {
                releases.push({ index: into.length, constraintName: sentinel.constraintName })
                into.push(sentinelRelease(table, sentinel, owner))
              }
              return releases
            })

          if (isSoftDeleteEnabled() || hasUniqueConstraints || isRetainEnabled()) {
            const readFirst = Effect.gen(function* () {
              if (isSoftDeleteEnabled()) {
                // --- Soft delete path ---
                // Read current item
                const result = yield* client.getItem({
                  TableName: tableName,
                  Key: marshalledKey,
                  ConsistentRead: true,
                })

                if (!result.Item) {
                  return yield* missing(new ItemNotFound({ entityType, key: encodedKey }))
                }
                // The tombstone (and snapshot) copy the item read: the delete is
                // conditioned on it being unchanged, so no concurrent update is lost
                // into them (#133).
                yield* checkVersion(result.Item, "delete")
                const softGuard = deleteGuard(result.Item, "item", userCondition?.expression)
                if (softGuard instanceof ValidationError) return yield* softGuard

                const raw = fromAttributeMap(result.Item) as globalThis.Record<string, unknown>
                // Clock-backed time source; `now` is the ISO timestamp used for the
                // deleted SK + deletedAt, `dtNow` drives the optional TTL + snapshot.
                const dtNow = yield* DateTime.now
                const now = nowIso(dtNow)

                // Build soft-deleted item: same PK, replace SK with deleted key, strip GSI keys
                const deletedItem: globalThis.Record<string, unknown> = { ...raw }

                // Strip GSI key fields — soft-deleted items must not appear in index queries
                for (const field of gsiKeyFields()) {
                  delete deletedItem[field]
                }

                // Vector attributes: stash the embedding under a non-indexed name
                // before stripping. Sparse semantics drop the tombstone out of the
                // vector index immediately, and `restore()` un-stashes without
                // paying for another Embedder call. See `DESIGN.md §14`.
                for (const [, definition] of vectorIndexEntries) {
                  const stored = deletedItem[definition.vectorField]
                  if (stored !== undefined) deletedItem[definition.stashField] = stored
                  delete deletedItem[definition.vectorField]
                  delete deletedItem[definition.partitionField]
                }

                // Replace SK with deleted sort key
                deletedItem[primary.sk.field] = DynamoSchema.composeDeletedKey(
                  schema,
                  entityType,
                  now,
                  historyKeyOptions(primaryKey[primary.sk.field]),
                )

                // Add deletedAt
                deletedItem.deletedAt = now

                // Add optional TTL
                const sdTtl = softDeleteTtl()
                if (sdTtl) {
                  deletedItem[ttlAttrName] =
                    DateTime.toEpochSeconds(dtNow) + normalizeTtlSeconds(sdTtl)
                }

                // Build transaction
                type TransactItem = {
                  Put?: { TableName: string; Item: globalThis.Record<string, AttributeValue> }
                  Delete?: {
                    TableName: string
                    Key: globalThis.Record<string, AttributeValue>
                    ConditionExpression?: string
                    ExpressionAttributeNames?: globalThis.Record<string, string>
                    ExpressionAttributeValues?: globalThis.Record<string, AttributeValue>
                  }
                }
                const transactItems: Array<TransactItem> = []

                // Delete current entity item — index 0, and the ONLY item in this
                // transaction carrying a ConditionExpression, so a cancellation
                // naming index 0 is unambiguously the user's condition. The guard
                // rides the transaction rather than being pre-checked against the
                // item read above: a client-side check would leave a race window
                // between the read and the write.
                const currentDelete: NonNullable<TransactItem["Delete"]> = {
                  TableName: tableName,
                  Key: marshalledKey,
                  ...guardedDeleteCondition(softGuard),
                }
                transactItems.push({ Delete: currentDelete })

                // Put soft-deleted item
                transactItems.push({
                  Put: {
                    TableName: tableName,
                    Item: toAttributeMap(deletedItem),
                  },
                })

                // Version snapshot if retain is enabled — never over existing history.
                let snapshotAt: { readonly index: number; readonly version: number } | undefined
                if (isRetainEnabled()) {
                  const currentVersion = systemFields.version
                    ? ((raw[systemFields.version] as number | undefined) ?? 0)
                    : 0
                  const snapshotItem = buildSnapshotItem(
                    raw,
                    currentVersion,
                    primary.pk.field,
                    primary.sk.field,
                    ttlAttrName,
                    dtNow,
                  )
                  snapshotAt = { index: transactItems.length, version: currentVersion }
                  transactItems.push({ Put: snapshotPut(tableName, snapshotItem) })
                }

                // Release sentinels if not preserving unique — only those this item
                // owns (#133). Sparse — fields that were unset on the live item
                // never had a sentinel, so nothing to release.
                const releases =
                  hasUniqueConstraints && !preserveUnique()
                    ? yield* ownedReleases(tableName, raw, transactItems)
                    : []

                yield* checkTransactionLimit(entityType, "delete", transactItems)
                yield* guardedDelete(
                  client.transactWriteItems({ TransactItems: transactItems }),
                  result.Item,
                  softGuard.inputs,
                  snapshotAt,
                  releases,
                )
                return result.Item
              } else {
                // --- Hard delete with unique constraints and/or retained history ---
                // The item is read first: the sentinels to release are keyed by its
                // unique values, and a retain entity snapshots its final state.
                const result = yield* client.getItem({
                  TableName: tableName,
                  Key: marshalledKey,
                  ConsistentRead: true,
                })

                if (!result.Item) {
                  if (hasUniqueConstraints || mustExist) {
                    return yield* missing(new ItemNotFound({ entityType, key: encodedKey }))
                  }
                  // Retain only: deleting a missing item writes nothing, as a plain
                  // `DeleteItem` would. The caller's condition is still judged
                  // against no item — and the delete never removes an item created
                  // since the read, which would leave its final state unsnapshotted.
                  if (!userCondition) return undefined
                  const values = userCondition.values
                  yield* client
                    .deleteItem({
                      TableName: tableName,
                      Key: marshalledKey,
                      ConditionExpression: `attribute_not_exists(#dpk) AND (${userCondition.expression})`,
                      ExpressionAttributeNames: {
                        ...userCondition.names,
                        "#dpk": primary.pk.field,
                      },
                      ...(Object.keys(values).length > 0 && { ExpressionAttributeValues: values }),
                      ReturnValuesOnConditionCheckFailure: "ALL_OLD",
                    })
                    .pipe(
                      Effect.mapError((err) =>
                        isAwsConditionalCheckFailed(err.cause)
                          ? conditionRejection(
                              encodedKey as globalThis.Record<string, unknown>,
                              err.cause.Item,
                              err.cause.Item === undefined ? undefined : { version: 0 },
                              true,
                            )
                          : err,
                      ),
                    )
                  return undefined
                }
                // The sentinel deletes are keyed by the unique values read, and the
                // snapshot copies the item read: the delete is conditioned on it —
                // its version and incarnation — so no sentinel is orphaned and no
                // concurrent write is lost from the history (#133).
                yield* checkVersion(result.Item, "delete")
                const hardGuard = deleteGuard(
                  result.Item,
                  hasUniqueConstraints
                    ? [
                        ...new Set(
                          Object.values(config.unique!).flatMap((def) =>
                            resolveUniqueFields(def).map(resolveDbName),
                          ),
                        ),
                      ]
                    : [],
                  userCondition?.expression,
                )
                if (hardGuard instanceof ValidationError) return yield* hardGuard

                const raw = fromAttributeMap(result.Item) as globalThis.Record<string, unknown>

                const transactItems: Array<TransactWriteItem> = []

                // Delete entity item — index 0, and the only item conditioned on the
                // item read (the sentinel Deletes are conditioned on ownership, the
                // snapshot on history), so an index-0 cancellation is read from it.
                transactItems.push({
                  Delete: {
                    TableName: tableName,
                    Key: marshalledKey,
                    ...guardedDeleteCondition(hardGuard),
                  },
                })

                // Retain: the final state is history too. Snapshotted at its own
                // version — under the same never-overwrite guard as every snapshot —
                // so an item created again at this key continues PAST it, and a
                // writer holding this version can never match the new incarnation.
                let snapshotAt: { readonly index: number; readonly version: number } | undefined
                if (isRetainEnabled()) {
                  const currentVersion = storedVersionOf(result.Item) ?? 0
                  snapshotAt = { index: transactItems.length, version: currentVersion }
                  transactItems.push({
                    Put: snapshotPut(
                      tableName,
                      buildSnapshotItem(
                        raw,
                        currentVersion,
                        primary.pk.field,
                        primary.sk.field,
                        ttlAttrName,
                        yield* DateTime.now,
                      ),
                    ),
                  })
                }

                // Release sentinels — only those this item owns (#133). Sparse: no
                // sentinel was ever written for a constraint whose fields are unset.
                const releases = hasUniqueConstraints
                  ? yield* ownedReleases(tableName, raw, transactItems)
                  : []

                yield* checkTransactionLimit(entityType, "delete", transactItems)
                yield* guardedDelete(
                  client.transactWriteItems({ TransactItems: transactItems }),
                  result.Item,
                  hardGuard.inputs,
                  snapshotAt,
                  releases,
                )
                return result.Item
              }
            })
            // The caller asserted nothing about the item, so a concurrent write
            // between the read and the transaction — a changed item, or a
            // sentinel release that changed hands — is not a refusal: the
            // delete reads the item again and is written again, as a put is.
            // With a `.condition()` the read is what the condition was judged
            // against, and a race fails.
            // Deleted concurrently on every attempt: what a delete that read
            // it missing reports.
            const deleted = (yield* userCondition === undefined
              ? readFirst.pipe(
                  Effect.retry({
                    times: GUARDED_PUT_ATTEMPTS - 1,
                    while: (e) =>
                      e instanceof OptimisticLockError ||
                      e instanceof ConcurrentModification ||
                      e instanceof DeletedConcurrently,
                  }),
                  Effect.catchIf(
                    (e): e is DeletedConcurrently => e instanceof DeletedConcurrently,
                    () =>
                      missing(
                        hasUniqueConstraints || isSoftDeleteEnabled()
                          ? new ItemNotFound({ entityType, key: encodedKey })
                          : undefined,
                      ),
                  ),
                )
              : readFirst) as globalThis.Record<string, AttributeValue> | undefined
            return yield* deletedResult(deleted)
          } else {
            // Simple delete
            const deleteInput: DeleteItemCommandInput = {
              TableName: tableName,
              Key: marshalledKey,
            }
            if (userCondition) {
              deleteInput.ConditionExpression = userCondition.expression
              deleteInput.ExpressionAttributeNames = userCondition.names
              if (Object.keys(userCondition.values).length > 0) {
                deleteInput.ExpressionAttributeValues = userCondition.values
              }
            }
            if (opts.returnValues !== undefined) {
              deleteInput.ReturnValues = opts.returnValues === "allOld" ? "ALL_OLD" : "NONE"
            }
            const output = yield* client
              .deleteItem(deleteInput)
              .pipe(Effect.mapError(mapDeleteConditionFailure))
            return yield* deletedResult(
              output.Attributes as globalThis.Record<string, AttributeValue> | undefined,
            )
          }
        }),
      self,
      key as globalThis.Record<string, unknown>,
    )

  // ---------------------------------------------------------------------------
  // deleteIfExists operation — delete + attribute_exists condition
  // ---------------------------------------------------------------------------

  /** Compile a delete for `Transaction.transactWrite` — see `_planDelete`. */
  const planDelete = (
    key: unknown,
    opts: {
      readonly condition: Expr | ConditionInput | undefined
      readonly mustExist: boolean
    },
  ) =>
    planned(
      "transactWrite.delete",
      key,
      () => del(key)._builder({ ...opts, returnValues: undefined }),
      { payload: {}, callerCondition: opts.condition !== undefined },
    )

  const deleteIfExists = (key: unknown) => {
    const op = del(key)
    return new EntityDeleteImpl(
      op._builder,
      op._entity,
      op._key,
      op._condition,
      op._returnValues,
      true,
    )
  }

  // ---------------------------------------------------------------------------
  // upsert operation — create-or-update via UpdateItem with if_not_exists()
  // ---------------------------------------------------------------------------

  /**
   * `upsert` that reads the item first (#133) — of an entity with unique
   * constraints or `versioned: { retain: true }`, or an input that omits a
   * defaulted index composite. A single UpdateItem cannot write, rotate or
   * check a sentinel, snapshot the replaced item, or tell whether to store a
   * default or keep the stored value. Missing: a `create` (sentinels guarded
   * by `attribute_not_exists`, the retain snapshot, omitted defaults stored).
   * Present: an update of the upserted fields — sentinels rotate for changed
   * values, unchanged ones are left alone, immutable fields, `createdAt` and
   * fields the input omits keep their stored values, retain snapshots the
   * replaced item — under the update's version / incarnation or attribute
   * guards, from that one read. The whole input is validated either way. A
   * concurrent create or delete between the read and the write is retried the
   * other way, and a sentinel release whose reservation changed hands is
   * planned again from a fresh read (as a put's is); a race lost on every
   * attempt fails with the concurrency error. A concurrent change of the item
   * itself fails the upsert, as it fails an update.
   */
  const guardedUpsert = (input: unknown, mode: DecodeMode, opts: EntityPutOpts) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const { name: tableName } = yield* tableTag
      yield* checkWithVectorNames(opts.withVectors, "upsert")
      // Required fields are required whether or not the item exists.
      yield* encodeOrDecodeEncode(
        schemas.inputSchema as Schema.Codec<any>,
        yield* fillDecodingDefaults(input),
        entityType,
        "upsert",
      )
      const record = (input ?? {}) as globalThis.Record<string, unknown>
      const primary = config.indexes.primary
      const keyFields = [...primary.pk.composite, ...primary.sk.composite]
      const key = Object.fromEntries(keyFields.map((field) => [field, record[field]]))
      const encodedKey = yield* encodeKey(key, "upsert.decodeKey")
      const marshalledKey = toAttributeMap(composePrimaryKey(encodedKey))
      const dropped = new Set<string>([
        ...keyFields,
        ...immutableFields,
        ...(systemFields.createdAt ? [systemFields.createdAt] : []),
        ...(systemFields.version ? [systemFields.version] : []),
      ])
      const updates = Object.fromEntries(
        Object.entries(record).filter(([field]) => !dropped.has(field)),
      )
      const read = () =>
        client
          .getItem({ TableName: tableName, Key: marshalledKey, ConsistentRead: true })
          .pipe(Effect.map((result) => result.Item))
      let current = yield* read()
      let expected = 0
      let lost: ConcurrentModification | undefined
      for (let attempt = 0; attempt < GUARDED_PUT_ATTEMPTS; attempt++) {
        yield* checkVersion(current, "upsert")
        expected = storedVersionOf(current) ?? 0
        if (current === undefined) {
          const created = yield* Effect.exit(
            put(input)._builder(mode, {
              condition: opts.condition,
              withVectors: opts.withVectors,
              putKind: "create",
              operation: "upsert",
            }) as Effect.Effect<unknown, unknown>,
          )
          if (created._tag === "Success") return created.value
          const error = Cause.findErrorOption(created.cause)
          if (Option.isNone(error) || !(error.value instanceof ConditionalCheckFailed)) {
            return yield* created
          }
          // Created concurrently: upsert it as the existing item it now is.
          // Still missing: the caller's condition rejected it.
          current = yield* read()
          if (current === undefined) return yield* created
          continue
        }
        const updated = yield* Effect.exit(
          runUpdate(
            key,
            mode,
            {
              ...emptyUpdateState,
              updates,
              condition: opts.condition,
              withVectors: opts.withVectors,
            },
            current,
          ).pipe(
            // Its errors name the upsert, not the update it runs.
            Effect.mapError((e) =>
              e instanceof ValidationError && e.operation.startsWith("update")
                ? new ValidationError({
                    entityType: e.entityType,
                    operation: `upsert${e.operation.slice("update".length)}`,
                    cause: e.cause,
                  })
                : e,
            ),
          ) as Effect.Effect<unknown, unknown>,
        )
        if (updated._tag === "Success") return updated.value
        const error = Cause.findErrorOption(updated.cause)
        if (Option.isNone(error)) return yield* updated
        // A sentinel it would release changed hands since its ownership read:
        // nothing was written, and the upsert is planned again from a fresh
        // read, as a put is.
        if (error.value instanceof ConcurrentModification && releaseRaces.has(error.value)) {
          lost = error.value
        } else if (!(error.value instanceof ItemNotFound)) {
          return yield* updated
        }
        // Deleted concurrently: upserted as the missing item it now is.
        current = yield* read()
      }
      if (lost !== undefined) return yield* lost
      // Every attempt raced a concurrent create or delete.
      return yield* systemFields.version
        ? new OptimisticLockError({
            entityType,
            key: encodedKey,
            expectedVersion: expected,
            actualVersion: storedVersionOf(current) ?? -1,
          })
        : new ConcurrentModification({
            entityType,
            key: encodedKey,
            attributes: [],
            current: Option.none(),
          })
    })

  const upsert = (input: unknown) =>
    new EntityPutImpl(
      (mode: DecodeMode, opts: EntityPutOpts) =>
        Effect.gen(function* () {
          if (
            (config.unique != null && Object.keys(config.unique).length > 0) ||
            isRetainEnabled() ||
            omitsIndexedDefault(input)
          ) {
            return yield* guardedUpsert(input, mode, opts)
          }
          const client = yield* DynamoClient
          const { name: tableName } = yield* tableTag
          // Clock-backed time source for createdAt/updatedAt SET clauses.
          const now = yield* DateTime.now

          yield* checkWithVectorNames(opts.withVectors, "upsert")

          // Encode user input → wire form (see `put` for strategy). An input
          // that omits a defaulted index composite took the guarded path: the
          // default is stored on create, the stored value kept on update.
          const encodedInput = yield* encodeOrDecodeEncode(
            schemas.inputSchema as Schema.Codec<any>,
            input,
            entityType,
            "upsert",
          )

          // Hydrate refs: replace ID fields with full entity domain data
          const encoded = hasRefs
            ? yield* hydrateRefs(encodedInput as globalThis.Record<string, unknown>)
            : encodedInput

          const item = encoded as globalThis.Record<string, unknown>

          // Compose primary key
          const primaryKey = composePrimaryKey(item)
          const marshalledKey = toAttributeMap(primaryKey)

          // Build UpdateExpression with if_not_exists for immutable fields + createdAt
          const setClauses: Array<string> = []
          const upsertRemoveClauses: Array<string> = []
          const names: globalThis.Record<string, string> = {}
          const values: globalThis.Record<string, AttributeValue> = {}
          let counter = 0

          // The physical key attributes (`pk` / `sk`) live in `Key` and cannot
          // appear in an UpdateExpression. The COMPOSITE SOURCE fields are
          // ordinary attributes and must be written like any other model field —
          // skipping them (as this path used to) left the stored item without
          // e.g. `productId`, so every read of an upserted item failed to decode
          // with `Missing key`. Their values are fixed by the key, so a plain
          // SET is idempotent.
          const keyAttributeFields = new Set([
            config.indexes.primary.pk.field,
            config.indexes.primary.sk.field,
          ])

          // System-colliding fields are written below in the system-field block
          // (so their semantics — if_not_exists / always-set — stay consistent).
          const systemColliders = new Set<string>()
          if (systemFields.createdAtCollision && systemFields.createdAt)
            systemColliders.add(systemFields.createdAt)
          if (systemFields.updatedAtCollision && systemFields.updatedAt)
            systemColliders.add(systemFields.updatedAt)
          if (systemFields.versionCollision && systemFields.version)
            systemColliders.add(systemFields.version)

          // `item` is already in wire-form via `Schema.encode(inputSchema)`.

          // All model fields (excluding the physical key attributes themselves)
          for (const [attr, val] of Object.entries(item)) {
            if (keyAttributeFields.has(resolveDbName(attr))) continue
            if (systemColliders.has(attr)) continue
            if (val === undefined) continue

            // Sparse fields: expand into one SET per bucket. Each bucket
            // attribute is named `<prefix>#<key>` and goes through
            // ExpressionAttributeNames as a literal (the `#` survives).
            if (hasSparseFields && attr in sparseFields) {
              if (val === null || typeof val !== "object") continue
              const sparse = sparseFields[attr]!
              for (const [k, v] of Object.entries(val as globalThis.Record<string, unknown>)) {
                try {
                  if (typeof k !== "string" || k.length === 0 || k.includes("#")) {
                    throw new Error(`Sparse map "${attr}": invalid key ${JSON.stringify(k)}`)
                  }
                } catch (e) {
                  return yield* new ValidationError({
                    entityType,
                    operation: "upsert.sparse",
                    cause: e instanceof Error ? e.message : String(e),
                  })
                }
                const nameKey = `#u${counter}`
                const valKey = `:u${counter}`
                names[nameKey] = `${sparse.prefix}#${k}`
                values[valKey] = toAttributeValue(v)
                setClauses.push(`${nameKey} = ${valKey}`)
                counter++
              }
              continue
            }

            const nameKey = `#u${counter}`
            const valKey = `:u${counter}`
            names[nameKey] = resolveDbName(attr)
            values[valKey] = toAttributeValue(val)

            // Immutable fields use if_not_exists — only set on first create
            if (immutableFields.has(attr)) {
              setClauses.push(`${nameKey} = if_not_exists(${nameKey}, ${valKey})`)
            } else {
              setClauses.push(`${nameKey} = ${valKey}`)
            }
            counter++
          }

          // Add all index keys (including GSIs)
          const allKeys = composeAllKeys(item)
          for (const [field, value] of Object.entries(allKeys)) {
            // Skip primary key fields (they're in Key, not UpdateExpression)
            if (
              field === config.indexes.primary.pk.field ||
              field === config.indexes.primary.sk.field
            )
              continue
            const nameKey = `#u${counter}`
            const valKey = `:u${counter}`
            names[nameKey] = field
            values[valKey] = toAttributeValue(value)
            setClauses.push(`${nameKey} = ${valKey}`)
            counter++
          }

          // Vector search: upsert writes the full item, so it always re-embeds
          // (same contract as `put` — see `DESIGN.md §14 Write path`). Emitted
          // as plain SETs, never `if_not_exists`: an upsert that overwrites the
          // source fields must overwrite the vector derived from them, or the
          // item would stay searchable under its previous description forever.
          if (hasVectorIndexes) {
            const vectorWrite = yield* computeVectorAttributes(item, opts.withVectors, "all")
            for (const [field, value] of Object.entries(vectorWrite.sets)) {
              const nameKey = `#u${counter}`
              const valKey = `:u${counter}`
              names[nameKey] = field
              values[valKey] = toAttributeValue(value)
              setClauses.push(`${nameKey} = ${valKey}`)
              counter++
            }
            // An upsert whose input carries no source text takes the item out
            // of the index, exactly as a `put` of the same input would.
            for (const field of new Set(vectorWrite.removes)) {
              const nameKey = `#vr${counter}`
              names[nameKey] = field
              upsertRemoveClauses.push(nameKey)
              counter++
            }
          }

          // Add entity type discriminator
          {
            const nameKey = `#u${counter}`
            const valKey = `:u${counter}`
            names[nameKey] = "__edd_e__"
            values[valKey] = toAttributeValue(entityType)
            setClauses.push(`${nameKey} = ${valKey}`)
            counter++
          }

          // Add createdAt with if_not_exists — only set on first create.
          // User-supplied value wins (domain input → already serialized above);
          // else fall back to a freshly generated storage primitive.
          if (systemFields.createdAt) {
            const userSupplied = item[systemFields.createdAt]
            const nameKey = `#u${counter}`
            const valKey = `:u${counter}`
            names[nameKey] = systemFields.createdAt
            values[valKey] = toAttributeValue(
              userSupplied !== undefined
                ? userSupplied
                : generateTimestamp(systemFields.createdAtEncoding, now),
            )
            setClauses.push(`${nameKey} = if_not_exists(${nameKey}, ${valKey})`)
            counter++
          }

          // Add updatedAt — user-supplied wins, else always set to current time.
          if (systemFields.updatedAt) {
            const userSupplied = item[systemFields.updatedAt]
            const nameKey = `#u${counter}`
            const valKey = `:u${counter}`
            names[nameKey] = systemFields.updatedAt
            values[valKey] = toAttributeValue(
              userSupplied !== undefined
                ? userSupplied
                : generateTimestamp(systemFields.updatedAtEncoding, now),
            )
            setClauses.push(`${nameKey} = ${valKey}`)
            counter++
          }

          // Add version: if_not_exists(version, 0) + 1
          if (systemFields.version) {
            const nameKey = `#u${counter}`
            const zeroKey = `:u${counter}z`
            names[nameKey] = systemFields.version
            values[zeroKey] = toAttributeValue(0)
            values[":vinc"] = toAttributeValue(1)
            setClauses.push(`${nameKey} = if_not_exists(${nameKey}, ${zeroKey}) + :vinc`)
            counter++
          }
          // An upsert that creates the item gives it an incarnation token (#133).
          if (stampsIncarnation) {
            names["#eddInc"] = INCARNATION_TOKEN
            values[":eddInc"] = toAttributeValue(yield* freshIncarnation)
            setClauses.push("#eddInc = if_not_exists(#eddInc, :eddInc)")
          }

          const updateExpression =
            upsertRemoveClauses.length > 0
              ? `SET ${setClauses.join(", ")} REMOVE ${upsertRemoveClauses.join(", ")}`
              : `SET ${setClauses.join(", ")}`

          // Optional user condition
          const condParts: Array<string> = []
          if (systemFields.version) {
            // Never version an item whose version was removed outside the library
            // (incarnation token, no version) as if it predated versioning (#133).
            names["#intVer"] = systemFields.version
            names["#intInc"] = INCARNATION_TOKEN
            condParts.push("(attribute_exists(#intVer) OR attribute_not_exists(#intInc))")
          }
          const uc = opts.condition ? compileCondition(opts.condition, resolveDbName) : undefined
          if (uc) {
            condParts.push(`(${uc.expression})`)
            Object.assign(names, uc.names)
            Object.assign(values, uc.values)
          }

          const result = yield* client
            .updateItem({
              TableName: tableName,
              Key: marshalledKey,
              UpdateExpression: updateExpression,
              ExpressionAttributeNames: names,
              ExpressionAttributeValues: values,
              ConditionExpression: condParts.length > 0 ? condParts.join(" AND ") : undefined,
              // Only the corruption guard needs the stored item.
              ...(systemFields.version && {
                ReturnValuesOnConditionCheckFailure: "ALL_OLD" as const,
              }),
              ReturnValues: "ALL_NEW",
            })
            .pipe(
              Effect.mapError(
                (err): DynamoClientError | ConditionalCheckFailed | ValidationError => {
                  if (isAwsConditionalCheckFailed(err.cause)) {
                    const corrupt = versionCorruption(err.cause.Item, "upsert")
                    if (corrupt !== undefined) return corrupt
                    if (opts.condition) return new ConditionalCheckFailed({ entityType, key: item })
                  }
                  return err
                },
              ),
            )

          if (!result.Attributes) {
            return yield* new ItemNotFound({ entityType, key: item })
          }

          const raw = fromAttributeMap(result.Attributes)
          return yield* decodeAs(raw, result.Attributes, mode)
        }),
      self,
      input as globalThis.Record<string, unknown>,
      undefined,
      undefined,
      // Tagged so the transact/batch compilers can refuse it: this op is an
      // UpdateItem with `if_not_exists`, not a Put (#100).
      "upsert",
    )

  /**
   * @internal Compare stored vs attempted `orderBy` to decide whether the CAS
   * predicate `attribute_not_exists(pk) OR stored < attempted` failed.
   *
   * Returns `true` (CAS failed → stale) when `stored >= attempted`.
   * Returns `false` (CAS held; the user's `.condition()` is the rejecter)
   * when `stored < attempted` or when `stored` is missing.
   *
   * `stored` arrives as a decoded domain value matching the configured schema
   * for the attribute: a `DateTime.Utc` for `Schema.DateTimeUtc`, a `Date` for
   * `Schema.Date`, a `number`/`bigint` for numeric schemas, a `string` for
   * lexicographic comparisons.
   */
  const isCasFailure = (stored: unknown, attempted: unknown): boolean => {
    if (stored == null) return false
    // DateTime.Utc — compare epoch ms.
    if (DateTime.isDateTime(stored) && DateTime.isDateTime(attempted)) {
      return DateTime.toEpochMillis(stored) >= DateTime.toEpochMillis(attempted)
    }
    // Date — compare epoch ms.
    if (stored instanceof Date && attempted instanceof Date) {
      return stored.getTime() >= attempted.getTime()
    }
    // Numeric — number or bigint (cast comparable via JS coercion).
    if (typeof stored === "number" && typeof attempted === "number") {
      return stored >= attempted
    }
    if (typeof stored === "bigint" && typeof attempted === "bigint") {
      return stored >= attempted
    }
    // Strings — lexicographic.
    if (typeof stored === "string" && typeof attempted === "string") {
      return stored >= attempted
    }
    // Mixed / unknown shapes — fall back to string compare on String(...).
    // Defensive: return true so we report StaleAppend (the safer default —
    // never suppress a CAS rejection by mis-classifying it as a user-condition
    // rejection).
    return String(stored) >= String(attempted)
  }

  // ---------------------------------------------------------------------------
  // append operation — time-series primitive (only when `timeSeries` configured)
  //
  // Two-item `TransactWriteItems`:
  //  - UpdateItem on current: scoped SET (appendInput fields only) + CAS on
  //    `orderBy`, with GSI keys recomposed when any appendInput field is a
  //    GSI composite. `createdAt` uses if_not_exists on first append.
  //  - Put of event: full decoded input + __edd_e__ + TTL attr (if configured),
  //    GSI keys stripped, SK replaced with `<currentSk>#e#<orderByValue>`.
  //
  // Concurrency outcome (stale-as-error contract — supersedes
  // `docs/designs/timeseries.md` §4.7 v1 stale-as-value):
  //
  // - Success path (default): on transaction success, issue a follow-up
  //   GetItem and return `{ current }` (decoded model).
  // - Stale path (default): on TransactionCancelled, issue a follow-up
  //   GetItem to disambiguate. If the stored `orderBy >= attempted`, the CAS
  //   fired → fail with `StaleAppend(current: Option.some)`. Otherwise the
  //   user-supplied `.condition()` rejected the write → fail with
  //   `ConditionalCheckFailed(current: Option.some)`.
  // - skipFollowUp success path: return `void`.
  // - skipFollowUp stale path: cannot disambiguate (no GetItem), so fail
  //   with `StaleAppend(current: Option.none)` for both cancellation modes.
  //   Documented as a deliberate trade-off in `guides/timeseries.mdx`.
  // - TTL-race / row vanished after success: surface as
  //   `ValidationError("append.followUp")`. Undetected on skipFollowUp path.
  // ---------------------------------------------------------------------------

  const timeSeriesConfig = config.timeSeries as TimeSeriesConfig<any> | undefined

  const append = (
    input: unknown,
    givenCondition?: Expr | ConditionInput,
    skipFollowUp = false,
    removeAttrs?: ReadonlyArray<string>,
  ) =>
    Effect.gen(function* () {
      if (!timeSeriesConfig) {
        return yield* new ValidationError({
          entityType,
          operation: "append",
          cause: "Entity is not configured with timeSeries. .append() requires timeSeries config.",
        })
      }
      // `append` takes its condition as an argument — no `condition`
      // combinator — so an empty one becomes none here (#133).
      const userCondition = nonEmptyCondition(givenCondition)
      const emptyPart =
        userCondition === undefined ? undefined : emptyPartProblem(toExpr(userCondition))
      if (emptyPart !== undefined) {
        return yield* new ValidationError({
          entityType,
          operation: "append.condition",
          cause: `append: ${emptyPart} Nothing was sent.`,
        })
      }
      const client = yield* DynamoClient
      const tc = yield* tableTag
      const tableName = tc.name
      const ttlAttrName = resolveTtlAttributeName(tc)
      const orderByField = timeSeriesConfig.orderBy
      const ttlDuration = timeSeriesConfig.ttl
      const appendInputSchema = schemas.appendInputSchema as Schema.Codec<any>
      // Clock-backed time source for current-item timestamps + event-item TTL.
      const now = yield* DateTime.now

      // ---------- Validate removeAttrs (issue #49) ----------
      // `.remove(attrs)` clears `appendInput` attributes in the same UpdateItem
      // as the scoped SET + CAS. Composite-attribute removal cascades through
      // `composeGsiKeysForUpdatePolicyAware` via the `removedSet` option.
      // Validation enforces the time-series invariants — orderBy, PK/SK
      // composites, and ref fields must not be cleared, and the caller cannot
      // remove enrichment fields outside `appendInput` (that's a `.update()`
      // operation, not an append).
      const removedSet =
        removeAttrs !== undefined && removeAttrs.length > 0 ? new Set(removeAttrs) : undefined
      if (removedSet !== undefined) {
        const primary = config.indexes.primary
        const pkSkComposites = new Set<string>([...primary.pk.composite, ...primary.sk.composite])
        const appendInputTop = timeSeriesConfig.appendInput as Schema.Top
        const appendInputFieldSet = new Set<string>(
          Object.keys(getSchemaFields(appendInputTop) ?? {}),
        )
        const model = config.model as Schema.Top
        for (const attr of removedSet) {
          if (!appendInputFieldSet.has(attr)) {
            return yield* new ValidationError({
              entityType,
              operation: "append.remove",
              cause:
                `Attribute "${attr}" passed to .remove() is not declared in appendInput. ` +
                `.append().remove() can only clear fields the entity exposes via appendInput. ` +
                `Use .update().remove([...]) on the entity for enrichment fields outside appendInput.`,
            })
          }
          if (attr === orderByField) {
            return yield* new ValidationError({
              entityType,
              operation: "append.remove",
              cause: `Attribute "${attr}" is the orderBy clock — removing it would invalidate the CAS anchor.`,
            })
          }
          if (pkSkComposites.has(attr)) {
            return yield* new ValidationError({
              entityType,
              operation: "append.remove",
              cause: `Attribute "${attr}" is a primary-key composite — removing it would orphan the item.`,
            })
          }
          if (isRefField(attr, model)) {
            return yield* new ValidationError({
              entityType,
              operation: "append.remove",
              cause: `Attribute "${attr}" is a ref field — refs are create-time denormalisations and cannot be cleared via .append(). Use .update() to reassign.`,
            })
          }
        }
      }

      // Encode user input via appendInputSchema (see `put` for strategy).
      const encodedInput = yield* encodeOrDecodeEncode(
        appendInputSchema,
        input,
        entityType,
        "append",
      )
      const encoded = encodedInput as globalThis.Record<string, unknown>

      // After encoding, detect SET/REMOVE conflict — DynamoDB rejects an
      // UpdateExpression that touches the same attribute in SET and REMOVE.
      // The SET loop (below) skips entries with `undefined` values, so the
      // conflict is only real when the payload carries a non-undefined value
      // for an attribute also named in `.remove()`.
      if (removedSet !== undefined) {
        for (const attr of removedSet) {
          if (
            attr in encoded &&
            (encoded as globalThis.Record<string, unknown>)[attr] !== undefined
          ) {
            return yield* new ValidationError({
              entityType,
              operation: "append.remove",
              cause:
                `Attribute "${attr}" appears in both the append payload and .remove(). ` +
                `Choose one — either set the new value or remove the attribute.`,
            })
          }
        }
      }

      // Compose current-item primary key (pk + sk derived from PK/SK composites)
      const primary = config.indexes.primary
      const appendKeyForm = keyForm(encoded)
      const pkValue = KeyComposer.composePk(schema, entityType, primary, appendKeyForm)
      const currentSk = KeyComposer.composeSk(
        schema,
        entityType,
        entityVersion,
        primary,
        appendKeyForm,
      )
      const marshalledKey = toAttributeMap({
        [primary.pk.field]: pkValue,
        [primary.sk.field]: currentSk,
      })

      // `encoded` is already in wire-form via Schema.encode — used directly.
      const serialisedInput: globalThis.Record<string, unknown> = { ...encoded }

      // ---------- Build UpdateItem (scoped SET + CAS) ----------
      const setClauses: Array<string> = []
      const names: globalThis.Record<string, string> = {}
      const values: globalThis.Record<string, AttributeValue> = {}
      let counter = 0

      // Only fields named in appendInput (the serialisedInput object). This is
      // the enrichment-preservation contract — fields outside appendInput are
      // never touched. PK composites ARE included: they're stored as regular
      // attributes on the item (mirrors `.put()`), and though their values
      // never change, writing them on every append makes the first append
      // (where the row doesn't yet exist) materialise the row correctly.
      for (const [attr, val] of Object.entries(serialisedInput)) {
        if (val === undefined) continue
        const nameKey = `#a${counter}`
        const valKey = `:a${counter}`
        names[nameKey] = resolveDbName(attr)
        values[valKey] = toAttributeValue(val)
        setClauses.push(`${nameKey} = ${valKey}`)
        counter++
      }

      // GSI key recomposition. v3 unifies append with update — both call
      // the same composer. v1.7.2 (closes #43): the full encoded record is
      // passed as both `updatePayload` and `keyRecord`. The previous
      // v1.7.0 / v1.7.1 path filtered PK composites out of the payload on
      // the rationale that they "never change during an append" — but
      // combined with v1.7.1's per-half evaluation gate (which only looked
      // at `updatePayload`), that filter caused any GSI half whose
      // composites are entirely entity-PK composites to be classified as
      // untouched on every append, silently skipping GSI evaluation. The
      // PK exclusion never solved a real problem (the composer doesn't
      // emit redundant SETs for the underlying composite fields, and
      // idempotent recomposition from immutable PK composites is fine), so
      // it's removed. The gate now broadens its "touched" check to also
      // count `keyRecord` membership, so PK-composite-only halves are
      // touched on every write and the structural rule composes them.
      // No try/catch — EDD-9024 was deprecated in v1.7.1 and the composer
      // no longer throws.
      const gsiUpdate = KeyComposer.composeGsiKeysForUpdatePolicyAware(
        schema,
        entityType,
        entityVersion,
        allIndexes,
        keyForm(encoded),
        keyForm(encoded),
        removedSet !== undefined ? { removedSet } : undefined,
      )
      for (const [field, value] of Object.entries(gsiUpdate.sets)) {
        const nameKey = `#a${counter}`
        const valKey = `:a${counter}`
        names[nameKey] = field
        values[valKey] = toAttributeValue(value)
        setClauses.push(`${nameKey} = ${valKey}`)
        counter++
      }
      const appendRemoveClauses: Array<string> = []
      for (const keyField of gsiUpdate.removes) {
        const nameKey = `#a${counter}`
        names[nameKey] = keyField
        appendRemoveClauses.push(nameKey)
        counter++
      }
      // User-supplied attribute REMOVEs (issue #49). Validation above ensured
      // these are safe — they're appendInput fields, not orderBy / PK
      // composites / refs, and don't overlap the encoded payload. The cascade
      // override on any GSI half whose composite list intersects `removedSet`
      // is already wired through `composeGsiKeysForUpdatePolicyAware` above.
      if (removedSet !== undefined) {
        for (const attr of removedSet) {
          const nameKey = `#a${counter}`
          names[nameKey] = resolveDbName(attr)
          appendRemoveClauses.push(nameKey)
          counter++
        }
      }

      // Entity type discriminator — idempotent; ensures existing items
      // without __edd_e__ still get tagged on first append.
      {
        const nameKey = `#a${counter}`
        const valKey = `:a${counter}`
        names[nameKey] = "__edd_e__"
        values[valKey] = toAttributeValue(entityType)
        setClauses.push(`${nameKey} = ${valKey}`)
        counter++
      }

      // createdAt (if configured) — if_not_exists so subsequent appends leave it alone
      if (systemFields.createdAt) {
        const nameKey = `#a${counter}`
        const valKey = `:a${counter}`
        names[nameKey] = systemFields.createdAt
        values[valKey] = toAttributeValue(generateTimestamp(systemFields.createdAtEncoding, now))
        setClauses.push(`${nameKey} = if_not_exists(${nameKey}, ${valKey})`)
        counter++
      }

      // CAS condition: attribute_not_exists(#pk) OR #ob < :newOb
      names["#_tspk"] = primary.pk.field
      names["#_tsob"] = resolveDbName(orderByField)
      // The comparison uses the stored domain-value representation — for
      // DateTime.Utc this is ISO (lexicographic == chronological), for numbers
      // it's numeric, for strings it's lexicographic. Matches how the current
      // item stores `orderByField`.
      const newObValue = serialisedInput[orderByField]
      values[":_tsNewOb"] = toAttributeValue(newObValue)

      const casCondition = "attribute_not_exists(#_tspk) OR #_tsob < :_tsNewOb"
      let finalCondition = casCondition
      const uc =
        userCondition !== undefined ? compileCondition(userCondition, resolveDbName) : undefined
      if (uc) {
        finalCondition = `(${casCondition}) AND (${uc.expression})`
        Object.assign(names, uc.names)
        Object.assign(values, uc.values)
      }

      const exprParts: Array<string> = []
      if (setClauses.length > 0) exprParts.push(`SET ${setClauses.join(", ")}`)
      if (appendRemoveClauses.length > 0) exprParts.push(`REMOVE ${appendRemoveClauses.join(", ")}`)
      const updateExpression = exprParts.join(" ")

      // ---------- Build Put of event item ----------
      const eventSk = KeyComposer.composeEventSk(currentSk, newObValue, schema.casing)
      // `encoded` is already in wire-form via Schema.encode; no extra
      // serialisation step is required for the Put.
      const eventItem: globalThis.Record<string, unknown> = { ...encoded }
      // Rename domain → DB names for the Put (matches put path)
      renameToDynamo(eventItem)
      // pk + sk (event)
      eventItem[primary.pk.field] = pkValue
      eventItem[primary.sk.field] = eventSk
      // __edd_e__
      eventItem.__edd_e__ = entityType
      // TTL — attribute name comes from TableConfig (default "_ttl")
      if (ttlDuration) {
        eventItem[ttlAttrName] = DateTime.toEpochSeconds(now) + normalizeTtlSeconds(ttlDuration)
      }
      // Events never participate in indexes: strip any GSI key fields that the
      // naive spread above may have carried over. gsiKeys weren't written into
      // eventItem (only decoded fields are), but defensively clear them.
      for (const field of gsiKeyFields()) {
        delete eventItem[field]
      }
      // Only the current item is searchable — event items carry no embedding.
      for (const field of vectorKeyFields()) {
        delete eventItem[field]
      }
      // Sparse-map fields are aggregate state, not event state. They live on
      // the current item only — strip from event items entirely. (Same
      // treatment as enrichment fields outside `appendInput`. By design,
      // `appendInput` cannot include sparse fields, so this is defensive.)
      if (hasSparseFields) {
        for (const fieldName of Object.keys(sparseFields)) {
          delete eventItem[fieldName]
          // Also strip any flattened bucket attrs that might have leaked in
          // via a future code change — defensive cleanup.
          const prefixWithDelim = `${sparseFields[fieldName]!.prefix}#`
          for (const k of Object.keys(eventItem)) {
            if (k.startsWith(prefixWithDelim)) delete eventItem[k]
          }
        }
      }

      const marshalledEventItem = toAttributeMap(eventItem)

      // ---------- Execute TransactWriteItems ----------
      const transactResult = yield* client
        .transactWriteItems({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: marshalledKey,
                UpdateExpression: updateExpression,
                ConditionExpression: finalCondition,
                ExpressionAttributeNames: names,
                ExpressionAttributeValues: values,
              },
            },
            {
              Put: {
                TableName: tableName,
                Item: marshalledEventItem,
              },
            },
          ],
        })
        .pipe(
          Effect.matchEffect({
            onSuccess: () => Effect.succeed({ stale: false as const }),
            onFailure: (err) => {
              // TransactionCancelled or ConditionalCheckFailed at the transaction
              // level both surface here. Disambiguation between CAS and
              // user-condition happens after the follow-up GetItem (when run).
              if (isAwsTransactionCancelled(err.cause) || isAwsConditionalCheckFailed(err.cause)) {
                return Effect.succeed({ stale: true as const })
              }
              return Effect.fail(err)
            },
          }),
        )

      // -----------------------------------------------------------------
      // skipFollowUp branch — no GetItem, cannot decode current, cannot
      // disambiguate CAS vs user-condition cancellation. Both modes
      // collapse to StaleAppend(current: Option.none) — documented.
      // -----------------------------------------------------------------
      if (skipFollowUp) {
        if (transactResult.stale) {
          return yield* new StaleAppend({
            entityType,
            orderByField,
            attemptedOrderBy: newObValue,
            current: Option.none(),
          })
        }
        return undefined
      }

      // -----------------------------------------------------------------
      // Default branch — one follow-up GetItem either way. Carries:
      //   - Decoded model on success (the post-append current).
      //   - Disambiguation data on cancellation (stored orderBy vs attempted)
      //     plus a model snapshot to attach to the error for the caller.
      // -----------------------------------------------------------------
      const followUp = yield* client.getItem({
        TableName: tableName,
        Key: marshalledKey,
      })
      if (!followUp.Item) {
        // TTL race or out-of-band delete — the previous state vanished
        // under us. Distinct from a CAS-stale outcome.
        return yield* new ValidationError({
          entityType,
          operation: "append.followUp",
          cause: "Current item not found after append — possible TTL race or concurrent delete.",
        })
      }

      const rawCurrent = fromAttributeMap(followUp.Item) as globalThis.Record<string, unknown>
      const currentModel = yield* decodeAs(rawCurrent, followUp.Item, "model")

      if (transactResult.stale) {
        // Disambiguate CAS-stale from user-condition rejection. We compare
        // in the WIRE form (what DynamoDB actually evaluated): `newObValue`
        // is the encoded value passed to the CAS, and `rawCurrent` carries
        // the encoded values read back. `decodeAs` mutated `rawCurrent` to
        // rename DB columns to domain field names, so `rawCurrent[orderByField]`
        // is the wire-form value under its domain name. Wire-form
        // comparison matches DynamoDB's evaluation: ISO-8601 strings sort
        // lexicographically (= chronologically), numbers/bigints are
        // numeric, strings are lexicographic.
        const storedOrderBy = (rawCurrent as globalThis.Record<string, unknown>)[orderByField]
        const attempted = newObValue
        const casFired = isCasFailure(storedOrderBy, attempted)

        if (!casFired && userCondition !== undefined) {
          // CAS held; the user's `.condition()` is the only thing that
          // could have rejected.
          return yield* new ConditionalCheckFailed({
            entityType,
            key: encoded as globalThis.Record<string, unknown>,
            current: Option.some(currentModel),
          })
        }
        // Either CAS fired, or no user condition was supplied (CAS is the
        // only possibility) — both map to StaleAppend.
        return yield* new StaleAppend({
          entityType,
          orderByField,
          attemptedOrderBy: attempted,
          current: Option.some(currentModel),
        })
      }
      return { current: currentModel as ModelType<TModel> }
    })

  // ---------------------------------------------------------------------------
  // history operation — BoundQuery for event items only
  // ---------------------------------------------------------------------------

  const history = (key: unknown) => {
    if (!timeSeriesConfig) {
      throw new Error(
        `[EDD-9010] Entity "${entityType}": .history() requires timeSeries config on the entity.`,
      )
    }
    const encodedKey = keyForm(encodeKeySync(key))
    const primary = config.indexes.primary
    const pkValue = KeyComposer.composePk(schema, entityType, primary, keyForm(encodedKey))
    const currentSk = KeyComposer.composeSk(
      schema,
      entityType,
      entityVersion,
      primary,
      keyForm(encodedKey as globalThis.Record<string, unknown>),
    )
    const prefix = KeyComposer.composeEventSkPrefix(currentSk, schema.casing)

    const decodeHistory = (raw: globalThis.Record<string, unknown>) => {
      // Sparse fields are aggregate state, not event state — event items DO
      // NOT carry `<prefix>#*` attributes. `deserializeSparseFields` populates
      // empty `{}` Records under the domain field names so the historyRecord
      // schema (which still includes the sparse Record fields) decodes.
      deserializeSparseFields(raw)
      renameFromDynamo(raw)
      return Schema.decodeUnknownEffect(schemas.historyRecordSchema as Schema.Codec<any>)(raw).pipe(
        Effect.map(attachPrototype),
        Effect.mapError(
          (cause) =>
            new ValidationError({
              entityType,
              operation: "history.decode",
              cause,
            }),
        ),
      )
    }

    return Query.make({
      tableName: "",
      indexName: undefined,
      pkField: primary.pk.field,
      pkValue,
      skField: primary.sk.field,
      entityTypes: [entityType],
      decoder: (raw) => decodeHistory(raw),
      resolveTableName: tableTag.useSync((tc: TableConfig) => tc.name),
      keyFields: [primary.pk.field, primary.sk.field],
    }).pipe(Query.where({ beginsWith: prefix }))
  }

  // ---------------------------------------------------------------------------
  // query namespace
  // ---------------------------------------------------------------------------

  const queryNamespace: globalThis.Record<
    string,
    (pk: globalThis.Record<string, unknown>) => Query.Query<any>
  > = {}
  for (const indexName of Object.keys(config.indexes)) {
    if (indexName === "primary") continue
    const indexDef = config.indexes[indexName]!
    queryNamespace[indexName] = (rawPk: globalThis.Record<string, unknown>) => {
      // Normalise composites to their key form before composing — same rule
      // and same function the write path uses (see `CompositeCodec`).
      const pkKeyForm = keyForm(rawPk)
      const pkValue = KeyComposer.composePk(schema, entityType, indexDef, pkKeyForm)
      const hasSkComposites = indexDef.sk.composite.some((attr) => pkKeyForm[attr] !== undefined)
      const query = Query.make({
        tableName: "",
        indexName: indexDef.index,
        globalIndex: indexDef.index !== undefined,
        pkField: indexDef.pk.field,
        pkValue,
        skField: indexDef.sk.field,
        entityTypes: [entityType],
        decoder: (raw) => decodeRecord(raw),
        resolveTableName: tableTag.useSync((tc: TableConfig) => tc.name),
        keyFields: [
          indexDef.pk.field,
          indexDef.sk.field,
          config.indexes.primary.pk.field,
          config.indexes.primary.sk.field,
        ],
      })
      if (hasSkComposites) {
        // `composeSortKeyBeginsWith`, not `composeSortKeyPrefix` — the operand
        // must terminate on a segment boundary when composites remain, or it
        // matches sibling values that merely start with the supplied one
        // (`status_done` also matching `status_done_archived`, issue #115).
        const skPrefix = KeyComposer.composeSortKeyBeginsWith(
          schema,
          entityType,
          entityVersion,
          indexDef,
          pkKeyForm,
        )
        return Query.where(query, { beginsWith: skPrefix })
      }
      return query
    }
  }

  // ---------------------------------------------------------------------------
  // scan operation
  // ---------------------------------------------------------------------------

  const scan = () =>
    Query.makeScan({
      tableName: "",
      indexName: undefined,
      entityTypes: [entityType],
      decoder: (raw) => decodeRecord(raw),
      resolveTableName: tableTag.useSync((tc: TableConfig) => tc.name),
      keyFields: [config.indexes.primary.pk.field, config.indexes.primary.sk.field],
      liveRows: liveRows(),
    })

  // ---------------------------------------------------------------------------
  // getVersion operation
  // ---------------------------------------------------------------------------

  const getVersion = (key: unknown, versionNumber: number) =>
    new EntityGetImpl(
      (mode: DecodeMode, opts: EntityGetOpts) =>
        Effect.gen(function* () {
          const client = yield* DynamoClient
          const { name: tableName } = yield* tableTag

          // Caller key: Type side in, ENCODED out (see `encodeKey`).
          const encodedKey = yield* encodeKey(key, "getVersion.decode")

          // Compose PK + version SK
          const primary = config.indexes.primary
          const pkValue = KeyComposer.composePk(schema, entityType, primary, keyForm(encodedKey))
          const liveSk = liveSkOf(encodedKey)
          const read = (versionSk: string) =>
            client
              .getItem({
                TableName: tableName,
                Key: toAttributeMap({
                  [primary.pk.field]: pkValue,
                  [primary.sk.field]: versionSk,
                }),
                ConsistentRead: opts.consistentRead || undefined,
              })
              .pipe(Effect.map((result) => result.Item))

          let item = yield* read(
            DynamoSchema.composeVersionKey(
              schema,
              entityType,
              versionNumber,
              historyKeyOptions(liveSk),
            ),
          )
          // Not under the item's own key: written by an earlier release, under
          // the partition-wide one — if it is this item's (#133).
          if (item === undefined && hasItemHistory) {
            const legacy = yield* read(
              DynamoSchema.composeVersionKey(schema, entityType, versionNumber),
            )
            if (legacy !== undefined && isItemsRow(legacy, liveSk)) item = legacy
          }

          if (!item) {
            return yield* new ItemNotFound({ entityType, key: encodedKey })
          }

          const raw = fromAttributeMap(item)
          return yield* decodeAs(raw, item, mode)
        }),
      self,
      key as globalThis.Record<string, unknown>,
    )

  // ---------------------------------------------------------------------------
  // versions operation
  // ---------------------------------------------------------------------------

  const versions = (key: unknown) => {
    const encodedKey = keyForm(encodeKeySync(key))
    const primary = config.indexes.primary
    const pkValue = KeyComposer.composePk(schema, entityType, primary, keyForm(encodedKey))
    const liveSk = liveSkOf(encodedKey)
    const versionPrefix = DynamoSchema.composeVersionKeyPrefix(
      schema,
      entityType,
      historyKeyOptions(liveSk),
    )

    /**
     * History an earlier release wrote for this item, under the partition-wide
     * keys (#133): when there is any, the query reads the partition's history
     * and keeps the item's own rows plus those — a version held both ways is
     * read from its own row. Without any, it reads only the item's own.
     */
    const withLegacy: Query.QueryPrepare | undefined = hasItemHistory
      ? (tableName) =>
          Effect.gen(function* () {
            const legacy = yield* legacyHistory({ tableName, pk: pkValue, liveSk, kind: "version" })
            if (legacy.length === 0) return {}
            const client = yield* DynamoClient
            const own = new Set<number>()
            let start: globalThis.Record<string, AttributeValue> | undefined
            do {
              const result = yield* client.query({
                TableName: tableName,
                KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :prefix)",
                ExpressionAttributeNames: { "#pk": primary.pk.field, "#sk": primary.sk.field },
                ExpressionAttributeValues: {
                  ":pk": toAttributeValue(pkValue),
                  ":prefix": toAttributeValue(versionPrefix),
                },
                ProjectionExpression: "#sk",
                ConsistentRead: true,
                ExclusiveStartKey: start,
              })
              for (const row of result.Items ?? []) own.add(versionOfSk(row[primary.sk.field]?.S))
              start = result.LastEvaluatedKey as
                | globalThis.Record<string, AttributeValue>
                | undefined
            } while (start !== undefined)
            const legacySks = new Set(
              legacy
                .map((row) => row[primary.sk.field]?.S ?? "")
                .filter((sk) => !own.has(versionOfSk(sk))),
            )
            return {
              replaceBeginsWith: {
                from: versionPrefix,
                to: DynamoSchema.composeVersionKeyPrefix(schema, entityType),
              },
              keep: (row: globalThis.Record<string, AttributeValue>) => {
                const sk = row[primary.sk.field]?.S ?? ""
                return sk.startsWith(versionPrefix) || legacySks.has(sk)
              },
            }
          })
      : undefined

    return Query.make({
      tableName: "",
      indexName: undefined,
      pkField: primary.pk.field,
      pkValue,
      skField: primary.sk.field,
      entityTypes: [entityType],
      decoder: (raw) => decodeRecord(raw),
      resolveTableName: tableTag.useSync((tc: TableConfig) => tc.name),
      keyFields: [primary.pk.field, primary.sk.field],
      prepare: withLegacy,
    }).pipe(Query.where({ beginsWith: versionPrefix }))
  }

  // ---------------------------------------------------------------------------
  // deleted namespace
  // ---------------------------------------------------------------------------

  const deletedGet = (key: unknown) =>
    new EntityGetImpl(
      (mode: DecodeMode, _opts: EntityGetOpts) =>
        Effect.gen(function* () {
          const { name: tableName } = yield* tableTag

          // Caller key: Type side in, ENCODED out (see `encodeKey`).
          const encodedKey = yield* encodeKey(key, "deleted.get.decode")

          // The item's latest tombstone — not a sibling's in the same partition,
          // and one an earlier release wrote counts (#133).
          const primary = config.indexes.primary
          const pkValue = KeyComposer.composePk(schema, entityType, primary, keyForm(encodedKey))
          const tombstone = yield* latestTombstone(tableName, pkValue, liveSkOf(encodedKey))

          if (tombstone === undefined) {
            return yield* new ItemNotFound({ entityType, key: encodedKey })
          }

          const raw = fromAttributeMap(tombstone)
          yield* checkVersion(raw, "deleted.get.decode")
          // Soft-deleted items have GSI keys stripped — always use deletedRecordSchema
          // (itemSchema would fail because it expects GSI key fields that aren't present)
          if (mode === "native") return tombstone
          // Sparse Map fields: rebuild domain Records from flattened attrs so
          // soft-deleted items decode correctly. Sparse data is preserved
          // verbatim across soft-delete (GSI keys are stripped, sparse data is
          // not).
          deserializeSparseFields(raw)
          // `deletedRecordSchema` is keyed by DOMAIN field name, exactly like
          // `recordSchema` — so the same rename `decodeRecord` does applies here
          // (#127). Without it a `field:`-renamed attribute decodes as a missing
          // key and every soft-deleted row of such an entity is unreadable.
          renameFromDynamo(raw)
          const targetSchema = schemas.deletedRecordSchema
          return yield* Schema.decodeUnknownEffect(targetSchema as Schema.Codec<any>)(raw).pipe(
            Effect.map(attachPrototype),
            Effect.mapError(
              (cause) =>
                new ValidationError({
                  entityType,
                  operation: "deleted.get.decode",
                  cause,
                }),
            ),
          )
        }),
      self,
      key as globalThis.Record<string, unknown>,
    )

  const deletedList = (key: unknown) => {
    const encodedKey = keyForm(encodeKeySync(key))
    const primary = config.indexes.primary
    const pkValue = KeyComposer.composePk(schema, entityType, primary, keyForm(encodedKey))
    // The whole partition's tombstones — every item's, with sort key
    // composites (#133): no item segment.
    const deletedPrefix = DynamoSchema.composeDeletedKeyPrefix(schema, entityType)

    const decodeDeleted = (raw: globalThis.Record<string, unknown>) => {
      const corruption = versionCorruption(raw, "deleted.list.decode")
      if (corruption !== undefined) return Effect.fail(corruption)
      // Sparse fields are domain data and are preserved across soft-delete.
      // Rebuild Records from flattened attrs before schema decode.
      deserializeSparseFields(raw)
      // Then attribute → domain names, same order as `decodeRecord` (#127).
      renameFromDynamo(raw)
      return Schema.decodeUnknownEffect(schemas.deletedRecordSchema as Schema.Codec<any>)(raw).pipe(
        Effect.map(attachPrototype),
        Effect.mapError(
          (cause) =>
            new ValidationError({
              entityType,
              operation: "deleted.list.decode",
              cause,
            }),
        ),
      )
    }

    return Query.make({
      tableName: "",
      indexName: undefined,
      pkField: primary.pk.field,
      pkValue,
      skField: primary.sk.field,
      entityTypes: [],
      decoder: (raw) => decodeDeleted(raw),
      resolveTableName: tableTag.useSync((tc: TableConfig) => tc.name),
      keyFields: [primary.pk.field, primary.sk.field],
    }).pipe(Query.where({ beginsWith: deletedPrefix }))
  }

  // ---------------------------------------------------------------------------
  // restore operation
  // ---------------------------------------------------------------------------

  const restore = (key: unknown) =>
    new EntityGetImpl(
      (mode: DecodeMode, _opts: EntityGetOpts) =>
        Effect.gen(function* () {
          const client = yield* DynamoClient
          const tc = yield* tableTag
          const tableName = tc.name
          const ttlAttrName = resolveTtlAttributeName(tc)
          // Clock-backed time source for the restored updatedAt + retain snapshot.
          const now = yield* DateTime.now

          // Caller key: Type side in, ENCODED out (see `encodeKey`).
          const encodedKey = yield* encodeKey(key, "restore.decode")

          // The item's latest tombstone — not a sibling's in the same partition,
          // and one an earlier release wrote counts (#133).
          const primary = config.indexes.primary
          const pkValue = KeyComposer.composePk(schema, entityType, primary, keyForm(encodedKey))
          const tombstone = yield* latestTombstone(tableName, pkValue, liveSkOf(encodedKey))

          if (tombstone === undefined) {
            return yield* new ItemNotFound({ entityType, key: encodedKey })
          }

          yield* checkVersion(tombstone, "restore")
          const deletedRaw = fromAttributeMap(tombstone)
          const deletedMarshalledKey = toAttributeMap({
            [primary.pk.field]: (deletedRaw as globalThis.Record<string, unknown>)[
              primary.pk.field
            ],
            [primary.sk.field]: (deletedRaw as globalThis.Record<string, unknown>)[
              primary.sk.field
            ],
          })

          // Build restored item: original SK, recompose all GSI keys, remove deletedAt + TTL
          const restoredItem: globalThis.Record<string, unknown> = {
            ...(deletedRaw as globalThis.Record<string, unknown>),
          }
          delete restoredItem.deletedAt
          delete restoredItem[ttlAttrName]

          // Increment version
          // 0: a tombstone of an item written before the entity was `versioned`.
          const currentVersion = systemFields.version
            ? ((restoredItem[systemFields.version] as number | undefined) ?? 0)
            : 0
          const newVersion = currentVersion + 1
          if (systemFields.version) restoredItem[systemFields.version] = newVersion
          if (systemFields.updatedAt)
            restoredItem[systemFields.updatedAt] = generateTimestamp(
              systemFields.updatedAtEncoding,
              now,
            )

          // Domain-keyed VIEW of the tombstone, taken once and used for every
          // read-by-field-name below: key composition, vector-partition
          // recomposition and unique-sentinel composition. `restoredItem` itself
          // stays attribute-keyed — it is the row being written back, and
          // `decodeAs` renames it in place at the end (#127).
          const restoredDomain = toDomainView(restoredItem)

          // Recompose all keys (original SK, GSI keys)
          const restoredKeys = composeAllKeys(restoredDomain)
          Object.assign(restoredItem, restoredKeys)

          // Un-stash the embedding and recompose the vector partition value —
          // restoring never costs an Embedder call. See `DESIGN.md §14`.
          for (const [, definition] of vectorIndexEntries) {
            const stashed = restoredItem[definition.stashField]
            if (stashed !== undefined) {
              restoredItem[definition.vectorField] = stashed
              delete restoredItem[definition.stashField]
            }
            if (restoredItem[definition.vectorField] === undefined) continue
            const partition = KeyComposer.tryComposeVectorPartition(
              schema,
              entityType,
              definition,
              keyForm(restoredDomain),
            )
            if (partition !== undefined) restoredItem[definition.partitionField] = partition
          }

          const marshalledRestoredItem = toAttributeMap(restoredItem)

          // Build transaction
          type TransactItem = {
            Put?: {
              TableName: string
              Item: globalThis.Record<string, AttributeValue>
              ConditionExpression?: string
              ExpressionAttributeNames?: globalThis.Record<string, string>
              ExpressionAttributeValues?: globalThis.Record<string, AttributeValue>
            }
            Delete?: {
              TableName: string
              Key: globalThis.Record<string, AttributeValue>
              ConditionExpression?: string
              ExpressionAttributeNames?: globalThis.Record<string, string>
            }
          }
          const transactItems: Array<TransactItem> = []

          // Delete the soft-deleted item — only while it is still there: a
          // concurrent restore that already consumed it must not restore twice.
          transactItems.push({
            Delete: {
              TableName: tableName,
              Key: deletedMarshalledKey,
              ConditionExpression: "attribute_exists(#pk)",
              ExpressionAttributeNames: { "#pk": primary.pk.field },
            },
          })

          // Put restored item — never over a live item (a re-created one, or
          // another restore's): that would lose it and orphan its sentinels.
          transactItems.push({
            Put: {
              TableName: tableName,
              Item: marshalledRestoredItem,
              ConditionExpression: "attribute_not_exists(#pk)",
              ExpressionAttributeNames: { "#pk": primary.pk.field },
            },
          })

          // Version snapshot if retain enabled. The source is the TOMBSTONE, so
          // the soft-delete markers have to come off first: `buildSnapshotItem`
          // only ever overrides the TTL attribute when `versioned.ttl` is set, so
          // a `softDelete: { ttl }` expiry rode along and the snapshot of a
          // version that is very much still live expired an hour later — while
          // `deletedAt` made it look like a tombstone. This Put lands on the same
          // `#v#<version>` SK the delete-time snapshot used (same version — the
          // restored item is version + 1), so a dirty item here REPLACED the
          // clean snapshot the delete wrote.
          if (isRetainEnabled()) {
            const snapshotSource = { ...(deletedRaw as globalThis.Record<string, unknown>) }
            delete snapshotSource.deletedAt
            delete snapshotSource[ttlAttrName]
            const snapshotItem = buildSnapshotItem(
              snapshotSource,
              currentVersion,
              primary.pk.field,
              primary.sk.field,
              ttlAttrName,
              now,
              restoredKeys[primary.sk.field],
            )
            // Replaces only the delete-time snapshot of this same state —
            // never another version's history.
            transactItems.push({
              Put: snapshotPut(tableName, snapshotItem),
            })
          }

          // Re-establish unique constraint sentinels (sparse — fields that are
          // unset on the restored item don't get a sentinel; matches put semantics)
          const sentinelConstraints: Array<string> = []
          if (config.unique && Object.keys(config.unique).length > 0) {
            for (const [constraintName, constraintDef] of Object.entries(config.unique)) {
              const sentinel = composeUniqueSentinel(
                schema,
                entityType,
                constraintName,
                constraintDef,
                restoredDomain,
              )
              if (!sentinel) continue
              sentinelConstraints.push(constraintName)
              const guard = restoreSentinelGuard(
                restoredKeys[primary.pk.field],
                restoredKeys[primary.sk.field],
              )
              transactItems.push({
                Put: {
                  TableName: tableName,
                  Item: toAttributeMap({
                    [primary.pk.field]: sentinel.key.pk,
                    [primary.sk.field]: sentinel.key.sk,
                    __edd_e__: `${entityType}._unique.${constraintName}`,
                    _entity_pk: restoredKeys[primary.pk.field],
                    _entity_sk: restoredKeys[primary.sk.field],
                  }),
                  ConditionExpression: guard.ConditionExpression,
                  ExpressionAttributeNames: guard.ExpressionAttributeNames,
                  ...(guard.ExpressionAttributeValues
                    ? { ExpressionAttributeValues: guard.ExpressionAttributeValues }
                    : {}),
                },
              })
            }
          }

          yield* checkTransactionLimit(entityType, "restore", transactItems)
          yield* client
            .transactWriteItems({
              TransactItems: transactItems,
            })
            .pipe(
              Effect.mapError(
                (
                  err,
                ):
                  | DynamoClientError
                  | UniqueConstraintViolation
                  | ItemNotFound
                  | ItemNotDeleted
                  | ValidationError => {
                  if (isAwsTransactionCancelled(err.cause)) {
                    const reasons = err.cause.CancellationReasons
                    // The tombstone is gone (restored concurrently), or a live
                    // item already exists under the key.
                    if (reasons?.[0]?.Code === "ConditionalCheckFailed") {
                      return new ItemNotFound({ entityType, key: encodedKey })
                    }
                    if (reasons?.[1]?.Code === "ConditionalCheckFailed") {
                      return new ItemNotDeleted({ entityType, key: encodedKey })
                    }
                    if (isRetainEnabled() && reasons?.[2]?.Code === "ConditionalCheckFailed") {
                      return historyConflict(currentVersion, "restore")
                    }
                    if (reasons && config.unique) {
                      // Sentinel Puts come after Delete + Put + optional snapshot, in the
                      // order recorded in `sentinelConstraints` (sparse-skipped entries absent)
                      const sentinelStart = isRetainEnabled() ? 3 : 2
                      for (let i = sentinelStart; i < reasons.length; i++) {
                        if (reasons[i]?.Code === "ConditionalCheckFailed") {
                          const constraintName = sentinelConstraints[i - sentinelStart] ?? "unknown"
                          const constraintDef = config.unique[constraintName]
                          const uniqueFields = constraintDef
                            ? resolveUniqueFields(constraintDef)
                            : []
                          const fieldsRecord: globalThis.Record<string, string> = {}
                          for (const f of uniqueFields) {
                            const v = restoredDomain[f]
                            if (v !== undefined && v !== null) {
                              fieldsRecord[f] = KeyComposer.serializeValue(v)
                            }
                          }
                          return new UniqueConstraintViolation({
                            entityType,
                            constraint: constraintName,
                            fields: fieldsRecord,
                          })
                        }
                      }
                    }
                  }
                  return err
                },
              ),
            )

          return yield* decodeAs(restoredItem, marshalledRestoredItem, mode)
        }),
      self,
      key as globalThis.Record<string, unknown>,
    )

  // ---------------------------------------------------------------------------
  // purge operation
  // ---------------------------------------------------------------------------

  const purge = (key: unknown) =>
    new EntityDeleteImpl(
      (opts: { readonly condition: Expr | ConditionInput | undefined }) =>
        Effect.gen(function* () {
          // `purge` queries the partition — every row of it, or with sort key
          // composites this item's rows in it (#133) — and batch-deletes them
          // in chunks, so there is no single item for a
          // ConditionExpression to guard and no way to make one atomic across
          // the batches. `.condition()` is structurally available because
          // `purge` returns an `EntityDelete`; refuse it loudly rather than
          // accept a guard that would never be sent.
          if (opts.condition) {
            return yield* new ValidationError({
              entityType,
              operation: "purge.condition",
              cause:
                "[EDD-9047] .condition() is not supported on purge() — purge deletes every item " +
                "in the partition across multiple batched writes, so a per-item ConditionExpression " +
                "cannot be applied atomically. Guard the individual delete with " +
                "delete(key).condition(...) instead.",
            })
          }
          const client = yield* DynamoClient
          const { name: tableName } = yield* tableTag

          // Caller key: Type side in, ENCODED out (see `encodeKey`).
          const encodedKey = yield* encodeKey(key, "purge.decode")

          const primary = config.indexes.primary
          const pkValue = KeyComposer.composePk(schema, entityType, primary, keyForm(encodedKey))
          const liveSk = liveSkOf(encodedKey)
          const history = historyKeyOptions(liveSk)

          /**
           * Whether a row of the partition is this item's (#133). Only this
           * entity's rows (`__edd_e__`): in a single-table design another
           * entity can share the partition — a collection on the primary key —
           * and purge never removes its rows. Without sort key composites that
           * is every row of this entity. With them, the partition holds
           * siblings, and the item's rows are its live row, the rows nested
           * under it (time-series events), and its own history. History an
           * earlier release wrote without an item segment is the item's when
           * the composites it carries compose the item's live key.
           */
          const ownVersions = DynamoSchema.composeVersionKeyPrefix(schema, entityType, history)
          const ownTombstones = DynamoSchema.composeDeletedKeyPrefix(schema, entityType, history)
          const sharedHistory = [
            DynamoSchema.composeVersionKeyPrefix(schema, entityType),
            DynamoSchema.composeDeletedKeyPrefix(schema, entityType),
          ]
          const belongsToItem = (row: globalThis.Record<string, AttributeValue>): boolean => {
            if (row.__edd_e__?.S !== entityType) return false
            if (history === undefined) return true
            const sk = row[primary.sk.field]?.S
            if (sk === undefined) return false
            if (
              sk === liveSk ||
              sk.startsWith(`${liveSk}#`) ||
              sk.startsWith(ownVersions) ||
              sk.startsWith(ownTombstones)
            ) {
              return true
            }
            const unsegmented = sharedHistory.some(
              (prefix) => sk.startsWith(prefix) && !sk.slice(prefix.length).includes("#"),
            )
            return unsegmented && isItemsRow(row, liveSk)
          }

          // Query ALL items in this partition (current + versions + deleted).
          // Keys and entity type only — plus, with sort key composites, the
          // composites that attribute an unsegmented history row to its item.
          const projected: globalThis.Record<string, string> = { "#ent": "__edd_e__" }
          if (history !== undefined) {
            primary.sk.composite.forEach((attr, i) => {
              projected[`#c${i}`] = resolveDbName(attr)
            })
          }
          const allItems: Array<globalThis.Record<string, AttributeValue>> = []
          let exclusiveStartKey: globalThis.Record<string, AttributeValue> | undefined

          do {
            const result = yield* client.query({
              TableName: tableName,
              KeyConditionExpression: "#pk = :pk",
              ExpressionAttributeNames: {
                "#pk": primary.pk.field,
                "#sk": primary.sk.field,
                ...projected,
              },
              ExpressionAttributeValues: { ":pk": toAttributeValue(pkValue) },
              ProjectionExpression: ["#pk", "#sk", ...Object.keys(projected)].join(", "),
              ExclusiveStartKey: exclusiveStartKey,
            })

            if (result.Items) {
              allItems.push(...result.Items.filter(belongsToItem))
            }
            exclusiveStartKey = result.LastEvaluatedKey as
              | globalThis.Record<string, AttributeValue>
              | undefined
          } while (exclusiveStartKey)

          // The unique sentinels the item holds — live, or soft-deleted with
          // `preserveUnique` — and owns (#133): a sentinel of a value the item
          // holds without owning its reservation belongs to another item.
          if (config.unique && Object.keys(config.unique).length > 0) {
            const owner = {
              pk: pkValue,
              sk: KeyComposer.composeSk(
                schema,
                entityType,
                entityVersion,
                primary,
                keyForm(encodedKey),
              ),
            }
            const holders: Array<globalThis.Record<string, AttributeValue>> = []
            const mainResult = yield* client.getItem({
              TableName: tableName,
              Key: toAttributeMap({ [primary.pk.field]: owner.pk, [primary.sk.field]: owner.sk }),
              ConsistentRead: true,
            })
            if (mainResult.Item) holders.push(mainResult.Item)
            // Every soft-deleted incarnation (each kept its reservations).
            const deletedPrefix = DynamoSchema.composeDeletedKeyPrefix(schema, entityType)
            let deletedStart: globalThis.Record<string, AttributeValue> | undefined
            do {
              const deletedResult = yield* client.query({
                TableName: tableName,
                KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :skPrefix)",
                ExpressionAttributeNames: {
                  "#pk": primary.pk.field,
                  "#sk": primary.sk.field,
                },
                ExpressionAttributeValues: {
                  ":pk": toAttributeValue(pkValue),
                  ":skPrefix": toAttributeValue(deletedPrefix),
                },
                ConsistentRead: true,
                ExclusiveStartKey: deletedStart,
              })
              holders.push(...(deletedResult.Items ?? []).filter(belongsToItem))
              deletedStart = deletedResult.LastEvaluatedKey as
                | globalThis.Record<string, AttributeValue>
                | undefined
            } while (deletedStart !== undefined)

            // Sparse — fields that were unset never had a sentinel. The stored
            // row is attribute-keyed, the constraint is not (#127).
            const held = new Map<string, { readonly pk: string; readonly sk: string }>()
            for (const holder of holders) {
              const holderDomain = toDomainView(fromAttributeMap(holder))
              for (const [constraintName, constraintDef] of Object.entries(config.unique)) {
                const sentinel = composeUniqueSentinel(
                  schema,
                  entityType,
                  constraintName,
                  constraintDef,
                  holderDomain,
                )
                if (sentinel) held.set(`${sentinel.key.pk}\u0000${sentinel.key.sk}`, sentinel.key)
              }
            }
            // Released one by one, each only while still this item's: a batch
            // delete cannot carry the ownership condition.
            for (const sentinel of yield* ownedSentinels(tableName, [...held.values()], owner)) {
              yield* client.deleteItem(sentinelRelease(tableName, sentinel, owner).Delete).pipe(
                Effect.catchIf(
                  (err) => isAwsConditionalCheckFailed(err.cause),
                  () => Effect.void,
                ),
              )
            }
          }

          // Batch delete in chunks of 25
          for (let i = 0; i < allItems.length; i += 25) {
            const chunk = allItems.slice(i, i + 25)
            yield* client.batchWriteItem({
              RequestItems: {
                [tableName]: chunk.map((item) => ({
                  DeleteRequest: {
                    Key: {
                      [primary.pk.field]: item[primary.pk.field]!,
                      [primary.sk.field]: item[primary.sk.field]!,
                    },
                  },
                })),
              },
            })
          }
        }),
      self,
      key as globalThis.Record<string, unknown>,
    )

  // ---------------------------------------------------------------------------
  // Expression combinators: condition, filter, select
  // ---------------------------------------------------------------------------

  // Sparse-field map used by createPathBuilder to wire `.entry(key)` accessors.
  const sparseFieldsForPath = hasSparseFields ? sparseFields : undefined
  const entityPathBuilder = createPathBuilder<any>([], resolveDbName, sparseFieldsForPath)
  const entityConditionOps = createConditionOps<any>()

  /**
   * Build a condition expression combinator from callback or shorthand.
   * Returns a function that applies the condition to an EntityPut/Update/Delete.
   */
  const entityCondition = (
    cbOrShorthand: ((...args: any[]) => any) | globalThis.Record<string, unknown>,
  ) => {
    const expr: Expr =
      typeof cbOrShorthand === "function"
        ? cbOrShorthand(entityPathBuilder, entityConditionOps)
        : parseSimpleShorthand(cbOrShorthand)
    return (self: any) => conditionCombinator(self, expr)
  }

  /**
   * Build a filter expression combinator from callback or shorthand.
   * Returns a function that applies the filter to a Query.
   */
  const entityFilter = (
    cbOrShorthand: ((...args: any[]) => any) | globalThis.Record<string, unknown>,
  ) => {
    const expr: Expr =
      typeof cbOrShorthand === "function"
        ? cbOrShorthand(entityPathBuilder, entityConditionOps)
        : parseSimpleShorthand(cbOrShorthand)
    return <A>(q: Query.Query<A>): Query.Query<A> => filterExpr(q, expr)
  }

  /**
   * Build a select (projection) combinator from callback or string array.
   */
  const entitySelect = (
    cbOrAttrs:
      | ((t: any) => ReadonlyArray<{ segments: ReadonlyArray<string | number> }>)
      | ReadonlyArray<string>,
  ) => {
    if (typeof cbOrAttrs === "function") {
      const paths = cbOrAttrs(entityPathBuilder)
      const segments = paths.map((p: { segments: ReadonlyArray<string | number> }) => p.segments)
      return <A>(q: Query.Query<A>): Query.Query<globalThis.Record<string, unknown>> =>
        selectPaths(q, segments)
    }
    return <A>(q: Query.Query<A>): Query.Query<globalThis.Record<string, unknown>> =>
      Query.select(q, cbOrAttrs)
  }

  // ---------------------------------------------------------------------------
  // Flattened query accessors (same as query namespace, but on entity directly)
  // ---------------------------------------------------------------------------

  const flattenedAccessors: globalThis.Record<string, (pk: any) => Query.Query<any>> = {}
  for (const indexName of Object.keys(config.indexes)) {
    if (indexName === "primary") continue
    flattenedAccessors[indexName] = queryNamespace[indexName]!
  }

  const entity = {
    _tag: "Entity" as const,
    model: config.model,
    entityType: config.entityType,
    get indexes() {
      return allIndexes
    },
    timestamps: config.timestamps as TTimestamps,
    versioned: config.versioned as TVersioned,
    softDelete: config.softDelete as TSoftDelete,
    unique: config.unique as TUnique,
    identifier: resolvedIdentifier as ExtractIdentifier<ConfiguredModel<TModel, TAttrs>>,
    timeSeries: config.timeSeries as TTimeSeries,
    generatedId: config.generatedId as TGeneratedId,
    vectorIndexes: config.vectorIndexes as TVectorIndexes,
    _vectorIndexes: vectorIndexes,
    _resolvedRefs: resolvedRefs,
    /** @internal Full decode pipeline: rename + schema decode. Used by Batch/Aggregate. */
    _decodeRecord: decodeRecord,
    /** @internal Domain field name → stored attribute name (`storedAs` renames). */
    _resolveDbName: resolveDbName,
    /** @internal Entity schema version baked into composed keys. */
    _entityVersion: entityVersion,
    _serializeSparseFields: serializeSparseFields,
    _keyForm: keyForm,
    _renameToDynamo: renameToDynamo,
    _incarnationToken: stampsIncarnation,
    _versionCorruption: versionCorruption,
    _fillDecodingDefaults: fillDecodingDefaults,
    _unsentineledDefaults: unsentineledDefaults,
    _planPut: planPut,
    _multiItemWriteFeatures: multiItemWriteFeatures(),
    _planUpdate: (key: unknown, state: UpdateState) =>
      planUpdate(key, state).pipe(
        // Planning stops at the write, so only what an update raises BEFORE
        // writing can surface; anything else is a defect (`isPlanUpdateError`).
        Effect.catch((e: unknown) => (isPlanUpdateError(e) ? Effect.fail(e) : Effect.die(e))),
      ),
    _planDelete: (
      key: unknown,
      opts: { readonly condition: Expr | ConditionInput | undefined; readonly mustExist: boolean },
    ) =>
      planDelete(key, opts).pipe(
        Effect.catch((e: unknown) => (isPlanDeleteError(e) ? Effect.fail(e) : Effect.die(e))),
      ),
    _liveRows: liveRows,
    _attachPrototype: attachPrototype,
    _configure: (
      injectedSchema: DynamoSchema.DynamoSchema,
      injectedTableTag: import("effect").Context.Service<TableConfig, TableConfig>,
    ) => {
      schema = injectedSchema
      tableTag = injectedTableTag
    },
    _injectIndex: (name: string, def: IndexDefinition) => {
      allIndexes = { ...allIndexes, [name]: def }
    },
    get _schema() {
      return schema
    },
    get _tableTag() {
      return tableTag
    },
    systemFields,
    schemas,
    inputSchema: schemas.inputSchema,
    createSchema: schemas.createSchema,
    updateSchema: schemas.updateSchema,

    get,
    put,
    create,
    patch,
    update,
    delete: del,
    deleteIfExists,
    upsert,
    scan,
    getVersion,
    versions,
    restore,
    purge,
    deleted: { get: deletedGet, list: deletedList },
    append,
    history,

    set,
    expectedVersion,

    query: queryNamespace,
    condition: entityCondition,
    filter: entityFilter,
    select: entitySelect,
    ...flattenedAccessors,
    // Cast rationale: Entity.make() builds the entity object incrementally from
    // closures that capture the config. The object literal's inferred type is a
    // union of all closure return types, which doesn't satisfy the fully-generic
    // Entity<TModel, ...> interface. The cast is safe because each method
    // is constructed with the correct types from the generic config parameters.
  } as unknown as Entity<
    TModel,
    TEntityType,
    TIndexes,
    TTimestamps,
    TVersioned,
    TSoftDelete,
    TUnique,
    TRefs,
    ExtractIdentifier<ConfiguredModel<TModel, TAttrs>>,
    TTimeSeries,
    TGeneratedId,
    TVectorIndexes
  >

  // Assign self so operation closures can reference the entity
  // Cast needed: Entity<TModel,...> set property is contravariant in updates type,
  // which makes it incompatible with the default Entity (Schema.Top).
  // This is safe — self is only used internally for extractTransactable.
  self = entity as unknown as Entity
  return entity
}

// ---------------------------------------------------------------------------
// Entity binding — resolve services, return BoundEntity with R = never
// ---------------------------------------------------------------------------

/**
 * Bind an Entity to resolved `DynamoClient` and `TableConfig` services.
 * Returns a {@link BoundEntity} where all operations have `R = never`.
 *
 * @internal Used by `DynamoClient.make()` to bind entities.
 */
export const bind = <
  TModel extends Schema.Top,
  TEntityType extends string,
  TIndexes extends globalThis.Record<string, IndexDefinition>,
  TTimestamps extends TimestampsConfig | undefined,
  TVersioned extends VersionedConfig | undefined,
  TSoftDelete extends SoftDeleteConfig | undefined,
  TUnique extends UniqueConfig | undefined,
  TRefs extends globalThis.Record<string, AnyRefValue> | undefined,
  TIdentifier extends string | undefined,
  TTimeSeries extends TimeSeriesConfig<any> | undefined = undefined,
  TGeneratedId extends GeneratedIdConfig | undefined = undefined,
  TVectorIndexes extends globalThis.Record<string, VectorIndexConfig> | undefined = undefined,
>(
  entity: Entity<
    TModel,
    TEntityType,
    TIndexes,
    TTimestamps,
    TVersioned,
    TSoftDelete,
    TUnique,
    TRefs,
    TIdentifier,
    TTimeSeries,
    TGeneratedId,
    TVectorIndexes
  >,
): Effect.Effect<
  BoundEntity<
    TModel,
    TIndexes,
    TRefs,
    EntityKeyType<TModel, TIndexes>,
    TTimeSeries,
    TTimestamps,
    TVersioned,
    TGeneratedId,
    TVectorIndexes
  >,
  never,
  DynamoClient | TableConfig
> =>
  Effect.gen(function* () {
    // Bundle a default `Crypto` service into the captured context. This is how
    // `generatedId` put auto-fills a UUID while keeping bound `put` at
    // `R = never`: the entity op yields `Crypto.Crypto`, and `provide` supplies
    // it from this bundled context rather than surfacing it as a requirement.
    // The `provide` helper is widened to admit `Crypto.Crypto` so the
    // Crypto-requiring entity ops typecheck. A platform override supplied via
    // `DynamoClient.make({ crypto })` is already in `baseCtx` and is respected
    // (we only fill the default when absent). See `DESIGN.md`.
    const baseCtx = yield* Effect.context<DynamoClient | TableConfig>()
    const ctx = Option.isSome(Context.getOption(baseCtx, Crypto.Crypto))
      ? (baseCtx as Context.Context<DynamoClient | TableConfig | Crypto.Crypto>)
      : Context.add(baseCtx, Crypto.Crypto, makeDefaultCrypto())
    const provide = <A, E>(
      effect: Effect.Effect<A, E, DynamoClient | TableConfig | Crypto.Crypto>,
    ): Effect.Effect<A, E, never> => Effect.provide(effect, ctx)

    type Key = EntityKeyType<TModel, TIndexes>
    type Input = WithGeneratedId<
      EntityRefInputType<TModel, TRefs, TTimestamps, TVersioned, TTimeSeries>,
      TGeneratedId
    >

    // Sparse-field map (if any) — extracted from the Entity's configured model
    // so the PathBuilder used by `.condition((t, ops) => ...)` exposes
    // `.entry(key)` accessors on sparse-map fields.
    const boundSparseFields = getSparseFields(entity.model as Schema.Top)
    const boundSparseFieldsForPath =
      Object.keys(boundSparseFields).length > 0 ? boundSparseFields : undefined

    // Shared config for the bound-CRUD builders — pre-resolved services plus
    // a typed PathBuilder/ConditionOps so `.condition((t, ops) => ...)` works.
    const boundCrudConfig: import("./internal/BoundCrud.js").BoundCrudConfig<unknown> = {
      pathBuilder: createPathBuilder(undefined, undefined, boundSparseFieldsForPath),
      conditionOps: createConditionOps(),
      provide,
    }

    // Helper: apply query combinators then execute
    const applyQuery = <A>(q: Query.Query<A>, combinators: ReadonlyArray<(q: any) => any>) => {
      let result: any = q
      for (const fn of combinators) result = fn(result)
      return result as Query.Query<A>
    }

    // Helper: wrap a raw entity-level Query<A> in a BoundQuery with this binding's
    // pre-resolved provide. Used by `versions` and `deleted.list` accessors so consumers
    // get the same fluent .collect() / .fetch() / .paginate() / .reverse() / .limit() /
    // .filter() ergonomics they get from index accessors and `.scan()`.
    const wrapAsBoundQuery = <A>(q: Query.Query<A>) => {
      const bqConfig: BoundQueryConfig<unknown> = {
        pathBuilder: createPathBuilder(),
        conditionOps: createConditionOps(),
        provide,
        ...entityNaming(entity._resolveDbName),
      }
      return new BoundQueryImpl(
        q,
        bqConfig,
      ) as unknown as import("./internal/BoundQuery.js").BoundQuery<A, never, A>
    }

    /**
     * Out-of-band embedding refresh.
     *
     * DynamoDB never recomputes a vector, so changing the embedding model (or
     * the declared `source.fields`) leaves every stored vector stale until
     * something rewrites it. `reembed` is that something: scan the entity's
     * items, re-derive the source text, embed, and write the vector +
     * partition attributes back.
     *
     * Version snapshots and soft-delete tombstones carry the same `__edd_e__`
     * discriminator as live items, so they are filtered out by recomposing the
     * primary sort key from the decoded record and comparing it with what is
     * stored — a snapshot's SK never round-trips.
     *
     * See `DESIGN.md §14 Write path`.
     */
    const reembed = (options?: {
      readonly concurrency?: number | undefined
    }): Effect.Effect<number, DynamoClientError | ValidationError | EmbeddingError, never> => {
      const vectorDefs = Object.entries(entity._vectorIndexes)
      const boundSchema = entity._schema
      const primary = entity.indexes.primary as IndexDefinition
      const boundEntityVersion = entity._entityVersion
      const concurrency = options?.concurrency ?? 4
      return provide(
        Effect.gen(function* () {
          if (vectorDefs.length === 0) return 0
          const client = yield* DynamoClient
          const { name: tableName } = yield* entity._tableTag as Context.Service<
            TableConfig,
            TableConfig
          >

          const reembedItem = (marshalled: globalThis.Record<string, AttributeValue>) =>
            Effect.gen(function* () {
              const raw = fromAttributeMap(marshalled) as globalThis.Record<string, unknown>
              const storedSk = raw[primary.sk.field]
              const decoded = (yield* entity._decodeRecord(raw)) as globalThis.Record<
                string,
                unknown
              >
              // Snapshots / tombstones live in the same partition under a
              // rewritten SK — their SK cannot be recomposed from the record.
              const decodedKeyForm = keyFormFor(entity, decoded)
              const liveSk = KeyComposer.composeSk(
                boundSchema,
                entity.entityType,
                boundEntityVersion,
                primary,
                decodedKeyForm,
              )
              if (storedSk !== liveSk) return false

              const attributes: globalThis.Record<string, unknown> = {}
              for (const [logicalName, definition] of vectorDefs) {
                const partition = KeyComposer.tryComposeVectorPartition(
                  boundSchema,
                  entity.entityType,
                  definition,
                  decodedKeyForm,
                )
                if (partition === undefined) continue
                const text = deriveSourceText(definition, decoded)
                if (text === undefined) continue
                const maybeEmbedder = yield* Effect.serviceOption(Embedder)
                if (Option.isNone(maybeEmbedder)) {
                  return yield* new EmbeddingError({
                    entityType: entity.entityType,
                    index: logicalName,
                    reason:
                      "No Embedder service is available. Provide one via " +
                      "DynamoClient.make({ embedder }) before calling reembed().",
                  })
                }
                const vector = yield* maybeEmbedder.value.embed(text)
                if (vector.length !== definition.dimensions) {
                  return yield* new EmbeddingError({
                    entityType: entity.entityType,
                    index: logicalName,
                    reason:
                      `Embedder produced ${vector.length} dimensions but vector index ` +
                      `"${definition.index}" declares ${definition.dimensions}.`,
                  })
                }
                attributes[definition.partitionField] = partition
                attributes[definition.vectorField] = vector
              }
              if (Object.keys(attributes).length === 0) return false

              const names: globalThis.Record<string, string> = { "#pk": primary.pk.field }
              const values: globalThis.Record<string, AttributeValue> = {}
              const sets: Array<string> = []
              let i = 0
              for (const [field, value] of Object.entries(attributes)) {
                names[`#v${i}`] = field
                values[`:v${i}`] = toAttributeValue(value)
                sets.push(`#v${i} = :v${i}`)
                i++
              }
              // Scan → embed → update is not atomic: an item can be hard-deleted
              // between the scan page and this write, and an unconditional
              // UpdateItem would resurrect it as a key-plus-vector fragment that
              // decodes to nothing but still answers searches. Guard on the item
              // still existing and treat the rejection as "skipped".
              return yield* client
                .updateItem({
                  TableName: tableName,
                  Key: {
                    [primary.pk.field]: marshalled[primary.pk.field]!,
                    [primary.sk.field]: marshalled[primary.sk.field]!,
                  },
                  UpdateExpression: `SET ${sets.join(", ")}`,
                  ConditionExpression: "attribute_exists(#pk)",
                  ExpressionAttributeNames: names,
                  ExpressionAttributeValues: values,
                })
                .pipe(
                  Effect.as(true),
                  Effect.catchIf(
                    (err: DynamoClientError) => isAwsConditionalCheckFailed(err.cause),
                    () => Effect.succeed(false),
                  ),
                )
            })

          let updated = 0
          let exclusiveStartKey: globalThis.Record<string, AttributeValue> | undefined
          do {
            const page = yield* client.scan({
              TableName: tableName,
              FilterExpression: "#et = :et",
              ExpressionAttributeNames: { "#et": "__edd_e__" },
              ExpressionAttributeValues: { ":et": { S: entity.entityType } },
              ExclusiveStartKey: exclusiveStartKey,
            })
            const results = yield* Effect.all(
              (page.Items ?? []).map((item) =>
                reembedItem(item as globalThis.Record<string, AttributeValue>),
              ),
              { concurrency },
            )
            updated += results.filter((applied) => applied).length
            exclusiveStartKey = page.LastEvaluatedKey as
              | globalThis.Record<string, AttributeValue>
              | undefined
          } while (exclusiveStartKey !== undefined)
          return updated
        }),
      )
    }

    return {
      // CRUD — fluent bound builders (yieldable, no .run() terminal)
      reembed,
      get: (key: Key) => makeBoundGet(entity.get(key) as any, boundCrudConfig),
      put: (input: Input) => makeBoundPut(entity.put(input), boundCrudConfig),
      create: (input: Input) => makeBoundPut(entity.create(input), boundCrudConfig),
      update: (key: Key) => makeBoundUpdate(entity.update(key), boundCrudConfig),
      delete: (key: Key) => makeBoundDelete(entity.delete(key), boundCrudConfig),
      upsert: (input: Input) => makeBoundPut(entity.upsert(input), boundCrudConfig),
      patch: (key: Key) => makeBoundUpdate(entity.patch(key), boundCrudConfig),
      deleteIfExists: (key: Key) => makeBoundDelete(entity.deleteIfExists(key), boundCrudConfig),
      // Lifecycle
      getVersion: (key: Key, version: number) =>
        provide((entity.getVersion(key, version) as any)._run("record")),
      versions: (key: Key) => wrapAsBoundQuery(entity.versions(key)),
      restore: (key: Key) => provide((entity.restore(key) as any)._run("record")),
      purge: (key: Key) => provide(entity.purge(key).asEffect()),
      deleted: {
        get: (key: Key) => provide((entity.deleted.get(key) as any)._run("record")),
        list: (key: Key) => wrapAsBoundQuery(entity.deleted.list(key)),
      },
      // Time-series (no-ops when entity is not configured; runtime check in
      // the entity-level `append` returns a ValidationError).
      append: (input: unknown) => {
        const appendCfg: import("./internal/BoundCrud.js").BoundAppendConfig<unknown> = {
          ...boundCrudConfig,
          run: (opts) =>
            provide(
              (
                entity.append as unknown as (
                  i: unknown,
                  c: Expr | ConditionInput | undefined,
                  s: boolean,
                  r: ReadonlyArray<string> | undefined,
                ) => Effect.Effect<any, any, any>
              )(opts.input, opts.condition, opts.skipFollowUp, opts.removeAttrs),
            ),
        }
        return makeBoundAppend(input, appendCfg)
      },
      history: (key: Key) => {
        const q = (entity.history as any as (k: Key) => Query.Query<any>)(key)
        const pathBuilder = createPathBuilder()
        const conditionOps = createConditionOps()
        const ts = (entity as unknown as { readonly timeSeries?: TimeSeriesConfig<any> }).timeSeries
        const orderBy = ts?.orderBy
        const entityInternals = entity as unknown as {
          readonly _schema: DynamoSchema.DynamoSchema
          readonly entityType: string
          readonly model: Schema.Top
          readonly schemas?: { readonly inputSchema?: Schema.Top | undefined }
        }
        const schemaRef = entityInternals._schema
        const entityTypeRef = entityInternals.entityType

        // composeSkCondition: rewrite user's .where() values by prefixing
        // with `<currentSk>#e#` and applying serialisation + casing to the
        // user-supplied orderBy value.
        const primary = entity.indexes.primary!
        const composeSkCondition = (cond: RawSortKeyCondition): Query.SortKeyCondition => {
          // We need the `currentSk` + prefix. The user's `key` lets us derive
          // `currentSk` via the same primary SK composer used by `history()`.
          // `key` is the caller's Type-side key; the stored SK was composed
          // from the encoded form, so normalise it the same way
          // `composePrimaryKey` does before recomposing.
          const currentSk = KeyComposer.composeSk(
            schemaRef,
            entityTypeRef,
            1,
            primary,
            keyFormFor(entityInternals, key as globalThis.Record<string, unknown>),
          )
          const prefix = KeyComposer.composeEventSkPrefix(currentSk, schemaRef.casing)
          // Key form first: the event SK is composed from the key form of the
          // orderBy value (`Entity.append` composes from `serialisedInput`), so
          // a transformed orderBy composite must take the same route here.
          const rewrite = (v: unknown) =>
            `${prefix}${DynamoSchema.applyCasing(
              KeyComposer.serializeValue(
                orderBy === undefined ? v : keyFormFor(entityInternals, { [orderBy]: v })[orderBy],
              ),
              schemaRef.casing,
            )}`
          if ("eq" in cond) return { eq: rewrite(cond.eq) }
          if ("lt" in cond) return { lt: rewrite(cond.lt) }
          if ("lte" in cond) return { lte: rewrite(cond.lte) }
          if ("gt" in cond) return { gt: rewrite(cond.gt) }
          if ("gte" in cond) return { gte: rewrite(cond.gte) }
          if ("between" in cond)
            return { between: [rewrite(cond.between[0]), rewrite(cond.between[1])] }
          if ("beginsWith" in cond) return { beginsWith: rewrite(cond.beginsWith) }
          return cond
        }

        const bqConfig: BoundQueryConfig<unknown> = {
          pathBuilder,
          conditionOps,
          provide,
          skFields: orderBy ? [orderBy] : [],
          composeSkCondition,
          ...entityNaming(entity._resolveDbName),
        }
        return new BoundQueryImpl(q, bqConfig)
      },
      // Query execution
      paginate: <A>(q: Query.Query<A>, ...combinators: ReadonlyArray<(q: any) => any>) => {
        const final = applyQuery(q, combinators)
        return Stream.unwrap(provide(Query.paginate(final))).pipe(
          Stream.flatMap((page) => Stream.fromIterable(page)),
        )
      },
      collect: <A>(q: Query.Query<A>, ...combinators: ReadonlyArray<(q: any) => any>) =>
        provide(Query.collect(applyQuery(q, combinators))),
      fetch: <A>(q: Query.Query<A>, ...combinators: ReadonlyArray<(q: any) => any>) =>
        provide(Query.execute(applyQuery(q, combinators))),
      scanFetch: (...combinators: ReadonlyArray<(q: any) => any>) =>
        provide(Query.execute(applyQuery(entity.scan(), combinators))),
    } as unknown as BoundEntity<
      TModel,
      TIndexes,
      TRefs,
      EntityKeyType<TModel, TIndexes>,
      TTimeSeries,
      TTimestamps,
      TVersioned,
      TGeneratedId,
      TVectorIndexes
    >
  })

// ---------------------------------------------------------------------------
// Extraction protocol — used by Transaction and Batch modules
// ---------------------------------------------------------------------------

/**
 * What planning an update for a transaction can fail with: everything the
 * update raises before it writes — its reads (`DynamoClientError`, `ItemNotFound`,
 * `RefNotFound`), a stale `expectedVersion`, `patch` of a missing row
 * (`ConditionalCheckFailed`), an op too large for one transaction, and a
 * refused op (`ValidationError`).
 */
export type PlanUpdateError =
  | DynamoClientError
  | ValidationError
  | ConditionalCheckFailed
  | ItemNotFound
  | TransactionOverflow
  | OptimisticLockError
  | RefNotFound
  | UniqueConstraintViolation

/** What planning a delete for a transaction can fail with — see {@link PlanUpdateError}. */
export type PlanDeleteError =
  | DynamoClientError
  | ValidationError
  | ConditionalCheckFailed
  | ItemNotFound
  | TransactionOverflow

const tagged =
  <A>(tags: ReadonlySet<string>) =>
  (e: unknown): e is A =>
    typeof e === "object" && e !== null && "_tag" in e && tags.has(String(e._tag))

const clientErrorTags = [
  "DynamoError",
  "ThrottlingError",
  "DynamoValidationError",
  "InternalServerError",
  "ResourceNotFoundError",
]
const planDeleteErrorTags = [
  ...clientErrorTags,
  "ValidationError",
  "ConditionalCheckFailed",
  "ItemNotFound",
  "TransactionOverflow",
]
const isPlanDeleteError = tagged<PlanDeleteError>(new Set(planDeleteErrorTags))
const isPlanUpdateError = tagged<PlanUpdateError>(
  new Set([
    ...planDeleteErrorTags,
    "OptimisticLockError",
    "RefNotFound",
    "UniqueConstraintViolation",
  ]),
)

export interface TransactableInfo {
  readonly opType: "get" | "put" | "update" | "delete"
  readonly entity: Entity
  readonly key?: globalThis.Record<string, unknown> | undefined
  readonly input?: globalThis.Record<string, unknown> | undefined
  /**
   * The condition attached to the op — via `.condition()` on a bound builder,
   * `Entity.condition()` on an unbound intermediate, or implicitly by
   * `Entity.create()` (`attribute_not_exists`) / `Entity.patch()`
   * (`attribute_exists`). Consumers that cannot express a condition (BatchWrite)
   * MUST reject the op rather than drop it.
   */
  readonly condition?: Expr | ConditionInput | undefined
  /**
   * For `opType: "put"`, which put-shaped op produced it. `"upsert"` does NOT
   * have `Put` semantics — it is an `UpdateItem` with `if_not_exists` on
   * `createdAt`, immutable fields and the version counter. Consumers that emit
   * a `Put` MUST reject `"upsert"` rather than compile it (#100).
   */
  readonly putKind?: PutKind | undefined
  /** For `opType: "update"` — the accumulated update (`.set()`, `.remove()`, `patch`, …). */
  readonly updateState?: UpdateState | undefined
  /**
   * For `opType: "delete"` — the request as the caller built it: its own
   * condition (without `deleteIfExists`'s guard, which `mustExist` carries),
   * and its return mode, which a transaction cannot honour (EDD-9060).
   */
  readonly deleteRequest?:
    | {
        readonly condition: Expr | ConditionInput | undefined
        readonly mustExist: boolean
        readonly returnValues: ReturnValuesMode | undefined
      }
    | undefined
}

/** @internal */
interface InternalEntityOp {
  readonly [EntityOpTypeId]: EntityOpTypeId
  readonly _opType: string
  readonly _entity: Entity
  readonly _key?: globalThis.Record<string, unknown>
  readonly _input?: globalThis.Record<string, unknown>
  readonly _condition?: Expr | ConditionInput | undefined
  readonly _updateState?: UpdateState
  readonly _putKind?: PutKind | undefined
}

/** @internal */
interface InternalEntityDelete {
  readonly [EntityDeleteTypeId]: EntityDeleteTypeId
  readonly _entity: Entity
  readonly _key: globalThis.Record<string, unknown>
  readonly _condition?: Expr | ConditionInput | undefined
  readonly _mustExist?: boolean
  readonly _returnValues?: ReturnValuesMode | undefined
}

const isEntityOp = (op: object): op is InternalEntityOp => EntityOpTypeId in op

const isEntityDelete = (op: object): op is InternalEntityDelete => EntityDeleteTypeId in op

/**
 * Extract transactable metadata from an Entity operation intermediate.
 * Returns undefined if the value is not a recognized entity operation.
 *
 * Bound ops (`db.entities.X.put(...)`, `.create(...)`, `.delete(...)`,
 * `.get(...)`, …) are unwrapped to the `EntityOp` / `EntityDelete` they wrap.
 * That unwrapping is what lets entities authored with the pure, AWS-free
 * `@effect-dynamodb/schema` `Entity.make` take part in `Batch.write`,
 * `Transaction.transactWrite`, `EventStore.append({ additionalItems })` (#100)
 * and — through the same one protocol, not a second one — `Batch.get`,
 * `Transaction.transactGet` and `Transaction.check` (#108). A pure definition
 * carries no operations, so the bound op is the only descriptor its author can
 * ever hold.
 */
export const extractTransactable = (op: unknown): TransactableInfo | undefined => {
  if (op == null || typeof op !== "object") return undefined

  // Unwrap a bound-CRUD builder to the intermediate it wraps. Combinators on the
  // builder (`.condition()`, `.set()`, …) are applied to that inner op, so the
  // unwrapped value carries the full request.
  const target = isBoundOp(op) ? (op._op as unknown) : op
  if (target == null || typeof target !== "object") return undefined

  // Check for EntityOp intermediates (get, put, update)
  if (isEntityOp(target)) {
    if (target._opType === "get") {
      return { opType: "get", entity: target._entity, key: target._key }
    }
    // `create`'s own not-exists guard is ANDed with the caller's condition:
    // no `.condition()` replaces it (#133).
    const primary = target._entity.indexes.primary!
    if (target._opType === "put") {
      const putKind = target._putKind ?? "put"
      return {
        opType: "put",
        entity: target._entity,
        input: target._input,
        condition: withGuard(
          putKind === "create"
            ? { attributeNotExists: [primary.pk.field, primary.sk.field] }
            : undefined,
          target._condition,
        ),
        putKind,
      }
    }
    if (target._opType === "update") {
      // `Transaction.transactWrite` plans an update from its whole state
      // (`_planUpdate`), which applies `patch`'s guard itself; every other
      // multi-item path refuses an update.
      return {
        opType: "update",
        entity: target._entity,
        key: target._key,
        condition: target._updateState?.condition,
        updateState: target._updateState,
      }
    }
  }

  // Check for EntityDelete intermediate — `deleteIfExists`'s exists guard
  // ANDed with the caller's condition.
  if (isEntityDelete(target)) {
    return {
      opType: "delete",
      entity: target._entity,
      key: target._key,
      condition: withGuard(
        target._mustExist
          ? { attributeExists: [target._entity.indexes.primary!.pk.field] }
          : undefined,
        target._condition,
      ),
      deleteRequest: {
        condition: target._condition,
        mustExist: target._mustExist === true,
        returnValues: target._returnValues,
      },
    }
  }

  return undefined
}

// ---------------------------------------------------------------------------
// Time-series public type alias
// ---------------------------------------------------------------------------

/**
 * Success type of `BoundEntity.append()` on the default path — carries the
 * post-append `current` (decoded model).
 *
 * `.skipFollowUp()` narrows this to `void`. Stale outcomes are on the error
 * channel — see {@link StaleAppend} and {@link ConditionalCheckFailed}.
 */
export type AppendSuccess<TModel extends Schema.Top> = AppendSuccessType<TModel>

// ---------------------------------------------------------------------------
// Type extractors — the 7 derived types
// ---------------------------------------------------------------------------

/**
 * Extract the pure model type from an Entity.
 *
 * `Entity.Model<typeof Users>` = `{ userId: string, email: string, ... }`
 */
export type Model<E extends { readonly model: Schema.Top }> = ModelType<E["model"]>

/**
 * Extract the record type (model + system fields) from an Entity.
 *
 * `Entity.Record<typeof Users>` = model fields + createdAt, updatedAt, version
 */
// eslint-disable-next-line @typescript-eslint/no-shadow
export type Record<
  E extends {
    readonly model: Schema.Top
    readonly timestamps: any
    readonly versioned: any
    readonly timeSeries?: any
  },
> = EntityRecordType<E["model"], E["timestamps"], E["versioned"], E["timeSeries"]>

/**
 * Extract the input type from an Entity. Ref-aware: when refs are present,
 * ref fields are replaced with `${field}Id: string`.
 *
 * `Entity.Input<typeof Users>` = model fields only
 * `Entity.Input<typeof TeamPlayerSelection>` = `{ teamId: string, playerId: string, ... }`
 */
export type Input<E extends { readonly model: Schema.Top }> =
  E extends Entity<infer M extends Schema.Top, any, any, any, any, any, any, infer R, any>
    ? EntityRefInputType<M, R>
    : EntityInputType<E["model"]>

/**
 * Extract the create type from an Entity — input fields minus the identifier.
 * Uses the `identifier` config to determine which field to omit.
 * Falls back to omitting all primary key composites when no identifier is set.
 *
 * `Entity.Create<typeof Teams>` = `{ name: string, gender: Gender, league: League }`
 */
export type Create<E extends { readonly model: Schema.Top; readonly indexes: any }> =
  E extends Entity<infer M extends Schema.Top, any, infer I, any, any, any, any, infer R, infer Id>
    ? EntityRefCreateType<M, I, R, Id extends string ? Id : undefined>
    : Omit<EntityInputType<E["model"]>, PrimaryKeyComposites<E["indexes"]>>

/**
 * Extract the update type from an Entity. Ref-aware: when refs are present,
 * ref fields are replaced with optional `${field}Id?: string`.
 *
 * `Entity.Update<typeof Users>` = `{ email?: string, displayName?: string, ... }`
 */
export type Update<E extends { readonly model: Schema.Top; readonly indexes: any }> =
  E extends Entity<infer M extends Schema.Top, any, infer I, any, any, any, any, infer R, any>
    ? EntityRefUpdateType<M, I, R>
    : EntityUpdateType<E["model"], E["indexes"]>

/**
 * Extract the key type from an Entity.
 * Primary index pk + sk composite attributes only.
 *
 * `Entity.Key<typeof Users>` = `{ userId: string }`
 */
export type Key<E extends { readonly model: Schema.Top; readonly indexes: any }> = EntityKeyType<
  E["model"],
  E["indexes"]
>

/**
 * Extract the full DynamoDB item type (unmarshalled).
 * Record + key attributes + __edd_e__.
 *
 * `Entity.Item<typeof Users>` = record + pk, sk, gsi1pk, etc. + __edd_e__
 */
export type Item<E extends { readonly schemas: DerivedSchemas }> = Schema.Schema.Type<
  E["schemas"]["itemSchema"]
>

/**
 * Marshalled DynamoDB item (AttributeValue format).
 * For now, a simple record type. Full typing comes in Group 5.
 */
export type Marshalled<_E> = globalThis.Record<
  string,
  {
    readonly S?: string
    readonly N?: string
    readonly BOOL?: boolean
    readonly NULL?: boolean
    readonly L?: ReadonlyArray<unknown>
    readonly M?: globalThis.Record<string, unknown>
  }
>

// ---------------------------------------------------------------------------
// Utility accessors
// ---------------------------------------------------------------------------

/** Minimal structural type for utility accessors (avoids Entity invariance issues). */
interface EntityLike {
  readonly indexes: globalThis.Record<string, IndexDefinition>
}

/**
 * Get the names of all primary key composite attributes for an entity.
 */
export const keyAttributes = (entity: EntityLike): ReadonlyArray<string> =>
  primaryKeyComposites(entity.indexes)

/**
 * Get the names of all key field attributes (pk, sk, gsi1pk, etc.) for an entity.
 */
export const keyFieldNames = (entity: EntityLike): ReadonlyArray<string> =>
  allKeyFieldNames(entity.indexes)

/**
 * Get the names of all composite attributes across all indexes.
 */
export const compositeAttributes = (entity: EntityLike): ReadonlyArray<string> =>
  allCompositeAttributes(entity.indexes)

// ---------------------------------------------------------------------------
// Schema accessors for raw data
// ---------------------------------------------------------------------------

/** Minimal structural type for schema accessors. */
interface EntityWithSchemas {
  readonly schemas: DerivedSchemas
  readonly _attachPrototype?: (decoded: any) => any
  /** See `versionCorruption` (#133): an item with a token but no version. */
  readonly _versionCorruption?: (
    item: Readonly<globalThis.Record<string, unknown>>,
    operation: string,
  ) => ValidationError | undefined
}

/**
 * Get the item schema for an entity.
 *
 * Returns a Schema that decodes unmarshalled DynamoDB items (plain JS objects
 * from `Marshaller.fromAttributeMap`) into Entity.Record instances.
 *
 * Useful for consuming DynamoDB Streams or working with raw query results.
 */
export const itemSchema = (entity: EntityWithSchemas): Schema.Codec<any> =>
  entity.schemas.itemSchema

/**
 * Decode a marshalled DynamoDB item (AttributeValue format) into a typed Entity.Record.
 *
 * Unmarshalls the AttributeValue map to plain JS, then decodes via the entity's item schema.
 * Useful for consuming DynamoDB Streams events or raw SDK responses.
 *
 * Returns an Effect that fails with `ValidationError` if the item doesn't match the schema.
 */
export const decodeMarshalledItem = (
  entity: EntityWithSchemas & { readonly entityType: string },
  marshalledItem: globalThis.Record<string, AttributeValue>,
): Effect.Effect<unknown, ValidationError> => {
  const corruption = entity._versionCorruption?.(marshalledItem, "decodeMarshalledItem")
  if (corruption !== undefined) return Effect.fail(corruption)
  return Schema.decodeUnknownEffect(entity.schemas.itemSchema)(
    fromAttributeMap(marshalledItem),
  ).pipe(
    Effect.map((decoded) => (entity._attachPrototype ? entity._attachPrototype(decoded) : decoded)),
    Effect.mapError(
      (cause) =>
        new ValidationError({
          entityType: entity.entityType,
          operation: "decodeMarshalledItem",
          cause,
        }),
    ),
  )
}
