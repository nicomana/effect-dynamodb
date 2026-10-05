/**
 * @internal BoundCrud — Fluent builders for bound-entity CRUD operations.
 *
 * These builders wrap the unbound `EntityOp` / `EntityDelete` intermediates with
 * a pre-resolved `provide` function so `yield* builder` returns an Effect with
 * `R = never`. Each chainable call returns a new builder (immutable, mirroring
 * `BoundQuery`).
 *
 * Write builders implement `Pipeable.Pipeable` + `[Symbol.iterator]` (via
 * `Utils.SingleShotGen`) — NOT `Effect.Effect`. They are yieldable inside
 * `Effect.gen`; use `.asEffect()` for Effect combinator interop.
 *
 * {@link BoundGet} is the deliberate exception: `get` has always returned a real
 * `Effect`, so it is built on `Effectable.Prototype` and IS one. See the note on
 * that type.
 */

import type { ConditionalCheckFailed } from "@effect-dynamodb/schema/Errors.js"
import type { Effect } from "effect"
import { Effectable, Pipeable, Utils } from "effect"
import {
  add as addCombinator,
  append as appendCombinator,
  cascade as cascadeCombinator,
  clearMap as clearMapCombinator,
  condition as conditionCombinator,
  deleteFromSet as deleteFromSetCombinator,
  expectedVersion as expectedVersionCombinator,
  pathAdd as pathAddCombinator,
  pathAppend as pathAppendCombinator,
  pathDelete as pathDeleteCombinator,
  pathIfNotExists as pathIfNotExistsCombinator,
  pathPrepend as pathPrependCombinator,
  pathRemove as pathRemoveCombinator,
  pathSet as pathSetCombinator,
  pathSubtract as pathSubtractCombinator,
  remove as removeCombinator,
  removeEntries as removeEntriesCombinator,
  returnValues as returnValuesCombinator,
  set as setCombinator,
  subtract as subtractCombinator,
  withVector as withVectorCombinator,
} from "./EntityCombinators.js"
import type {
  CascadeTarget,
  EntityDelete,
  EntityGet,
  EntityPut,
  EntityUpdate,
  PathAddOp,
  PathAppendOp,
  PathDeleteOp,
  PathIfNotExistsOp,
  PathPrependOp,
  PathSetOp,
  PathSubtractOp,
  ReturnValuesMode,
  UpdateReturn,
} from "./EntityOps.js"
import type { ConditionOps, Expr } from "./Expr.js"
import { parseSimpleShorthand } from "./Expr.js"
import type { PathBuilder } from "./PathBuilder.js"

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

/**
 * @internal Marker carried by every bound-CRUD builder so the transactable
 * extraction protocol (`Entity.extractTransactable`) can unwrap a builder back
 * to the `EntityOp` / `EntityDelete` intermediate it wraps.
 *
 * Bound builders are the ONLY way to author an operation for an entity declared
 * with the pure `@effect-dynamodb/schema` `Entity.make` (a pure definition
 * carries no operations), so Batch/Transaction/EventStore must accept them —
 * see #100 (writes) and #108 (reads).
 *
 * There is exactly ONE unwrap protocol: carry this marker plus `_op`, and
 * `Entity.extractTransactable` unwraps you. {@link BoundGet} joins it on the
 * read side without a second mechanism.
 */
export const BoundOpTypeId: unique symbol = Symbol.for("effect-dynamodb/BoundOp")
export type BoundOpTypeId = typeof BoundOpTypeId

/** @internal Shape every bound-CRUD builder exposes for op extraction. */
export interface BoundOp {
  readonly [BoundOpTypeId]: BoundOpTypeId
  /** @internal The wrapped unbound intermediate. */
  readonly _op: unknown
}

/** @internal Narrow an unknown value to a bound-CRUD builder. */
export const isBoundOp = (op: object): op is BoundOp => BoundOpTypeId in op

/**
 * A bound-CRUD builder accepted by `Batch.write`,
 * `Transaction.transactWrite`, and `EventStore.append({ additionalItems })`.
 *
 * Type parameters are erased deliberately: `BoundPut<any, any, any, any>` is not
 * a supertype of a concrete `BoundPut` (the `withVector` name parameter is
 * contravariant and narrows to `never` for entities with no vector indexes), so
 * the write unions match on the marker plus the op kind instead.
 */
export interface BoundWriteOp {
  readonly [BoundOpTypeId]: BoundOpTypeId
  readonly _boundOpType: "put" | "delete"
}

/**
 * A bound `update` builder, accepted by `Transaction.transactWrite` only — see
 * `TransactWriteUpdateOp`. Erased for the same reason as {@link BoundWriteOp}.
 */
export interface BoundUpdateOp {
  readonly [BoundOpTypeId]: BoundOpTypeId
  readonly _boundOpType: "update"
}

/** Condition input accepted by `.condition()` — callback or shorthand record. */
export type ConditionArg<Model> =
  | ((t: PathBuilder<Model, Model, never>, ops: ConditionOps<Model>) => Expr)
  | globalThis.Record<string, unknown>

/** Execution wiring shared by all bound-CRUD builders. */
export interface BoundCrudConfig<Model> {
  readonly pathBuilder: PathBuilder<Model, Model, never>
  readonly conditionOps: ConditionOps<Model>
  readonly provide: <X, E>(eff: Effect.Effect<X, E, any>) => Effect.Effect<X, E, never>
}

/** Build a compiled condition Expr from a callback or simple shorthand record.
 * Simple `{ field: value }` shorthand is parsed via `parseSimpleShorthand`,
 * matching the behaviour of entity-scoped `Entity.condition(...)`.
 */
const buildCondition = <Model>(cfg: BoundCrudConfig<Model>, arg: ConditionArg<Model>): Expr => {
  if (typeof arg === "function") return arg(cfg.pathBuilder, cfg.conditionOps)
  return parseSimpleShorthand(arg as globalThis.Record<string, unknown>)
}

// ---------------------------------------------------------------------------
// BoundPut — put / create / upsert share this shape
// ---------------------------------------------------------------------------

/**
 * Fluent builder for put/create/upsert.
 *
 * ```ts
 * yield* db.entities.Users.put(input)
 * yield* db.entities.Users.put(input).condition({ status: "active" })
 * yield* db.entities.Users.create(input).condition((t, { eq }) => eq(t.status, "active"))
 * ```
 */
export interface BoundPut<Model, A, E, VN extends string = string> extends Pipeable.Pipeable {
  readonly [BoundOpTypeId]: BoundOpTypeId
  readonly _boundOpType: "put"
  /**
   * Add a condition expression. Callback or shorthand.
   *
   * Widens the error channel with {@link ConditionalCheckFailed} — DynamoDB
   * rejecting the condition is the failure this combinator makes reachable, so
   * `Effect.catchTag("ConditionalCheckFailed", ...)` type-checks downstream.
   */
  readonly condition: (
    cond: ConditionArg<Model>,
  ) => BoundPut<Model, A, E | ConditionalCheckFailed, VN>
  /**
   * Supply a pre-computed embedding for a named vector index, skipping the
   * `Embedder` for that index on this write. `name` is the LOGICAL vector index
   * name declared on `Entity.make({ vectorIndexes })`.
   */
  readonly withVector: (name: VN, vector: ReadonlyArray<number>) => BoundPut<Model, A, E, VN>
  /** Convert to an executable Effect for Effect combinator interop. */
  readonly asEffect: () => Effect.Effect<A, E, never>
  /** Yield support for `Effect.gen`. */
  readonly [Symbol.iterator]: () => Iterator<Effect.Effect<A, E, never>, A>
}

/** @internal */
export class BoundPutImpl<Model, A, E, VN extends string = string>
  implements BoundPut<Model, A, E, VN>
{
  readonly [BoundOpTypeId]: BoundOpTypeId = BoundOpTypeId as BoundOpTypeId
  readonly _boundOpType = "put" as const
  constructor(
    readonly _op: EntityPut<A, any, E, any>,
    readonly _config: BoundCrudConfig<Model>,
  ) {}

  condition(cond: ConditionArg<Model>): BoundPutImpl<Model, A, E | ConditionalCheckFailed, VN> {
    const compiled = buildCondition(this._config, cond)
    const next = conditionCombinator(this._op, compiled)
    return new BoundPutImpl<Model, A, E | ConditionalCheckFailed, VN>(next, this._config)
  }

  withVector(name: VN, vector: ReadonlyArray<number>): BoundPutImpl<Model, A, E, VN> {
    return new BoundPutImpl(withVectorCombinator(this._op, name, vector), this._config)
  }

  asEffect(): Effect.Effect<A, E, never> {
    return this._config.provide(
      (this._op as unknown as { _run: (m: string) => Effect.Effect<A, E, any> })._run("record"),
    )
  }

  [Symbol.iterator]() {
    return new Utils.SingleShotGen(this.asEffect()) as any
  }

  pipe() {
    return Pipeable.pipeArguments(this, arguments)
  }
}

// ---------------------------------------------------------------------------
// BoundDelete — delete / deleteIfExists share this shape
// ---------------------------------------------------------------------------

/**
 * Fluent builder for delete/deleteIfExists.
 *
 * ```ts
 * yield* db.entities.Tasks.delete({ taskId })
 * yield* db.entities.Tasks.delete({ taskId }).condition({ status: "archived" })
 * yield* db.entities.Tasks.delete({ taskId }).returnValues("allOld")
 * ```
 */
export interface BoundDelete<Model, E, A = void> extends Pipeable.Pipeable {
  readonly [BoundOpTypeId]: BoundOpTypeId
  readonly _boundOpType: "delete"
  /**
   * Add a condition expression. Callback or shorthand.
   *
   * Widens the error channel with {@link ConditionalCheckFailed} — DynamoDB
   * rejecting the condition is the failure this combinator makes reachable, so
   * `Effect.catchTag("ConditionalCheckFailed", ...)` type-checks downstream.
   */
  readonly condition: (
    cond: ConditionArg<Model>,
  ) => BoundDelete<Model, E | ConditionalCheckFailed, A>
  /**
   * Set ReturnValues mode: `"allOld"` returns the item the delete removed
   * (`undefined` when there was none); `"none"` returns nothing. DynamoDB has no
   * other mode for a delete: any other fails with a `ValidationError` before
   * anything is sent.
   */
  readonly returnValues: <M extends ReturnValuesMode>(
    mode: M,
  ) => BoundDelete<Model, E, M extends "allOld" ? Model | undefined : void>
  /** Convert to an executable Effect for Effect combinator interop. */
  readonly asEffect: () => Effect.Effect<A, E, never>
  /** Yield support for `Effect.gen`. */
  readonly [Symbol.iterator]: () => Iterator<Effect.Effect<A, E, never>, A>
}

/** @internal */
export class BoundDeleteImpl<Model, E, A = void> implements BoundDelete<Model, E, A> {
  readonly [BoundOpTypeId]: BoundOpTypeId = BoundOpTypeId as BoundOpTypeId
  readonly _boundOpType = "delete" as const
  constructor(
    readonly _op: EntityDelete<E, any, any, any>,
    readonly _config: BoundCrudConfig<Model>,
  ) {}

  condition(cond: ConditionArg<Model>): BoundDeleteImpl<Model, E | ConditionalCheckFailed, A> {
    const compiled = buildCondition(this._config, cond)
    const next = conditionCombinator(this._op, compiled)
    return new BoundDeleteImpl<Model, E | ConditionalCheckFailed, A>(next as any, this._config)
  }

  returnValues<M extends ReturnValuesMode>(
    mode: M,
  ): BoundDeleteImpl<Model, E, M extends "allOld" ? Model | undefined : void> {
    const next = returnValuesCombinator(this._op, mode)
    return new BoundDeleteImpl(next as any, this._config)
  }

  asEffect(): Effect.Effect<A, E, never> {
    return this._config.provide(this._op.asEffect()) as Effect.Effect<A, E, never>
  }

  [Symbol.iterator]() {
    return new Utils.SingleShotGen(this.asEffect()) as any
  }

  pipe() {
    return Pipeable.pipeArguments(this, arguments)
  }
}

// ---------------------------------------------------------------------------
// BoundUpdate — update / patch share this shape
// ---------------------------------------------------------------------------

/**
 * Fluent builder for update/patch.
 *
 * ```ts
 * yield* db.entities.Tasks.update(key).set({ status: "done" })
 * yield* db.entities.Tasks.update(key).set({ status: "done" }).expectedVersion(3)
 * yield* db.entities.Products.update(key)
 *   .set({ price: 24.99 })
 *   .add({ viewCount: 1 })
 *   .subtract({ stock: 3 })
 *   .append({ tags: ["clearance"] })
 *   .remove(["temporaryFlag"])
 * ```
 */
export interface BoundUpdate<Model, A, U, E, VN extends string = string> extends Pipeable.Pipeable {
  readonly [BoundOpTypeId]: BoundOpTypeId
  readonly _boundOpType: "update"
  /** Set the fields to update (record-based SET). */
  readonly set: (updates: U) => BoundUpdate<Model, A, U, E, VN>
  /** Remove attributes (REMOVE clause). */
  readonly remove: (fields: ReadonlyArray<string>) => BoundUpdate<Model, A, U, E, VN>
  /** Atomic numeric ADD. */
  readonly add: (values: globalThis.Record<string, number>) => BoundUpdate<Model, A, U, E, VN>
  /** Numeric SET subtraction (synthesized as `SET #f = #f - :v`). */
  readonly subtract: (values: globalThis.Record<string, number>) => BoundUpdate<Model, A, U, E, VN>
  /** List append (synthesized as `SET #f = list_append(#f, :v)`). */
  readonly append: (
    values: globalThis.Record<string, ReadonlyArray<unknown>>,
  ) => BoundUpdate<Model, A, U, E, VN>
  /** Delete elements from a set attribute. */
  readonly deleteFromSet: (
    values: globalThis.Record<string, unknown>,
  ) => BoundUpdate<Model, A, U, E, VN>
  /** Optimistic concurrency — expected version. */
  readonly expectedVersion: (version: number) => BoundUpdate<Model, A, U, E, VN>
  /**
   * Add a condition expression. Callback or shorthand.
   *
   * Widens the error channel with {@link ConditionalCheckFailed} — DynamoDB
   * rejecting the condition is the failure this combinator makes reachable, so
   * `Effect.catchTag("ConditionalCheckFailed", ...)` type-checks downstream.
   *
   * Note: when `.expectedVersion(...)` is also set (or the update is a
   * version-checked read-then-write), the stored item DynamoDB returns on the
   * rejection tells the two apart: a version race is an `OptimisticLockError`,
   * a failed condition a `ConditionalCheckFailed`.
   */
  readonly condition: (
    cond: ConditionArg<Model>,
  ) => BoundUpdate<Model, A, U, E | ConditionalCheckFailed, VN>
  /**
   * Supply a pre-computed embedding for a named vector index, skipping the
   * `Embedder` for that index on this write. `name` is the LOGICAL vector index
   * name declared on `Entity.make({ vectorIndexes })`.
   */
  readonly withVector: (name: VN, vector: ReadonlyArray<number>) => BoundUpdate<Model, A, U, E, VN>
  /**
   * What the update returns. `"allNew"` (the default) / `"allOld"`: the whole
   * item after / before it. `"updatedNew"` / `"updatedOld"`: only the
   * top-level attributes the update wrote, after / before it, as a partial
   * model. `"none"`: `undefined`. Exact on every update path.
   */
  readonly returnValues: <M extends ReturnValuesMode>(
    mode: M,
  ) => BoundUpdate<Model, UpdateReturn<Model, M>, U, E, VN>
  /** Configure cascade updates to denormalized target entities. */
  readonly cascade: (config: {
    readonly targets: ReadonlyArray<CascadeTarget>
    readonly filter?: globalThis.Record<string, unknown> | undefined
    readonly mode?: "eventual" | "transactional" | undefined
  }) => BoundUpdate<Model, A, U, E, VN>
  /** Path-based SET. */
  readonly pathSet: (op: PathSetOp) => BoundUpdate<Model, A, U, E, VN>
  /** Path-based REMOVE. */
  readonly pathRemove: (segments: ReadonlyArray<string | number>) => BoundUpdate<Model, A, U, E, VN>
  /** Path-based ADD. */
  readonly pathAdd: (op: PathAddOp) => BoundUpdate<Model, A, U, E, VN>
  /** Path-based SUBTRACT. */
  readonly pathSubtract: (op: PathSubtractOp) => BoundUpdate<Model, A, U, E, VN>
  /** Path-based APPEND. */
  readonly pathAppend: (op: PathAppendOp) => BoundUpdate<Model, A, U, E, VN>
  /** Path-based PREPEND. */
  readonly pathPrepend: (op: PathPrependOp) => BoundUpdate<Model, A, U, E, VN>
  /** Path-based if_not_exists. */
  readonly pathIfNotExists: (op: PathIfNotExistsOp) => BoundUpdate<Model, A, U, E, VN>
  /** Path-based DELETE (set removal). */
  readonly pathDelete: (op: PathDeleteOp) => BoundUpdate<Model, A, U, E, VN>
  /**
   * Remove specific entries from a sparse-map field. Compiles to
   * `REMOVE <prefix>#k1, <prefix>#k2, ...`. Removing an entry that doesn't
   * exist is a no-op (DynamoDB REMOVE semantics).
   */
  readonly removeEntries: (
    field: string,
    keys: ReadonlyArray<string>,
  ) => BoundUpdate<Model, A, U, E, VN>
  /**
   * Clear all entries of a sparse-map field. Two-op helper: reads the current
   * item, then folds REMOVE clauses for the discovered `<prefix>#*` attrs into
   * the same final UpdateItem. Atomic via the version CAS for `versioned`
   * entities; best-effort for non-versioned.
   */
  readonly clearMap: (field: string) => BoundUpdate<Model, A, U, E, VN>
  /** Convert to an executable Effect for Effect combinator interop. */
  readonly asEffect: () => Effect.Effect<A, E, never>
  /** Yield support for `Effect.gen`. */
  readonly [Symbol.iterator]: () => Iterator<Effect.Effect<A, E, never>, A>
}

/** @internal */
export class BoundUpdateImpl<Model, A, U, E, VN extends string = string>
  implements BoundUpdate<Model, A, U, E, VN>
{
  readonly [BoundOpTypeId]: BoundOpTypeId = BoundOpTypeId as BoundOpTypeId
  readonly _boundOpType = "update" as const
  constructor(
    readonly _op: EntityUpdate<A, any, U, E, any>,
    readonly _config: BoundCrudConfig<Model>,
  ) {}

  private _with(next: EntityUpdate<A, any, U, E, any>): BoundUpdateImpl<Model, A, U, E, VN> {
    return new BoundUpdateImpl(next, this._config)
  }

  set(updates: U): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(setCombinator(this._op, updates))
  }

  remove(fields: ReadonlyArray<string>): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(removeCombinator(this._op, fields))
  }

  add(values: globalThis.Record<string, number>): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(addCombinator(this._op, values))
  }

  subtract(values: globalThis.Record<string, number>): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(subtractCombinator(this._op, values))
  }

  append(
    values: globalThis.Record<string, ReadonlyArray<unknown>>,
  ): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(appendCombinator(this._op, values))
  }

  deleteFromSet(values: globalThis.Record<string, unknown>): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(deleteFromSetCombinator(this._op, values))
  }

  expectedVersion(version: number): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(expectedVersionCombinator(this._op, version))
  }

  condition(
    cond: ConditionArg<Model>,
  ): BoundUpdateImpl<Model, A, U, E | ConditionalCheckFailed, VN> {
    const compiled = buildCondition(this._config, cond)
    return new BoundUpdateImpl<Model, A, U, E | ConditionalCheckFailed, VN>(
      conditionCombinator(this._op, compiled),
      this._config,
    )
  }

  withVector(name: VN, vector: ReadonlyArray<number>): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(withVectorCombinator(this._op, name, vector))
  }

  returnValues<M extends ReturnValuesMode>(
    mode: M,
  ): BoundUpdateImpl<Model, UpdateReturn<Model, M>, U, E, VN> {
    return new BoundUpdateImpl(
      returnValuesCombinator(this._op, mode) as unknown as EntityUpdate<
        UpdateReturn<Model, M>,
        any,
        U,
        E,
        any
      >,
      this._config,
    )
  }

  cascade(config: {
    readonly targets: ReadonlyArray<CascadeTarget>
    readonly filter?: globalThis.Record<string, unknown> | undefined
    readonly mode?: "eventual" | "transactional" | undefined
  }): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(cascadeCombinator(this._op, config) as EntityUpdate<A, any, U, E, any>)
  }

  pathSet(op: PathSetOp): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(pathSetCombinator(this._op, op))
  }

  pathRemove(segments: ReadonlyArray<string | number>): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(pathRemoveCombinator(this._op, segments))
  }

  pathAdd(op: PathAddOp): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(pathAddCombinator(this._op, op))
  }

  pathSubtract(op: PathSubtractOp): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(pathSubtractCombinator(this._op, op))
  }

  pathAppend(op: PathAppendOp): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(pathAppendCombinator(this._op, op))
  }

  pathPrepend(op: PathPrependOp): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(pathPrependCombinator(this._op, op))
  }

  pathIfNotExists(op: PathIfNotExistsOp): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(pathIfNotExistsCombinator(this._op, op))
  }

  pathDelete(op: PathDeleteOp): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(pathDeleteCombinator(this._op, op))
  }

  removeEntries(field: string, keys: ReadonlyArray<string>): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(removeEntriesCombinator(this._op, field, keys))
  }

  clearMap(field: string): BoundUpdateImpl<Model, A, U, E, VN> {
    return this._with(clearMapCombinator(this._op, field))
  }

  asEffect(): Effect.Effect<A, E, never> {
    return this._config.provide(
      (this._op as unknown as { _run: (m: string) => Effect.Effect<A, E, any> })._run("record"),
    )
  }

  [Symbol.iterator]() {
    return new Utils.SingleShotGen(this.asEffect()) as any
  }

  pipe() {
    return Pipeable.pipeArguments(this, arguments)
  }
}

// ---------------------------------------------------------------------------
// BoundGet — the read descriptor, which is ALSO an Effect
// ---------------------------------------------------------------------------

/**
 * Effect prototype backing {@link BoundGet}.
 *
 * The write builders being yieldable-but-not-Effect was a free choice:
 * `db.entities.X.put(...)` never returned an `Effect` in the first place. `get`
 * did. Callers write `db.entities.X.get(k).pipe(Effect.catchTag("ItemNotFound",
 * …))` and hand the result to `Effect.map` / `Effect.all`, so a wrapper that was
 * merely yieldable would be a silent, wide breaking change (#108).
 *
 * `Effectable.Prototype` is the supported way to make a descriptor evaluate as
 * an Effect — the same mechanism `effect`'s own `Statement` (a query builder
 * that is also an Effect) is built on. The result is not "Effect-like": it
 * carries the Effect type id and prototype, so every combinator, `yield*` and
 * `pipe` behaves exactly as it did before the wrapper existed.
 *
 * A `function` DECLARATION, deliberately: `BoundGetImpl` extends this, so it
 * must have a [[Construct]] slot. An arrow function does not — rewriting it to
 * one (as a formatter will happily do) fails at import time with "Class extends
 * value is not a constructor".
 */
function BoundGetCtor(this: unknown) {}
BoundGetCtor.prototype = Effectable.Prototype<Effect.Effect<unknown, unknown, never>>({
  label: "BoundGet",
  evaluate(this: Effect.Effect<unknown, unknown, never>) {
    return (
      this as unknown as { asEffect: () => Effect.Effect<unknown, unknown, never> }
    ).asEffect()
  },
})

const BoundGetBase = BoundGetCtor as unknown as new <A, E>() => Effect.Effect<A, E, never>

/**
 * The value returned by `db.entities.X.get(key)`.
 *
 * It IS an `Effect<A, E, never>` — yield it, pipe it, hand it to any Effect
 * combinator, exactly as before. It additionally carries the bound-op marker, so
 * `Batch.get`, `Transaction.transactGet` and `Transaction.check` can unwrap it
 * back to the `EntityGet` descriptor they need, through the SAME protocol that
 * unwraps the write builders (`Entity.extractTransactable`).
 *
 * That is what lets an entity authored with the pure, AWS-free
 * `@effect-dynamodb/schema` `Entity.make` take part in batch reads,
 * transactional reads and cross-entity condition checks: a pure definition
 * carries no operations, so the bound client is the only surface its author ever
 * holds (#108).
 *
 * ```ts
 * const user = yield* db.entities.Users.get({ userId })          // Effect
 * yield* db.entities.Users.get({ userId }).pipe(                 // Effect
 *   Effect.catchTag("ItemNotFound", () => Effect.succeed(null)),
 * )
 * yield* Batch.get([db.entities.Users.get({ userId })])          // descriptor
 * yield* Transaction.transactWrite([
 *   Transaction.check(db.entities.Users.get({ userId }), cond),  // descriptor
 * ])
 * ```
 */
export interface BoundGet<A, E> extends Effect.Effect<A, E, never> {
  readonly [BoundOpTypeId]: BoundOpTypeId
  readonly _boundOpType: "get"
  /**
   * The Effect this value already is. Present so `.asEffect()` reads the same
   * on every bound op.
   */
  readonly asEffect: () => Effect.Effect<A, E, never>
}

/** @internal */
export class BoundGetImpl<A, E> extends BoundGetBase<A, E> implements BoundGet<A, E> {
  readonly [BoundOpTypeId]: BoundOpTypeId = BoundOpTypeId as BoundOpTypeId
  readonly _boundOpType = "get" as const
  constructor(
    readonly _op: EntityGet<A, any, E, any>,
    readonly _config: BoundCrudConfig<any>,
  ) {
    super()
  }

  asEffect(): Effect.Effect<A, E, never> {
    // `"record"` — the decode mode bound `get` has always run through.
    return this._config.provide(
      (this._op as unknown as { _run: (m: string) => Effect.Effect<A, E, any> })._run("record"),
    )
  }
}

/**
 * Union of read descriptors accepted by `Batch.get`, `Transaction.transactGet`
 * and `Transaction.check`: the unbound `EntityGet` intermediate, or the
 * {@link BoundGet} returned by `db.entities.X.get(...)`.
 */
export type AnyGet = EntityGet<any, any, any, any> | BoundGet<any, any>

/** Success type carried by an {@link AnyGet}, whichever half it is. */
export type GetSuccess<T> =
  T extends EntityGet<infer A, any, any, any> ? A : T extends BoundGet<infer A, any> ? A : never

// ---------------------------------------------------------------------------
// Factory helpers
// ---------------------------------------------------------------------------

/** @internal */
export const makeBoundGet = <A, E>(
  op: EntityGet<A, any, E, any>,
  config: BoundCrudConfig<any>,
): BoundGet<A, E> => new BoundGetImpl<A, E>(op, config)

/** @internal */
export const makeBoundPut = <Model, A, E, VN extends string = string>(
  op: EntityPut<A, any, E, any>,
  config: BoundCrudConfig<Model>,
): BoundPut<Model, A, E, VN> => new BoundPutImpl<Model, A, E, VN>(op, config)

/** @internal */
export const makeBoundDelete = <Model, E>(
  op: EntityDelete<E, any, any, any>,
  config: BoundCrudConfig<Model>,
): BoundDelete<Model, E> => new BoundDeleteImpl(op, config)

/** @internal */
export const makeBoundUpdate = <Model, A, U, E, VN extends string = string>(
  op: EntityUpdate<A, any, U, E, any>,
  config: BoundCrudConfig<Model>,
): BoundUpdate<Model, A, U, E, VN> => new BoundUpdateImpl<Model, A, U, E, VN>(op, config)

// ---------------------------------------------------------------------------
// BoundAppend — time-series append() builder
// ---------------------------------------------------------------------------

/**
 * Fluent builder for `Entity.append()` on time-series entities.
 *
 * `.append(input)` returns a `BoundAppend` carrying `{ current }` on success
 * and failing with `StaleAppend` (CAS) or `ConditionalCheckFailed`
 * (user-condition rejected, default path only) on the error channel.
 *
 * Combinators:
 * - `.condition(...)` — AND a user condition onto the CAS predicate.
 * - `.remove(attrs)` — emit REMOVE clauses for `appendInput` attributes the
 *   caller wants cleared atomically. The same UpdateItem carries SET +
 *   REMOVE + CAS; any GSI half whose composite list intersects `attrs` drops
 *   via the v1.7.1 cascade override.
 * - `.skipFollowUp()` — skip the post-transaction GetItem; success is `void`,
 *   user-condition failures collapse into `StaleAppend` (cannot disambiguate).
 *
 * ```ts
 * yield* db.entities.Telemetry.append(input)                        // → { current }
 * yield* db.entities.Telemetry.append(input).condition(c)           // → { current }
 * yield* db.entities.Telemetry.append(input).remove(["alertState"]) // → { current }
 * yield* db.entities.Telemetry.append(input).skipFollowUp()         // → void
 * yield* db.entities.Telemetry.append(input).condition(c).skipFollowUp() // → void
 * ```
 */
export interface BoundAppend<Model, A, E, ESkip> extends Pipeable.Pipeable {
  /** Add a condition expression. Callback or shorthand. */
  readonly condition: (cond: ConditionArg<Model>) => BoundAppend<Model, A, E, ESkip>
  /**
   * Emit REMOVE clauses for `appendInput` attributes the caller wants cleared
   * on the current item. Cleared attributes also cascade-drop any GSI half
   * whose composite list intersects `attrs` (v1.7.1 cascade override). The
   * REMOVE clauses ride the same UpdateItem as the scoped SET + CAS — no
   * separate write, no race window.
   *
   * Validated at execution time:
   * - Names must appear in `appendInput` (enrichment fields outside
   *   `appendInput` cannot be cleared via `.append()` — use `.update()`).
   * - Names must not name `orderBy`, a primary-key composite, or a
   *   ref-derived `${name}Id` field.
   * - Names must not overlap the encoded payload (DynamoDB rejects an
   *   UpdateExpression that touches the same attribute in both SET and
   *   REMOVE).
   *
   * Chained `.remove()` calls accumulate.
   */
  readonly remove: (attrs: ReadonlyArray<string>) => BoundAppend<Model, A, E, ESkip>
  /** Skip the follow-up GetItem; success becomes `void`. */
  readonly skipFollowUp: () => BoundAppend<Model, void, ESkip, ESkip>
  /** Convert to an executable Effect for Effect combinator interop. */
  readonly asEffect: () => Effect.Effect<A, E, never>
  /** Yield support for `Effect.gen`. */
  readonly [Symbol.iterator]: () => Iterator<Effect.Effect<A, E, never>, A>
}

/** @internal Execution wiring for the time-series append() path. */
export interface BoundAppendConfig<Model> extends BoundCrudConfig<Model> {
  /** Run the entity's append() Effect with assembled options. */
  readonly run: (opts: {
    readonly input: unknown
    readonly condition: Expr | undefined
    readonly skipFollowUp: boolean
    readonly removeAttrs: ReadonlyArray<string> | undefined
  }) => Effect.Effect<unknown, unknown, never>
}

/** @internal */
export class BoundAppendImpl<Model, A, E, ESkip> implements BoundAppend<Model, A, E, ESkip> {
  constructor(
    readonly _input: unknown,
    readonly _config: BoundAppendConfig<Model>,
    readonly _condition: Expr | undefined = undefined,
    readonly _skip: boolean = false,
    readonly _removeAttrs: ReadonlyArray<string> | undefined = undefined,
  ) {}

  condition(cond: ConditionArg<Model>): BoundAppendImpl<Model, A, E, ESkip> {
    const compiled = buildCondition(this._config, cond)
    return new BoundAppendImpl(this._input, this._config, compiled, this._skip, this._removeAttrs)
  }

  remove(attrs: ReadonlyArray<string>): BoundAppendImpl<Model, A, E, ESkip> {
    // Chained `.remove()` calls accumulate.
    const merged = this._removeAttrs === undefined ? attrs : [...this._removeAttrs, ...attrs]
    return new BoundAppendImpl(this._input, this._config, this._condition, this._skip, merged)
  }

  skipFollowUp(): BoundAppendImpl<Model, void, ESkip, ESkip> {
    return new BoundAppendImpl(this._input, this._config, this._condition, true, this._removeAttrs)
  }

  asEffect(): Effect.Effect<A, E, never> {
    return this._config.run({
      input: this._input,
      condition: this._condition,
      skipFollowUp: this._skip,
      removeAttrs: this._removeAttrs,
    }) as Effect.Effect<A, E, never>
  }

  [Symbol.iterator]() {
    return new Utils.SingleShotGen(this.asEffect()) as any
  }

  pipe() {
    return Pipeable.pipeArguments(this, arguments)
  }
}

/** @internal */
export const makeBoundAppend = <Model, A, E, ESkip>(
  input: unknown,
  config: BoundAppendConfig<Model>,
): BoundAppend<Model, A, E, ESkip> => new BoundAppendImpl<Model, A, E, ESkip>(input, config)
