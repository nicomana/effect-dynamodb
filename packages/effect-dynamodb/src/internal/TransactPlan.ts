/**
 * TransactPlan — the items one entity write would issue, compiled but not sent.
 *
 * `update` and a multi-item `delete` (an entity with `unique`, `versioned: {
 * retain: true }` or `softDelete`) derive their write from the STORED row: the
 * sentinels to rotate or release, the snapshot, the tombstone, and the guard
 * that proves the row is still what was read. Rather than re-derive any of
 * that, `Transaction.transactWrite` runs the op itself against a recording
 * client ({@link planWrite}): reads go to DynamoDB as usual, and the op's
 * write is recorded instead of sent. The plan is therefore exactly the write
 * the standalone op would have issued — same items, same conditions — and the
 * two cannot drift, because there is only one code path.
 *
 * Lives in its own module so `Entity.ts` can produce it and
 * `TransactWriteOps.ts` can consume it without a runtime import cycle.
 */

import type {
  AttributeValue,
  DeleteItemCommandInput,
  PutItemCommandInput,
  TransactWriteItem,
  UpdateItemCommandInput,
} from "@aws-sdk/client-dynamodb"
import { DynamoError } from "@effect-dynamodb/schema/Errors.js"
import { Effect, Exit } from "effect"
import { DynamoClient, type DynamoClientService } from "../DynamoClient.js"
import type { PutVerdict, WriteCancellationReason } from "../Entity.js"

/**
 * A planned write: its items, and how to read a cancellation of them — the
 * same contract a guarded put's plan (`Entity._planPut`) has, so
 * `judgeCancellation` reads both alike. `verdict` returns `undefined` when
 * none of the plan's items failed.
 */
export interface TransactPlan {
  /** Empty when the op resolves to no write (an update with nothing to set). */
  readonly items: ReadonlyArray<TransactWriteItem>
  readonly verdict: (
    reasons: ReadonlyArray<WriteCancellationReason | undefined>,
  ) => PutVerdict | undefined
}

/** The op tried to write twice — it could not have been one atomic write. */
export class UnplannableWrite extends Error {
  constructor(readonly operation: string) {
    super(
      `${operation}: the op issued a second write after its first; a transaction can only ` +
        "carry an op whose write is one request. This is a defect in effect-dynamodb.",
    )
  }
}

/**
 * The condition fields every transact item takes. A single-item request also
 * carries response-shaping fields (`ReturnValues`, `ReturnConsumedCapacity`)
 * that a transact item does not, so only these are copied.
 */
const conditionOf = (input: {
  readonly ConditionExpression?: string | undefined
  readonly ExpressionAttributeNames?: Record<string, string> | undefined
  readonly ExpressionAttributeValues?: TransactWriteItem["Put"] extends infer P
    ? P extends { readonly ExpressionAttributeValues?: infer V }
      ? V
      : never
    : never
  readonly ReturnValuesOnConditionCheckFailure?: "ALL_OLD" | "NONE" | undefined
}) => ({
  ...(input.ConditionExpression !== undefined && {
    ConditionExpression: input.ConditionExpression,
  }),
  ...(input.ExpressionAttributeNames !== undefined && {
    ExpressionAttributeNames: input.ExpressionAttributeNames,
  }),
  ...(input.ExpressionAttributeValues !== undefined && {
    ExpressionAttributeValues: input.ExpressionAttributeValues,
  }),
  ...(input.ReturnValuesOnConditionCheckFailure !== undefined && {
    ReturnValuesOnConditionCheckFailure: input.ReturnValuesOnConditionCheckFailure,
  }),
})

const asPut = (input: PutItemCommandInput): TransactWriteItem => ({
  Put: { TableName: input.TableName, Item: input.Item, ...conditionOf(input) },
})
const asUpdate = (input: UpdateItemCommandInput): TransactWriteItem => ({
  Update: {
    TableName: input.TableName,
    Key: input.Key,
    UpdateExpression: input.UpdateExpression,
    ...conditionOf(input),
  },
})
const asDelete = (input: DeleteItemCommandInput): TransactWriteItem => ({
  Delete: { TableName: input.TableName, Key: input.Key, ...conditionOf(input) },
})

/**
 * Run `effect` (one entity op) with its writes recorded instead of sent, and
 * return what it would have written as transact items.
 *
 * Reads go to the real client, so every read the op makes — the row, owned
 * sentinels, refs — happens as it would standalone. The first write is
 * recorded and fails with a `DynamoError` so the op stops there; nothing after
 * the write (decode, cascade, return values) runs. The recording is kept on the
 * side rather than read back from the error, so it survives however the op
 * maps that failure. An op that finishes without writing contributes nothing.
 *
 * Any other failure — a validation error, `ItemNotFound` from the read, a
 * stale `expectedVersion` — is the op's own outcome and is returned as is.
 */
export const planWrite = <A, E, R>(
  operation: string,
  effect: Effect.Effect<A, E, R>,
  /**
   * The op's own primary key, marshalled. The op's read of that row is kept
   * as `ownRow`, so a plan can describe what it would reserve.
   */
  ownKey: Record<string, AttributeValue>,
): Effect.Effect<
  {
    readonly items: ReadonlyArray<TransactWriteItem>
    readonly ownRow: Record<string, AttributeValue> | undefined
  },
  E,
  R | DynamoClient
> =>
  Effect.gen(function* () {
    const real = yield* DynamoClient
    let recorded: ReadonlyArray<TransactWriteItem> | undefined
    let wroteTwice = false
    let ownRow: Record<string, AttributeValue> | undefined
    const isOwnKey = (key: Record<string, AttributeValue> | undefined) =>
      key !== undefined &&
      Object.keys(ownKey).length === Object.keys(key).length &&
      Object.entries(ownKey).every(([name, value]) => key[name]?.S === value.S)
    const record = (name: string, items: ReadonlyArray<TransactWriteItem>) => {
      if (recorded !== undefined) {
        wroteTwice = true
        return Effect.die(new UnplannableWrite(operation))
      }
      recorded = items
      return Effect.fail(
        new DynamoError({ operation: name, cause: "recorded for a transaction, not sent" }),
      )
    }
    const recording: DynamoClientService = {
      ...real,
      getItem: (input) =>
        real.getItem(input).pipe(
          Effect.tap((output) =>
            Effect.sync(() => {
              if (isOwnKey(input.Key) && output.Item !== undefined) ownRow = output.Item
            }),
          ),
        ),
      putItem: (input) => record("PutItem", [asPut(input)]),
      updateItem: (input) => record("UpdateItem", [asUpdate(input)]),
      deleteItem: (input) => record("DeleteItem", [asDelete(input)]),
      transactWriteItems: (input) => record("TransactWriteItems", input.TransactItems ?? []),
      batchWriteItem: () => Effect.die(new UnplannableWrite(operation)),
    }
    const exit = yield* Effect.exit(effect.pipe(Effect.provideService(DynamoClient, recording)))
    // Checked first: the op may have swallowed the defect, but a plan of only
    // its first write would silently drop the second.
    if (wroteTwice) return yield* Effect.die(new UnplannableWrite(operation))
    if (recorded !== undefined) return { items: recorded, ownRow }
    if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause)
    return { items: [], ownRow }
  })
