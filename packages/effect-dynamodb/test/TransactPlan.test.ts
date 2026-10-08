import { it } from "@effect/vitest"
import { ItemNotFound } from "@effect-dynamodb/schema/Errors.js"
import { Cause, Effect, Exit } from "effect"
import { describe, expect, vi } from "vitest"
import { DynamoClient } from "../src/DynamoClient.js"
import { planWrite, UnplannableWrite } from "../src/internal/TransactPlan.js"
import { toAttributeMap } from "../src/Marshaller.js"
import { mockDynamoClientLayer } from "./helpers/MockDynamoClient.js"

const ownKey = toAttributeMap({ pk: "row", sk: "row" })
const ownRow = toAttributeMap({ pk: "row", sk: "row", label: "L" })

const sends = vi.fn()
const TestClient = mockDynamoClientLayer({
  getItem: (input) =>
    Effect.succeed(
      input.Key?.pk?.S === "row" ? { Item: ownRow, $metadata: {} } : { $metadata: {} },
    ),
  putItem: () => Effect.sync(() => sends("putItem")) as any,
  updateItem: () => Effect.sync(() => sends("updateItem")) as any,
  transactWriteItems: () => Effect.sync(() => sends("transactWriteItems")) as any,
})

describe("planWrite", () => {
  it.effect("records the op's write instead of sending it, without response fields", () =>
    Effect.gen(function* () {
      const plan = yield* planWrite(
        "test",
        Effect.gen(function* () {
          const client = yield* DynamoClient
          yield* client.getItem({ TableName: "t", Key: ownKey, ConsistentRead: true })
          yield* client.updateItem({
            TableName: "t",
            Key: ownKey,
            UpdateExpression: "SET #a = :a",
            ConditionExpression: "attribute_exists(#pk)",
            ExpressionAttributeNames: { "#a": "label", "#pk": "pk" },
            ExpressionAttributeValues: { ":a": { S: "L2" } },
            ReturnValues: "ALL_OLD",
            ReturnValuesOnConditionCheckFailure: "ALL_OLD",
          })
          return "never reached"
        }),
        ownKey,
      )

      expect(sends).not.toHaveBeenCalled()
      expect(plan.items).toHaveLength(1)
      const update = plan.items[0]!.Update!
      expect(update.UpdateExpression).toBe("SET #a = :a")
      expect(update.ConditionExpression).toBe("attribute_exists(#pk)")
      expect(update).not.toHaveProperty("ReturnValues")
      expect(update.ReturnValuesOnConditionCheckFailure).toBe("ALL_OLD")
      // The op's own read of its row is kept, so a plan can describe it.
      expect(plan.ownRow).toEqual(ownRow)
    }).pipe(Effect.provide(TestClient)),
  )

  it.effect("records a transaction's items as they are", () =>
    Effect.gen(function* () {
      const items = [
        { Put: { TableName: "t", Item: ownRow } },
        { Delete: { TableName: "t", Key: toAttributeMap({ pk: "s", sk: "s" }) } },
      ]
      const plan = yield* planWrite(
        "test",
        Effect.gen(function* () {
          const client = yield* DynamoClient
          yield* client.transactWriteItems({ TransactItems: items })
        }),
        ownKey,
      )
      expect(plan.items).toEqual(items)
    }).pipe(Effect.provide(TestClient)),
  )

  it.effect("an op that writes nothing plans nothing", () =>
    Effect.gen(function* () {
      const plan = yield* planWrite("test", Effect.succeed("no-op"), ownKey)
      expect(plan.items).toEqual([])
    }).pipe(Effect.provide(TestClient)),
  )

  it.effect("the op's own failure before writing is returned as is", () =>
    Effect.gen(function* () {
      const error = yield* planWrite(
        "test",
        Effect.fail(new ItemNotFound({ entityType: "Row", key: {} })),
        ownKey,
      ).pipe(Effect.flip)
      expect(error._tag).toBe("ItemNotFound")
    }).pipe(Effect.provide(TestClient)),
  )

  it.effect("with rowMissing, a guarded write is answered as a missing row, not recorded", () =>
    Effect.gen(function* () {
      const plan = yield* planWrite(
        "test",
        Effect.gen(function* () {
          const client = yield* DynamoClient
          // The op's guarded write — answered as DynamoDB would on a missing row…
          const first = yield* Effect.flip(
            client.updateItem({
              TableName: "t",
              Key: ownKey,
              UpdateExpression: "SET #a = :a",
              ConditionExpression: "attribute_exists(#pk)",
              ExpressionAttributeNames: { "#a": "label", "#pk": "pk" },
              ExpressionAttributeValues: { ":a": { S: "L2" } },
            }),
          )
          expect((first.cause as { name: string }).name).toBe("ConditionalCheckFailedException")
          // …so it falls back, and that write is what gets recorded.
          yield* client.putItem({
            TableName: "t",
            Item: ownRow,
            ConditionExpression: "attribute_not_exists(#pk)",
            ExpressionAttributeNames: { "#pk": "pk" },
          })
        }),
        ownKey,
        { rowMissing: true },
      )
      expect(plan.items).toHaveLength(1)
      expect(plan.items[0]!.Put).toBeDefined()
      expect(plan.readOwnRow).toBe(false)
    }).pipe(Effect.provide(TestClient)),
  )

  it.effect("a second write is a defect — the op could not have been one atomic write", () =>
    Effect.gen(function* () {
      const exit = yield* planWrite(
        "test",
        Effect.gen(function* () {
          const client = yield* DynamoClient
          yield* Effect.ignore(client.putItem({ TableName: "t", Item: ownRow }))
          yield* client.putItem({ TableName: "t", Item: ownRow })
        }),
        ownKey,
      ).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      const defect = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined
      expect(defect).toBeInstanceOf(UnplannableWrite)
      expect(sends).not.toHaveBeenCalled()
    }).pipe(Effect.provide(TestClient)),
  )
})
