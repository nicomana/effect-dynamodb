import { it } from "@effect/vitest"
import * as DynamoModel from "@effect-dynamodb/schema/DynamoModel.js"
import * as DynamoSchema from "@effect-dynamodb/schema/DynamoSchema.js"
import {
  DynamoError,
  type OptimisticLockError,
  type TransactionCancelled,
  type UniqueConstraintViolation,
  type ValidationError,
} from "@effect-dynamodb/schema/Errors.js"
import { Effect, Layer, Schema } from "effect"
import { beforeEach, describe, expect, vi } from "vitest"
import { DynamoClient } from "../src/DynamoClient.js"
import * as Entity from "../src/Entity.js"
import * as Expression from "../src/Expression.js"
import { itemBytes, transactItemBytes } from "../src/internal/ItemSize.js"
import { refuseOversizedTransaction, transactItemTarget } from "../src/internal/TransactWriteOps.js"
import { fromAttributeMap, toAttributeMap } from "../src/Marshaller.js"
import * as Table from "../src/Table.js"
import * as Transaction from "../src/Transaction.js"
import { mockDynamoClientLayer } from "./helpers/MockDynamoClient.js"

// --- Test Models ---

const AppSchema = DynamoSchema.make({ name: "myapp", version: 1 })

class User extends Schema.Class<User>("User")({
  userId: Schema.String,
  email: Schema.String,
  name: Schema.NonEmptyString,
  role: Schema.Literals(["admin", "member"]),
}) {}

class Order extends Schema.Class<Order>("Order")({
  orderId: Schema.String,
  userId: Schema.String,
  product: Schema.NonEmptyString,
  quantity: Schema.Number,
  status: Schema.Literals(["pending", "shipped", "delivered"]),
}) {}

const UserEntity = Entity.make({
  model: User,
  entityType: "User",
  primaryKey: {
    pk: { field: "pk", composite: ["userId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byRole: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["role"] },
      sk: { field: "gsi1sk", composite: ["userId"] },
    },
  },
})

const OrderEntity = Entity.make({
  model: Order,
  entityType: "Order",
  primaryKey: {
    pk: { field: "pk", composite: ["orderId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byUser: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["userId"] },
      sk: { field: "gsi1sk", composite: ["orderId"] },
    },
  },
})

// Fixtures for the #100 capability gate — entities whose write contract the
// single-item transact compile path cannot reproduce.
class RefProject extends Schema.Class<RefProject>("RefProject")({
  projectId: Schema.String,
  projectName: Schema.String,
}) {}

const RefProjects = Entity.make({
  model: DynamoModel.configure(RefProject, { projectId: { identifier: true } }),
  entityType: "RefProject",
  primaryKey: { pk: { field: "pk", composite: ["projectId"] }, sk: { field: "sk", composite: [] } },
})

class RefTask extends Schema.Class<RefTask>("RefTask")({
  taskId: Schema.String,
  title: Schema.String,
  project: DynamoModel.ref(RefProject),
}) {}

const RefTasks = Entity.make({
  model: RefTask,
  entityType: "RefTask",
  primaryKey: { pk: { field: "pk", composite: ["taskId"] }, sk: { field: "sk", composite: [] } },
  refs: { project: { entity: RefProjects } },
})

class GenDoc extends Schema.Class<GenDoc>("GenDoc")({
  docId: Schema.String,
  title: Schema.String,
}) {}

const GenDocs = Entity.make({
  model: GenDoc,
  entityType: "GenDoc",
  primaryKey: { pk: { field: "pk", composite: ["docId"] }, sk: { field: "sk", composite: [] } },
  generatedId: { field: "docId" },
})

// #113 fixtures — entities whose write contract needs more than one item.
class LifecycleMember extends Schema.Class<LifecycleMember>("LifecycleMember")({
  memberId: Schema.String,
  email: Schema.String,
  label: Schema.String,
}) {}

const LifecycleMembers = Entity.make({
  model: LifecycleMember,
  entityType: "LifecycleMember",
  primaryKey: { pk: { field: "pk", composite: ["memberId"] }, sk: { field: "sk", composite: [] } },
  unique: { email: ["email"] },
  versioned: { retain: true },
})

class SparseMember extends Schema.Class<SparseMember>("SparseMember")({
  memberId: Schema.String,
  email: Schema.optional(Schema.String),
  label: Schema.String,
}) {}

const SparseMembers = Entity.make({
  model: SparseMember,
  entityType: "SparseMember",
  primaryKey: { pk: { field: "pk", composite: ["memberId"] }, sk: { field: "sk", composite: [] } },
  unique: { email: ["email"] },
})

/**
 * #127 — a unique constraint names DOMAIN fields, but the item this path hands
 * to `Entity._buildPutSideItems` has already been renamed to stored attribute
 * names. Reading the constraint field off it yields `undefined`, and the sparse
 * rule then emits no sentinel at all: the constraint silently stops being
 * enforced for anything written through `transactWrite`.
 */
class RenamedMember extends Schema.Class<RenamedMember>("RenamedMember")({
  memberId: Schema.String,
  email: Schema.String,
}) {}

const RenamedMembers = Entity.make({
  model: DynamoModel.configure(RenamedMember, { email: { field: "memberEmail" } }),
  entityType: "RenamedMember",
  primaryKey: { pk: { field: "pk", composite: ["memberId"] }, sk: { field: "sk", composite: [] } },
  unique: { email: ["email"] },
})

class SoftNote extends Schema.Class<SoftNote>("SoftNote")({
  noteId: Schema.String,
  body: Schema.String,
}) {}

const SoftNotes = Entity.make({
  model: SoftNote,
  entityType: "SoftNote",
  primaryKey: { pk: { field: "pk", composite: ["noteId"] }, sk: { field: "sk", composite: [] } },
  softDelete: true,
})

class VersionedNote extends Schema.Class<VersionedNote>("VersionedNote")({
  noteId: Schema.String,
  body: Schema.String,
}) {}

/** Versioned without retain: a plain update writes without reading first. */
const VersionedNotes = Entity.make({
  model: VersionedNote,
  entityType: "VersionedNote",
  primaryKey: { pk: { field: "pk", composite: ["noteId"] }, sk: { field: "sk", composite: [] } },
  versioned: true,
})

class Ticket extends Schema.Class<Ticket>("Ticket")({
  ticketId: Schema.String,
  seq: Schema.Number,
}) {}

/** A numeric unique field, changed by `.add()`. */
const Tickets = Entity.make({
  model: Ticket,
  entityType: "Ticket",
  primaryKey: { pk: { field: "pk", composite: ["ticketId"] }, sk: { field: "sk", composite: [] } },
  unique: { seq: ["seq"] },
})

class VectorDoc extends Schema.Class<VectorDoc>("VectorDoc")({
  docId: Schema.String,
  body: Schema.String,
}) {}

const VectorDocs = Entity.make({
  model: VectorDoc,
  entityType: "VectorDoc",
  primaryKey: { pk: { field: "pk", composite: ["docId"] }, sk: { field: "sk", composite: [] } },
  vectorIndexes: {
    byBody: { name: "vec1", dimensions: 4, distance: "cosine", source: { fields: ["body"] } },
  },
})

const MainTable = Table.make({
  schema: AppSchema,
  entities: {
    UserEntity,
    OrderEntity,
    RefProjects,
    RefTasks,
    GenDocs,
    LifecycleMembers,
    SparseMembers,
    RenamedMembers,
    SoftNotes,
    VectorDocs,
    VersionedNotes,
    Tickets,
  },
})

// --- Mock DynamoClient ---

const mockTransactGetItems = vi.fn()
const mockTransactWriteItems = vi.fn()
const mockGetItem = vi.fn()

const TestDynamoClient = mockDynamoClientLayer({
  // A guarded put (versioned / unique) reads its item; unanswered, it is missing.
  getItem: (input) =>
    Effect.tryPromise({
      try: async () => (await mockGetItem(input)) ?? {},
      catch: (e) => new DynamoError({ operation: "GetItem", cause: e }),
    }),
  // …and a missing retain item looks for its retained history: none.
  query: () => Effect.succeed({ Items: [] } as any),
  transactGetItems: (input) =>
    Effect.tryPromise({
      try: () => mockTransactGetItems(input),
      catch: (e) => new DynamoError({ operation: "TransactGetItems", cause: e }),
    }),
  transactWriteItems: (input) =>
    Effect.tryPromise({
      try: () => mockTransactWriteItems(input),
      catch: (e) => new DynamoError({ operation: "TransactWriteItems", cause: e }),
    }),
})

const TestTableConfig = MainTable.layer({ name: "test-table" })
const TestLayer = Layer.merge(TestDynamoClient, TestTableConfig)

beforeEach(() => {
  vi.resetAllMocks()
})

describe("Transaction", () => {
  describe("transactGet", () => {
    it.effect("atomically gets multiple items across entities", () =>
      Effect.gen(function* () {
        const userItem = toAttributeMap({
          userId: "u-1",
          email: "alice@example.com",
          name: "Alice",
          role: "admin",
          pk: "$myapp#v1#user#userid_u-1",
          sk: "$myapp#v1#user",
          __edd_e__: "User",
        })
        const orderItem = toAttributeMap({
          orderId: "ord-1",
          userId: "u-1",
          product: "Widget",
          quantity: 3,
          status: "pending",
          pk: "$myapp#v1#order#orderid_ord-1",
          sk: "$myapp#v1#order",
          __edd_e__: "Order",
        })

        mockTransactGetItems.mockResolvedValueOnce({
          Responses: [{ Item: userItem }, { Item: orderItem }],
        })

        const [user, order] = yield* Transaction.transactGet([
          UserEntity.get({ userId: "u-1" }),
          OrderEntity.get({ orderId: "ord-1" }),
        ])

        expect(user?.userId).toBe("u-1")
        expect(user?.email).toBe("alice@example.com")
        expect(order?.orderId).toBe("ord-1")
        expect(order?.product).toBe("Widget")

        // Verify the composed keys were sent correctly
        expect(mockTransactGetItems).toHaveBeenCalledOnce()
        const call = mockTransactGetItems.mock.calls[0]![0]
        expect(call.TransactItems).toHaveLength(2)

        const userKey = fromAttributeMap(call.TransactItems[0].Get.Key)
        expect(userKey.pk).toBe("$myapp#v1#user#userid_u-1")
        expect(userKey.sk).toBe("$myapp#v1#user")

        const orderKey = fromAttributeMap(call.TransactItems[1].Get.Key)
        expect(orderKey.pk).toBe("$myapp#v1#order#orderid_ord-1")
        expect(orderKey.sk).toBe("$myapp#v1#order")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("returns empty array for empty input", () =>
      Effect.gen(function* () {
        const results = yield* Transaction.transactGet([])
        expect(results).toHaveLength(0)
        expect(mockTransactGetItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("returns undefined for non-existent items", () =>
      Effect.gen(function* () {
        mockTransactGetItems.mockResolvedValueOnce({
          Responses: [{ Item: undefined }],
        })

        const [user] = yield* Transaction.transactGet([UserEntity.get({ userId: "nonexistent" })])

        expect(user).toBeUndefined()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("fails with DynamoError when exceeding 100-item limit", () =>
      Effect.gen(function* () {
        const items = Array.from({ length: 101 }, (_, i) => UserEntity.get({ userId: `u-${i}` }))

        const error = yield* Transaction.transactGet(items).pipe(Effect.flip)
        expect(error._tag).toBe("DynamoError")
        expect((error as DynamoError).operation).toBe("TransactGetItems")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("maps TransactionCanceledException to TransactionCancelled", () =>
      Effect.gen(function* () {
        const txError = new Error("Transaction cancelled")
        ;(txError as any).name = "TransactionCanceledException"
        ;(txError as any).CancellationReasons = [
          { Code: "ConditionalCheckFailed", Message: "Condition not met" },
        ]

        mockTransactGetItems.mockRejectedValueOnce(txError)

        const error = yield* Transaction.transactGet([UserEntity.get({ userId: "u-1" })]).pipe(
          Effect.flip,
        )

        expect(error._tag).toBe("TransactionCancelled")
        const txCancelled = error as TransactionCancelled
        expect(txCancelled.operation).toBe("TransactGetItems")
        expect(txCancelled.reasons).toHaveLength(1)
        expect(txCancelled.reasons[0]?.code).toBe("ConditionalCheckFailed")
        expect(txCancelled.reasons[0]?.message).toBe("Condition not met")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("fails with ValidationError for malformed item data", () =>
      Effect.gen(function* () {
        const malformedItem = toAttributeMap({
          userId: "u-1",
          email: "alice@example.com",
          name: "Alice",
          role: "invalid-role", // not "admin" or "member"
          pk: "$myapp#v1#user#userid_u-1",
          sk: "$myapp#v1#user",
          __edd_e__: "User",
        })

        mockTransactGetItems.mockResolvedValueOnce({
          Responses: [{ Item: malformedItem }],
        })

        const error = yield* Transaction.transactGet([UserEntity.get({ userId: "u-1" })]).pipe(
          Effect.flip,
        )

        expect(error._tag).toBe("ValidationError")
        expect((error as ValidationError).entityType).toBe("User")
        expect((error as ValidationError).operation).toBe("decode")
      }).pipe(Effect.provide(TestLayer)),
    )
  })

  describe("transactWrite", () => {
    it.effect("atomically writes puts across entities", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          UserEntity.put({
            userId: "u-1",
            email: "alice@example.com",
            name: "Alice",
            role: "admin",
          }),
          OrderEntity.put({
            orderId: "ord-1",
            userId: "u-1",
            product: "Widget",
            quantity: 3,
            status: "pending",
          }),
        ])

        expect(mockTransactWriteItems).toHaveBeenCalledOnce()
        const call = mockTransactWriteItems.mock.calls[0]![0]
        expect(call.TransactItems).toHaveLength(2)
        expect(call.TransactItems[0].Put).toBeDefined()
        expect(call.TransactItems[1].Put).toBeDefined()

        // Verify composed keys and entity type discriminator
        const userItem = fromAttributeMap(call.TransactItems[0].Put.Item)
        expect(userItem.pk).toBe("$myapp#v1#user#userid_u-1")
        expect(userItem.sk).toBe("$myapp#v1#user")
        expect(userItem.__edd_e__).toBe("User")
        expect(userItem.gsi1pk).toBe("$myapp#v1#user#role_admin")
        expect(userItem.gsi1sk).toBe("$myapp#v1#user#userid_u-1")

        const orderItem = fromAttributeMap(call.TransactItems[1].Put.Item)
        expect(orderItem.pk).toBe("$myapp#v1#order#orderid_ord-1")
        expect(orderItem.sk).toBe("$myapp#v1#order")
        expect(orderItem.__edd_e__).toBe("Order")
        expect(orderItem.gsi1pk).toBe("$myapp#v1#order#userid_u-1")
        expect(orderItem.gsi1sk).toBe("$myapp#v1#order#orderid_ord-1")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("supports delete operations", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([UserEntity.delete({ userId: "u-1" })])

        expect(mockTransactWriteItems).toHaveBeenCalledOnce()
        const call = mockTransactWriteItems.mock.calls[0]![0]
        expect(call.TransactItems).toHaveLength(1)
        expect(call.TransactItems[0].Delete).toBeDefined()

        const deleteKey = fromAttributeMap(call.TransactItems[0].Delete.Key)
        expect(deleteKey.pk).toBe("$myapp#v1#user#userid_u-1")
        expect(deleteKey.sk).toBe("$myapp#v1#user")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("supports conditionCheck via Transaction.check", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        const cond = Expression.condition({
          attributeExists: "email",
        })

        yield* Transaction.transactWrite([
          UserEntity.get({ userId: "u-1" }).pipe(Transaction.check(cond)),
        ])

        expect(mockTransactWriteItems).toHaveBeenCalledOnce()
        const call = mockTransactWriteItems.mock.calls[0]![0]
        expect(call.TransactItems).toHaveLength(1)
        expect(call.TransactItems[0].ConditionCheck).toBeDefined()
        expect(call.TransactItems[0].ConditionCheck.ConditionExpression).toBe(
          "attribute_exists(#email)",
        )
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("supports mixed put, delete, and conditionCheck operations", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        const cond = Expression.condition({
          attributeExists: "email",
        })

        yield* Transaction.transactWrite([
          OrderEntity.put({
            orderId: "ord-1",
            userId: "u-1",
            product: "Widget",
            quantity: 3,
            status: "pending",
          }),
          OrderEntity.delete({ orderId: "ord-old" }),
          UserEntity.get({ userId: "u-1" }).pipe(Transaction.check(cond)),
        ])

        const call = mockTransactWriteItems.mock.calls[0]![0]
        expect(call.TransactItems).toHaveLength(3)
        expect(call.TransactItems[0].Put).toBeDefined()
        expect(call.TransactItems[1].Delete).toBeDefined()
        expect(call.TransactItems[2].ConditionCheck).toBeDefined()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("Transaction.check works data-first", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        const cond = Expression.condition({
          attributeNotExists: "pk",
        })

        yield* Transaction.transactWrite([
          Transaction.check(UserEntity.get({ userId: "u-1" }), cond),
        ])

        const call = mockTransactWriteItems.mock.calls[0]![0]
        const condCheck = call.TransactItems[0].ConditionCheck
        expect(condCheck.ConditionExpression).toBe("attribute_not_exists(#pk)")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("does nothing for empty operations", () =>
      Effect.gen(function* () {
        yield* Transaction.transactWrite([])
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("fails with DynamoError when exceeding 100-item limit", () =>
      Effect.gen(function* () {
        const operations = Array.from({ length: 101 }, (_, i) =>
          UserEntity.delete({ userId: `u-${i}` }),
        )

        const error = yield* Transaction.transactWrite(operations).pipe(Effect.flip)
        expect(error._tag).toBe("DynamoError")
        expect((error as DynamoError).operation).toBe("TransactWriteItems")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("maps TransactionCanceledException to TransactionCancelled", () =>
      Effect.gen(function* () {
        const txError = new Error("Transaction cancelled")
        ;(txError as any).name = "TransactionCanceledException"
        ;(txError as any).CancellationReasons = [
          { Code: "ConditionalCheckFailed", Message: "Condition not met" },
          { Code: "None" },
        ]

        mockTransactWriteItems.mockRejectedValueOnce(txError)

        const error = yield* Transaction.transactWrite([
          UserEntity.put({
            userId: "u-1",
            email: "alice@example.com",
            name: "Alice",
            role: "admin",
          }),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("TransactionCancelled")
        const txCancelled = error as TransactionCancelled
        expect(txCancelled.operation).toBe("TransactWriteItems")
        expect(txCancelled.reasons).toHaveLength(2)
        expect(txCancelled.reasons[0]?.code).toBe("ConditionalCheckFailed")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("fails with ValidationError for invalid put data", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          UserEntity.put({
            userId: "u-1",
            email: "alice@example.com",
            name: "",
            role: "admin",
          } as any),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect((error as ValidationError).operation).toBe("transactWrite.put")
        expect((error as ValidationError).entityType).toBe("User")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("propagates non-transaction DynamoError as-is", () =>
      Effect.gen(function* () {
        const genericError = new Error("Network timeout")
        mockTransactWriteItems.mockRejectedValueOnce(genericError)

        const error = yield* Transaction.transactWrite([UserEntity.delete({ userId: "u-1" })]).pipe(
          Effect.flip,
        )

        expect(error._tag).toBe("DynamoError")
        expect((error as DynamoError).operation).toBe("TransactWriteItems")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("cancellation with multiple detailed reasons per item", () =>
      Effect.gen(function* () {
        const txError = new Error("Transaction cancelled")
        ;(txError as any).name = "TransactionCanceledException"
        ;(txError as any).CancellationReasons = [
          { Code: "ConditionalCheckFailed", Message: "Item already exists" },
          { Code: "TransactionConflict", Message: "Conflicting operation in progress" },
          { Code: "None" },
          { Code: "ValidationError", Message: "Schema mismatch" },
        ]

        mockTransactWriteItems.mockRejectedValueOnce(txError)

        const error = yield* Transaction.transactWrite([
          UserEntity.put({
            userId: "u-1",
            email: "alice@example.com",
            name: "Alice",
            role: "admin",
          }),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("TransactionCancelled")
        const txCancelled = error as TransactionCancelled
        expect(txCancelled.reasons).toHaveLength(4)
        expect(txCancelled.reasons[0]?.code).toBe("ConditionalCheckFailed")
        expect(txCancelled.reasons[0]?.message).toBe("Item already exists")
        expect(txCancelled.reasons[1]?.code).toBe("TransactionConflict")
        expect(txCancelled.reasons[1]?.message).toBe("Conflicting operation in progress")
        expect(txCancelled.reasons[2]?.code).toBe("None")
        expect(txCancelled.reasons[3]?.code).toBe("ValidationError")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("cancellation with empty reasons array", () =>
      Effect.gen(function* () {
        const txError = new Error("Transaction cancelled")
        ;(txError as any).name = "TransactionCanceledException"
        ;(txError as any).CancellationReasons = []

        mockTransactWriteItems.mockRejectedValueOnce(txError)

        const error = yield* Transaction.transactWrite([UserEntity.delete({ userId: "u-1" })]).pipe(
          Effect.flip,
        )

        expect(error._tag).toBe("TransactionCancelled")
        const txCancelled = error as TransactionCancelled
        expect(txCancelled.reasons).toHaveLength(0)
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("cancellation without CancellationReasons property", () =>
      Effect.gen(function* () {
        const txError = new Error("Transaction cancelled")
        ;(txError as any).name = "TransactionCanceledException"
        // No CancellationReasons property at all

        mockTransactWriteItems.mockRejectedValueOnce(txError)

        const error = yield* Transaction.transactWrite([UserEntity.delete({ userId: "u-1" })]).pipe(
          Effect.flip,
        )

        expect(error._tag).toBe("TransactionCancelled")
        const txCancelled = error as TransactionCancelled
        expect(txCancelled.reasons).toHaveLength(0)
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("cancellation with mixed reason codes (ConditionalCheckFailed + None)", () =>
      Effect.gen(function* () {
        const txError = new Error("Transaction cancelled")
        ;(txError as any).name = "TransactionCanceledException"
        ;(txError as any).CancellationReasons = [
          { Code: "ConditionalCheckFailed", Message: "Condition not met" },
          { Code: "None" },
          { Code: "ConditionalCheckFailed", Message: "Already exists" },
        ]

        mockTransactWriteItems.mockRejectedValueOnce(txError)

        const error = yield* Transaction.transactWrite([
          UserEntity.delete({ userId: "u-1" }),
          UserEntity.delete({ userId: "u-2" }),
          UserEntity.delete({ userId: "u-3" }),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("TransactionCancelled")
        const txCancelled = error as TransactionCancelled
        expect(txCancelled.reasons).toHaveLength(3)
        expect(txCancelled.reasons[0]?.code).toBe("ConditionalCheckFailed")
        expect(txCancelled.reasons[1]?.code).toBe("None")
        expect(txCancelled.reasons[2]?.code).toBe("ConditionalCheckFailed")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("transactGet handles mixed found/not-found items", () =>
      Effect.gen(function* () {
        const userItem = toAttributeMap({
          userId: "u-1",
          email: "alice@example.com",
          name: "Alice",
          role: "admin",
          pk: "$myapp#v1#user#userid_u-1",
          sk: "$myapp#v1#user",
          __edd_e__: "User",
        })

        // Second item not found, third item found
        mockTransactGetItems.mockResolvedValueOnce({
          Responses: [
            { Item: userItem },
            { Item: undefined },
            {
              Item: toAttributeMap({
                orderId: "ord-1",
                userId: "u-1",
                product: "Widget",
                quantity: 3,
                status: "pending",
                pk: "$myapp#v1#order#orderid_ord-1",
                sk: "$myapp#v1#order",
                __edd_e__: "Order",
              }),
            },
          ],
        })

        const [user, missingUser, order] = yield* Transaction.transactGet([
          UserEntity.get({ userId: "u-1" }),
          UserEntity.get({ userId: "u-nonexistent" }),
          OrderEntity.get({ orderId: "ord-1" }),
        ])

        expect(user?.userId).toBe("u-1")
        expect(missingUser).toBeUndefined()
        expect(order?.orderId).toBe("ord-1")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("conditionCheck with multiple expression types", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        const cond = Expression.condition({
          eq: { role: "admin" },
          attributeExists: "email",
          gt: { version: 0 },
        })

        yield* Transaction.transactWrite([
          UserEntity.get({ userId: "u-1" }).pipe(Transaction.check(cond)),
        ])

        const call = mockTransactWriteItems.mock.calls[0]![0]
        const condCheck = call.TransactItems[0].ConditionCheck
        expect(condCheck.ConditionExpression).toContain("=")
        expect(condCheck.ConditionExpression).toContain("attribute_exists")
        expect(condCheck.ConditionExpression).toContain(">")
        expect(condCheck.ExpressionAttributeNames["#role"]).toBe("role")
        expect(condCheck.ExpressionAttributeNames["#email"]).toBe("email")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("transactWrite at exactly 100-item limit succeeds", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        const operations = Array.from({ length: 100 }, (_, i) =>
          UserEntity.delete({ userId: `u-${i}` }),
        )

        yield* Transaction.transactWrite(operations)

        const call = mockTransactWriteItems.mock.calls[0]![0]
        expect(call.TransactItems).toHaveLength(100)
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("transactGet at exactly 100-item limit succeeds", () =>
      Effect.gen(function* () {
        const responses = Array.from({ length: 100 }, () => ({ Item: undefined }))
        mockTransactGetItems.mockResolvedValueOnce({ Responses: responses })

        const items = Array.from({ length: 100 }, (_, i) => UserEntity.get({ userId: `u-${i}` }))

        const results = yield* Transaction.transactGet(items)
        expect(results).toHaveLength(100)
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("sparse GSI: put omits GSI keys when composites missing", () =>
      Effect.gen(function* () {
        class SparseItem extends Schema.Class<SparseItem>("SparseItem")({
          itemId: Schema.String,
          name: Schema.String,
          tenantId: Schema.optional(Schema.String),
        }) {}

        const SparseEntity = Entity.make({
          model: SparseItem,
          entityType: "SparseItem",
          primaryKey: {
            pk: { field: "pk", composite: ["itemId"] },
            sk: { field: "sk", composite: [] },
          },
          indexes: {
            byTenant: {
              name: "gsi1",
              pk: { field: "gsi1pk", composite: ["tenantId"] },
              sk: { field: "gsi1sk", composite: [] },
            },
          },
        })
        SparseEntity._configure(AppSchema, MainTable.Tag)

        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([SparseEntity.put({ itemId: "i-1", name: "NoTenant" })])

        const call = mockTransactWriteItems.mock.calls[0]![0]
        const item = fromAttributeMap(call.TransactItems[0].Put.Item)
        expect(item.pk).toBe("$myapp#v1#sparseitem#itemid_i-1")
        expect(item.__edd_e__).toBe("SparseItem")
        expect(item.gsi1pk).toBeUndefined()
        expect(item.gsi1sk).toBeUndefined()
      }).pipe(Effect.provide(TestLayer)),
    )
  })

  // -------------------------------------------------------------------------
  // #100 — bound-CRUD builders as transact ops + conditions carried through
  // -------------------------------------------------------------------------

  describe("bound-CRUD builders as transact ops (#100)", () => {
    it.effect("accepts a bound put from db.entities.*", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})
        const db = yield* DynamoClient.make({
          entities: { UserEntity, OrderEntity },
          tables: { MainTable },
        })

        yield* Transaction.transactWrite([
          db.entities.UserEntity.put({
            userId: "u-1",
            email: "a@x.io",
            name: "Alice",
            role: "admin",
          }),
        ])

        const call = mockTransactWriteItems.mock.calls[0]![0]
        expect(call.TransactItems).toHaveLength(1)
        const item = fromAttributeMap(call.TransactItems[0].Put.Item)
        expect(item.pk).toBe("$myapp#v1#user#userid_u-1")
        expect(item.__edd_e__).toBe("User")
        expect(call.TransactItems[0].Put.ConditionExpression).toBeUndefined()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("accepts a bound delete from db.entities.*", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})
        const db = yield* DynamoClient.make({
          entities: { UserEntity, OrderEntity },
          tables: { MainTable },
        })

        yield* Transaction.transactWrite([db.entities.UserEntity.delete({ userId: "u-1" })])

        const call = mockTransactWriteItems.mock.calls[0]![0]
        const key = fromAttributeMap(call.TransactItems[0].Delete.Key)
        expect(key.pk).toBe("$myapp#v1#user#userid_u-1")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("carries `.condition()` from a bound put into the transact item", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})
        const db = yield* DynamoClient.make({
          entities: { UserEntity, OrderEntity },
          tables: { MainTable },
        })

        yield* Transaction.transactWrite([
          db.entities.UserEntity.put({
            userId: "u-1",
            email: "a@x.io",
            name: "Alice",
            role: "admin",
          }).condition({ role: "admin" }),
        ])

        const put = mockTransactWriteItems.mock.calls[0]![0].TransactItems[0].Put
        expect(put.ConditionExpression).toBeDefined()
        expect(Object.values(put.ExpressionAttributeNames)).toContain("role")
        expect(Object.values(put.ExpressionAttributeValues)).toEqual([{ S: "admin" }])
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("carries create()'s implicit attribute_not_exists guard", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          UserEntity.create({ userId: "u-1", email: "a@x.io", name: "Alice", role: "admin" }),
        ])

        const put = mockTransactWriteItems.mock.calls[0]![0].TransactItems[0].Put
        expect(put.ConditionExpression).toBe(
          "(attribute_not_exists(#e0)) AND (attribute_not_exists(#e1))",
        )
        expect(put.ExpressionAttributeNames).toEqual({ "#e0": "pk", "#e1": "sk" })
        // A value-free condition must NOT send an empty ExpressionAttributeValues map.
        expect(put.ExpressionAttributeValues).toBeUndefined()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("carries a condition attached to an unbound delete", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          UserEntity.deleteIfExists({ userId: "u-1" }) as Transaction.TransactWriteOp,
        ])

        const del = mockTransactWriteItems.mock.calls[0]![0].TransactItems[0].Delete
        expect(del.ConditionExpression).toBe("attribute_exists(#e0)")
        expect(del.ExpressionAttributeNames).toEqual({ "#e0": "pk" })
        expect(del.ExpressionAttributeValues).toBeUndefined()
      }).pipe(Effect.provide(TestLayer)),
    )
  })

  // -------------------------------------------------------------------------
  // #100 review — ops the compile path cannot reproduce must be REJECTED, never
  // silently compiled into something with different semantics.
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  // #113 — a put of an entity with multi-item lifecycle config expands.
  // -------------------------------------------------------------------------

  describe("multi-item side writes (#113)", () => {
    const memberInput = { memberId: "m-1", email: "a@x.io", label: "L" } as const

    it.effect("a unique + retain put emits the item, its sentinel and the v1 snapshot", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([LifecycleMembers.put(memberInput)])

        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        expect(items).toHaveLength(3)

        const main = fromAttributeMap(items[0].Put.Item)
        expect(main.__edd_e__).toBe("LifecycleMember")

        const sentinel = fromAttributeMap(items[1].Put.Item)
        expect(sentinel.__edd_e__).toBe("LifecycleMember._unique.email")
        expect(sentinel._entity_pk).toBe(main.pk)
        // The guard IS the constraint — without it the sentinel enforces nothing.
        expect(items[1].Put.ConditionExpression).toBe("attribute_not_exists(#sentinel_pk)")
        expect(items[1].Put.ExpressionAttributeNames).toEqual({ "#sentinel_pk": "pk" })

        const snapshot = fromAttributeMap(items[2].Put.Item)
        expect(snapshot.sk).toBe("$myapp#v1#lifecyclemember#v#0000001")
        // Never over another incarnation's history (#133).
        expect(items[2].Put.ConditionExpression).toMatch(/^attribute_not_exists\(#snap\) OR /)
        // The item was read missing: it must still be missing (#133).
        expect(items[0].Put.ConditionExpression).toBe("attribute_not_exists(#pk)")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a put whose unique field is RENAMED still emits its sentinel (#127)", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          RenamedMembers.put({ memberId: "m-3", email: "renamed@x.io" }),
        ])

        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        // Item + sentinel. Reading `email` off the already-renamed item read
        // `undefined` and dropped the sentinel entirely.
        expect(items).toHaveLength(2)
        const main = fromAttributeMap(items[0].Put.Item)
        expect(main.memberEmail).toBe("renamed@x.io")
        const sentinel = fromAttributeMap(items[1].Put.Item)
        expect(sentinel.__edd_e__).toBe("RenamedMember._unique.email")
        expect(sentinel.pk).toBe("$myapp#v1#renamedmember.email#renamed@x.io")
        expect(sentinel._entity_pk).toBe(main.pk)
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a constraint whose fields are unset emits no sentinel (sparse)", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          SparseMembers.put({ memberId: "m-2", label: "no-email" } as never),
        ])

        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        // Just the item — reserving `undefined` would collide across records.
        expect(items).toHaveLength(1)
        expect(fromAttributeMap(items[0].Put.Item).__edd_e__).toBe("SparseMember")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a failed sentinel is reported as UniqueConstraintViolation, not a bare cancel", () =>
      Effect.gen(function* () {
        const txError = new Error("cancelled")
        ;(txError as any).name = "TransactionCanceledException"
        // Position 1 is the sentinel emitted for position-0's op.
        ;(txError as any).CancellationReasons = [
          { Code: "None" },
          { Code: "ConditionalCheckFailed" },
          { Code: "None" },
        ]
        mockTransactWriteItems.mockRejectedValueOnce(txError)

        const error = yield* Transaction.transactWrite([LifecycleMembers.put(memberInput)]).pipe(
          Effect.flip,
        )

        expect(error._tag).toBe("UniqueConstraintViolation")
        const violation = error as UniqueConstraintViolation
        expect(violation.entityType).toBe("LifecycleMember")
        expect(violation.constraint).toBe("email")
        expect(violation.fields).toEqual({ email: "a@x.io" })
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("the 100-item cap counts EXPANDED items and says so", () =>
      Effect.gen(function* () {
        // 34 ops x 3 items each = 102 > 100, while 34 ops alone would pass.
        const ops = Array.from({ length: 34 }, (_, i) =>
          LifecycleMembers.put({ memberId: `m-${i}`, email: `e${i}@x.io`, label: "L" }),
        )

        const error = yield* Transaction.transactWrite(ops).pipe(Effect.flip)

        expect(error._tag).toBe("DynamoError")
        const message = String((error as DynamoError).cause)
        expect(message).toContain("34 operation(s) expanded to 102 items")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    const memberRow = (version: number) =>
      toAttributeMap({
        pk: "$myapp#v1#lifecyclemember#memberid_m-1",
        sk: "$myapp#v1#lifecyclemember",
        __edd_e__: "LifecycleMember",
        memberId: "m-1",
        email: "a@x.io",
        label: "L",
        version,
      })

    it.effect(
      "a delete of a lifecycle entity is planned from a read and joins the transaction",
      () =>
        Effect.gen(function* () {
          mockGetItem.mockImplementation(async (input: any) =>
            fromAttributeMap(input.Key).sk === "$myapp#v1#lifecyclemember"
              ? { Item: memberRow(1) }
              : {},
          )
          mockTransactWriteItems.mockResolvedValueOnce({})

          yield* Transaction.transactWrite([
            LifecycleMembers.delete({ memberId: "m-1" }),
            OrderEntity.put({
              orderId: "o-1",
              userId: "u-1",
              product: "Widget",
              quantity: 1,
              status: "pending",
            }),
          ])

          // The entity's own delete read the row — consistently, as standalone.
          expect(mockGetItem).toHaveBeenCalled()
          expect(mockTransactWriteItems).toHaveBeenCalledTimes(1)
          const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
          const main = items.find(
            (i: any) =>
              i.Delete && fromAttributeMap(i.Delete.Key).sk === "$myapp#v1#lifecyclemember",
          )
          // Guarded on what was read, exactly as the standalone delete is.
          expect(main?.Delete.ConditionExpression).toBeDefined()
          // The retain snapshot of the outgoing row joins it.
          expect(
            items.some(
              (i: any) => i.Put && String(fromAttributeMap(i.Put.Item).sk).includes("#v#"),
            ),
          ).toBe(true)
          // …and so does the other op.
          expect(
            items.some((i: any) => i.Put && fromAttributeMap(i.Put.Item).__edd_e__ === "Order"),
          ).toBe(true)
        }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a delete of a softDelete entity writes the tombstone it reads for", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async () => ({
          Item: toAttributeMap({
            pk: "$myapp#v1#softnote#noteid_n-1",
            sk: "$myapp#v1#softnote",
            __edd_e__: "SoftNote",
            noteId: "n-1",
            body: "b",
          }),
        }))
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([SoftNotes.delete({ noteId: "n-1" })])

        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        expect(items.some((i: any) => i.Delete !== undefined)).toBe(true)
        const tombstone = items.find((i: any) => i.Put)?.Put
        expect(String(fromAttributeMap(tombstone.Item).sk)).toContain("#deleted#")
        expect(fromAttributeMap(tombstone.Item).body).toBe("b")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect(
      "a multi-item delete of a missing row fails with ItemNotFound and writes nothing",
      () =>
        Effect.gen(function* () {
          const error = yield* Transaction.transactWrite([
            LifecycleMembers.delete({ memberId: "m-gone" }),
            UserEntity.put({ userId: "u-1", email: "a@x.io", name: "A", role: "admin" }),
          ]).pipe(Effect.flip)

          expect(error._tag).toBe("ItemNotFound")
          expect(mockTransactWriteItems).not.toHaveBeenCalled()
        }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("rejects a delete returning an item, on a plain entity — EDD-9060", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          // @ts-expect-error — a delete that returns an item is not a transact op;
          // the runtime refusal below is the backstop for untyped callers.
          UserEntity.delete({ userId: "u-1" }).pipe(Entity.returnValues("allOld")),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("[EDD-9060]")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("rejects a delete returning an item, on a unique entity — before reading", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          // @ts-expect-error — a delete that returns an item is not a transact op;
          // the runtime refusal below is the backstop for untyped callers.
          LifecycleMembers.delete({ memberId: "m-1" }).pipe(Entity.returnValues("allOld")),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("[EDD-9060]")
        expect(mockGetItem).not.toHaveBeenCalled()
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect(
      "a PUT of a softDelete entity is still allowed — softDelete only affects deletes",
      () =>
        Effect.gen(function* () {
          mockTransactWriteItems.mockResolvedValueOnce({})

          yield* Transaction.transactWrite([SoftNotes.put({ noteId: "n-1", body: "b" })])

          const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
          expect(items).toHaveLength(1)
        }).pipe(Effect.provide(TestLayer)),
    )
  })

  it.effect("a transacted op keeps its own guard whatever .condition() is added (#133)", () =>
    Effect.gen(function* () {
      mockTransactWriteItems.mockResolvedValue({})
      const db = yield* DynamoClient.make({
        entities: { UserEntity, OrderEntity },
        tables: { MainTable },
      })
      const input = { userId: "u-g", email: "g@x.io", name: "G", role: "member" } as const
      const notExists = "(attribute_not_exists(#e0)) AND (attribute_not_exists(#e1))"
      yield* Transaction.transactWrite([
        UserEntity.create(input).pipe(
          UserEntity.condition({ name: "a" }),
          UserEntity.condition({}),
        ),
        db.entities.OrderEntity.deleteIfExists({ orderId: "o-g" })
          .condition({ status: "shipped" })
          .condition({}),
      ])
      const [put, del] = mockTransactWriteItems.mock.calls[0]![0].TransactItems
      expect(put.Put.ConditionExpression).toBe(notExists)
      expect(put.Put.ExpressionAttributeNames).toEqual({ "#e0": "pk", "#e1": "sk" })
      expect(del.Delete.ConditionExpression).toBe("attribute_exists(#e0)")
      expect(del.Delete.ExpressionAttributeNames).toEqual({ "#e0": "pk" })
      yield* Transaction.transactWrite([
        db.entities.UserEntity.create(input).condition({ name: "G" }),
      ])
      const guarded = mockTransactWriteItems.mock.calls[1]![0].TransactItems[0].Put
      expect(guarded.ConditionExpression).toBe(`(${notExists}) AND (#e2 = :e3)`)
      expect(guarded.ExpressionAttributeNames).toEqual({ "#e0": "pk", "#e1": "sk", "#e2": "name" })
    }).pipe(Effect.provide(TestLayer)),
  )

  it.effect("a Transaction.check with an empty condition is refused (#134)", () =>
    Effect.gen(function* () {
      const error = yield* Transaction.transactWrite([
        Transaction.check(UserEntity.get({ userId: "u-1" }), Expression.condition({})),
      ]).pipe(Effect.flip)
      expect(error._tag).toBe("ValidationError")
      expect(mockTransactWriteItems).not.toHaveBeenCalled()
    }).pipe(Effect.provide(TestLayer)),
  )

  it.effect("an empty part under or or not in a transacted condition is refused (#133)", () =>
    Effect.gen(function* () {
      const input = { userId: "u-r", email: "r@x.io", name: "R", role: "member" } as const
      for (const cond of [
        UserEntity.condition((t, { or, eq, and }) => or(eq(t.name, "a"), and())),
        UserEntity.condition((_, { not, and }) => not(and())),
        UserEntity.condition((_, { or }) => or()),
      ]) {
        const error = yield* Transaction.transactWrite([UserEntity.put(input).pipe(cond)]).pipe(
          Effect.flip,
        )
        expect(error._tag).toBe("ValidationError")
      }
      expect(mockTransactWriteItems).not.toHaveBeenCalled()
    }).pipe(Effect.provide(TestLayer)),
  )

  it.effect("an empty condition on a transaction op is no condition (#133)", () =>
    Effect.gen(function* () {
      mockTransactWriteItems.mockResolvedValueOnce({})
      yield* Transaction.transactWrite([
        UserEntity.put({ userId: "u-e", email: "e@x.io", name: "E", role: "member" }).pipe(
          UserEntity.condition({}),
        ),
        OrderEntity.delete({ orderId: "o-e" }).pipe(OrderEntity.condition({})),
      ])
      const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
      expect(items[0].Put.ConditionExpression).toBeUndefined()
      expect(items[1].Delete.ConditionExpression).toBeUndefined()
    }).pipe(Effect.provide(TestLayer)),
  )

  describe("one op per item: a repeated item is refused before writing (#133)", () => {
    const user = (userId: string, name: string) =>
      ({ userId, email: `${userId}@x.io`, name, role: "member" }) as const

    it.effect("two ops on one plain item are refused, and nothing is written", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          UserEntity.put(user("u-1", "A")),
          UserEntity.delete({ userId: "u-1" }),
        ]).pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
        const failure = error as ValidationError
        expect(failure.entityType).toBe("User")
        expect(String(failure.cause)).toContain("touches one item more than once")
        expect(String(failure.cause)).toMatch(/operation 0 \(User\) and operation 1 \(User\)/)
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("two puts of one versioned retain item are refused, not judged a lost race", () =>
      Effect.gen(function* () {
        const member = (label: string) => ({ memberId: "m-9", email: "m9@x.io", label })
        const error = yield* Transaction.transactWrite([
          LifecycleMembers.put(member("a")),
          LifecycleMembers.put(member("b")),
        ]).pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
        expect((error as ValidationError).entityType).toBe("LifecycleMember")
        expect(String((error as ValidationError).cause)).toContain(
          "touches one item more than once",
        )
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("swapping unique values between two items repeats a sentinel and is refused", () =>
      Effect.gen(function* () {
        // Both items exist, each holding (and owning) its own value's sentinel.
        const stored: Record<string, Record<string, unknown>> = {
          "$myapp#v1#sparsemember#memberid_s-1": { memberId: "s-1", email: "s1@x.io" },
          "$myapp#v1#sparsemember#memberid_s-2": { memberId: "s-2", email: "s2@x.io" },
        }
        const owners: Record<string, string> = {
          "$myapp#v1#sparsemember.email#s1@x.io": "$myapp#v1#sparsemember#memberid_s-1",
          "$myapp#v1#sparsemember.email#s2@x.io": "$myapp#v1#sparsemember#memberid_s-2",
        }
        mockGetItem.mockImplementation(async (input: any) => {
          const pk = input.Key.pk.S as string
          if (input.ProjectionExpression === "#epk, #esk") {
            const owner = owners[pk]
            return owner === undefined
              ? {}
              : {
                  Item: toAttributeMap({ _entity_pk: owner, _entity_sk: "$myapp#v1#sparsemember" }),
                }
          }
          const item = stored[pk]
          return item === undefined
            ? {}
            : {
                Item: toAttributeMap({
                  ...item,
                  label: "L",
                  pk,
                  sk: "$myapp#v1#sparsemember",
                  __edd_e__: "SparseMember",
                }),
              }
        })
        const error = yield* Transaction.transactWrite([
          SparseMembers.put({ memberId: "s-1", email: "s2@x.io", label: "L" }),
          SparseMembers.put({ memberId: "s-2", email: "s1@x.io", label: "L" }),
        ]).pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
        expect((error as ValidationError).entityType).toBe("SparseMember")
        expect(String((error as ValidationError).cause)).toContain(
          "touches one item more than once",
        )
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("distinct items in one transaction are written as before", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})
        yield* Transaction.transactWrite([
          UserEntity.put(user("u-1", "A")),
          UserEntity.delete({ userId: "u-2" }),
          OrderEntity.delete({ orderId: "u-1" }),
        ])
        expect(mockTransactWriteItems).toHaveBeenCalledOnce()
      }).pipe(Effect.provide(TestLayer)),
    )
  })

  describe("a transaction over DynamoDB's 4 MB is refused before writing (#133)", () => {
    const big = "x".repeat(380_000)
    const user = (i: number) =>
      ({ userId: `u-${i}`, email: `u${i}@x.io`, name: big, role: "member" }) as const
    const member = (i: number) => ({ memberId: `m-${i}`, email: `m${i}@x.io`, label: big })

    it.effect("items totalling more than 4 MB are refused, naming the largest", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite(
          Array.from({ length: 12 }, (_, i) => UserEntity.put(user(i))),
        ).pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
        const failure = error as ValidationError
        expect(failure.entityType).toBe("User")
        expect(String(failure.cause)).toContain("over its limit of 4194304 bytes (4 MB)")
        expect(String(failure.cause)).toMatch(/The largest is operation \d+ \(User\)'s/)
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a retain put counts twice: its item and its snapshot", () =>
      Effect.gen(function* () {
        // Six 380 KB items: 2.3 MB as plain puts — written…
        mockTransactWriteItems.mockResolvedValueOnce({})
        yield* Transaction.transactWrite(
          Array.from({ length: 6 }, (_, i) => UserEntity.put(user(i))),
        )
        expect(mockTransactWriteItems).toHaveBeenCalledOnce()
        // …but 4.6 MB as retain puts, each snapshotted in the same transaction.
        const error = yield* Transaction.transactWrite(
          Array.from({ length: 6 }, (_, i) => LifecycleMembers.put(member(i))),
        ).pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
        expect((error as ValidationError).entityType).toBe("LifecycleMember")
        expect(String((error as ValidationError).cause)).toContain("counts twice")
        expect(mockTransactWriteItems).toHaveBeenCalledOnce()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("the limit is exactly 4 MB by DynamoDB's item-size rules", () =>
      Effect.gen(function* () {
        // An item of one attribute `a`: 1 byte of name plus its value.
        const put = (bytes: number) => ({
          Put: { TableName: "t", Item: { a: { S: "x".repeat(bytes - 1) } } },
        })
        const target = transactItemTarget(put(2), "t", ["a"], "Doc", "operation 0 (Doc)")
        yield* refuseOversizedTransaction([put(4 * 1024 * 1024)], [target], "transactWrite")
        const over = yield* refuseOversizedTransaction(
          [put(4 * 1024 * 1024 + 1)],
          [target],
          "transactWrite",
        ).pipe(Effect.flip)
        expect(over._tag).toBe("ValidationError")
        expect(String(over.cause)).toContain("total at least 4194305 bytes")
      }),
    )

    it.effect("numbers count as DynamoDB stores them: 25 lists of 12,000 floats fit", () =>
      Effect.gen(function* () {
        // ~2.9 MB as DynamoDB counts it (a byte per two significant digits,
        // plus one); counted by digits it read as 5.5 MB and was refused.
        const vec = Array.from({ length: 12_000 }, (_, i) => ({
          N: String(0.1234567890123 + i * 1e-13),
        }))
        const items = Array.from({ length: 25 }, (_, i) => ({
          Put: { TableName: "t", Item: { pk: { S: `p${i}` }, vec: { L: vec } } },
        }))
        const targets = items.map((item, i) =>
          transactItemTarget(item, "t", ["pk"], "Vec", `operation ${i} (Vec)`),
        )
        yield* refuseOversizedTransaction(items, targets, "transactWrite")
      }),
    )

    it("number sizes trim leading and trailing zeros and count digit pairs", () => {
      const n = (value: string) => itemBytes({ a: { N: value } }) - 1
      expect(n("5")).toBe(2)
      expect(n("12")).toBe(2)
      expect(n("123")).toBe(3)
      expect(n("-0.00012300")).toBe(3)
      expect(n("1000000")).toBe(2)
      expect(n("0.1234567890123")).toBe(8)
      expect(n("1e+21")).toBe(2)
      // Zero has no significant digits: one byte.
      expect(n("0")).toBe(1)
      expect(n("-0.000")).toBe(1)
      // The batch budget keeps its higher count.
      expect(itemBytes({ a: { N: "0.1234567890123" } }, "upper")).toBeGreaterThan(9)
    })

    it("an Update counts only its key: its values may be the condition's", () => {
      expect(
        transactItemBytes({
          Update: {
            TableName: "t",
            Key: { pk: { S: "abc" } },
            UpdateExpression: "SET #a = :a",
            ConditionExpression: "#b = :b",
            ExpressionAttributeNames: { "#a": "a", "#b": "b" },
            ExpressionAttributeValues: { ":a": { S: "x" }, ":b": { S: "y".repeat(1000) } },
          },
        }),
      ).toBe(5)
    })

    it("item sizes count attribute names, UTF-8 strings and raw binary", () => {
      expect(itemBytes({ ab: { S: "é" } })).toBe(4)
      expect(itemBytes({ b: { B: new Uint8Array(10) } })).toBe(11)
      expect(transactItemBytes({ Delete: { TableName: "t", Key: { pk: { S: "abc" } } } })).toBe(5)
    })
  })

  describe("update ops", () => {
    const sparseRow = (email: string) =>
      toAttributeMap({
        pk: "$myapp#v1#sparsemember#memberid_m-1",
        sk: "$myapp#v1#sparsemember",
        __edd_e__: "SparseMember",
        memberId: "m-1",
        email,
        label: "L",
      })

    it.effect("an update of a plain entity is one Update item, after one existence read", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async () => ({
          Item: toAttributeMap({
            pk: "$myapp#v1#user#userid_u-1",
            sk: "$myapp#v1#user",
            __edd_e__: "User",
            userId: "u-1",
            email: "a@x.io",
            name: "A",
            role: "admin",
          }),
        }))
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          UserEntity.update({ userId: "u-1" }).pipe(Entity.set({ name: "Renamed" })),
          OrderEntity.put({
            orderId: "o-1",
            userId: "u-1",
            product: "Widget",
            quantity: 1,
            status: "pending",
          }),
        ])

        // One consistent read: the row must exist for the Update to apply —
        // a missing one would take the update's own create / ItemNotFound path.
        expect(mockGetItem).toHaveBeenCalledTimes(1)
        expect(mockGetItem.mock.calls[0]![0].ConsistentRead).toBe(true)
        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        expect(items).toHaveLength(2)
        expect(fromAttributeMap(items[0].Update.Key).pk).toBe("$myapp#v1#user#userid_u-1")
        expect(Object.values(items[0].Update.ExpressionAttributeNames)).toContain("name")
        // A transact item takes no ReturnValues — only the single-item request did.
        expect(items[0].Update).not.toHaveProperty("ReturnValues")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a bound update from db.entities.* is accepted", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: { UserEntity }, tables: { MainTable } })
        mockGetItem.mockImplementation(async () => ({
          Item: toAttributeMap({
            pk: "$myapp#v1#user#userid_u-1",
            sk: "$myapp#v1#user",
            __edd_e__: "User",
            userId: "u-1",
            email: "a@x.io",
            name: "A",
            role: "admin",
          }),
        }))
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          db.entities.UserEntity.update({ userId: "u-1" }).set({ name: "Bound" }),
        ])

        expect(mockTransactWriteItems.mock.calls[0]![0].TransactItems[0].Update).toBeDefined()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("patch() keeps its attribute_exists guard on the Update item", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async () => ({
          Item: toAttributeMap({
            pk: "$myapp#v1#user#userid_u-1",
            sk: "$myapp#v1#user",
            __edd_e__: "User",
            userId: "u-1",
            email: "a@x.io",
            name: "A",
            role: "admin",
          }),
        }))
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          UserEntity.patch({ userId: "u-1" }).pipe(Entity.set({ name: "Patched" })),
        ])

        const update = mockTransactWriteItems.mock.calls[0]![0].TransactItems[0].Update
        expect(update.ConditionExpression).toContain("attribute_exists")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a taken unique value is reported as UniqueConstraintViolation with its fields", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async (input: any) =>
          fromAttributeMap(input.Key).sk === "$myapp#v1#sparsemember"
            ? { Item: sparseRow("old@x.io") }
            : {},
        )
        // Cancel the transaction AT the new email's reservation, wherever the
        // entity's own update placed it.
        mockTransactWriteItems.mockImplementationOnce(async (input: any) => {
          const error = new Error("cancelled")
          ;(error as any).name = "TransactionCanceledException"
          ;(error as any).CancellationReasons = input.TransactItems.map((i: any) =>
            i.Put && fromAttributeMap(i.Put.Item).__edd_e__ === "SparseMember._unique.email"
              ? { Code: "ConditionalCheckFailed" }
              : { Code: "None" },
          )
          throw error
        })

        const error = yield* Transaction.transactWrite([
          SparseMembers.update({ memberId: "m-1" }).pipe(Entity.set({ email: "taken@x.io" })),
          UserEntity.put({ userId: "u-1", email: "a@x.io", name: "A", role: "admin" }),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("UniqueConstraintViolation")
        const violation = error as UniqueConstraintViolation
        expect(violation.entityType).toBe("SparseMember")
        expect(violation.constraint).toBe("email")
        expect(violation.fields).toEqual({ email: "taken@x.io" })
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a transaction of only no-op updates sends nothing", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async () => ({ Item: sparseRow("a@x.io") }))

        yield* Transaction.transactWrite([
          SparseMembers.update({ memberId: "m-1" }).pipe(Entity.set({})),
        ])

        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("rejects cascade — EDD-9059", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          RefProjects.update({ projectId: "p-1" }).pipe(
            Entity.set({ projectName: "Renamed" }),
            Entity.cascade({ targets: [RefTasks] }),
          ),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("[EDD-9059] cascade")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("rejects an update returning an item — EDD-9060", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          UserEntity.update({ userId: "u-1" }).pipe(
            Entity.set({ name: "Renamed" }),
            Entity.returnValues("allOld"),
          ),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("[EDD-9060] returnValues")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("rejects an update of a vector-indexed entity — EDD-9061", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          VectorDocs.update({ docId: "d-1" }).pipe(Entity.set({ body: "new" })),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("[EDD-9061]")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )
  })

  describe("planned ops follow the standalone op's own paths", () => {
    it.effect("a complete update of a missing row is planned as its create", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          UserEntity.update({ userId: "u-new" }).pipe(
            // Every required field and key composite: what `put` would write.
            Entity.set({ userId: "u-new", email: "n@x.io", name: "New", role: "member" }),
          ),
        ])

        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        // The standalone update's guarded write would find the row missing and
        // fall back to create — so the transaction writes the create.
        expect(items).toHaveLength(1)
        expect(items[0].Put).toBeDefined()
        expect(fromAttributeMap(items[0].Put.Item).userId).toBe("u-new")
        expect(items[0].Put.ConditionExpression).toContain("attribute_not_exists")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("an incomplete update of a missing row fails ItemNotFound before sending", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          UserEntity.update({ userId: "u-new" }).pipe(Entity.set({ name: "New" })),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ItemNotFound")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a patch of a missing row fails ConditionalCheckFailed before sending", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          UserEntity.patch({ userId: "u-new" }).pipe(Entity.set({ name: "New" })),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ConditionalCheckFailed")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a stale expectedVersion on the plain path is refused before sending", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async () => ({
          Item: toAttributeMap({
            pk: "$myapp#v1#versionednote#noteid_n-1",
            sk: "$myapp#v1#versionednote",
            __edd_e__: "VersionedNote",
            noteId: "n-1",
            body: "b",
            version: 3,
          }),
        }))

        const error = yield* Transaction.transactWrite([
          VersionedNotes.update({ noteId: "n-1" }).pipe(
            Entity.set({ body: "b2" }),
            Entity.expectedVersion(2),
          ),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("OptimisticLockError")
        expect((error as OptimisticLockError).actualVersion).toBe(3)
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("deleteIfExists' own guard is not a caller condition: a race is retried", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async (input: any) =>
          fromAttributeMap(input.Key).sk === "$myapp#v1#lifecyclemember"
            ? {
                Item: toAttributeMap({
                  pk: "$myapp#v1#lifecyclemember#memberid_m-1",
                  sk: "$myapp#v1#lifecyclemember",
                  __edd_e__: "LifecycleMember",
                  memberId: "m-1",
                  email: "a@x.io",
                  label: "L",
                  version: 1,
                }),
              }
            : {},
        )
        // First attempt: the row changed under the guard (it is still there).
        mockTransactWriteItems.mockImplementationOnce(async (input: any) => {
          const error = new Error("cancelled")
          ;(error as any).name = "TransactionCanceledException"
          ;(error as any).CancellationReasons = input.TransactItems.map((i: any) =>
            i.Delete && fromAttributeMap(i.Delete.Key).sk === "$myapp#v1#lifecyclemember"
              ? { Code: "ConditionalCheckFailed", Item: toAttributeMap({ version: 2 }) }
              : { Code: "None" },
          )
          throw error
        })
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([LifecycleMembers.deleteIfExists({ memberId: "m-1" })])

        expect(mockTransactWriteItems).toHaveBeenCalledTimes(2)
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a unique value reached by .add() is reported with its new value", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async (input: any) =>
          fromAttributeMap(input.Key).sk === "$myapp#v1#ticket"
            ? {
                Item: toAttributeMap({
                  pk: "$myapp#v1#ticket#ticketid_t-1",
                  sk: "$myapp#v1#ticket",
                  __edd_e__: "Ticket",
                  ticketId: "t-1",
                  seq: 5,
                }),
              }
            : {},
        )
        mockTransactWriteItems.mockImplementationOnce(async (input: any) => {
          const error = new Error("cancelled")
          ;(error as any).name = "TransactionCanceledException"
          ;(error as any).CancellationReasons = input.TransactItems.map((i: any) =>
            i.Put && fromAttributeMap(i.Put.Item).__edd_e__ === "Ticket._unique.seq"
              ? { Code: "ConditionalCheckFailed" }
              : { Code: "None" },
          )
          throw error
        })

        const error = yield* Transaction.transactWrite([
          Tickets.update({ ticketId: "t-1" }).pipe(Entity.add({ seq: 1 })),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("UniqueConstraintViolation")
        // 5 + 1, in the library's serialized (padded) form — not `{}`.
        expect((error as UniqueConstraintViolation).fields).toEqual({ seq: "0000000000000006" })
      }).pipe(Effect.provide(TestLayer)),
    )
  })

  describe("a cancelled planned write is judged like a guarded put", () => {
    const userRow = (role: string) =>
      toAttributeMap({
        pk: "$myapp#v1#user#userid_u-1",
        sk: "$myapp#v1#user",
        __edd_e__: "User",
        userId: "u-1",
        email: "a@x.io",
        name: "A",
        role,
      })
    /** Cancel every attempt at the op's own row, returning `stored` as ALL_OLD. */
    const cancelMainWith = (stored: Record<string, unknown> | undefined) =>
      mockTransactWriteItems.mockImplementation(async (input: any) => {
        const error = new Error("cancelled")
        ;(error as any).name = "TransactionCanceledException"
        // On the plain path the op's own row is the transaction's one Update.
        ;(error as any).CancellationReasons = input.TransactItems.map((i: any) =>
          i.Update
            ? { Code: "ConditionalCheckFailed", ...(stored ? { Item: stored } : {}) }
            : { Code: "None" },
        )
        throw error
      })

    it.effect(
      "the caller's condition rejecting an unchanged row is TransactionCancelled, sent once",
      () =>
        Effect.gen(function* () {
          mockGetItem.mockImplementation(async () => ({ Item: userRow("admin") }))
          cancelMainWith(userRow("admin"))

          const error = yield* Transaction.transactWrite([
            UserEntity.update({ userId: "u-1" }).pipe(
              Entity.set({ name: "Renamed" }),
              UserEntity.condition({ role: "member" }),
            ),
          ]).pipe(Effect.flip)

          expect(error._tag).toBe("TransactionCancelled")
          // Not retried: nothing raced, so a fresh plan could only fail the same way.
          expect(mockTransactWriteItems).toHaveBeenCalledTimes(1)
        }).pipe(Effect.provide(TestLayer)),
    )

    it.effect(
      "a conditioned attribute changed under the read is a race, not the caller's condition",
      () =>
        Effect.gen(function* () {
          mockGetItem.mockImplementation(async () => ({ Item: userRow("admin") }))
          // Every attempt finds `role` changed since the read: a race each time.
          cancelMainWith(userRow("member"))

          const error = yield* Transaction.transactWrite([
            UserEntity.update({ userId: "u-1" }).pipe(
              Entity.set({ name: "Renamed" }),
              UserEntity.condition({ role: "admin" }),
            ),
          ]).pipe(Effect.flip)

          expect(error._tag).toBe("ConcurrentModification")
          expect((error as any).attributes).toContain("role")
          expect(mockTransactWriteItems.mock.calls.length).toBeGreaterThan(1)
        }).pipe(Effect.provide(TestLayer)),
    )

    it.effect(
      "a versioned race lost on every attempt is OptimisticLockError with both versions",
      () =>
        Effect.gen(function* () {
          const note = (version: number) =>
            toAttributeMap({
              pk: "$myapp#v1#versionednote#noteid_n-1",
              sk: "$myapp#v1#versionednote",
              __edd_e__: "VersionedNote",
              noteId: "n-1",
              body: "b",
              version,
            })
          mockGetItem.mockImplementation(async () => ({ Item: note(3) }))
          cancelMainWith(note(4))

          const error = yield* Transaction.transactWrite([
            VersionedNotes.update({ noteId: "n-1" }).pipe(Entity.set({ body: "b2" })),
          ]).pipe(Effect.flip)

          expect(error._tag).toBe("OptimisticLockError")
          expect((error as OptimisticLockError).expectedVersion).toBe(3)
          expect((error as OptimisticLockError).actualVersion).toBe(4)
        }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("a retain snapshot the history already holds fails historyConflict, sent once", () =>
      Effect.gen(function* () {
        mockGetItem.mockImplementation(async (input: any) =>
          fromAttributeMap(input.Key).sk === "$myapp#v1#lifecyclemember"
            ? {
                Item: toAttributeMap({
                  pk: "$myapp#v1#lifecyclemember#memberid_m-1",
                  sk: "$myapp#v1#lifecyclemember",
                  __edd_e__: "LifecycleMember",
                  memberId: "m-1",
                  email: "a@x.io",
                  label: "L",
                  version: 2,
                }),
              }
            : {},
        )
        mockTransactWriteItems.mockImplementation(async (input: any) => {
          const error = new Error("cancelled")
          ;(error as any).name = "TransactionCanceledException"
          ;(error as any).CancellationReasons = input.TransactItems.map((i: any) =>
            i.Put && String(fromAttributeMap(i.Put.Item).sk).includes("#v#")
              ? { Code: "ConditionalCheckFailed" }
              : { Code: "None" },
          )
          throw error
        })

        const error = yield* Transaction.transactWrite([
          LifecycleMembers.update({ memberId: "m-1" }).pipe(Entity.set({ label: "L2" })),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("snapshot already exists")
        expect(mockTransactWriteItems).toHaveBeenCalledTimes(1)
      }).pipe(Effect.provide(TestLayer)),
    )
  })

  describe("unsupported ops are rejected, not silently reinterpreted (#100)", () => {
    const upsertInput = { userId: "u-1", email: "a@x.io", name: "Alice", role: "admin" } as const

    it.effect("rejects upsert — it is an UpdateItem with if_not_exists, not a Put", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([UserEntity.upsert(upsertInput)]).pipe(
          Effect.flip,
        )

        expect(error._tag).toBe("ValidationError")
        const ve = error as ValidationError
        expect(ve.entityType).toBe("User")
        expect(String(ve.cause)).toContain("upsert")
        expect(String(ve.cause)).toContain("if_not_exists")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("rejects a bound upsert from db.entities.*", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { UserEntity, OrderEntity },
          tables: { MainTable },
        })

        const error = yield* Transaction.transactWrite([
          db.entities.UserEntity.upsert(upsertInput),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    // The put-kind tag rides on the op intermediate, and every combinator
    // reconstructs that intermediate. If one forgets to carry `_putKind`, an
    // upsert silently becomes a plain Put again — exactly the bug this closes.
    it.effect("`.condition()` on an upsert preserves the upsert rejection", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { UserEntity, OrderEntity },
          tables: { MainTable },
        })

        const error = yield* Transaction.transactWrite([
          db.entities.UserEntity.upsert(upsertInput).condition({ role: "admin" }),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("upsert")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("plain put and create are still accepted (the tag does not over-reject)", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([
          UserEntity.put(upsertInput),
          UserEntity.create({ ...upsertInput, userId: "u-2" }),
        ])

        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        expect(items).toHaveLength(2)
        expect(items[0].Put.ConditionExpression).toBeUndefined()
        expect(items[1].Put.ConditionExpression).toContain("attribute_not_exists")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("rejects an entity whose ref hydration this path cannot perform", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          RefTasks.put({ taskId: "t-1", title: "Land", projectId: "p-1" } as never),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("ref")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    // #120 — the gate is on the INPUT, not the entity configuration.
    //
    // `Entity.put` reaches `Crypto` only when the caller omitted the field
    // (`fillGeneratedId` returns the input untouched when it is present), so a
    // supplied id needs nothing this path lacks. Gating on the configuration
    // barred the entity outright and named a dependency that did not apply —
    // while the workaround the message pointed at (supply the id) was already
    // in effect.
    it.effect("rejects a generatedId entity when the id is OMITTED — that needs Crypto", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([GenDocs.put({ title: "t" } as never)]).pipe(
          Effect.flip,
        )

        expect(error._tag).toBe("ValidationError")
        const cause = String((error as ValidationError).cause)
        expect(cause).toContain("omitted generated id")
        // The message names the field and the way out, not a blanket ban.
        expect(cause).toContain("docId")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("accepts a generatedId entity when the caller SUPPLIES the id", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([GenDocs.put({ docId: "d-1", title: "t" })])

        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        expect(items).toHaveLength(1)
        // Written as given — the id composes into the key exactly as `put` does.
        expect(items[0].Put.Item.docId.S).toBe("d-1")
        expect(items[0].Put.Item.pk.S).toContain("docid_d-1")
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("an empty or null id still counts as omitted", () =>
      Effect.gen(function* () {
        const error = yield* Transaction.transactWrite([
          GenDocs.put({ docId: null, title: "t" } as never),
        ]).pipe(Effect.flip)

        expect(error._tag).toBe("ValidationError")
        expect(String((error as ValidationError).cause)).toContain("omitted generated id")
        expect(mockTransactWriteItems).not.toHaveBeenCalled()
      }).pipe(Effect.provide(TestLayer)),
    )

    it.effect("delete on a generatedId entity was never gated and still is not", () =>
      Effect.gen(function* () {
        mockTransactWriteItems.mockResolvedValueOnce({})

        yield* Transaction.transactWrite([GenDocs.delete({ docId: "d-1" })])

        const items = mockTransactWriteItems.mock.calls[0]![0].TransactItems
        expect(items).toHaveLength(1)
        expect(items[0].Delete.Key.pk.S).toContain("docid_d-1")
      }).pipe(Effect.provide(TestLayer)),
    )
  })
})
