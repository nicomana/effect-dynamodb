/**
 * Connected integration tests — runs against DynamoDB Local.
 *
 * Prerequisites:
 *   docker run -p 8000:8000 amazon/dynamodb-local
 *
 * Run:
 *   pnpm test:connected
 *
 * These tests exercise real DynamoDB behavior that mocks cannot validate:
 * pagination cursors, expression evaluation, conditional check failures,
 * batch unprocessed-item retry, transaction atomicity, and GSI propagation.
 */

import { it } from "@effect/vitest"
import {
  Cause,
  Config,
  Context,
  Data,
  DateTime,
  Duration,
  Effect,
  Equal,
  Exit,
  Layer,
  Option,
  Redacted,
  Schema,
  SchemaGetter,
  Stream,
} from "effect"
import { TestClock } from "effect/testing"
import { afterAll, beforeAll, beforeEach, describe, expect } from "vitest"

/**
 * Fixed instant for TTL/timestamp assertions. `it.effect` runs under a
 * `TestClock` frozen at epoch 0; the library's `DateTime.now`-backed TTLs are
 * therefore deterministic once the clock is advanced to a known instant, so
 * exact `frozen + duration` values can be asserted (2026-06-01T00:00:00Z).
 */
const FROZEN_MS = 1_780_272_000_000
const FROZEN_SECONDS = 1_780_272_000

import * as DynamoModel from "@effect-dynamodb/schema/DynamoModel.js"
import * as DynamoSchema from "@effect-dynamodb/schema/DynamoSchema.js"
import { Embedder } from "@effect-dynamodb/schema/Embedder.js"
import * as PureEntity from "@effect-dynamodb/schema/Entity.js"
import type {
  AdditionalItemConditionFailed,
  AppendTooLarge,
  DuplicateCommand,
  UniqueConstraintViolation,
  ValidationError,
  VersionConflict,
} from "@effect-dynamodb/schema/Errors.js"
import * as Aggregate from "../src/Aggregate.js"
import * as Batch from "../src/Batch.js"
import * as Collection from "../src/Collection.js"
import { DynamoClient, type DynamoClientService } from "../src/DynamoClient.js"
import * as Entity from "../src/Entity.js"
import * as EventStore from "../src/EventStore.js"
import * as Expression from "../src/Expression.js"
import { fromAttributeMap, toAttributeMap } from "../src/Marshaller.js"
import * as Query from "../src/Query.js"
import * as Table from "../src/Table.js"
import * as Transaction from "../src/Transaction.js"
import * as VectorSearchEmulation from "../src/VectorSearchEmulation.js"

// ---------------------------------------------------------------------------
// Skip if DynamoDB Local is not available
// ---------------------------------------------------------------------------

const ENDPOINT = Effect.runSync(
  Config.String("DYNAMODB_ENDPOINT").pipe(Config.withDefault("http://localhost:8000")),
)

let dynamoAvailable = false
try {
  const res = await fetch(ENDPOINT, { method: "POST", signal: AbortSignal.timeout(1000) }).catch(
    () => null,
  )
  dynamoAvailable = res !== null
} catch {
  dynamoAvailable = false
}

const describeConnected = dynamoAvailable ? describe : describe.skip

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

class User extends Schema.Class<User>("User")({
  userId: Schema.String,
  email: Schema.String,
  displayName: Schema.NonEmptyString,
  role: Schema.Literals(["admin", "member"]),
  bio: Schema.optional(Schema.String),
  createdBy: Schema.String,
}) {}

const UserModel = DynamoModel.configure(User, {
  createdBy: { immutable: true },
})

class Task extends Schema.Class<Task>("Task")({
  taskId: Schema.String,
  userId: Schema.String,
  title: Schema.NonEmptyString,
  status: Schema.Literals(["todo", "in-progress", "done"]),
  priority: Schema.Number,
  tags: Schema.optional(Schema.Array(Schema.String)),
}) {}

// ---------------------------------------------------------------------------
// Schema, Table, Entities
// ---------------------------------------------------------------------------

const AppSchema = DynamoSchema.make({ name: "connected-test", version: 1 })
const tableName = `connected-test-${Date.now()}`

const Users = Entity.make({
  model: UserModel,
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
    byEmail: {
      name: "gsi2",
      pk: { field: "gsi2pk", composite: ["email"] },
      sk: { field: "gsi2sk", composite: [] },
    },
  },
  unique: { email: ["email"] },
  timestamps: true,
  versioned: { retain: true },
})

const Tasks = Entity.make({
  model: Task,
  entityType: "Task",
  primaryKey: {
    pk: { field: "pk", composite: ["taskId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byUser: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["userId"] },
      sk: { field: "gsi1sk", composite: ["status", "taskId"] },
    },
  },
  timestamps: true,
  versioned: true,
  softDelete: true,
})

// Shared-PK join-table fixture — exercises the `.primary()` accessor where
// many items live under one partition key and are distinguished by SK.
class Membership extends Schema.Class<Membership>("Membership")({
  orgId: Schema.String,
  userId: Schema.String,
  role: Schema.Literals(["owner", "admin", "member"]),
  joinedAt: Schema.String,
}) {}

const Memberships = Entity.make({
  model: Membership,
  entityType: "Membership",
  primaryKey: {
    pk: { field: "pk", composite: ["orgId"] },
    sk: { field: "sk", composite: ["userId"] },
  },
})

// Sparse-unique fixture — issue #25. Two unique constraints reference optional
// model fields; omitting them must not synthesize a "_unique.<name>#undefined"
// sentinel that collides across records.
class Vehicle extends Schema.Class<Vehicle>("Vehicle")({
  vehicleId: Schema.String,
  accountId: Schema.String,
  name: Schema.NonEmptyString,
  deviceBinding: Schema.optional(Schema.String),
  transponderId: Schema.optional(Schema.String),
}) {}

const Vehicles = Entity.make({
  model: Vehicle,
  entityType: "Vehicle",
  primaryKey: {
    pk: { field: "pk", composite: ["vehicleId"] },
    sk: { field: "sk", composite: [] },
  },
  unique: {
    nameInAccount: ["accountId", "name"],
    deviceBinding: ["deviceBinding"],
    transponderId: ["transponderId"],
  },
  timestamps: true,
})

const MainTable = Table.make({
  schema: AppSchema,
  entities: { Users, Tasks, Memberships, Vehicles },
})

// ---------------------------------------------------------------------------
// Aggregate + Ref models
// ---------------------------------------------------------------------------

class Author extends Schema.Class<Author>("Author")({
  id: Schema.String.pipe(DynamoModel.identifier),
  name: Schema.String,
}) {}

class Comment extends Schema.Class<Comment>("Comment")({
  id: Schema.String,
  text: Schema.String,
  commenter: Schema.String,
}) {}

class PostMeta extends Schema.Class<PostMeta>("PostMeta")({
  summary: Schema.String,
  wordCount: Schema.Number,
}) {}

class BlogPost extends Schema.Class<BlogPost>("BlogPost")({
  id: Schema.String,
  title: Schema.String,
  author: Author.pipe(DynamoModel.ref),
  meta: PostMeta,
  comments: Schema.Array(Comment),
}) {}

// Entity with refs — article embeds a denormalized author
class Article extends Schema.Class<Article>("Article")({
  articleId: Schema.String,
  title: Schema.String,
  author: Author.pipe(DynamoModel.ref),
  status: Schema.Literals(["draft", "published"]),
}) {}

// Sub-aggregate models for discriminator testing
class ReviewerNote extends Schema.Class<ReviewerNote>("ReviewerNote")({
  reviewer: Author.pipe(DynamoModel.ref),
  rating: Schema.Number,
  text: Schema.String,
}) {}

const AggSchema = DynamoSchema.make({ name: "agg-test", version: 1 })

const aggTableName = `agg-test-${Date.now()}`

const Authors = Entity.make({
  model: Author,
  entityType: "Author",
  primaryKey: {
    pk: { field: "pk", composite: ["id"] },
    sk: { field: "sk", composite: [] },
  },
})

const Articles = Entity.make({
  model: Article,
  entityType: "Article",
  primaryKey: {
    pk: { field: "pk", composite: ["articleId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byAuthor: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["authorId"] },
      sk: { field: "gsi1sk", composite: ["articleId"] },
    },
  },
  refs: {
    author: { entity: Authors },
  },
})

// Officials: the #103 shape — one entity appearing twice in one aggregate,
// distinguished by a field on the edge element rather than by the ref.
class Official extends Schema.Class<Official>("Official")({
  officialId: Schema.String.pipe(DynamoModel.identifier),
  name: Schema.String,
}) {}

const Officials = Entity.make({
  model: Official,
  entityType: "Official",
  primaryKey: {
    pk: { field: "pk", composite: ["officialId"] },
    sk: { field: "sk", composite: [] },
  },
})

class MatchOfficial extends Schema.Class<MatchOfficial>("MatchOfficial")({
  official: Official,
  role: Schema.Literals(["onfield", "third", "referee"]),
}) {}

class OfficiatedMatch extends Schema.Class<OfficiatedMatch>("OfficiatedMatch")({
  id: Schema.String,
  name: Schema.String,
  officials: Schema.Array(MatchOfficial),
}) {}

const AggTable = Table.make({ schema: AggSchema, entities: { Authors, Articles, Officials } })

// Sub-aggregate: reviewer note (bound with discriminator for editorial vs peer)
const ReviewerNoteAggregate = Aggregate.make(ReviewerNote, {
  root: { entityType: "ReviewerNote" },
  edges: {
    reviewer: Aggregate.ref(Authors),
  },
})

const BlogPostAggregate = Aggregate.make(BlogPost, {
  table: AggTable,
  schema: AggSchema,
  pk: { field: "pk", composite: ["id"] },
  collection: {
    index: "gsi2",
    name: "blogpost",
    sk: { field: "gsi2sk", composite: ["title"] },
  },
  root: { entityType: "BlogPostRoot" },
  edges: {
    author: Aggregate.ref(Authors),
    meta: Aggregate.one("meta", { entityType: "BlogPostMeta" }),
    comments: Aggregate.many("comments", { entityType: "BlogPostComment" }),
  },
})

// The same umpire may officiate in more than one role. `sk.composite` names the
// element field that separates them; without it both rows compose one key (#103).
const OfficiatedMatchAggregate = Aggregate.make(OfficiatedMatch, {
  table: AggTable,
  schema: AggSchema,
  pk: { field: "pk", composite: ["id"] },
  collection: {
    index: "gsi2",
    name: "officiated",
    sk: { field: "gsi2sk", composite: ["name"] },
  },
  root: { entityType: "OfficiatedMatchRoot" },
  edges: {
    officials: Aggregate.many("officials", {
      entityType: "MatchOfficial",
      entity: Officials,
      // "onfield" seats two umpires and one umpire holds two appointments, so
      // neither half is unique alone.
      sk: { composite: ["role", "official.officialId"] },
    }),
  },
})

// Identical, minus the declared sort key — both elements collapse onto one row.
const CollidingMatchAggregate = Aggregate.make(OfficiatedMatch, {
  table: AggTable,
  schema: AggSchema,
  pk: { field: "pk", composite: ["id"] },
  collection: {
    index: "gsi2",
    name: "colliding",
    sk: { field: "gsi2sk", composite: ["name"] },
  },
  root: { entityType: "CollidingMatchRoot" },
  edges: {
    officials: Aggregate.many("officials", {
      entityType: "CollidingMatchOfficial",
      entity: Officials,
    }),
  },
})

// Aggregate with system timestamps (#98) — ElectroDB-shaped epoch-millis
// storage, the shape a downstream sync guards with
// `attribute_exists(updated) and updated < :updated`.
class TimestampedPost extends Schema.Class<TimestampedPost>("TimestampedPost")({
  id: Schema.String,
  title: Schema.String,
  meta: PostMeta,
  comments: Schema.Array(Comment),
}) {}

const TimestampedPostAggregate = Aggregate.make(TimestampedPost, {
  table: AggTable,
  schema: AggSchema,
  pk: { field: "pk", composite: ["id"] },
  collection: {
    index: "gsi2",
    name: "tspost",
    sk: { field: "gsi2sk", composite: ["title"] },
  },
  root: { entityType: "TsPostRoot" },
  edges: {
    meta: Aggregate.one("meta", { entityType: "TsPostMeta" }),
    comments: Aggregate.many("comments", { entityType: "TsPostComment" }),
  },
  timestamps: {
    created: { field: "created", schema: DynamoModel.DateEpochMs },
    updated: { field: "updated", schema: DynamoModel.DateEpochMs },
  },
})

// ---------------------------------------------------------------------------
// Shared Layer
// ---------------------------------------------------------------------------

const ClientLayer = DynamoClient.layer({
  region: "us-east-1",
  endpoint: ENDPOINT,
  credentials: { accessKeyId: "local", secretAccessKey: "local" },
})

const TestLayer = Layer.mergeAll(ClientLayer, MainTable.layer({ name: tableName }))
const AggTestLayer = Layer.mergeAll(ClientLayer, AggTable.layer({ name: aggTableName }))

const provide = Effect.provide(TestLayer)
const provideAgg = Effect.provide(AggTestLayer)

// ---------------------------------------------------------------------------
// Table setup / teardown
// ---------------------------------------------------------------------------

describeConnected("Connected integration tests", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: tableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(MainTable),
        })
      }).pipe(provide, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: tableName })
      }).pipe(
        provide,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // -------------------------------------------------------------------------
  // CRUD — put, get, create, update, delete
  // -------------------------------------------------------------------------

  describe("CRUD", () => {
    it.effect("put + get round-trip returns model fields", () =>
      Effect.gen(function* () {
        const created = yield* Users.put({
          userId: "u-crud-1",
          email: "crud1@test.com",
          displayName: "Crud One",
          role: "admin",
          createdBy: "test",
        }).asEffect()
        expect(created.userId).toBe("u-crud-1")
        expect(created.email).toBe("crud1@test.com")
        expect(created.role).toBe("admin")

        const fetched = yield* Users.get({ userId: "u-crud-1" }).asEffect()
        expect(fetched.userId).toBe("u-crud-1")
        expect(fetched.displayName).toBe("Crud One")
      }).pipe(provide),
    )

    it.effect("asRecord includes system fields", () =>
      Effect.gen(function* () {
        const record = yield* Users.get({ userId: "u-crud-1" }).pipe(Entity.asRecord)
        expect(record.version).toBeGreaterThanOrEqual(1)
        expect(record.createdAt).toBeDefined()
        expect(record.updatedAt).toBeDefined()
      }).pipe(provide),
    )

    it.effect("get non-existent item fails with ItemNotFound", () =>
      Effect.gen(function* () {
        const err = yield* Users.get({ userId: "u-nonexistent" }).asEffect().pipe(Effect.flip)
        expect(err._tag).toBe("ItemNotFound")
      }).pipe(provide),
    )

    it.effect("create fails on duplicate primary key", () =>
      Effect.gen(function* () {
        // Use Tasks (no unique constraints) to test create without the transact path
        yield* Tasks.create({
          taskId: "t-dup",
          userId: "u-dup",
          title: "Original",
          status: "todo",
          priority: 1,
        }).asEffect()
        const err = yield* Tasks.create({
          taskId: "t-dup",
          userId: "u-dup",
          title: "Duplicate",
          status: "todo",
          priority: 2,
        })
          .asEffect()
          .pipe(Effect.flip)
        expect(err._tag).toBe("ConditionalCheckFailed")
      }).pipe(provide),
    )

    it.effect("update modifies fields and increments version", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-upd",
          email: "upd@test.com",
          displayName: "Before",
          role: "member",
          createdBy: "test",
        }).asEffect()
        const before = yield* Users.get({ userId: "u-upd" }).pipe(Entity.asRecord)

        const after = yield* Users.update({ userId: "u-upd" }).pipe(
          Users.set({ displayName: "After" }),
          Entity.asRecord,
        )
        expect(after.displayName).toBe("After")
        expect(after.version).toBe(before.version + 1)
      }).pipe(provide),
    )

    it.effect("immutable field is preserved across updates", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-imm",
          email: "imm@test.com",
          displayName: "Immutable",
          role: "member",
          createdBy: "original-creator",
        }).asEffect()
        yield* Users.update({ userId: "u-imm" }).pipe(
          Users.set({ displayName: "Changed" }),
          Entity.asModel,
        )
        const fetched = yield* Users.get({ userId: "u-imm" }).asEffect()
        expect(fetched.createdBy).toBe("original-creator")
      }).pipe(provide),
    )

    it.effect("delete removes item", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-del",
          email: "del@test.com",
          displayName: "Delete Me",
          role: "member",
          createdBy: "test",
        }).asEffect()
        yield* Users.delete({ userId: "u-del" }).asEffect()
        const err = yield* Users.get({ userId: "u-del" }).asEffect().pipe(Effect.flip)
        expect(err._tag).toBe("ItemNotFound")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Decode modes — asModel, asRecord, asItem, asNative
  // -------------------------------------------------------------------------

  describe("Decode modes", () => {
    it.effect("asItem includes key fields and __edd_e__", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-modes",
          email: "modes@test.com",
          displayName: "Modes",
          role: "admin",
          createdBy: "test",
        }).asEffect()
        const item = yield* Users.get({ userId: "u-modes" }).pipe(Entity.asItem)
        expect(item.__edd_e__).toBe("User")
        expect(item.pk).toBeDefined()
        expect(item.sk).toBeDefined()
      }).pipe(provide),
    )

    it.effect("asNative returns AttributeValue format", () =>
      Effect.gen(function* () {
        const native = yield* Users.get({ userId: "u-modes" }).pipe(Entity.asNative)
        expect(native.pk).toHaveProperty("S")
        expect(native.__edd_e__).toHaveProperty("S")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Rich update operations
  // -------------------------------------------------------------------------

  describe("Rich updates", () => {
    it.effect("add increments numeric fields", () =>
      Effect.gen(function* () {
        yield* Tasks.put({
          taskId: "t-add",
          userId: "u-1",
          title: "Add Test",
          status: "todo",
          priority: 1,
        }).asEffect()
        yield* Tasks.update({ taskId: "t-add" }).pipe(Entity.add({ priority: 5 }), Entity.asModel)
        const task = yield* Tasks.get({ taskId: "t-add" }).asEffect()
        expect(task.priority).toBe(6)
      }).pipe(provide),
    )

    it.effect("subtract decrements numeric fields", () =>
      Effect.gen(function* () {
        yield* Tasks.put({
          taskId: "t-sub",
          userId: "u-1",
          title: "Sub Test",
          status: "todo",
          priority: 10,
        }).asEffect()
        yield* Tasks.update({ taskId: "t-sub" }).pipe(
          Entity.subtract({ priority: 3 }),
          Entity.asModel,
        )
        const task = yield* Tasks.get({ taskId: "t-sub" }).asEffect()
        expect(task.priority).toBe(7)
      }).pipe(provide),
    )

    it.effect("append adds to list fields", () =>
      Effect.gen(function* () {
        yield* Tasks.put({
          taskId: "t-list",
          userId: "u-1",
          title: "List Test",
          status: "todo",
          priority: 1,
          tags: ["initial"],
        }).asEffect()
        yield* Tasks.update({ taskId: "t-list" }).pipe(
          Entity.append({ tags: ["added"] }),
          Entity.asModel,
        )
        const task = yield* Tasks.get({ taskId: "t-list" }).asEffect()
        expect(task.tags).toEqual(["initial", "added"])
      }).pipe(provide),
    )

    it.effect("remove deletes attribute", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-rem",
          email: "rem@test.com",
          displayName: "Remove",
          role: "member",
          bio: "will be removed",
          createdBy: "test",
        }).asEffect()
        yield* Users.update({ userId: "u-rem" }).pipe(Entity.remove(["bio"]), Entity.asModel)
        const item = yield* Users.get({ userId: "u-rem" }).pipe(Entity.asItem)
        expect(item.bio).toBeUndefined()
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Optimistic locking
  // -------------------------------------------------------------------------

  describe("Optimistic locking", () => {
    it.effect("expectedVersion succeeds with correct version", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-lock",
          email: "lock@test.com",
          displayName: "Lock",
          role: "member",
          createdBy: "test",
        }).asEffect()
        const record = yield* Users.get({ userId: "u-lock" }).pipe(Entity.asRecord)
        const updated = yield* Users.update({ userId: "u-lock" }).pipe(
          Users.set({ displayName: "Locked Update" }),
          Users.expectedVersion(record.version),
          Entity.asRecord,
        )
        expect(updated.displayName).toBe("Locked Update")
        expect(updated.version).toBe(record.version + 1)
      }).pipe(provide),
    )

    it.effect("expectedVersion fails with wrong version", () =>
      Effect.gen(function* () {
        const err = yield* Users.update({ userId: "u-lock" }).pipe(
          Users.set({ displayName: "Stale" }),
          Users.expectedVersion(1),
          (op) => op.asEffect(),
          Effect.flip,
        )
        expect(err._tag).toBe("OptimisticLockError")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Unique constraints
  // -------------------------------------------------------------------------

  describe("Unique constraints", () => {
    it.effect("rejects duplicate email on create", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-uniq1",
          email: "unique@test.com",
          displayName: "Unique1",
          role: "member",
          createdBy: "test",
        }).asEffect()
        const err = yield* Users.put({
          userId: "u-uniq2",
          email: "unique@test.com",
          displayName: "Unique2",
          role: "member",
          createdBy: "test",
        })
          .asEffect()
          .pipe(Effect.flip)
        expect(err._tag).toBe("UniqueConstraintViolation")
      }).pipe(provide),
    )

    it.effect("update rotates sentinel when unique field changes", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-uniq-upd",
          email: "old-email@test.com",
          displayName: "Update Unique",
          role: "member",
          createdBy: "test",
        }).asEffect()

        // Update email — should rotate the sentinel atomically
        yield* Users.update({ userId: "u-uniq-upd" }).pipe(
          Users.set({ email: "new-email@test.com" }),
          Entity.asModel,
        )

        const updated = yield* Users.get({ userId: "u-uniq-upd" }).asEffect()
        expect(updated.email).toBe("new-email@test.com")
      }).pipe(provide),
    )

    it.effect("old email is released after update", () =>
      Effect.gen(function* () {
        // Another user can now claim the old email
        yield* Users.put({
          userId: "u-uniq-claim",
          email: "old-email@test.com",
          displayName: "Claimed Old",
          role: "member",
          createdBy: "test",
        }).asEffect()

        const claimed = yield* Users.get({ userId: "u-uniq-claim" }).asEffect()
        expect(claimed.email).toBe("old-email@test.com")
      }).pipe(provide),
    )

    it.effect("update to taken email fails with UniqueConstraintViolation", () =>
      Effect.gen(function* () {
        // u-uniq-upd has "new-email@test.com", try to update u-uniq-claim to the same
        const err = yield* Users.update({ userId: "u-uniq-claim" }).pipe(
          Users.set({ email: "new-email@test.com" }),
          (op) => op.asEffect(),
          Effect.flip,
        )
        expect(err._tag).toBe("UniqueConstraintViolation")

        // Original value unchanged
        const unchanged = yield* Users.get({ userId: "u-uniq-claim" }).asEffect()
        expect(unchanged.email).toBe("old-email@test.com")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Sparse unique constraints — issue #25
  //
  // End-to-end coverage of the four transition states for an entity with two
  // unique constraints on optional fields (deviceBinding, transponderId) and
  // one on a required compound (nameInAccount). Verifies against real DynamoDB
  // that no false collisions occur for unset fields and that sentinels rotate
  // correctly across undefined↔defined transitions.
  // -------------------------------------------------------------------------

  describe("Sparse unique constraints (issue #25)", () => {
    it.effect("two records with omitted optional unique fields both succeed", () =>
      Effect.gen(function* () {
        // Under the v1.3.1 bug, the second create here failed with a false
        // UniqueConstraintViolation on `deviceBinding` (and on `transponderId`).
        yield* Vehicles.create({
          vehicleId: "veh-sparse-1",
          accountId: "acct-sparse",
          name: "v1",
        }).asEffect()
        yield* Vehicles.create({
          vehicleId: "veh-sparse-2",
          accountId: "acct-sparse",
          name: "v2",
        }).asEffect()

        const v1 = yield* Vehicles.get({ vehicleId: "veh-sparse-1" }).asEffect()
        const v2 = yield* Vehicles.get({ vehicleId: "veh-sparse-2" }).asEffect()
        expect(v1.deviceBinding).toBeUndefined()
        expect(v2.deviceBinding).toBeUndefined()
      }).pipe(provide),
    )

    it.effect("required compound constraint still enforced when optional ones are sparse", () =>
      Effect.gen(function* () {
        const err = yield* Vehicles.create({
          vehicleId: "veh-sparse-3",
          accountId: "acct-sparse",
          name: "v1", // collides with veh-sparse-1
        })
          .asEffect()
          .pipe(Effect.flip)
        expect(err._tag).toBe("UniqueConstraintViolation")
        if (err._tag === "UniqueConstraintViolation") {
          expect(err.constraint).toBe("nameInAccount")
        }
      }).pipe(provide),
    )

    it.effect("update undefined → defined claims the sentinel (and blocks collisions)", () =>
      Effect.gen(function* () {
        yield* Vehicles.update({ vehicleId: "veh-sparse-1" }).pipe(
          Vehicles.set({ deviceBinding: "device-xyz" }),
          (op) => op.asEffect(),
        )

        const err = yield* Vehicles.update({ vehicleId: "veh-sparse-2" }).pipe(
          Vehicles.set({ deviceBinding: "device-xyz" }),
          (op) => op.asEffect(),
          Effect.flip,
        )
        expect(err._tag).toBe("UniqueConstraintViolation")
        if (err._tag === "UniqueConstraintViolation") {
          expect(err.constraint).toBe("deviceBinding")
          expect(err.fields).toEqual({ deviceBinding: "device-xyz" })
        }
      }).pipe(provide),
    )

    it.effect("update defined → undefined releases the sentinel for re-use", () =>
      Effect.gen(function* () {
        yield* Vehicles.update({ vehicleId: "veh-sparse-1" }).pipe(
          Entity.remove(["deviceBinding"]),
          (op) => op.asEffect(),
        )

        yield* Vehicles.update({ vehicleId: "veh-sparse-2" }).pipe(
          Vehicles.set({ deviceBinding: "device-xyz" }),
          (op) => op.asEffect(),
        )
        const v2 = yield* Vehicles.get({ vehicleId: "veh-sparse-2" }).asEffect()
        expect(v2.deviceBinding).toBe("device-xyz")
      }).pipe(provide),
    )

    it.effect("delete with undefined unique fields succeeds", () =>
      Effect.gen(function* () {
        yield* Vehicles.delete({ vehicleId: "veh-sparse-1" }).asEffect()
        const err = yield* Vehicles.get({ vehicleId: "veh-sparse-1" }).asEffect().pipe(Effect.flip)
        expect(err._tag).toBe("ItemNotFound")
      }).pipe(provide),
    )

    it.effect("name freed by delete is reusable", () =>
      Effect.gen(function* () {
        yield* Vehicles.create({
          vehicleId: "veh-sparse-4",
          accountId: "acct-sparse",
          name: "v1",
        }).asEffect()
        const v4 = yield* Vehicles.get({ vehicleId: "veh-sparse-4" }).asEffect()
        expect(v4.name).toBe("v1")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Conditional writes
  // -------------------------------------------------------------------------

  describe("Conditional writes", () => {
    it.effect("condition passes when expression is true", () =>
      Effect.gen(function* () {
        yield* Tasks.put({
          taskId: "t-cond",
          userId: "u-1",
          title: "Conditional",
          status: "todo",
          priority: 1,
        }).asEffect()
        // Use priority (non-GSI composite) to avoid GSI key recomposition issues
        const updated = yield* Tasks.update({ taskId: "t-cond" }).pipe(
          Tasks.set({ priority: 99 }),
          Tasks.condition({ status: "todo" }),
          Entity.asModel,
        )
        expect(updated.priority).toBe(99)
      }).pipe(provide),
    )

    it.effect("condition fails when expression is false", () =>
      Effect.gen(function* () {
        const err = yield* Tasks.update({ taskId: "t-cond" }).pipe(
          Tasks.set({ priority: 50 }),
          Tasks.condition({ status: "done" }),
          (op) => op.asEffect(),
          Effect.flip,
        )
        expect(err._tag).toBe("ConditionalCheckFailed")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // GSI queries
  // -------------------------------------------------------------------------

  describe("GSI queries", () => {
    it.effect("query returns items matching partition key", () =>
      Effect.gen(function* () {
        yield* Tasks.put({
          taskId: "t-q1",
          userId: "u-query",
          title: "Query 1",
          status: "todo",
          priority: 1,
        }).asEffect()
        yield* Tasks.put({
          taskId: "t-q2",
          userId: "u-query",
          title: "Query 2",
          status: "done",
          priority: 2,
        }).asEffect()
        yield* Tasks.put({
          taskId: "t-q3",
          userId: "u-other",
          title: "Other User",
          status: "todo",
          priority: 1,
        }).asEffect()

        const results = yield* Tasks.query.byUser({ userId: "u-query" }).pipe(Query.collect)
        expect(results).toHaveLength(2)
        expect(results.every((t) => t.userId === "u-query")).toBe(true)
      }).pipe(provide),
    )

    it.effect("query with sort key beginsWith filters correctly", () =>
      Effect.gen(function* () {
        // KeyComposer prefixes each composite value with its attribute name
        // (`status_<value>`), so the full SK prefix for Tasks with status=todo
        // under byUser (gsi1sk composite: ["status", "taskId"]) is
        // `$<schema>#v1#<entity>#status_<value>`.
        const todos = yield* Tasks.query.byUser({ userId: "u-query" }).pipe(
          Query.where({
            beginsWith: `$connected-test#v1#task#status_todo`,
          }),
          Query.collect,
        )
        expect(todos).toHaveLength(1)
        expect(todos[0]!.status).toBe("todo")
      }).pipe(provide),
    )

    it.effect("query reverse returns descending order", () =>
      Effect.gen(function* () {
        const results = yield* Tasks.query
          .byUser({ userId: "u-query" })
          .pipe(Query.reverse, Query.collect)
        expect(results).toHaveLength(2)
        // Reversed: 'todo' sorts after 'done', so reversed puts todo first
        expect(results[0]!.status).toBe("todo")
      }).pipe(provide),
    )

    it.effect("query with limit restricts the number of items returned", () =>
      Effect.gen(function* () {
        // Query.limit bounds the RESULT — one item comes back out of two.
        const page = yield* Tasks.query
          .byUser({ userId: "u-query" })
          .pipe(Query.limit(1), Query.execute)
        expect(page.items).toHaveLength(1)
      }).pipe(provide),
    )

    it.effect("query count returns total items", () =>
      Effect.gen(function* () {
        const count = yield* Tasks.query.byUser({ userId: "u-query" }).pipe(Query.count)
        expect(count).toBe(2)
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // `.condition()` widens the error channel — GH #102.
  //
  // A conditional write raises `ConditionalCheckFailed` at runtime; these tests
  // assert the standard idempotent-projector idiom
  // (`Effect.catchTag("ConditionalCheckFailed", ...)`) actually catches it, on
  // every surface `.condition()` is exposed. The type half — the tag being
  // present in the declared channel only after `.condition()` — is asserted in
  // `Entity.types.test.ts`, which `tsconfig.test.json` type-checks.
  // -------------------------------------------------------------------------

  describe(".condition() error channel (#102)", () => {
    const makeDb = DynamoClient.make({
      entities: { Memberships, Tasks, Vehicles },
      tables: { MainTable },
    })

    it.effect("put(...).condition(...) rejection is catchable by tag", () =>
      Effect.gen(function* () {
        const db = yield* makeDb
        const key = { orgId: "org-cond-102", userId: "u-put" }

        yield* db.entities.Memberships.put({ ...key, role: "owner", joinedAt: "2025-01-01" })

        const outcome = yield* db.entities.Memberships.put({
          ...key,
          role: "member",
          joinedAt: "2025-02-01",
        })
          .condition((t, { notExists }) => notExists(t.orgId))
          .asEffect()
          .pipe(
            Effect.as("written"),
            Effect.catchTag("ConditionalCheckFailed", () => Effect.succeed("redelivery")),
          )

        expect(outcome).toBe("redelivery")

        // The rejected write left the stored item untouched.
        const stored = yield* db.entities.Memberships.get(key)
        expect(stored.role).toBe("owner")
      }).pipe(provide),
    )

    it.effect("update(...).condition(...) rejection is catchable by tag", () =>
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.entities.Tasks.put({
          taskId: "t-cond-102",
          userId: "u-cond-102",
          title: "Original",
          status: "todo",
          priority: 1,
        })

        const outcome = yield* db.entities.Tasks.update({ taskId: "t-cond-102" })
          .set({ title: "Updated" })
          .condition({ status: "done" })
          .asEffect()
          .pipe(
            Effect.as("written"),
            Effect.catchTag("ConditionalCheckFailed", () => Effect.succeed("skipped")),
          )

        expect(outcome).toBe("skipped")

        const stored = yield* db.entities.Tasks.get({ taskId: "t-cond-102" })
        expect(stored.title).toBe("Original")
      }).pipe(provide),
    )

    it.effect("delete(...).condition(...) rejection is catchable by tag", () =>
      Effect.gen(function* () {
        const db = yield* makeDb
        const key = { orgId: "org-cond-102", userId: "u-delete" }
        yield* db.entities.Memberships.put({ ...key, role: "admin", joinedAt: "2025-01-01" })

        const outcome = yield* db.entities.Memberships.delete(key)
          .condition({ role: "owner" })
          .asEffect()
          .pipe(
            Effect.as("deleted"),
            Effect.catchTag("ConditionalCheckFailed", () => Effect.succeed("kept")),
          )

        expect(outcome).toBe("kept")

        const stored = yield* db.entities.Memberships.get(key)
        expect(stored.role).toBe("admin")
      }).pipe(provide),
    )

    it.effect("a satisfied condition still succeeds through the widened channel", () =>
      Effect.gen(function* () {
        const db = yield* makeDb
        const key = { orgId: "org-cond-102", userId: "u-ok" }

        const created = yield* db.entities.Memberships.put({
          ...key,
          role: "member",
          joinedAt: "2025-03-01",
        })
          .condition((t, { notExists }) => notExists(t.orgId))
          .asEffect()
          .pipe(Effect.catchTag("ConditionalCheckFailed", () => Effect.die("unexpected")))

        expect(created.role).toBe("member")
      }).pipe(provide),
    )

    it.effect(
      "unversioned unique-touching update reports the user condition, not a lock error",
      () =>
        // The unique-constraint transact path ANDs the version CAS with the user
        // condition on the main item. With no version attribute the user
        // condition is the ONLY predicate, so a rejection must surface as
        // ConditionalCheckFailed — reporting OptimisticLockError there would
        // name a version conflict that cannot exist.
        Effect.gen(function* () {
          const db = yield* makeDb
          yield* db.entities.Vehicles.put({
            vehicleId: "veh-cond-102",
            accountId: "acct-cond-102",
            name: "Original",
          })

          const err = yield* db.entities.Vehicles.update({ vehicleId: "veh-cond-102" })
            .set({ name: "Renamed" })
            .condition({ accountId: "acct-someone-else" })
            .asEffect()
            .pipe(Effect.flip)

          expect(err._tag).toBe("ConditionalCheckFailed")

          const stored = yield* db.entities.Vehicles.get({ vehicleId: "veh-cond-102" })
          expect(stored.name).toBe("Original")
        }).pipe(provide),
    )

    // -----------------------------------------------------------------------
    // The condition must reach DynamoDB on EVERY delete path, not just the
    // simple DeleteItem one. It used to be compiled and then dropped on the
    // soft-delete and unique-constraint transaction paths, so the guard never
    // shipped and the delete proceeded unconditionally.
    // -----------------------------------------------------------------------

    it.effect("soft-delete path: false condition keeps the item, raises the tag", () =>
      Effect.gen(function* () {
        const db = yield* makeDb
        const key = { taskId: "t-softcond-102" }
        yield* db.entities.Tasks.put({
          ...key,
          userId: "u-softcond",
          title: "Live",
          status: "todo",
          priority: 1,
        })

        const err = yield* db.entities.Tasks.delete(key)
          .condition({ status: "done" })
          .asEffect()
          .pipe(Effect.flip)
        expect(err._tag).toBe("ConditionalCheckFailed")

        // Nothing moved: the item is still live, and no tombstone was written.
        const live = yield* db.entities.Tasks.get(key)
        expect(live.title).toBe("Live")
        const tombstones = yield* db.entities.Tasks.deleted.list(key).collect()
        expect(tombstones).toHaveLength(0)
      }).pipe(provide),
    )

    it.effect("soft-delete path: true condition tombstones the item", () =>
      Effect.gen(function* () {
        const db = yield* makeDb
        const key = { taskId: "t-softcond-102-ok" }
        yield* db.entities.Tasks.put({
          ...key,
          userId: "u-softcond",
          title: "Doomed",
          status: "done",
          priority: 1,
        })

        yield* db.entities.Tasks.delete(key).condition({ status: "done" })

        const stillLive = yield* db.entities.Tasks.get(key).pipe(Effect.flip)
        expect(stillLive._tag).toBe("ItemNotFound")
        const tombstones = yield* db.entities.Tasks.deleted.list(key).collect()
        expect(tombstones).toHaveLength(1)
      }).pipe(provide),
    )

    it.effect("unique-constraint path: false condition rolls back the whole transaction", () =>
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.entities.Vehicles.put({
          vehicleId: "veh-delcond-102",
          accountId: "acct-delcond",
          name: "Keeper",
          transponderId: "tp-delcond-102",
        })

        const err = yield* db.entities.Vehicles.delete({ vehicleId: "veh-delcond-102" })
          .condition({ name: "SomethingElse" })
          .asEffect()
          .pipe(Effect.flip)
        expect(err._tag).toBe("ConditionalCheckFailed")

        const stored = yield* db.entities.Vehicles.get({ vehicleId: "veh-delcond-102" })
        expect(stored.name).toBe("Keeper")

        // The sentinel Deletes rode the same transaction, so they rolled back
        // too — the constraint must still be held.
        const violation = yield* db.entities.Vehicles.put({
          vehicleId: "veh-delcond-102-other",
          accountId: "acct-delcond",
          name: "Other",
          transponderId: "tp-delcond-102",
        })
          .asEffect()
          .pipe(Effect.flip)
        expect(violation._tag).toBe("UniqueConstraintViolation")
      }).pipe(provide),
    )

    it.effect("unique-constraint path: true condition deletes and releases sentinels", () =>
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.entities.Vehicles.put({
          vehicleId: "veh-delcond-102-ok",
          accountId: "acct-delcond-ok",
          name: "Doomed",
          transponderId: "tp-delcond-102-ok",
        })

        yield* db.entities.Vehicles.delete({ vehicleId: "veh-delcond-102-ok" }).condition({
          name: "Doomed",
        })

        const gone = yield* db.entities.Vehicles.get({ vehicleId: "veh-delcond-102-ok" }).pipe(
          Effect.flip,
        )
        expect(gone._tag).toBe("ItemNotFound")

        // Sentinel released — the transponderId is reusable.
        const reused = yield* db.entities.Vehicles.put({
          vehicleId: "veh-delcond-102-reuse",
          accountId: "acct-delcond-ok",
          name: "Reuse",
          transponderId: "tp-delcond-102-ok",
        })
        expect(reused.vehicleId).toBe("veh-delcond-102-reuse")
      }).pipe(provide),
    )

    it.effect("deleteIfExists on a soft-delete entity still tombstones (guard now ships)", () =>
      // `deleteIfExists` is `delete` carrying an `attribute_exists(pk)`
      // condition, so it rode the same drop. Now that the guard reaches the
      // transaction it also closes the read-then-write race that could
      // resurrect a concurrently-deleted item as a tombstone.
      Effect.gen(function* () {
        const db = yield* makeDb
        const key = { taskId: "t-delifexists-102" }
        yield* db.entities.Tasks.put({
          ...key,
          userId: "u-delifexists",
          title: "Doomed",
          status: "todo",
          priority: 1,
        })

        yield* db.entities.Tasks.deleteIfExists(key)

        const gone = yield* db.entities.Tasks.get(key).pipe(Effect.flip)
        expect(gone._tag).toBe("ItemNotFound")
        const tombstones = yield* db.entities.Tasks.deleted.list(key).collect()
        expect(tombstones).toHaveLength(1)

        // Second call: nothing live to delete.
        const err = yield* db.entities.Tasks.deleteIfExists(key).asEffect().pipe(Effect.flip)
        expect(["ItemNotFound", "ConditionalCheckFailed"]).toContain(err._tag)
      }).pipe(provide),
    )

    it.effect("purge() refuses a condition rather than silently dropping it", () =>
      Effect.gen(function* () {
        // `purge` is partition-wide and batched, so no per-item
        // ConditionExpression can guard it atomically. `.condition()` is
        // structurally reachable on it (purge returns an EntityDelete), so it
        // must fail loudly instead of ignoring the guard.
        yield* Vehicles.put({
          vehicleId: "veh-purgecond-102",
          accountId: "acct-purgecond",
          name: "Untouched",
        }).asEffect()

        const err = yield* Vehicles.purge({ vehicleId: "veh-purgecond-102" })
          .pipe(Vehicles.condition({ name: "Nope" }))
          .asEffect()
          .pipe(Effect.flip)
        expect(err._tag).toBe("ValidationError")

        const stored = yield* Vehicles.get({ vehicleId: "veh-purgecond-102" }).asEffect()
        expect(stored.name).toBe("Untouched")
      }).pipe(provide),
    )

    it.effect("unbound Entity.condition() pipeline is catchable by tag", () =>
      Effect.gen(function* () {
        yield* Memberships.put({
          orgId: "org-cond-102",
          userId: "u-unbound",
          role: "member",
          joinedAt: "2025-04-01",
        }).asEffect()

        const outcome = yield* Memberships.put({
          orgId: "org-cond-102",
          userId: "u-unbound",
          role: "owner",
          joinedAt: "2025-05-01",
        }).pipe(
          Memberships.condition((t, { notExists }) => notExists(t.orgId)),
          Entity.asModel,
          Effect.as("written"),
          Effect.catchTag("ConditionalCheckFailed", () => Effect.succeed("redelivery")),
        )

        expect(outcome).toBe("redelivery")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Primary index query accessor (.primary) — GH #2.
  //
  // Symmetric to GSI accessors: required PK composites, optional SK composites
  // (prefix-ordered). Exercises the shared-PK join-table pattern where many
  // items live under one partition key and are distinguished by SK.
  // -------------------------------------------------------------------------

  describe(".primary() accessor (bound client)", () => {
    // Use unique orgIds per describe block so we don't collide with other
    // describe blocks that happen to reuse `Memberships`. Each primary test
    // seeds its own partition explicitly; no cross-test dependencies.
    const seedAcmeMemberships = Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Memberships },
        tables: { MainTable },
      })
      yield* db.entities.Memberships.put({
        orgId: "org-acme",
        userId: "u-alice",
        role: "owner",
        joinedAt: "2025-01-01",
      })
      yield* db.entities.Memberships.put({
        orgId: "org-acme",
        userId: "u-bob",
        role: "admin",
        joinedAt: "2025-02-01",
      })
      yield* db.entities.Memberships.put({
        orgId: "org-acme",
        userId: "u-carol",
        role: "member",
        joinedAt: "2025-03-01",
      })
      yield* db.entities.Memberships.put({
        orgId: "org-other",
        userId: "u-alice",
        role: "admin",
        joinedAt: "2025-01-15",
      })
    })

    it.effect("lists all items under a shared primary partition key", () =>
      Effect.gen(function* () {
        yield* seedAcmeMemberships

        const db = yield* DynamoClient.make({
          entities: { Memberships },
          tables: { MainTable },
        })

        const acmeMembers = yield* db.entities.Memberships.primary({
          orgId: "org-acme",
        }).collect()

        expect(acmeMembers).toHaveLength(3)
        expect(acmeMembers.every((m) => m.orgId === "org-acme")).toBe(true)
        expect(acmeMembers.map((m) => m.userId).sort()).toEqual(["u-alice", "u-bob", "u-carol"])

        // Cross-partition isolation — org-other should not leak into org-acme.
        const otherMembers = yield* db.entities.Memberships.primary({
          orgId: "org-other",
        }).collect()
        expect(otherMembers).toHaveLength(1)
        expect(otherMembers[0]!.userId).toBe("u-alice")
      }).pipe(provide),
    )

    it.effect("PK + full SK composite narrows to a single item", () =>
      Effect.gen(function* () {
        yield* seedAcmeMemberships

        const db = yield* DynamoClient.make({
          entities: { Memberships },
          tables: { MainTable },
        })

        const bobs = yield* db.entities.Memberships.primary({
          orgId: "org-acme",
          userId: "u-bob",
        }).collect()

        expect(bobs).toHaveLength(1)
        expect(bobs[0]!.role).toBe("admin")
      }).pipe(provide),
    )

    it.effect("chains .filter() to post-filter results by a non-key attribute", () =>
      Effect.gen(function* () {
        yield* seedAcmeMemberships

        const db = yield* DynamoClient.make({
          entities: { Memberships },
          tables: { MainTable },
        })

        const admins = yield* db.entities.Memberships.primary({ orgId: "org-acme" })
          .filter({ role: "admin" })
          .collect()

        expect(admins).toHaveLength(1)
        expect(admins[0]!.userId).toBe("u-bob")
      }).pipe(provide),
    )

    it.effect("chains .limit() + .reverse() + .fetch() for paged descending reads", () =>
      Effect.gen(function* () {
        yield* seedAcmeMemberships

        const db = yield* DynamoClient.make({
          entities: { Memberships },
          tables: { MainTable },
        })

        const page = yield* db.entities.Memberships.primary({ orgId: "org-acme" })
          .reverse()
          .limit(1)
          .fetch()

        expect(page.items).toHaveLength(1)
        // Reversed SK sort: u-carol sorts last ascending, so it comes first reversed.
        expect(page.items[0]!.userId).toBe("u-carol")
        expect(page.cursor).not.toBeNull()
      }).pipe(provide),
    )

    it.effect(".count() returns the number of items in the partition", () =>
      Effect.gen(function* () {
        yield* seedAcmeMemberships

        const db = yield* DynamoClient.make({
          entities: { Memberships },
          tables: { MainTable },
        })

        const count = yield* db.entities.Memberships.primary({ orgId: "org-acme" }).count()
        expect(count).toBe(3)
      }).pipe(provide),
    )

    it.effect("works for entities with no SK composites (single-item partitions)", () =>
      // Use Tasks — its primary key has `sk: { composite: [] }` (empty SK
      // composites) and `versioned: true` WITHOUT retain, so there are no
      // snapshot items polluting the partition.
      Effect.gen(function* () {
        yield* Tasks.put({
          taskId: "t-primary-single-1",
          userId: "u-primary-single",
          title: "Primary-only task",
          status: "todo",
          priority: 1,
        }).asEffect()

        const db = yield* DynamoClient.make({
          entities: { Tasks },
          tables: { MainTable },
        })

        const result = yield* db.entities.Tasks.primary({
          taskId: "t-primary-single-1",
        }).collect()
        expect(result).toHaveLength(1)
        expect(result[0]!.taskId).toBe("t-primary-single-1")
        expect(result[0]!.title).toBe("Primary-only task")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Pagination
  // -------------------------------------------------------------------------

  describe("Pagination", () => {
    it.effect("paginate yields all items across pages", () =>
      Effect.gen(function* () {
        for (let i = 0; i < 5; i++) {
          yield* Tasks.put({
            taskId: `t-page-${i}`,
            userId: "u-paginate",
            title: `Page Task ${i}`,
            status: "todo",
            priority: i,
          }).asEffect()
        }

        // pageSize(2) reads 2 rows per request — every item still comes back.
        const stream = yield* Tasks.query
          .byUser({ userId: "u-paginate" })
          .pipe(Query.pageSize(2), Query.paginate)

        const pages = yield* Stream.runCollect(stream)
        const allItems = Array.from(pages).flat()
        expect(allItems).toHaveLength(5)
      }).pipe(provide),
    )

    it.effect("paginate with limit stops the stream at n items", () =>
      Effect.gen(function* () {
        for (let i = 0; i < 5; i++) {
          yield* Tasks.put({
            taskId: `t-page-limit-${i}`,
            userId: "u-paginate-limit",
            title: `Page Task ${i}`,
            status: "todo",
            priority: i,
          }).asEffect()
        }

        // limit(2) bounds the RESULT — the stream ends after 2 items even
        // though 5 match.
        const stream = yield* Tasks.query
          .byUser({ userId: "u-paginate-limit" })
          .pipe(Query.limit(2), Query.paginate)

        const pages = yield* Stream.runCollect(stream)
        expect(Array.from(pages).flat()).toHaveLength(2)
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // limit vs pageSize (#105)
  //
  // `limit(n)`    — at most n items come back (a contract on RESULTS)
  // `pageSize(n)` — DynamoDB `Limit`, rows examined per request (ROUND TRIPS)
  //
  // Exercised against real DynamoDB because the interesting cases are the ones
  // mocks cannot reproduce: a filter rejecting whole pages, a request
  // over-reading past the limit, and DynamoDB accepting a cursor that the
  // library rebuilt from the last item it handed back.
  // -------------------------------------------------------------------------

  describe("limit vs pageSize", () => {
    const LIMIT_USER = "u-limit-pagesize"

    // priority 9 on the last two only — a filter on it is selective and,
    // unlike `status`, it is not part of the byUser sort key.
    const seedLimitTasks = Effect.gen(function* () {
      for (let i = 1; i <= 6; i++) {
        yield* Tasks.put({
          taskId: `t-lim-${String(i).padStart(2, "0")}`,
          userId: LIMIT_USER,
          title: `Limit task ${i}`,
          status: "todo",
          priority: i >= 5 ? 9 : 1,
        }).asEffect()
      }
    })

    const boundTasks = Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Tasks },
        tables: { MainTable },
      })
      return db.entities.Tasks
    })

    it.effect("collect: limit bounds the result, pageSize does not", () =>
      Effect.gen(function* () {
        yield* seedLimitTasks
        const tasks = yield* boundTasks

        const limited = yield* tasks.byUser({ userId: LIMIT_USER }).limit(4).collect()
        expect(limited).toHaveLength(4)

        const batched = yield* tasks.byUser({ userId: LIMIT_USER }).pageSize(2).collect()
        expect(batched).toHaveLength(6)

        const both = yield* tasks.byUser({ userId: LIMIT_USER }).pageSize(2).limit(5).collect()
        expect(both).toHaveLength(5)
      }).pipe(provide),
    )

    it.effect("collect: limit is satisfied across pages that the filter empties", () =>
      Effect.gen(function* () {
        yield* seedLimitTasks
        const tasks = yield* boundTasks

        // pageSize(1) with a filter only the last two items pass: the first
        // four requests come back empty and pagination must keep going.
        const highPriority = yield* tasks
          .byUser({ userId: LIMIT_USER })
          .filter((t, { gt }) => gt(t.priority, 5))
          .pageSize(1)
          .limit(2)
          .collect()

        expect(highPriority.map((t) => t.taskId)).toEqual(["t-lim-05", "t-lim-06"])
      }).pipe(provide),
    )

    it.effect("collect: a limit larger than the partition returns what exists", () =>
      Effect.gen(function* () {
        yield* seedLimitTasks
        const tasks = yield* boundTasks

        const all = yield* tasks.byUser({ userId: LIMIT_USER }).limit(100).collect()
        expect(all).toHaveLength(6)
      }).pipe(provide),
    )

    it.effect("fetch: an over-read page rebuilds a cursor DynamoDB accepts", () =>
      Effect.gen(function* () {
        yield* seedLimitTasks
        const tasks = yield* boundTasks

        // A filtered request cannot be sized, so it examines the whole
        // partition and accepts all 6 items while only 2 were asked for. The
        // cursor must resume after the 2nd — not after the 6th, which is where
        // LastEvaluatedKey would point.
        const first = yield* tasks
          .byUser({ userId: LIMIT_USER })
          .filter({ status: "todo" })
          .limit(2)
          .fetch()
        expect(first.items.map((t) => t.taskId)).toEqual(["t-lim-01", "t-lim-02"])
        expect(first.cursor).not.toBeNull()

        const second = yield* tasks
          .byUser({ userId: LIMIT_USER })
          .filter({ status: "todo" })
          .limit(2)
          .startFrom(first.cursor!)
          .fetch()
        expect(second.items.map((t) => t.taskId)).toEqual(["t-lim-03", "t-lim-04"])

        const third = yield* tasks
          .byUser({ userId: LIMIT_USER })
          .filter({ status: "todo" })
          .limit(2)
          .startFrom(second.cursor!)
          .fetch()
        expect(third.items.map((t) => t.taskId)).toEqual(["t-lim-05", "t-lim-06"])
        // Nothing left after the 6th item.
        expect(third.cursor).toBeNull()
      }).pipe(provide),
    )

    it.effect("fetch: limit accumulates across requests to fill the page", () =>
      Effect.gen(function* () {
        yield* seedLimitTasks
        const tasks = yield* boundTasks

        // Each request examines a single row, most of which the filter drops —
        // the page is still filled.
        const page = yield* tasks
          .byUser({ userId: LIMIT_USER })
          .filter((t, { gt }) => gt(t.priority, 5))
          .pageSize(1)
          .limit(2)
          .fetch()

        expect(page.items.map((t) => t.taskId)).toEqual(["t-lim-05", "t-lim-06"])
      }).pipe(provide),
    )

    it.effect("paginate: limit caps the stream, pageSize sizes the requests", () =>
      Effect.gen(function* () {
        yield* seedLimitTasks
        const tasks = yield* boundTasks

        const capped = yield* Stream.runCollect(
          tasks.byUser({ userId: LIMIT_USER }).limit(3).paginate(),
        )
        expect(Array.from(capped)).toHaveLength(3)

        const streamed = yield* Stream.runCollect(
          tasks.byUser({ userId: LIMIT_USER }).pageSize(2).paginate(),
        )
        expect(Array.from(streamed)).toHaveLength(6)
      }).pipe(provide),
    )

    it.effect("count: limit caps the count, pageSize does not", () =>
      Effect.gen(function* () {
        yield* seedLimitTasks
        const tasks = yield* boundTasks

        expect(yield* tasks.byUser({ userId: LIMIT_USER }).count()).toBe(6)
        expect(yield* tasks.byUser({ userId: LIMIT_USER }).pageSize(2).count()).toBe(6)
        expect(yield* tasks.byUser({ userId: LIMIT_USER }).limit(4).count()).toBe(4)
        expect(yield* tasks.byUser({ userId: LIMIT_USER }).limit(100).count()).toBe(6)
        expect(
          yield* tasks
            .byUser({ userId: LIMIT_USER })
            .filter((t, { gt }) => gt(t.priority, 5))
            .limit(1)
            .count(),
        ).toBe(1)
      }).pipe(provide),
    )

    it.effect("scan: limit bounds the result across the whole table", () =>
      Effect.gen(function* () {
        yield* seedLimitTasks
        const tasks = yield* boundTasks

        const scanned = yield* tasks.scan().limit(3).collect()
        expect(scanned).toHaveLength(3)

        const filtered = yield* tasks.scan().filter({ status: "todo" }).limit(2).collect()
        expect(filtered).toHaveLength(2)
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Scan
  // -------------------------------------------------------------------------

  describe("Scan", () => {
    it.effect("scan returns items of the entity type", () =>
      Effect.gen(function* () {
        const results = yield* Tasks.scan().pipe(Query.collect)
        expect(results.length).toBeGreaterThan(0)
        for (const t of results) {
          expect(t.taskId).toBeDefined()
          expect(t.title).toBeDefined()
        }
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Consistent reads
  // -------------------------------------------------------------------------

  describe("Consistent reads", () => {
    it.effect("consistentRead on get returns item", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-consist",
          email: "consist@test.com",
          displayName: "Consistent",
          role: "member",
          createdBy: "test",
        }).asEffect()
        const user = yield* Users.get({ userId: "u-consist" }).pipe(
          Entity.consistentRead(),
          Entity.asModel,
        )
        expect(user.userId).toBe("u-consist")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Projection
  // -------------------------------------------------------------------------

  describe("Projection", () => {
    it.effect("project returns only selected attributes", () =>
      Effect.gen(function* () {
        const result = yield* Users.get({ userId: "u-crud-1" }).pipe(
          Entity.project(["userId", "email"]),
        )
        expect(result.userId).toBe("u-crud-1")
        expect(result.email).toBe("crud1@test.com")
        expect(result.displayName).toBeUndefined()
      }).pipe(provide),
    )

    it.effect("Entity.select returns only selected attributes", () =>
      Effect.gen(function* () {
        const results = yield* Tasks.query
          .byUser({ userId: "u-query" })
          .pipe(Tasks.select(["taskId", "title"]), Query.collect)
        expect(results.length).toBeGreaterThan(0)
        for (const r of results) {
          expect(r.taskId).toBeDefined()
          expect(r.title).toBeDefined()
          expect(r.priority).toBeUndefined()
        }
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Batch operations
  // -------------------------------------------------------------------------

  describe("Batch", () => {
    it.effect("Batch.get fetches multiple items in one call", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-batch1",
          email: "batch1@test.com",
          displayName: "Batch1",
          role: "member",
          createdBy: "test",
        }).asEffect()
        yield* Users.put({
          userId: "u-batch2",
          email: "batch2@test.com",
          displayName: "Batch2",
          role: "admin",
          createdBy: "test",
        }).asEffect()

        const [u1, u2, u3] = yield* Batch.get([
          Users.get({ userId: "u-batch1" }),
          Users.get({ userId: "u-batch2" }),
          Users.get({ userId: "u-nonexistent-batch" }),
        ])
        expect(u1?.userId).toBe("u-batch1")
        expect(u2?.userId).toBe("u-batch2")
        expect(u3).toBeUndefined()
      }).pipe(provide),
    )

    it.effect("Batch.write puts and deletes in one call", () =>
      Effect.gen(function* () {
        yield* Batch.write([
          Tasks.put({
            taskId: "t-bw1",
            userId: "u-bw",
            title: "Batch Write 1",
            status: "todo",
            priority: 1,
          }),
          Tasks.put({
            taskId: "t-bw2",
            userId: "u-bw",
            title: "Batch Write 2",
            status: "done",
            priority: 2,
          }),
        ])
        const t1 = yield* Tasks.get({ taskId: "t-bw1" }).asEffect()
        const t2 = yield* Tasks.get({ taskId: "t-bw2" }).asEffect()
        expect(t1.title).toBe("Batch Write 1")
        expect(t2.title).toBe("Batch Write 2")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Transactions
  // -------------------------------------------------------------------------

  describe("Transactions", () => {
    it.effect("transactWrite creates multiple items atomically", () =>
      Effect.gen(function* () {
        yield* Transaction.transactWrite([
          Users.put({
            userId: "u-tx",
            email: "tx@test.com",
            displayName: "TxUser",
            role: "admin",
            createdBy: "test",
          }),
          Tasks.put({
            taskId: "t-tx",
            userId: "u-tx",
            title: "Tx Task",
            status: "todo",
            priority: 1,
          }),
        ])

        const user = yield* Users.get({ userId: "u-tx" }).asEffect()
        const task = yield* Tasks.get({ taskId: "t-tx" }).asEffect()
        expect(user.displayName).toBe("TxUser")
        expect(task.title).toBe("Tx Task")

        // #113 — `Users` carries `unique: { email }` + `versioned: { retain }`,
        // so this ONE caller op must have emitted three items: the row, its
        // uniqueness sentinel, and the v1 snapshot. Before #113 only the row
        // landed and the constraint was silently unenforced.
        const client = yield* DynamoClient
        const sentinel = yield* client.getItem({
          TableName: tableName,
          Key: {
            pk: { S: "$connected-test#v1#user.email#tx@test.com" },
            sk: { S: "$connected-test#v1#user.email" },
          },
        })
        expect(sentinel.Item?.__edd_e__?.S).toBe("User._unique.email")
        // The sentinel points back at the row it reserves for.
        expect(sentinel.Item?._entity_pk?.S).toBe("$connected-test#v1#user#userid_u-tx")

        // ...and the v1 retain snapshot, in the row's own partition.
        const snapshot = yield* client.getItem({
          TableName: tableName,
          Key: {
            pk: { S: "$connected-test#v1#user#userid_u-tx" },
            sk: { S: "$connected-test#v1#user#v#0000001" },
          },
        })
        expect(snapshot.Item?.displayName?.S).toBe("TxUser")
      }).pipe(provide),
    )

    it.effect("transactGet fetches multiple items atomically", () =>
      Effect.gen(function* () {
        const [user, task] = yield* Transaction.transactGet([
          Users.get({ userId: "u-tx" }),
          Tasks.get({ taskId: "t-tx" }),
        ])
        expect(user?.displayName).toBe("TxUser")
        expect(task?.title).toBe("Tx Task")
      }).pipe(provide),
    )

    it.effect("transactWrite with condition check rolls back on failure", () =>
      Effect.gen(function* () {
        const err = yield* Transaction.transactWrite([
          Users.put({
            userId: "u-tx-fail",
            email: "txfail@test.com",
            displayName: "Should Not Exist",
            role: "member",
            createdBy: "test",
          }),
          // Condition check: u-tx has role "admin", checking for "member" should fail
          Transaction.check(
            Users.get({ userId: "u-tx" }),
            Expression.condition({ eq: { role: "member" } }),
          ),
        ]).pipe(Effect.asVoid, Effect.flip)
        expect(err._tag).toBe("TransactionCancelled")

        // Verify the put was rolled back
        const result = yield* Users.get({ userId: "u-tx-fail" })
          .asEffect()
          .pipe(
            Effect.map(() => "exists"),
            Effect.catchTag("ItemNotFound", () => Effect.succeed("not found")),
          )
        expect(result).toBe("not found")
      }).pipe(provide),
    )

    it.effect("transactWrite enforces the unique constraint it now writes (#113)", () =>
      Effect.gen(function* () {
        // The sentinel from the previous test reserves tx@test.com. A second
        // write of the same email through the SAME path must now be refused —
        // before #113 it succeeded and left two rows sharing the value.
        const err = yield* Transaction.transactWrite([
          Users.put({
            userId: "u-tx-dup",
            email: "tx@test.com",
            displayName: "Duplicate",
            role: "member",
            createdBy: "test",
          }),
        ]).pipe(Effect.flip)

        expect(err._tag).toBe("UniqueConstraintViolation")
        const violation = err as UniqueConstraintViolation
        expect(violation.entityType).toBe("User")
        expect(violation.constraint).toBe("email")
        expect(violation.fields).toEqual({ email: "tx@test.com" })

        // All-or-nothing: the row must not exist.
        const result = yield* Users.get({ userId: "u-tx-dup" })
          .asEffect()
          .pipe(
            Effect.map(() => "exists"),
            Effect.catchTag("ItemNotFound", () => Effect.succeed("not found")),
          )
        expect(result).toBe("not found")
      }).pipe(provide),
    )

    const sentinelOwner = (email: string) =>
      Effect.gen(function* () {
        const client = yield* DynamoClient
        const sentinel = yield* client.getItem({
          TableName: tableName,
          Key: {
            pk: { S: `$connected-test#v1#user.email#${email}` },
            sk: { S: "$connected-test#v1#user.email" },
          },
        })
        return sentinel.Item?._entity_pk?.S
      })

    const taskExists = (taskId: string) =>
      Tasks.get({ taskId })
        .asEffect()
        .pipe(
          Effect.map(() => true),
          Effect.catchTag("ItemNotFound", () => Effect.succeed(false)),
        )

    const txTask = (taskId: string) =>
      Tasks.put({ taskId, userId: "u-tx", title: "Audit row", status: "todo", priority: 1 })

    it.effect("transactWrite updates a unique field and rotates its sentinel atomically", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-txu",
          email: "txu-old@test.com",
          displayName: "Before",
          role: "member",
          createdBy: "test",
        }).asEffect()

        yield* Transaction.transactWrite([
          Users.update({ userId: "u-txu" }).pipe(Entity.set({ email: "txu-new@test.com" })),
          txTask("t-txu"),
        ])

        const user = yield* Users.get({ userId: "u-txu" }).asEffect()
        expect(user.email).toBe("txu-new@test.com")
        expect(yield* taskExists("t-txu")).toBe(true)
        // The old value is released and the new one is reserved for this row.
        expect(yield* sentinelOwner("txu-old@test.com")).toBeUndefined()
        expect(yield* sentinelOwner("txu-new@test.com")).toBe(
          "$connected-test#v1#user#userid_u-txu",
        )
        // Released means reusable.
        yield* Users.put({
          userId: "u-txu-reuse",
          email: "txu-old@test.com",
          displayName: "Reuse",
          role: "member",
          createdBy: "test",
        }).asEffect()
      }).pipe(provide),
    )

    it.effect("a retain entity's transactional update bumps the version and snapshots it", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-txr",
          email: "txr@test.com",
          displayName: "v1",
          role: "member",
          createdBy: "test",
        }).asEffect()
        const before = yield* Users.get({ userId: "u-txr" }).pipe(Entity.asRecord)

        // displayName is not unique: the retain path alone forces the read-merge Put.
        yield* Transaction.transactWrite([
          Users.update({ userId: "u-txr" }).pipe(Entity.set({ displayName: "v2" })),
          txTask("t-txr"),
        ])

        const after = yield* Users.get({ userId: "u-txr" }).pipe(Entity.asRecord)
        expect(after.displayName).toBe("v2")
        expect(after.version).toBe(before.version + 1)
        expect(yield* taskExists("t-txr")).toBe(true)

        // The outgoing version was retained, exactly as a standalone update does.
        const client = yield* DynamoClient
        const snapshot = yield* client.getItem({
          TableName: tableName,
          Key: {
            pk: { S: "$connected-test#v1#user#userid_u-txr" },
            sk: {
              S: `$connected-test#v1#user#v#${String(before.version).padStart(7, "0")}`,
            },
          },
        })
        expect(snapshot.Item?.displayName?.S).toBe("v1")
      }).pipe(provide),
    )

    it.effect("a taken unique value on update rolls the whole transaction back", () =>
      Effect.gen(function* () {
        // tx@test.com is held by u-tx (first test in this block).
        const err = yield* Transaction.transactWrite([
          Users.update({ userId: "u-txu" }).pipe(Entity.set({ email: "tx@test.com" })),
          txTask("t-txu-rollback"),
        ]).pipe(Effect.flip)

        expect(err._tag).toBe("UniqueConstraintViolation")
        expect((err as UniqueConstraintViolation).fields).toEqual({ email: "tx@test.com" })

        // Nothing landed: the other write, the row and both sentinels are untouched.
        expect(yield* taskExists("t-txu-rollback")).toBe(false)
        const user = yield* Users.get({ userId: "u-txu" }).asEffect()
        expect(user.email).toBe("txu-new@test.com")
        expect(yield* sentinelOwner("txu-new@test.com")).toBe(
          "$connected-test#v1#user#userid_u-txu",
        )
        expect(yield* sentinelOwner("tx@test.com")).toBe("$connected-test#v1#user#userid_u-tx")
      }).pipe(provide),
    )

    it.effect("a stale expectedVersion on update refuses the transaction before sending", () =>
      Effect.gen(function* () {
        const current = yield* Users.get({ userId: "u-txu" }).pipe(Entity.asRecord)
        const err = yield* Transaction.transactWrite([
          Users.update({ userId: "u-txu" }).pipe(
            Entity.set({ displayName: "Stale" }),
            Entity.expectedVersion(current.version - 1),
          ),
          txTask("t-txu-stale"),
        ]).pipe(Effect.flip)

        expect(err._tag).toBe("OptimisticLockError")
        expect(yield* taskExists("t-txu-stale")).toBe(false)
      }).pipe(provide),
    )

    it.effect("transactWrite deletes a unique entity and releases its sentinel", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-txd",
          email: "txd@test.com",
          displayName: "ToDelete",
          role: "member",
          createdBy: "test",
        }).asEffect()

        yield* Transaction.transactWrite([Users.delete({ userId: "u-txd" }), txTask("t-txd")])

        const gone = yield* Users.get({ userId: "u-txd" })
          .asEffect()
          .pipe(
            Effect.map(() => "exists"),
            Effect.catchTag("ItemNotFound", () => Effect.succeed("not found")),
          )
        expect(gone).toBe("not found")
        expect(yield* taskExists("t-txd")).toBe(true)
        // Before this path read the row, the sentinel was orphaned and the
        // value unusable forever.
        expect(yield* sentinelOwner("txd@test.com")).toBeUndefined()
      }).pipe(provide),
    )

    /**
     * Run `body` against a client that lets `concurrent` land between the
     * FIRST read and everything after it — the window a plan's read opens.
     */
    const raceAfterFirstRead = <A, E>(
      concurrent: Effect.Effect<unknown, unknown, DynamoClient | Table.TableConfig>,
      body: Effect.Effect<A, E, DynamoClient | Table.TableConfig>,
    ) =>
      Effect.gen(function* () {
        const real = yield* DynamoClient
        let fired = false
        return yield* body.pipe(
          Effect.provideService(DynamoClient, {
            ...real,
            getItem: (input) =>
              real.getItem(input).pipe(
                Effect.tap(() => {
                  if (fired) return Effect.void
                  fired = true
                  return concurrent.pipe(Effect.provideService(DynamoClient, real), Effect.orDie)
                }),
              ),
          }),
        )
      })

    it.effect(
      "a row changed between a delete's read and its write is planned again and deleted",
      () =>
        Effect.gen(function* () {
          yield* Users.put({
            userId: "u-race",
            email: "race-a@test.com",
            displayName: "Race",
            role: "member",
            createdBy: "test",
          }).asEffect()

          // The plan reads email A; a concurrent update then moves it to B. The
          // guarded delete is cancelled, and — the caller set no condition — the
          // transaction is planned again from a fresh read and written.
          yield* raceAfterFirstRead(
            Users.update({ userId: "u-race" })
              .pipe(Entity.set({ email: "race-b@test.com" }))
              .asEffect(),
            Transaction.transactWrite([Users.delete({ userId: "u-race" }), txTask("t-race")]),
          )

          const gone = yield* Users.get({ userId: "u-race" })
            .asEffect()
            .pipe(
              Effect.map(() => "exists"),
              Effect.catchTag("ItemNotFound", () => Effect.succeed("not found")),
            )
          expect(gone).toBe("not found")
          // B's sentinel is released, not orphaned — the delete was planned from B.
          expect(yield* sentinelOwner("race-a@test.com")).toBeUndefined()
          expect(yield* sentinelOwner("race-b@test.com")).toBeUndefined()
          expect(yield* taskExists("t-race")).toBe(true)
        }).pipe(provide),
    )

    it.effect("the same race under a caller condition cancels the transaction", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-race-c",
          email: "racec-a@test.com",
          displayName: "Race",
          role: "member",
          createdBy: "test",
        }).asEffect()

        const err = yield* raceAfterFirstRead(
          Users.update({ userId: "u-race-c" })
            .pipe(Entity.set({ email: "racec-b@test.com" }))
            .asEffect(),
          Transaction.transactWrite([
            Users.delete({ userId: "u-race-c" }).pipe(Users.condition({ role: "member" })),
            txTask("t-race-c"),
          ]),
        ).pipe(Effect.flip)

        // A cancelled guard cannot be told apart from the caller's condition,
        // so it is not retried — exactly as for a guarded put.
        expect(err._tag).toBe("TransactionCancelled")
        const user = yield* Users.get({ userId: "u-race-c" }).asEffect()
        expect(user.email).toBe("racec-b@test.com")
        expect(yield* sentinelOwner("racec-b@test.com")).toBe(
          "$connected-test#v1#user#userid_u-race-c",
        )
        expect(yield* taskExists("t-race-c")).toBe(false)
      }).pipe(provide),
    )

    it.effect("an unversioned update raced on an unrelated field commits and keeps it", () =>
      Effect.gen(function* () {
        yield* Vehicles.create({
          vehicleId: "veh-race",
          accountId: "acct-race",
          name: "before",
          deviceBinding: "dev-a",
        }).asEffect()

        // Renaming touches `nameInAccount`, so the plan reads the row. A
        // concurrent rebind lands in between. The update writes only what it
        // changes, so the rebind survives and nothing needs retrying.
        yield* raceAfterFirstRead(
          Vehicles.update({ vehicleId: "veh-race" })
            .pipe(Entity.set({ deviceBinding: "dev-b" }))
            .asEffect(),
          Transaction.transactWrite([
            Vehicles.update({ vehicleId: "veh-race" }).pipe(Entity.set({ name: "after" })),
            txTask("t-race-veh"),
          ]),
        )

        const vehicle = yield* Vehicles.get({ vehicleId: "veh-race" }).asEffect()
        expect(vehicle.name).toBe("after")
        expect(vehicle.deviceBinding).toBe("dev-b")
        expect(yield* taskExists("t-race-veh")).toBe(true)
      }).pipe(provide),
    )

    it.effect("the entity's own delete releases the sentinel it wrote (#113)", () =>
      Effect.gen(function* () {
        yield* Users.delete({ userId: "u-tx" })

        // The released email is reusable through the transact path, which proves
        // the sentinel really went away rather than being orphaned.
        yield* Transaction.transactWrite([
          Users.put({
            userId: "u-tx-reuse",
            email: "tx@test.com",
            displayName: "Reused",
            role: "member",
            createdBy: "test",
          }),
        ])
        const reused = yield* Users.get({ userId: "u-tx-reuse" }).asEffect()
        expect(reused.displayName).toBe("Reused")
      }).pipe(provide),
    )

    it.effect(
      "a softDelete entity's transact delete writes the tombstone, not a hard delete (#113)",
      () =>
        Effect.gen(function* () {
          // `Tasks` is softDelete. Before #113 this hard-deleted the row, losing
          // the tombstone; then it was refused (EDD-9048). It now runs the
          // entity's own delete with its write recorded, and writes that.
          yield* Tasks.put({
            taskId: "t-sd",
            userId: "u-sd",
            title: "Soft",
            status: "todo",
            priority: 1,
          }).asEffect()

          yield* Transaction.transactWrite([Tasks.delete({ taskId: "t-sd" })])

          // One row left in the partition: the tombstone, with GSI keys stripped.
          const client = yield* DynamoClient
          const partition = yield* client.query({
            TableName: tableName,
            KeyConditionExpression: "#pk = :pk",
            ExpressionAttributeNames: { "#pk": "pk" },
            ExpressionAttributeValues: { ":pk": { S: "$connected-test#v1#task#taskid_t-sd" } },
          })
          const rows = partition.Items ?? []
          expect(rows).toHaveLength(1)
          expect(rows[0]?.sk?.S).toContain("#deleted#")
          expect(rows[0]?.deletedAt?.S).toBeDefined()
          expect(rows[0]?.gsi1pk).toBeUndefined()
        }).pipe(provide),
    )

    it.effect("Batch.write refuses the lifecycle configs it cannot express (#113)", () =>
      Effect.gen(function* () {
        const uniqueErr = yield* Batch.write([
          Users.put({
            userId: "u-bw-unique",
            email: "bwunique@test.com",
            displayName: "BW",
            role: "member",
            createdBy: "test",
          }),
        ]).pipe(Effect.flip)
        expect(uniqueErr._tag).toBe("ValidationError")
        expect(String((uniqueErr as ValidationError).cause)).toContain("EDD-9049")

        const softErr = yield* Batch.write([Tasks.delete({ taskId: "t-bw-sd" })]).pipe(Effect.flip)
        expect(softErr._tag).toBe("ValidationError")
        expect(String((softErr as ValidationError).cause)).toContain("EDD-9049")

        // Nothing was written by the refused unique put.
        const result = yield* Users.get({ userId: "u-bw-unique" })
          .asEffect()
          .pipe(
            Effect.map(() => "exists"),
            Effect.catchTag("ItemNotFound", () => Effect.succeed("not found")),
          )
        expect(result).toBe("not found")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Version history (versioned: { retain: true })
  // -------------------------------------------------------------------------

  describe("Version history", () => {
    it.effect("getVersion retrieves specific version snapshot", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-ver",
          email: "ver@test.com",
          displayName: "V1",
          role: "member",
          createdBy: "test",
        }).asEffect()
        yield* Users.update({ userId: "u-ver" }).pipe(
          Users.set({ displayName: "V2" }),
          Entity.asModel,
        )
        yield* Users.update({ userId: "u-ver" }).pipe(
          Users.set({ displayName: "V3" }),
          Entity.asModel,
        )

        const v1 = yield* Users.getVersion({ userId: "u-ver" }, 1).asEffect()
        const v2 = yield* Users.getVersion({ userId: "u-ver" }, 2).asEffect()
        const current = yield* Users.get({ userId: "u-ver" }).asEffect()

        expect(v1.displayName).toBe("V1")
        expect(v2.displayName).toBe("V2")
        expect(current.displayName).toBe("V3")
      }).pipe(provide),
    )

    it.effect("versions returns all snapshots", () =>
      Effect.gen(function* () {
        // put creates v1 snapshot; update #1 overwrites v1 snapshot; update #2 creates v2 snapshot
        const versions = yield* Users.versions({ userId: "u-ver" }).pipe(Query.collect)
        expect(versions.length).toBeGreaterThanOrEqual(2)
      }).pipe(provide),
    )

    it.effect("getVersion for non-existent version fails with ItemNotFound", () =>
      Effect.gen(function* () {
        const err = yield* Users.getVersion({ userId: "u-ver" }, 99).asEffect().pipe(Effect.flip)
        expect(err._tag).toBe("ItemNotFound")
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Soft delete + restore
  // -------------------------------------------------------------------------

  describe("Soft delete + restore", () => {
    it.effect("soft-deleted item vanishes from queries but is retrievable", () =>
      Effect.gen(function* () {
        yield* Tasks.put({
          taskId: "t-soft",
          userId: "u-soft",
          title: "Soft Delete Me",
          status: "todo",
          priority: 1,
        }).asEffect()

        const before = yield* Tasks.query.byUser({ userId: "u-soft" }).pipe(Query.collect)
        expect(before).toHaveLength(1)

        yield* Tasks.delete({ taskId: "t-soft" }).asEffect()

        const after = yield* Tasks.query.byUser({ userId: "u-soft" }).pipe(Query.collect)
        expect(after).toHaveLength(0)

        const deleted = yield* Tasks.deleted.get({ taskId: "t-soft" }).pipe(Entity.asRecord)
        expect(deleted.title).toBe("Soft Delete Me")
        expect((deleted as any).deletedAt).toBeDefined()
      }).pipe(provide),
    )

    it.effect("restore brings soft-deleted item back", () =>
      Effect.gen(function* () {
        yield* Tasks.restore({ taskId: "t-soft" }).asEffect()

        const restored = yield* Tasks.get({ taskId: "t-soft" }).asEffect()
        expect(restored.title).toBe("Soft Delete Me")

        const results = yield* Tasks.query.byUser({ userId: "u-soft" }).pipe(Query.collect)
        expect(results).toHaveLength(1)
      }).pipe(provide),
    )
  })

  // -------------------------------------------------------------------------
  // Purge
  // -------------------------------------------------------------------------

  describe("Purge", () => {
    it.effect("purge removes item, versions, and soft-deleted copies", () =>
      Effect.gen(function* () {
        yield* Users.put({
          userId: "u-purge",
          email: "purge@test.com",
          displayName: "Purge Me",
          role: "member",
          createdBy: "test",
        }).asEffect()
        yield* Users.update({ userId: "u-purge" }).pipe(
          Users.set({ displayName: "V2" }),
          Entity.asModel,
        )

        yield* Users.purge({ userId: "u-purge" }).asEffect()

        const err = yield* Users.get({ userId: "u-purge" }).asEffect().pipe(Effect.flip)
        expect(err._tag).toBe("ItemNotFound")

        const versions = yield* Users.versions({ userId: "u-purge" }).pipe(Query.collect)
        expect(versions).toHaveLength(0)
      }).pipe(provide),
    )
  })
})

// ===========================================================================
// Time-series Connected Tests (separate table; one current + N event items)
// ===========================================================================

class Telemetry extends Schema.Class<Telemetry>("Telemetry")({
  channel: Schema.String,
  deviceId: Schema.String,
  accountId: Schema.optional(Schema.String),
  timestamp: Schema.DateTimeUtc,
  location: Schema.optional(Schema.String),
  alert: Schema.optional(Schema.Boolean),
  gpio: Schema.optional(Schema.Number),
}) {}

const TelemetryAppendInput = Schema.Struct({
  channel: Schema.String,
  deviceId: Schema.String,
  timestamp: Schema.DateTimeUtc,
  location: Schema.optional(Schema.String),
  alert: Schema.optional(Schema.Boolean),
  gpio: Schema.optional(Schema.Number),
})

const tsSchema = DynamoSchema.make({ name: "ts-test", version: 1 })
const tsTableName = `ts-test-${Date.now()}`

const Telemetries = Entity.make({
  model: Telemetry,
  entityType: "Telemetry",
  primaryKey: {
    pk: { field: "pk", composite: ["channel", "deviceId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byAccount: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["accountId"] },
      sk: { field: "gsi1sk", composite: ["deviceId"] },
    },
  },
  timestamps: true,
  timeSeries: {
    orderBy: "timestamp",
    ttl: Duration.days(7),
    appendInput: TelemetryAppendInput,
  },
})

const TsTable = Table.make({ schema: tsSchema, entities: { Telemetries } })
const TsTestLayer = Layer.mergeAll(ClientLayer, TsTable.layer({ name: tsTableName }))
const provideTs = Effect.provide(TsTestLayer)

describeConnected("timeSeries integration tests", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: tsTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(TsTable),
        })
      }).pipe(provideTs, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: tsTableName })
      }).pipe(
        provideTs,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("append round-trip: current reflects latest orderBy", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      const t1 = DateTime.makeUnsafe("2026-04-22T10:00:00.000Z")
      const r = yield* db.entities.Telemetries.append({
        channel: "c-round",
        deviceId: "d-1",
        timestamp: t1,
        location: "rack-1",
      })
      expect(r.current.channel).toBe("c-round")

      const fetched = yield* db.entities.Telemetries.get({
        channel: "c-round",
        deviceId: "d-1",
      })
      expect(fetched.location).toBe("rack-1")
    }).pipe(provideTs),
  )

  it.effect("sequential monotone appends reflected in history", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      const t1 = DateTime.makeUnsafe("2026-04-22T10:00:00.000Z")
      const t2 = DateTime.makeUnsafe("2026-04-22T10:05:00.000Z")
      const t3 = DateTime.makeUnsafe("2026-04-22T10:10:00.000Z")

      for (const ts of [t1, t2, t3]) {
        const r = yield* db.entities.Telemetries.append({
          channel: "c-seq",
          deviceId: "d-1",
          timestamp: ts,
        })
        expect(r.current.channel).toBe("c-seq")
      }

      const history = yield* db.entities.Telemetries.history({
        channel: "c-seq",
        deviceId: "d-1",
      }).collect()
      expect(history).toHaveLength(3)

      // Event items keep the entity's type, but they are not items: a query of
      // the partition and a scan return the current item only.
      const key = { channel: "c-seq", deviceId: "d-1" }
      const current = yield* db.entities.Telemetries.primary(key).collect()
      expect(current.map((t) => DateTime.formatIso(t.timestamp))).toEqual([
        "2026-04-22T10:10:00.000Z",
      ])
      expect(yield* db.entities.Telemetries.primary(key).count()).toBe(1)
      const scanned = yield* db.entities.Telemetries.scan().filter({ channel: "c-seq" }).collect()
      expect(scanned).toHaveLength(1)
    }).pipe(provideTs),
  )

  it.effect("stale append: older orderBy fails with StaleAppend on the error channel", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      const newer = DateTime.makeUnsafe("2026-04-22T12:00:00.000Z")
      const older = DateTime.makeUnsafe("2026-04-22T11:00:00.000Z")

      yield* db.entities.Telemetries.append({
        channel: "c-stale",
        deviceId: "d-1",
        timestamp: newer,
      })

      const result = yield* db.entities.Telemetries.append({
        channel: "c-stale",
        deviceId: "d-1",
        timestamp: older,
      })
        .asEffect()
        .pipe(Effect.flip)

      expect(result._tag).toBe("StaleAppend")
      if (result._tag === "StaleAppend") {
        expect(Option.isSome(result.current)).toBe(true)
      }

      const history = yield* db.entities.Telemetries.history({
        channel: "c-stale",
        deviceId: "d-1",
      }).collect()
      expect(history).toHaveLength(1) // the winning event only
    }).pipe(provideTs),
  )

  it.effect("duplicate orderBy is strictly < (second fails with StaleAppend, no dup event)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      const same = DateTime.makeUnsafe("2026-04-22T13:00:00.000Z")

      yield* db.entities.Telemetries.append({
        channel: "c-dup",
        deviceId: "d-1",
        timestamp: same,
      })

      const result = yield* db.entities.Telemetries.append({
        channel: "c-dup",
        deviceId: "d-1",
        timestamp: same,
      })
        .asEffect()
        .pipe(Effect.flip)
      expect(result._tag).toBe("StaleAppend")

      const history = yield* db.entities.Telemetries.history({
        channel: "c-dup",
        deviceId: "d-1",
      }).collect()
      expect(history).toHaveLength(1)
    }).pipe(provideTs),
  )

  it.effect(
    "user-condition violation (CAS held) fails with ConditionalCheckFailed carrying current",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { Telemetries },
          tables: { TsTable },
        })

        // Seed an item with a known location.
        yield* db.entities.Telemetries.append({
          channel: "c-cond",
          deviceId: "d-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T10:00:00.000Z"),
          location: "rack-A",
        })

        // Attempt an append with a NEWER orderBy (CAS would hold) but a
        // condition that does not match the current.
        const result = yield* db.entities.Telemetries.append({
          channel: "c-cond",
          deviceId: "d-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T10:01:00.000Z"),
          location: "rack-B",
        })
          .condition({ eq: { location: "rack-Z" } })
          .asEffect()
          .pipe(Effect.flip)

        expect(result._tag).toBe("ConditionalCheckFailed")
        if (result._tag === "ConditionalCheckFailed") {
          // `current` is optional on `ConditionalCheckFailed` — absent when the
          // follow-up GetItem was skipped. Here it must be a Some.
          expect(result.current !== undefined && Option.isSome(result.current)).toBe(true)
        }
      }).pipe(provideTs),
  )

  it.effect("skipFollowUp on success: returns void", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      const result = yield* db.entities.Telemetries.append({
        channel: "c-skip",
        deviceId: "d-1",
        timestamp: DateTime.makeUnsafe("2026-04-22T10:00:00.000Z"),
      }).skipFollowUp()
      expect(result).toBeUndefined()

      // Round-trip — the row was written even though we didn't read it back.
      const fetched = yield* db.entities.Telemetries.get({
        channel: "c-skip",
        deviceId: "d-1",
      })
      expect(fetched.deviceId).toBe("d-1")
    }).pipe(provideTs),
  )

  it.effect("skipFollowUp on stale: fails with StaleAppend(current=Option.none)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      yield* db.entities.Telemetries.append({
        channel: "c-skip-stale",
        deviceId: "d-1",
        timestamp: DateTime.makeUnsafe("2026-04-22T12:00:00.000Z"),
      })

      const result = yield* db.entities.Telemetries.append({
        channel: "c-skip-stale",
        deviceId: "d-1",
        timestamp: DateTime.makeUnsafe("2026-04-22T11:00:00.000Z"),
      })
        .skipFollowUp()
        .asEffect()
        .pipe(Effect.flip)

      expect(result._tag).toBe("StaleAppend")
      if (result._tag === "StaleAppend") {
        expect(Option.isNone(result.current)).toBe(true)
      }
    }).pipe(provideTs),
  )

  it.effect("enrichment preservation: put sets accountId, append leaves it intact", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      const t0 = DateTime.makeUnsafe("2026-04-22T14:00:00.000Z")
      // Seed the current row using append — accountId is NOT in appendInput.
      // Attach accountId enrichment via .update().
      yield* db.entities.Telemetries.append({
        channel: "c-enrich",
        deviceId: "d-1",
        timestamp: t0,
        location: "rack-A",
      })
      yield* db.entities.Telemetries.update({ channel: "c-enrich", deviceId: "d-1" }).set({
        accountId: "acct-1",
      })

      // Append without accountId in input — must not overwrite enrichment.
      const t1 = DateTime.makeUnsafe("2026-04-22T14:05:00.000Z")
      yield* db.entities.Telemetries.append({
        channel: "c-enrich",
        deviceId: "d-1",
        timestamp: t1,
        location: "rack-B",
      })

      const fetched = yield* db.entities.Telemetries.get({
        channel: "c-enrich",
        deviceId: "d-1",
      })
      expect(fetched.accountId).toBe("acct-1")
      expect(fetched.location).toBe("rack-B")
    }).pipe(provideTs),
  )

  it.effect("GSI on current: byAccount query returns current item, not events", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      // Use .put() to set a complete row with accountId populated.
      yield* db.entities.Telemetries.put({
        channel: "c-gsi",
        deviceId: "d-1",
        timestamp: DateTime.makeUnsafe("2026-04-22T15:00:00.000Z"),
        accountId: "acct-gsi",
      })

      const later = DateTime.makeUnsafe("2026-04-22T15:05:00.000Z")
      yield* db.entities.Telemetries.append({
        channel: "c-gsi",
        deviceId: "d-1",
        timestamp: later,
      })

      const rows = yield* db.entities.Telemetries.byAccount({ accountId: "acct-gsi" }).collect()
      // One row — the current; events don't carry GSI keys.
      expect(rows).toHaveLength(1)
      expect(rows[0]!.deviceId).toBe("d-1")
    }).pipe(provideTs),
  )

  it.effect("history range via .where(between)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      // Seed 5 events over a ~1-hour window.
      const base = new Date("2026-04-22T16:00:00.000Z").getTime()
      for (let i = 0; i < 5; i++) {
        yield* db.entities.Telemetries.append({
          channel: "c-range",
          deviceId: "d-1",
          timestamp: DateTime.makeUnsafe(new Date(base + i * 15 * 60_000).toISOString()),
        })
      }

      // Window covers events 1..3 (indices 1,2,3 — three events).
      const from = new Date(base + 10 * 60_000).toISOString()
      const to = new Date(base + 50 * 60_000).toISOString()

      const window = yield* db.entities.Telemetries.history({ channel: "c-range", deviceId: "d-1" })
        .where((t, { between }) => between(t.timestamp, from, to))
        .collect()
      expect(window).toHaveLength(3)
    }).pipe(provideTs),
  )

  it.effect("_ttl attribute present on event items with sensible epoch value", () =>
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.millis(FROZEN_MS))
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      yield* db.entities.Telemetries.append({
        channel: "c-ttl",
        deviceId: "d-1",
        timestamp: DateTime.makeUnsafe("2026-04-22T17:00:00.000Z"),
      })

      const raw = yield* (yield* DynamoClient).query({
        TableName: tsTableName,
        KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :skPrefix)",
        ExpressionAttributeNames: { "#pk": "pk", "#sk": "sk" },
        ExpressionAttributeValues: {
          ":pk": { S: "$ts-test#v1#telemetry#channel_c-ttl#deviceid_d-1" },
          ":skPrefix": { S: "$ts-test#v1#telemetry#e#" },
        },
      })
      expect(raw.Items).toBeDefined()
      expect(raw.Items!.length).toBeGreaterThanOrEqual(1)
      const event = raw.Items![0]!
      const ttl = event._ttl?.N ? Number(event._ttl.N) : undefined
      expect(ttl).toBeDefined()
      // Clock-backed TTL is deterministic under TestClock: exactly frozen + 7 days.
      expect(ttl!).toBe(FROZEN_SECONDS + 7 * 86400)
    }).pipe(provideTs),
  )

  it.effect("concurrent appenders: final current = max(orderBy)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { Telemetries },
        tables: { TsTable },
      })

      const ts = (iso: string) => DateTime.makeUnsafe(iso)
      const times = [
        "2026-04-22T18:00:00.000Z",
        "2026-04-22T18:00:05.000Z",
        "2026-04-22T18:00:10.000Z",
      ]
      yield* Effect.all(
        times.map((iso) =>
          db.entities.Telemetries.append({
            channel: "c-concur",
            deviceId: "d-1",
            timestamp: ts(iso),
          })
            .asEffect()
            // Some concurrent attempts will lose the CAS; surface that as a
            // value so Effect.all doesn't short-circuit on StaleAppend.
            .pipe(Effect.catchTag("StaleAppend", () => Effect.void)),
        ),
        { concurrency: "unbounded" },
      )

      const current = yield* db.entities.Telemetries.get({
        channel: "c-concur",
        deviceId: "d-1",
      })
      // The latest timestamp is `18:00:10`.
      const iso = DateTime.formatIso(current.timestamp)
      expect(iso).toBe("2026-04-22T18:00:10.000Z")
    }).pipe(provideTs),
  )
})

// ===========================================================================
// timeSeries .append().remove() — atomic SET + REMOVE + CAS — closes #49
// ===========================================================================
//
// `.append(input).remove(attrs)` clears `appendInput` attributes in the same
// UpdateItem as the scoped SET and CAS predicate, closing the race window
// that the two-write workaround (.append() then .update().remove()) suffered
// from. Any GSI half whose composite list intersects the removed attribute
// cascades through `composeGsiKeysForUpdatePolicyAware` via `removedSet`.
//
// Fixture entity: sparse-PK GSI keyed on `alertState`, mirroring the issue
// #49 motivating IoT case. End-to-end: write with alertState, observe via
// byCurrentAlert; remove alertState, observe item drop from the GSI; item
// remains addressable via primary key.
// ===========================================================================

class TsAlertDevice extends Schema.Class<TsAlertDevice>("TsAlertDevice")({
  channel: Schema.String,
  deviceId: Schema.String,
  timestamp: Schema.DateTimeUtc,
  alertState: Schema.optional(Schema.String),
  location: Schema.optional(Schema.String),
  accountId: Schema.optional(Schema.String), // enrichment — not in appendInput
}) {}

const TsAlertDeviceAppendInput = Schema.Struct({
  channel: Schema.String,
  deviceId: Schema.String,
  timestamp: Schema.DateTimeUtc,
  alertState: Schema.optional(Schema.String),
  location: Schema.optional(Schema.String),
})

const tsAlertSchema = DynamoSchema.make({ name: "ts-alert-test", version: 1 })
const tsAlertTableName = `ts-alert-test-${Date.now()}`

const TsAlertDevices = Entity.make({
  model: TsAlertDevice,
  entityType: "TsAlertDevice",
  primaryKey: {
    pk: { field: "pk", composite: ["channel", "deviceId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    // Sparse-PK GSI on alertState — the issue #49 motivating shape.
    byCurrentAlert: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["alertState"] },
      sk: { field: "gsi1sk", composite: ["timestamp"] },
      indexPolicy: { pk: "sparse", sk: "preserve" },
    },
    // PK-composites-only GSI — must remain populated under .remove() of an
    // unrelated attribute (preserves the #43 fix under the new code path).
    byChannel: {
      name: "gsi2",
      pk: { field: "gsi2pk", composite: ["channel"] },
      sk: { field: "gsi2sk", composite: ["deviceId"] },
      indexPolicy: { pk: "preserve", sk: "preserve" },
    },
  },
  timeSeries: {
    orderBy: "timestamp",
    appendInput: TsAlertDeviceAppendInput,
  },
})

const TsAlertTable = Table.make({ schema: tsAlertSchema, entities: { TsAlertDevices } })
const TsAlertLayer = Layer.mergeAll(ClientLayer, TsAlertTable.layer({ name: tsAlertTableName }))
const provideTsAlert = Effect.provide(TsAlertLayer)

describeConnected("timeSeries .append().remove() integration tests (closes #49)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: tsAlertTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(TsAlertTable),
        })
      }).pipe(provideTsAlert, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: tsAlertTableName })
      }).pipe(
        provideTsAlert,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect(
    "round-trip: .remove() clears the attribute on the current item, item remains addressable",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { TsAlertDevices },
          tables: { TsAlertTable },
        })

        // 1. Initial append with alertState set.
        yield* db.entities.TsAlertDevices.append({
          channel: "ch-rt",
          deviceId: "d-rt-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T10:00:00.000Z"),
          alertState: "ACTIVE",
          location: "rack-1",
        })

        const beforeRemove = yield* db.entities.TsAlertDevices.get({
          channel: "ch-rt",
          deviceId: "d-rt-1",
        })
        expect(beforeRemove.alertState).toBe("ACTIVE")
        expect(beforeRemove.location).toBe("rack-1")

        // 2. Append with .remove(['alertState']) clears it atomically.
        yield* db.entities.TsAlertDevices.append({
          channel: "ch-rt",
          deviceId: "d-rt-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T10:05:00.000Z"),
          location: "rack-2",
        }).remove(["alertState"])

        const afterRemove = yield* db.entities.TsAlertDevices.get({
          channel: "ch-rt",
          deviceId: "d-rt-1",
        })
        // alertState absent (Schema.optional decodes to undefined).
        expect(afterRemove.alertState).toBeUndefined()
        // Other appendInput field unaffected.
        expect(afterRemove.location).toBe("rack-2")
        // orderBy updated.
        expect(DateTime.formatIso(afterRemove.timestamp)).toBe("2026-04-22T10:05:00.000Z")
      }).pipe(provideTsAlert),
  )

  it.effect(
    "sparse-PK GSI cascade: item drops from byCurrentAlert after .remove(['alertState'])",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { TsAlertDevices },
          tables: { TsAlertTable },
        })

        // 1. Initial append → item visible under alertState=ACTIVE.
        yield* db.entities.TsAlertDevices.append({
          channel: "ch-drop",
          deviceId: "d-drop-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T11:00:00.000Z"),
          alertState: "ACTIVE",
        })

        const beforeRows = yield* db.entities.TsAlertDevices.byCurrentAlert({
          alertState: "ACTIVE",
        }).collect()
        expect(beforeRows).toHaveLength(1)
        expect(beforeRows[0]!.deviceId).toBe("d-drop-1")

        // 2. .remove(['alertState']) cascades a drop on the byCurrentAlert PK
        //    half via `removedSet` → `'sparse'` → can't-compose → REMOVE.
        yield* db.entities.TsAlertDevices.append({
          channel: "ch-drop",
          deviceId: "d-drop-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T11:05:00.000Z"),
        }).remove(["alertState"])

        // 3. Item must no longer be visible to byCurrentAlert(alertState=ACTIVE).
        const afterRows = yield* db.entities.TsAlertDevices.byCurrentAlert({
          alertState: "ACTIVE",
        }).collect()
        expect(afterRows).toHaveLength(0)
      }).pipe(provideTsAlert),
  )

  it.effect(
    "PK-composites-only GSI preserved: byChannel still returns item after unrelated .remove()",
    () =>
      Effect.gen(function* () {
        // Removing `location` (not part of any GSI composite) must NOT
        // disturb the byChannel GSI — the #43 fix must hold under this path.
        const db = yield* DynamoClient.make({
          entities: { TsAlertDevices },
          tables: { TsAlertTable },
        })

        yield* db.entities.TsAlertDevices.append({
          channel: "ch-preserve",
          deviceId: "d-preserve-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T12:00:00.000Z"),
          location: "rack-X",
        })

        yield* db.entities.TsAlertDevices.append({
          channel: "ch-preserve",
          deviceId: "d-preserve-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T12:05:00.000Z"),
        }).remove(["location"])

        const rows = yield* db.entities.TsAlertDevices.byChannel({
          channel: "ch-preserve",
        }).collect()
        expect(rows).toHaveLength(1)
        expect(rows[0]!.deviceId).toBe("d-preserve-1")
        // location is gone from the item.
        expect(rows[0]!.location).toBeUndefined()
      }).pipe(provideTsAlert),
  )

  it.effect(
    "enrichment preservation: .remove(['alertState']) leaves out-of-appendInput accountId intact",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { TsAlertDevices },
          tables: { TsAlertTable },
        })

        // Seed via append with alert state.
        yield* db.entities.TsAlertDevices.append({
          channel: "ch-enrich",
          deviceId: "d-enrich-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T13:00:00.000Z"),
          alertState: "ACTIVE",
        })

        // Out-of-band enrichment via .update() — accountId is NOT in appendInput.
        yield* db.entities.TsAlertDevices.update({
          channel: "ch-enrich",
          deviceId: "d-enrich-1",
        }).set({ accountId: "acct-7" })

        // Append + remove alertState. accountId must survive — it is outside
        // appendInput, so .append() never touches it (enrichment-preservation
        // contract). .remove(['alertState']) operates on alertState only.
        yield* db.entities.TsAlertDevices.append({
          channel: "ch-enrich",
          deviceId: "d-enrich-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T13:05:00.000Z"),
        }).remove(["alertState"])

        const cur = yield* db.entities.TsAlertDevices.get({
          channel: "ch-enrich",
          deviceId: "d-enrich-1",
        })
        expect(cur.accountId).toBe("acct-7")
        expect(cur.alertState).toBeUndefined()
      }).pipe(provideTsAlert),
  )

  it.effect("stale .remove(): older orderBy → StaleAppend, REMOVE not applied", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { TsAlertDevices },
        tables: { TsAlertTable },
      })

      // Seed at t=14:00 with alertState=ACTIVE.
      yield* db.entities.TsAlertDevices.append({
        channel: "ch-stale",
        deviceId: "d-stale-1",
        timestamp: DateTime.makeUnsafe("2026-04-22T14:00:00.000Z"),
        alertState: "ACTIVE",
      })

      // Stale append with older orderBy + .remove() — CAS fires, transaction
      // rejected, alertState is NOT cleared.
      const result = yield* db.entities.TsAlertDevices.append({
        channel: "ch-stale",
        deviceId: "d-stale-1",
        timestamp: DateTime.makeUnsafe("2026-04-22T13:00:00.000Z"),
      })
        .remove(["alertState"])
        .asEffect()
        .pipe(Effect.flip)

      expect(result._tag).toBe("StaleAppend")

      const cur = yield* db.entities.TsAlertDevices.get({
        channel: "ch-stale",
        deviceId: "d-stale-1",
      })
      // alertState preserved — the failed remove did not land.
      expect(cur.alertState).toBe("ACTIVE")
    }).pipe(provideTsAlert),
  )

  it.effect(
    "validation: .remove() of an attribute outside appendInput fails fast with ValidationError",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { TsAlertDevices },
          tables: { TsAlertTable },
        })

        const err = yield* db.entities.TsAlertDevices.append({
          channel: "ch-val",
          deviceId: "d-val-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T15:00:00.000Z"),
        })
          .remove(["accountId"]) // accountId is NOT in appendInput
          .asEffect()
          .pipe(Effect.flip)

        expect(err._tag).toBe("ValidationError")
      }).pipe(provideTsAlert),
  )
})

// ===========================================================================
// PK-composites-only GSI shape — closes #43
// ===========================================================================
//
// The bug: GSIs whose composites are entirely entity-PK composites (e.g.
// `byChannel: { pk: [channel], sk: [deviceId] }` on an entity with
// `primaryKey: [channel, deviceId]`) had their `gsi*pk` / `gsi*sk` keys
// silently skipped under v1.7.0 / v1.7.1. Items written through `.append()`
// (and updates that didn't restate the PK composites in payload) were
// invisible to channel-scoped GSI queries.
//
// These tests exercise the v1.7.2 fix end-to-end against DDB Local: the
// items must be visible to byChannel queries after both `.append()` and
// `.update()` writes, and the GSI keys must be preserved across multiple
// updates (idempotent re-SET).
// ===========================================================================

class ChannelDevice extends Schema.Class<ChannelDevice>("ChannelDevice")({
  channel: Schema.String,
  deviceId: Schema.String,
  accountId: Schema.optional(Schema.String),
  alertState: Schema.optional(Schema.String),
  timestamp: Schema.DateTimeUtc,
  reading: Schema.optional(Schema.Number),
  otherField: Schema.optional(Schema.String),
}) {}

const ChannelDeviceAppendInput = Schema.Struct({
  channel: Schema.String,
  deviceId: Schema.String,
  timestamp: Schema.DateTimeUtc,
  reading: Schema.optional(Schema.Number),
})

const cdSchema = DynamoSchema.make({ name: "cd-test", version: 1 })
const cdTableName = `cd-test-${Date.now()}`

// PK-composites-only GSI shape — the #43 bug repro entity. Time-series
// because that was the originally-reported failure surface.
const ChannelDevicesTs = Entity.make({
  model: ChannelDevice,
  entityType: "ChannelDeviceTs",
  primaryKey: {
    pk: { field: "pk", composite: ["channel", "deviceId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byChannel: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["channel"] },
      sk: { field: "gsi1sk", composite: ["deviceId"] },
      indexPolicy: { pk: "preserve", sk: "preserve" },
    },
  },
  timestamps: true,
  timeSeries: {
    orderBy: "timestamp",
    ttl: Duration.days(7),
    appendInput: ChannelDeviceAppendInput,
  },
})

// Same shape but no time-series — exercises the standard `.update()` path.
const ChannelDevicesPlain = Entity.make({
  model: ChannelDevice,
  entityType: "ChannelDevicePlain",
  primaryKey: {
    pk: { field: "pk", composite: ["channel", "deviceId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byChannel: {
      name: "gsi2",
      pk: { field: "gsi2pk", composite: ["channel"] },
      sk: { field: "gsi2sk", composite: ["deviceId"] },
      indexPolicy: { pk: "preserve", sk: "preserve" },
    },
  },
  timestamps: true,
})

// Multi-writer entity: TWO GSIs — one PK-composites-only (byChannel on gsi3),
// one multi-writer with non-PK composites (byCurrentAlert on gsi4). Used to
// verify both behaviors coexist — PK-only fires every write (idempotent SET),
// multi-writer GSI is untouched by stamps.
const ChannelDevicesMixed = Entity.make({
  model: ChannelDevice,
  entityType: "ChannelDeviceMixed",
  primaryKey: {
    pk: { field: "pk", composite: ["channel", "deviceId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byChannel: {
      // PK-composites-only — must fire on every write (#43).
      name: "gsi3",
      pk: { field: "gsi3pk", composite: ["channel"] },
      sk: { field: "gsi3sk", composite: ["deviceId"] },
      indexPolicy: { pk: "preserve", sk: "preserve" },
    },
    byCurrentAlert: {
      // Multi-writer — neither composite is in the PK; stamp updates must
      // leave both halves untouched (v1.7.1 multi-writer fix preserved).
      name: "gsi4",
      pk: { field: "gsi4pk", composite: ["accountId"] },
      sk: { field: "gsi4sk", composite: ["alertState"] },
      indexPolicy: { pk: "preserve", sk: "preserve" },
    },
  },
  timestamps: true,
})

const CdTable = Table.make({
  schema: cdSchema,
  entities: { ChannelDevicesTs, ChannelDevicesPlain, ChannelDevicesMixed },
})
const CdLayer = Layer.mergeAll(ClientLayer, CdTable.layer({ name: cdTableName }))
const provideCd = Effect.provide(CdLayer)

describeConnected("PK-composites-only GSI shape (closes #43)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: cdTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(CdTable),
        })
      }).pipe(provideCd, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: cdTableName })
      }).pipe(
        provideCd,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect(
    "Entity.append() writes gsi1pk + gsi1sk on PK-composites-only GSI; byChannel query returns the item",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { ChannelDevicesTs },
          tables: { CdTable },
        })

        yield* db.entities.ChannelDevicesTs.append({
          channel: "ch-append",
          deviceId: "d-append-1",
          timestamp: DateTime.makeUnsafe("2026-04-30T10:00:00.000Z"),
          reading: 42,
        })

        // Pre-v1.7.2 this query returned 0 items because gsi1pk / gsi1sk
        // were never written by .append().
        const rows = yield* db.entities.ChannelDevicesTs.byChannel({
          channel: "ch-append",
        }).collect()
        expect(rows).toHaveLength(1)
        expect(rows[0]!.deviceId).toBe("d-append-1")
      }).pipe(provideCd),
  )

  it.effect(
    "Entity.update().set() preserves gsi keys on PK-composites-only GSI; byChannel still returns the item",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { ChannelDevicesTs },
          tables: { CdTable },
        })

        // Seed via append.
        yield* db.entities.ChannelDevicesTs.append({
          channel: "ch-update",
          deviceId: "d-update-1",
          timestamp: DateTime.makeUnsafe("2026-04-30T10:00:00.000Z"),
        })

        // Issue an update that doesn't mention channel or deviceId in the
        // payload. Pre-v1.7.2 the per-half gate skipped both halves and
        // the gsi keys persisted unchanged (correct here only because
        // append already wrote them — but pre-v1.7.2 append didn't, so the
        // item was invisible from the start). Post-v1.7.2 the SK halves
        // SET via the broadened gate using keyRecord membership.
        yield* db.entities.ChannelDevicesTs.update({
          channel: "ch-update",
          deviceId: "d-update-1",
        }).set({ otherField: "X" })

        const rows = yield* db.entities.ChannelDevicesTs.byChannel({
          channel: "ch-update",
        }).collect()
        expect(rows).toHaveLength(1)
        expect(rows[0]!.deviceId).toBe("d-update-1")
        expect(rows[0]!.otherField).toBe("X")
      }).pipe(provideCd),
  )

  it.effect("Multiple sequential updates idempotently re-SET the same gsi key values", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { ChannelDevicesPlain },
        tables: { CdTable },
      })

      yield* db.entities.ChannelDevicesPlain.put({
        channel: "ch-idem",
        deviceId: "d-idem-1",
        timestamp: DateTime.makeUnsafe("2026-04-30T10:00:00.000Z"),
      }).asEffect()

      // Multiple updates — each one SETs gsi2pk and gsi2sk to the same
      // composed value (PK composites are immutable). DDB SET to the
      // same value is a noop on the byte representation but a billable
      // write — that's expected and acknowledged in DESIGN.md §7.
      for (const i of [1, 2, 3]) {
        yield* db.entities.ChannelDevicesPlain.update({
          channel: "ch-idem",
          deviceId: "d-idem-1",
        }).set({ otherField: `update-${i}` })
      }

      const rows = yield* db.entities.ChannelDevicesPlain.byChannel({
        channel: "ch-idem",
      }).collect()
      expect(rows).toHaveLength(1)
      expect(rows[0]!.deviceId).toBe("d-idem-1")
      expect(rows[0]!.otherField).toBe("update-3")

      // Sanity-check the actual GSI keys via a direct query — they must
      // hold the composed values across the three SETs.
      const raw = yield* (yield* DynamoClient).getItem({
        TableName: cdTableName,
        Key: {
          pk: {
            S: "$cd-test#v1#channeldeviceplain#channel_ch-idem#deviceid_d-idem-1",
          },
          sk: { S: "$cd-test#v1#channeldeviceplain" },
        },
      })
      expect(raw.Item).toBeDefined()
      expect(raw.Item!.gsi2pk?.S).toBe("$cd-test#v1#channeldeviceplain#channel_ch-idem")
      expect(raw.Item!.gsi2sk?.S).toBe("$cd-test#v1#channeldeviceplain#deviceid_d-idem-1")
    }).pipe(provideCd),
  )

  it.effect(
    "Multi-writer GSI (NOT PK-composites-only) — stamp update leaves byCurrentAlert untouched",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { ChannelDevicesMixed },
          tables: { CdTable },
        })

        // Seed: enrichment sets accountId, telemetry sets alertState.
        yield* db.entities.ChannelDevicesMixed.put({
          channel: "ch-mixed-mw",
          deviceId: "d-mw-1",
          accountId: "acct-1",
          alertState: "active",
          timestamp: DateTime.makeUnsafe("2026-04-30T10:00:00.000Z"),
        }).asEffect()

        // Capture the byCurrentAlert key composition baseline.
        const baseline = yield* (yield* DynamoClient).getItem({
          TableName: cdTableName,
          Key: {
            pk: {
              S: "$cd-test#v1#channeldevicemixed#channel_ch-mixed-mw#deviceid_d-mw-1",
            },
            sk: { S: "$cd-test#v1#channeldevicemixed" },
          },
        })
        const baselineGsi4pk = baseline.Item?.gsi4pk?.S
        const baselineGsi4sk = baseline.Item?.gsi4sk?.S
        expect(baselineGsi4pk).toBeDefined()
        expect(baselineGsi4sk).toBeDefined()

        // Stamp writer — touches a non-composite. byCurrentAlert (gsi4)
        // composites (accountId, alertState) are NOT in the payload AND
        // NOT in keyRecord (only channel/deviceId are). Per-half gate must
        // skip both halves of byCurrentAlert. v1.7.1 multi-writer fix.
        yield* db.entities.ChannelDevicesMixed.update({
          channel: "ch-mixed-mw",
          deviceId: "d-mw-1",
        }).set({ otherField: "stamp-value" })

        const after = yield* (yield* DynamoClient).getItem({
          TableName: cdTableName,
          Key: {
            pk: {
              S: "$cd-test#v1#channeldevicemixed#channel_ch-mixed-mw#deviceid_d-mw-1",
            },
            sk: { S: "$cd-test#v1#channeldevicemixed" },
          },
        })
        // byCurrentAlert keys unchanged — multi-writer fix preserved.
        expect(after.Item?.gsi4pk?.S).toBe(baselineGsi4pk)
        expect(after.Item?.gsi4sk?.S).toBe(baselineGsi4sk)
        // byChannel keys (gsi3, PK-composites-only) SET on every write
        // (idempotent) — values unchanged but the SET clause was emitted.
        expect(after.Item?.gsi3pk?.S).toBe("$cd-test#v1#channeldevicemixed#channel_ch-mixed-mw")
        expect(after.Item?.gsi3sk?.S).toBe("$cd-test#v1#channeldevicemixed#deviceid_d-mw-1")
      }).pipe(provideCd),
  )

  it.effect(
    "Mixed entity (PK-composites-only + multi-writer GSIs) — both behaviors coexist on the same write",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { ChannelDevicesMixed },
          tables: { CdTable },
        })

        yield* db.entities.ChannelDevicesMixed.put({
          channel: "ch-mixed",
          deviceId: "d-mixed-1",
          accountId: "acct-2",
          alertState: "warning",
          timestamp: DateTime.makeUnsafe("2026-04-30T11:00:00.000Z"),
        }).asEffect()

        // Stamp update: only otherField changes. byChannel (gsi3,
        // PK-composites-only) must SET (idempotent re-compose).
        // byCurrentAlert (gsi4, multi-writer) must noop.
        yield* db.entities.ChannelDevicesMixed.update({
          channel: "ch-mixed",
          deviceId: "d-mixed-1",
        }).set({ otherField: "stamped" })

        // byChannel query (gsi3) returns the item — proves PK-composites-only
        // SET happened.
        const byChannelRows = yield* db.entities.ChannelDevicesMixed.byChannel({
          channel: "ch-mixed",
        }).collect()
        expect(byChannelRows).toHaveLength(1)
        expect(byChannelRows[0]!.deviceId).toBe("d-mixed-1")
        expect(byChannelRows[0]!.otherField).toBe("stamped")

        // byCurrentAlert query (gsi4) also returns the item — proves the
        // multi-writer GSI keys were preserved across the stamp update.
        const byAlertRows = yield* db.entities.ChannelDevicesMixed.byCurrentAlert({
          accountId: "acct-2",
        }).collect()
        expect(byAlertRows).toHaveLength(1)
        expect(byAlertRows[0]!.alertState).toBe("warning")
      }).pipe(provideCd),
  )
})

// ===========================================================================
// Entity Refs + Aggregate Connected Tests (separate table with GSI for aggregates)
// ===========================================================================

describeConnected("Entity refs and Aggregate integration tests", () => {
  beforeAll(async () => {
    // Go through DynamoClient.make so the aggregate→table auto-merge wires
    // `BlogPostAggregate`'s gsi2 collection index into the CreateTable call.
    // `BlogPostAggregate` can't be registered on `Table.make` directly because
    // it references `AggTable` — the classic circular-reference case that the
    // `DynamoClient.make({ aggregates, tables })` merge was designed for.
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { Authors, Articles, Officials },
          aggregates: {
            BlogPostAggregate,
            TimestampedPostAggregate,
            OfficiatedMatchAggregate,
            CollidingMatchAggregate,
          },
          tables: { AggTable },
        })
        yield* db.tables.AggTable.create()
      }).pipe(provideAgg, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: aggTableName })
      }).pipe(
        provideAgg,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // -------------------------------------------------------------------------
  // Seed reference data
  // -------------------------------------------------------------------------

  describe("Entity refs", () => {
    it.effect("seed authors for ref tests", () =>
      Effect.gen(function* () {
        yield* Authors.put({ id: "alice", name: "Alice Johnson" }).asEffect()
        yield* Authors.put({ id: "bob", name: "Bob Williams" }).asEffect()

        const alice = yield* Authors.get({ id: "alice" }).asEffect()
        expect(alice.name).toBe("Alice Johnson")
      }).pipe(provideAgg),
    )

    it.effect("put with ref ID hydrates on get", () =>
      Effect.gen(function* () {
        yield* Articles.put({
          articleId: "art-1",
          title: "Effect TS Guide",
          authorId: "alice",
          status: "published",
        }).asEffect()

        const article = yield* Articles.get({ articleId: "art-1" }).asEffect()
        expect(article.title).toBe("Effect TS Guide")
        expect(article.author.id).toBe("alice")
        expect(article.author.name).toBe("Alice Johnson")
      }).pipe(provideAgg),
    )

    it.effect("asRecord includes ref data", () =>
      Effect.gen(function* () {
        const record = yield* Articles.get({ articleId: "art-1" }).pipe(Entity.asRecord)
        expect(record.author.id).toBe("alice")
        expect(record.author.name).toBe("Alice Johnson")
      }).pipe(provideAgg),
    )

    it.effect("RefNotFound when ref entity does not exist", () =>
      Effect.gen(function* () {
        const err = yield* Articles.put({
          articleId: "art-bad",
          title: "Bad Ref",
          authorId: "nonexistent",
          status: "draft",
        })
          .asEffect()
          .pipe(Effect.flip)
        expect(err._tag).toBe("RefNotFound")
      }).pipe(provideAgg),
    )

    it.effect("cascade propagates source entity changes to embedded refs", () =>
      Effect.gen(function* () {
        // Create a second article by the same author
        yield* Articles.put({
          articleId: "art-2",
          title: "Second Article",
          authorId: "alice",
          status: "draft",
        }).asEffect()

        // Update the author with cascade to Articles
        yield* Authors.update({ id: "alice" }).pipe(
          Entity.set({ name: "Alice J. Johnson" }),
          Entity.cascade({ targets: [Articles] }),
          Entity.asModel,
        )

        // Verify cascade propagated — both articles should have the updated author
        const art1 = yield* Articles.get({ articleId: "art-1" }).asEffect()
        expect(art1.author.name).toBe("Alice J. Johnson")

        const art2 = yield* Articles.get({ articleId: "art-2" }).asEffect()
        expect(art2.author.name).toBe("Alice J. Johnson")
      }).pipe(provideAgg),
    )

    it.effect("cascade does not affect articles by other authors", () =>
      Effect.gen(function* () {
        // Create an article by Bob
        yield* Articles.put({
          articleId: "art-bob",
          title: "Bob's Article",
          authorId: "bob",
          status: "published",
        }).asEffect()

        // Update Alice — should not affect Bob's article
        yield* Authors.update({ id: "alice" }).pipe(
          Entity.set({ name: "Alice Johnson" }),
          Entity.cascade({ targets: [Articles] }),
          Entity.asModel,
        )

        const bobArt = yield* Articles.get({ articleId: "art-bob" }).asEffect()
        expect(bobArt.author.name).toBe("Bob Williams")
      }).pipe(provideAgg),
    )
  })

  // -------------------------------------------------------------------------
  // Aggregate CRUD
  // -------------------------------------------------------------------------

  describe("Aggregate create + get", () => {
    it.effect("create decomposes into items and hydrates refs", () =>
      Effect.gen(function* () {
        const post = yield* BlogPostAggregate.create({
          id: "post-1",
          title: "Hello World",
          authorId: "alice",
          meta: { summary: "First post", wordCount: 500 },
          comments: [
            { id: "c1", text: "Great post!", commenter: "Charlie" },
            { id: "c2", text: "Thanks for sharing", commenter: "Dana" },
          ],
        })

        expect(post.id).toBe("post-1")
        expect(post.title).toBe("Hello World")
        // RefEdge: author hydrated from Authors entity
        expect(post.author.id).toBe("alice")
        expect(post.author.name).toBe("Alice Johnson")
        // OneEdge: meta
        expect(post.meta.summary).toBe("First post")
        expect(post.meta.wordCount).toBe(500)
        // ManyEdge: comments
        expect(post.comments).toHaveLength(2)
        expect(post.comments[0]!.text).toBe("Great post!")
        expect(post.comments[1]!.commenter).toBe("Dana")
      }).pipe(provideAgg),
    )

    it.effect("get assembles items into full domain object", () =>
      Effect.gen(function* () {
        const fetched = yield* BlogPostAggregate.get({ id: "post-1" })

        expect(fetched.id).toBe("post-1")
        expect(fetched.title).toBe("Hello World")
        expect(fetched.author.id).toBe("alice")
        expect(fetched.author.name).toBe("Alice Johnson")
        expect(fetched.meta.summary).toBe("First post")
        expect(fetched.meta.wordCount).toBe(500)
        expect(fetched.comments).toHaveLength(2)
        expect(fetched.comments.map((c) => c.commenter).sort()).toEqual(["Charlie", "Dana"])
      }).pipe(provideAgg),
    )

    it.effect("get non-existent aggregate fails with AggregateAssemblyError", () =>
      Effect.gen(function* () {
        const err = yield* BlogPostAggregate.get({ id: "nonexistent" }).pipe(Effect.flip)
        expect(err._tag).toBe("AggregateAssemblyError")
      }).pipe(provideAgg),
    )
  })

  // -------------------------------------------------------------------------
  // ManyEdge declared sort key (#103)
  // -------------------------------------------------------------------------

  describe("ManyEdge sk.composite", () => {
    it.effect(
      "a full panel round-trips: a role with two officials, an official with two roles",
      () =>
        Effect.gen(function* () {
          yield* Officials.put({ officialId: "off-1", name: "Ravi Bowen" }).asEffect()
          yield* Officials.put({ officialId: "off-2", name: "Kumar D." }).asEffect()
          yield* Officials.put({ officialId: "off-3", name: "Marais E." }).asEffect()

          const created = yield* OfficiatedMatchAggregate.create({
            id: "match-103",
            name: "AUS vs IND",
            officials: [
              // Multi-occupancy role: two on-field umpires.
              { officialId: "off-1", role: "onfield" },
              { officialId: "off-2", role: "onfield" },
              // Repeated official: off-1 is also the third umpire.
              { officialId: "off-1", role: "third" },
              { officialId: "off-3", role: "referee" },
            ],
          })

          expect(created.officials).toHaveLength(4)

          // Round-trip: all four rows survive as distinct items.
          const fetched = yield* OfficiatedMatchAggregate.get({ id: "match-103" })
          expect(fetched.officials).toHaveLength(4)

          const appointments = fetched.officials
            .map((o) => `${o.role}:${o.official.officialId}`)
            .sort()
          expect(appointments).toEqual([
            "onfield:off-1",
            "onfield:off-2",
            "referee:off-3",
            "third:off-1",
          ])

          // Both of off-1's rows hydrate the same official.
          const offOne = fetched.officials.filter((o) => o.official.officialId === "off-1")
          expect(offOne).toHaveLength(2)
          expect(offOne.every((o) => o.official.name === "Ravi Bowen")).toBe(true)
        }).pipe(provideAgg),
    )

    it.effect("sort keys carry the declared composite", () =>
      Effect.gen(function* () {
        const client = yield* DynamoClient
        const result = yield* client.query({
          TableName: aggTableName,
          KeyConditionExpression: "#pk = :pk",
          ExpressionAttributeNames: { "#pk": "pk" },
          ExpressionAttributeValues: { ":pk": { S: "$agg-test#v1#officiated#match-103" } },
        })

        const officialSks = (result.Items ?? [])
          .filter((item) => item.__edd_e__?.S === "MatchOfficial")
          .map((item) => item.sk?.S)
          .sort()

        expect(officialSks).toEqual([
          "$agg-test#v1#matchofficial#onfield#off-1",
          "$agg-test#v1#matchofficial#onfield#off-2",
          "$agg-test#v1#matchofficial#referee#off-3",
          "$agg-test#v1#matchofficial#third#off-1",
        ])
      }).pipe(provideAgg),
    )

    it.effect("without a declared sort key the collision is a typed error, not an AWS one", () =>
      Effect.gen(function* () {
        const error = yield* CollidingMatchAggregate.create({
          id: "match-103-collide",
          name: "ENG vs NZ",
          officials: [
            { officialId: "off-1", role: "onfield" },
            // Same official again — the ref-id default keys on the official
            // alone, so both appointments compose one row.
            { officialId: "off-1", role: "third" },
          ],
        }).pipe(Effect.flip)

        // Before #103 this reached the caller as DynamoValidationError wrapping
        // "Transaction request cannot include multiple operations on one item".
        expect(error._tag).toBe("AggregateDecompositionError")

        // Nothing was written.
        const client = yield* DynamoClient
        const result = yield* client.query({
          TableName: aggTableName,
          KeyConditionExpression: "#pk = :pk",
          ExpressionAttributeNames: { "#pk": "pk" },
          ExpressionAttributeValues: { ":pk": { S: "$agg-test#v1#colliding#match-103-collide" } },
        })
        expect(result.Items ?? []).toHaveLength(0)
      }).pipe(provideAgg),
    )
  })

  // -------------------------------------------------------------------------
  // Aggregate system timestamps (#98)
  // -------------------------------------------------------------------------

  describe("Aggregate timestamps", () => {
    /** Raw stored rows for the timestamped aggregate, keyed by sk. */
    const storedRows = Effect.gen(function* () {
      const client = yield* DynamoClient
      const result = yield* client.scan({ TableName: aggTableName })
      const rows = new Map<string, Record<string, { S?: string; N?: string }>>()
      for (const item of result.Items ?? []) {
        const entityType = (item.__edd_e__ as { S?: string } | undefined)?.S ?? ""
        if (!entityType.startsWith("TsPost")) continue
        rows.set((item.sk as { S?: string }).S!, item as never)
      }
      return rows
    })

    // `it.effect` runs on a TestClock frozen at epoch 0, so each test advances it
    // to a known instant — the stored values are then exact, not just "present".
    const T1 = 1_700_000_000_000
    const T2 = T1 + 60_000

    it.effect("create stamps every row as a DynamoDB number", () =>
      Effect.gen(function* () {
        yield* TestClock.adjust(Duration.millis(T1))
        yield* TimestampedPostAggregate.create({
          id: "ts-1",
          title: "Stamped",
          meta: { summary: "With timestamps", wordCount: 100 },
          comments: [
            { id: "c1", text: "First", commenter: "Charlie" },
            { id: "c2", text: "Second", commenter: "Dana" },
          ],
        })

        const rows = yield* storedRows
        // root + meta + 2 comments
        expect(rows.size).toBe(4)
        for (const row of rows.values()) {
          // Stored as `N`, not `S` — the whole point of the epoch-millis override.
          expect(row.created!.N).toBe(String(T1))
          expect(row.updated!.N).toBe(String(T1))
          expect(row.created!.S).toBeUndefined()
        }
      }).pipe(provideAgg),
    )

    it.effect("update preserves created and advances updated", () =>
      Effect.gen(function* () {
        yield* TestClock.adjust(Duration.millis(T2))
        const before = yield* storedRows

        yield* TimestampedPostAggregate.update({ id: "ts-1" }, ({ cursor }) =>
          cursor.key("title").replace("Stamped (Revised)"),
        )

        const after = yield* storedRows
        expect(after.size).toBe(before.size)
        for (const [sk, row] of after) {
          const prior = before.get(sk)!
          // PUT-based rewrites must not clobber the original create instant.
          expect(row.created!.N).toBe(prior.created!.N)
          expect(row.created!.N).toBe(String(T1))
          expect(row.updated!.N).toBe(String(T2))
        }
      }).pipe(provideAgg),
    )

    it.effect("get assembles without leaking timestamp attributes", () =>
      Effect.gen(function* () {
        const post = yield* TimestampedPostAggregate.get({ id: "ts-1" })

        expect(post.title).toBe("Stamped (Revised)")
        expect(post.comments).toHaveLength(2)
        expect(Object.keys(post)).not.toContain("created")
        expect(Object.keys(post)).not.toContain("updated")
        expect(Object.keys(post.meta)).not.toContain("updated")
        expect(Object.keys(post.comments[0]!)).not.toContain("updated")
      }).pipe(provideAgg),
    )
  })

  describe("Aggregate update", () => {
    it.effect("update root field via cursor", () =>
      Effect.gen(function* () {
        const updated = yield* BlogPostAggregate.update({ id: "post-1" }, ({ cursor }) =>
          cursor.key("title").replace("Hello World (Revised)"),
        )

        expect(updated.title).toBe("Hello World (Revised)")
        // Other fields unchanged
        expect(updated.author.name).toBe("Alice Johnson")
        expect(updated.meta.summary).toBe("First post")
        expect(updated.comments).toHaveLength(2)
      }).pipe(provideAgg),
    )

    it.effect("update OneEdge field via cursor", () =>
      Effect.gen(function* () {
        const updated = yield* BlogPostAggregate.update({ id: "post-1" }, ({ cursor }) =>
          cursor.key("meta").modify((m) => ({ ...m, wordCount: 750 })),
        )

        expect(updated.meta.wordCount).toBe(750)
        expect(updated.meta.summary).toBe("First post")
      }).pipe(provideAgg),
    )

    it.effect("update ManyEdge via cursor (add comment)", () =>
      Effect.gen(function* () {
        const updated = yield* BlogPostAggregate.update({ id: "post-1" }, ({ cursor }) =>
          cursor
            .key("comments")
            .modify((comments) => [
              ...comments,
              { id: "c3", text: "New comment", commenter: "Eve" },
            ]),
        )

        expect(updated.comments).toHaveLength(3)
        expect(updated.comments.map((c) => c.commenter)).toContain("Eve")
      }).pipe(provideAgg),
    )

    it.effect("updated aggregate persists — re-read verifies", () =>
      Effect.gen(function* () {
        const fetched = yield* BlogPostAggregate.get({ id: "post-1" })

        expect(fetched.title).toBe("Hello World (Revised)")
        expect(fetched.meta.wordCount).toBe(750)
        expect(fetched.comments).toHaveLength(3)
      }).pipe(provideAgg),
    )
  })

  describe("Aggregate delete", () => {
    it.effect("delete removes all items in the partition", () =>
      Effect.gen(function* () {
        // Create a second aggregate to delete
        yield* BlogPostAggregate.create({
          id: "post-del",
          title: "Delete Me",
          authorId: "bob",
          meta: { summary: "Temporary", wordCount: 100 },
          comments: [{ id: "c-del", text: "Ephemeral", commenter: "Frank" }],
        })

        // Verify it exists
        const before = yield* BlogPostAggregate.get({ id: "post-del" })
        expect(before.title).toBe("Delete Me")

        // Delete
        yield* BlogPostAggregate.delete({ id: "post-del" })

        // Verify it's gone
        const err = yield* BlogPostAggregate.get({ id: "post-del" }).pipe(Effect.flip)
        expect(err._tag).toBe("AggregateAssemblyError")
      }).pipe(provideAgg),
    )

    it.effect("first aggregate still intact after deleting second", () =>
      Effect.gen(function* () {
        const post1 = yield* BlogPostAggregate.get({ id: "post-1" })
        expect(post1.title).toBe("Hello World (Revised)")
        expect(post1.comments).toHaveLength(3)
      }).pipe(provideAgg),
    )
  })

  // -------------------------------------------------------------------------
  // Sub-aggregate with discriminator
  // -------------------------------------------------------------------------

  describe("Sub-aggregate with discriminator", () => {
    // We reuse ReviewerNoteAggregate bound with discriminators inside a parent
    // aggregate. This is the pattern from the cricket example (TeamSheet × 2).
    // We create a Review aggregate that has editorial and peer review notes.

    class ReviewedPost extends Schema.Class<ReviewedPost>("ReviewedPost")({
      id: Schema.String,
      title: Schema.String,
      editorial: ReviewerNote,
      peer: ReviewerNote,
    }) {}

    const ReviewedPostAggregate = Aggregate.make(ReviewedPost, {
      table: AggTable,
      schema: AggSchema,
      pk: { field: "pk", composite: ["id"] },
      collection: {
        index: "gsi2",
        name: "reviewedpost",
        sk: { field: "gsi2sk", composite: ["title"] },
      },
      root: { entityType: "ReviewedPostRoot" },
      edges: {
        editorial: ReviewerNoteAggregate.with({ discriminator: { reviewType: "editorial" } }),
        peer: ReviewerNoteAggregate.with({ discriminator: { reviewType: "peer" } }),
      },
    })

    it.effect("create with discriminated sub-aggregates", () =>
      Effect.gen(function* () {
        const post = yield* ReviewedPostAggregate.create({
          id: "rev-1",
          title: "Reviewed Article",
          editorial: { reviewerId: "alice", rating: 9, text: "Excellent work" },
          peer: { reviewerId: "bob", rating: 7, text: "Needs minor revisions" },
        })

        expect(post.editorial.reviewer.name).toBe("Alice Johnson")
        expect(post.editorial.rating).toBe(9)
        expect(post.peer.reviewer.name).toBe("Bob Williams")
        expect(post.peer.rating).toBe(7)
      }).pipe(provideAgg),
    )

    it.effect("get reassembles discriminated sub-aggregates", () =>
      Effect.gen(function* () {
        const fetched = yield* ReviewedPostAggregate.get({ id: "rev-1" })

        expect(fetched.title).toBe("Reviewed Article")
        expect(fetched.editorial.reviewer.id).toBe("alice")
        expect(fetched.editorial.text).toBe("Excellent work")
        expect(fetched.peer.reviewer.id).toBe("bob")
        expect(fetched.peer.text).toBe("Needs minor revisions")
      }).pipe(provideAgg),
    )

    it.effect("update one sub-aggregate without affecting the other", () =>
      Effect.gen(function* () {
        const updated = yield* ReviewedPostAggregate.update({ id: "rev-1" }, ({ cursor }) =>
          cursor.key("editorial").modify((ed) => ({ ...ed, rating: 10 })),
        )

        expect(updated.editorial.rating).toBe(10)
        // Peer unchanged
        expect(updated.peer.rating).toBe(7)
        expect(updated.peer.text).toBe("Needs minor revisions")
      }).pipe(provideAgg),
    )

    it.effect("delete discriminated aggregate removes all items", () =>
      Effect.gen(function* () {
        yield* ReviewedPostAggregate.delete({ id: "rev-1" })

        const err = yield* ReviewedPostAggregate.get({ id: "rev-1" }).pipe(Effect.flip)
        expect(err._tag).toBe("AggregateAssemblyError")
      }).pipe(provideAgg),
    )
  })

  // -------------------------------------------------------------------------
  // Edge removal via update (#74) — items dropped by the mutation must be
  // DELETEd, not left as orphan rows that re-appear on the next get. A many-edge
  // element (and a cleared one-edge) lives in its PARENT's transaction group, so
  // removing it shrinks a group rather than dropping a whole group; the item-level
  // diff must therefore emit a Delete for the orphaned row.
  // -------------------------------------------------------------------------

  describe("Aggregate update — edge removal (#74)", () => {
    // Raw base-table partition read so we can assert orphan rows are physically
    // gone — the strongest #74 regression check (get() also re-assembles only
    // surviving rows, so a left-behind row would re-appear there too).
    const rawSks = (pk: string) =>
      Effect.gen(function* () {
        const client = yield* DynamoClient
        const res = yield* client.query({
          TableName: aggTableName,
          KeyConditionExpression: "#pk = :pk",
          ExpressionAttributeNames: { "#pk": "pk" },
          ExpressionAttributeValues: { ":pk": { S: pk } },
        })
        return (res.Items ?? []).map((i) => (i.sk as { S: string }).S)
      })
    const blogPk = (id: string) => DynamoSchema.composeCollectionKey(AggSchema, "blogpost", [id])
    const commentSk = (id: string) => DynamoSchema.composeKey(AggSchema, "BlogPostComment", [id])

    it.effect("removes one many-edge element — re-read + raw partition confirm deletion", () =>
      Effect.gen(function* () {
        yield* BlogPostAggregate.create({
          id: "post-rm-1",
          title: "RM One",
          authorId: "alice",
          meta: { summary: "s", wordCount: 1 },
          comments: [
            { id: "c1", text: "a", commenter: "X" },
            { id: "c2", text: "b", commenter: "Y" },
            { id: "c3", text: "c", commenter: "Z" },
          ],
        })

        const updated = yield* BlogPostAggregate.update({ id: "post-rm-1" }, ({ state }) => ({
          ...state,
          comments: state.comments.filter((c) => c.id !== "c2"),
        }))
        expect(updated.comments.map((c) => c.id).sort()).toEqual(["c1", "c3"])

        const fetched = yield* BlogPostAggregate.get({ id: "post-rm-1" })
        expect(fetched.comments).toHaveLength(2)
        expect(fetched.comments.map((c) => c.id)).not.toContain("c2")
        expect(fetched.meta.summary).toBe("s") // one-edge preserved

        const sks = yield* rawSks(blogPk("post-rm-1"))
        expect(sks).not.toContain(commentSk("c2")) // orphan row physically deleted
        expect(sks).toContain(commentSk("c1"))
        expect(sks).toContain(commentSk("c3"))
      }).pipe(provideAgg),
    )

    it.effect("removes ALL many-edge elements — only root + one-edge survive", () =>
      Effect.gen(function* () {
        yield* BlogPostAggregate.create({
          id: "post-rm-2",
          title: "RM All",
          authorId: "alice",
          meta: { summary: "keep", wordCount: 2 },
          comments: [
            { id: "c1", text: "a", commenter: "X" },
            { id: "c2", text: "b", commenter: "Y" },
          ],
        })

        const updated = yield* BlogPostAggregate.update({ id: "post-rm-2" }, ({ state }) => ({
          ...state,
          comments: [],
        }))
        expect(updated.comments).toHaveLength(0)

        const fetched = yield* BlogPostAggregate.get({ id: "post-rm-2" })
        expect(fetched.comments).toHaveLength(0)
        expect(fetched.meta.summary).toBe("keep") // one-edge survives

        const sks = yield* rawSks(blogPk("post-rm-2"))
        expect(sks).not.toContain(commentSk("c1"))
        expect(sks).not.toContain(commentSk("c2"))
        expect(sks).toHaveLength(2) // only root + meta remain
      }).pipe(provideAgg),
    )

    it.effect("atomic add + remove in one update", () =>
      Effect.gen(function* () {
        yield* BlogPostAggregate.create({
          id: "post-rm-3",
          title: "RM Mix",
          authorId: "alice",
          meta: { summary: "s", wordCount: 3 },
          comments: [
            { id: "c1", text: "a", commenter: "X" },
            { id: "c2", text: "b", commenter: "Y" },
          ],
        })

        const updated = yield* BlogPostAggregate.update({ id: "post-rm-3" }, ({ state }) => ({
          ...state,
          comments: [
            ...state.comments.filter((c) => c.id !== "c1"),
            { id: "c9", text: "new", commenter: "Zoe" },
          ],
        }))
        expect(updated.comments.map((c) => c.id).sort()).toEqual(["c2", "c9"])

        const fetched = yield* BlogPostAggregate.get({ id: "post-rm-3" })
        expect(fetched.comments.map((c) => c.id).sort()).toEqual(["c2", "c9"])

        const sks = yield* rawSks(blogPk("post-rm-3"))
        expect(sks).toContain(commentSk("c2"))
        expect(sks).toContain(commentSk("c9"))
        expect(sks).not.toContain(commentSk("c1"))
      }).pipe(provideAgg),
    )

    it.effect("root-field-only update preserves all edge rows (no spurious delete)", () =>
      Effect.gen(function* () {
        yield* BlogPostAggregate.create({
          id: "post-rm-4",
          title: "Before",
          authorId: "alice",
          meta: { summary: "s", wordCount: 4 },
          comments: [
            { id: "c1", text: "a", commenter: "X" },
            { id: "c2", text: "b", commenter: "Y" },
          ],
        })
        const before = yield* rawSks(blogPk("post-rm-4"))

        const updated = yield* BlogPostAggregate.update({ id: "post-rm-4" }, ({ state }) => ({
          ...state,
          title: "After",
        }))
        expect(updated.title).toBe("After")
        expect(updated.comments).toHaveLength(2)

        const fetched = yield* BlogPostAggregate.get({ id: "post-rm-4" })
        expect(fetched.title).toBe("After")
        expect(fetched.comments.map((c) => c.id).sort()).toEqual(["c1", "c2"])
        expect(fetched.meta.summary).toBe("s")

        const after = yield* rawSks(blogPk("post-rm-4"))
        expect(after.sort()).toEqual(before.sort()) // no row added or deleted
      }).pipe(provideAgg),
    )
  })

  // -------------------------------------------------------------------------
  // Sub-aggregate edge removal (#74 — the exact issue repro: team1.players)
  // -------------------------------------------------------------------------

  describe("Aggregate update — sub-aggregate edge removal (#74 repro)", () => {
    class SquadCoach extends Schema.Class<SquadCoach>("SquadCoach")({
      name: Schema.String,
    }) {}
    class SquadPlayer extends Schema.Class<SquadPlayer>("SquadPlayer")({
      id: Schema.String, // required → extractRefIdentifiers gives each player a distinct SK
      name: Schema.String,
    }) {}
    class Squad extends Schema.Class<Squad>("Squad")({
      coach: Schema.optionalKey(SquadCoach), // optionalKey so it can be cleared
      players: Schema.Array(SquadPlayer),
    }) {}
    class MatchCard extends Schema.Class<MatchCard>("MatchCard")({
      id: Schema.String,
      title: Schema.String,
      team1: Squad,
      team2: Squad,
    }) {}

    const SquadAggregate = Aggregate.make(Squad, {
      root: { entityType: "MatchSquad" },
      edges: {
        coach: Aggregate.one("coach", { entityType: "SquadCoach" }),
        players: Aggregate.many("players", { entityType: "SquadPlayer" }),
      },
    })
    // Reuses the existing gsi2 on AggTable (no CreateTable change). Not bound via
    // DynamoClient.make — exercised directly like ReviewedPostAggregate above.
    const MatchCardAggregate = Aggregate.make(MatchCard, {
      table: AggTable,
      schema: AggSchema,
      pk: { field: "pk", composite: ["id"] },
      collection: {
        index: "gsi2",
        name: "matchcard",
        sk: { field: "gsi2sk", composite: ["title"] },
      },
      root: { entityType: "MatchCardRoot" },
      edges: {
        team1: SquadAggregate.with({ discriminator: { teamNumber: 1 } }),
        team2: SquadAggregate.with({ discriminator: { teamNumber: 2 } }),
      },
    })

    const matchPk = (id: string) => DynamoSchema.composeCollectionKey(AggSchema, "matchcard", [id])
    const rawSks = (pk: string) =>
      Effect.gen(function* () {
        const client = yield* DynamoClient
        const res = yield* client.query({
          TableName: aggTableName,
          KeyConditionExpression: "#pk = :pk",
          ExpressionAttributeNames: { "#pk": "pk" },
          ExpressionAttributeValues: { ":pk": { S: pk } },
        })
        return (res.Items ?? []).map((i) => (i.sk as { S: string }).S)
      })
    const playerSks = (sks: ReadonlyArray<string>) =>
      sks.filter((s) => s.toLowerCase().includes("squadplayer"))
    const coachSks = (sks: ReadonlyArray<string>) =>
      sks.filter((s) => s.toLowerCase().includes("squadcoach"))

    it.effect("removes one player from team1.players — team2 untouched", () =>
      Effect.gen(function* () {
        yield* MatchCardAggregate.create({
          id: "mc-1",
          title: "Final",
          team1: {
            coach: { name: "C1" },
            players: [
              { id: "p1", name: "A" },
              { id: "p2", name: "B" },
            ],
          },
          team2: { coach: { name: "C2" }, players: [{ id: "p3", name: "C" }] },
        })

        const updated = yield* MatchCardAggregate.update({ id: "mc-1" }, ({ state }) => ({
          ...state,
          team1: { ...state.team1, players: state.team1.players.filter((p) => p.id !== "p1") },
        }))
        expect(updated.team1.players.map((p) => p.id)).toEqual(["p2"])
        expect(updated.team2.players.map((p) => p.id)).toEqual(["p3"])

        const fetched = yield* MatchCardAggregate.get({ id: "mc-1" })
        expect(fetched.team1.players.map((p) => p.id)).toEqual(["p2"])
        expect(fetched.team2.players.map((p) => p.id)).toEqual(["p3"]) // sibling group untouched

        const players = playerSks(yield* rawSks(matchPk("mc-1")))
        expect(players).toHaveLength(2) // p2 (team1) + p3 (team2)
        expect(players.some((s) => s.endsWith("#p1"))).toBe(false) // removed row gone
        expect(players.some((s) => s.endsWith("#p2"))).toBe(true)
        expect(players.some((s) => s.endsWith("#p3"))).toBe(true)
      }).pipe(provideAgg),
    )

    it.effect("removes ALL players from team1 — sub-aggregate root + coach survive", () =>
      Effect.gen(function* () {
        yield* MatchCardAggregate.create({
          id: "mc-2",
          title: "Semi",
          team1: {
            coach: { name: "C1" },
            players: [
              { id: "p1", name: "A" },
              { id: "p2", name: "B" },
            ],
          },
          team2: { coach: { name: "C2" }, players: [{ id: "p3", name: "C" }] },
        })

        const updated = yield* MatchCardAggregate.update({ id: "mc-2" }, ({ state }) => ({
          ...state,
          team1: { ...state.team1, players: [] },
        }))
        expect(updated.team1.players).toHaveLength(0)

        const fetched = yield* MatchCardAggregate.get({ id: "mc-2" })
        expect(fetched.team1.players).toHaveLength(0)
        expect(fetched.team1.coach?.name).toBe("C1") // one-edge in same group survives
        expect(fetched.team2.players.map((p) => p.id)).toEqual(["p3"])

        const players = playerSks(yield* rawSks(matchPk("mc-2")))
        expect(players.some((s) => s.endsWith("#p1"))).toBe(false)
        expect(players.some((s) => s.endsWith("#p2"))).toBe(false)
        expect(players.some((s) => s.endsWith("#p3"))).toBe(true) // team2 intact
      }).pipe(provideAgg),
    )

    it.effect("clears team1.coach (one-edge) — team2.coach intact", () =>
      Effect.gen(function* () {
        yield* MatchCardAggregate.create({
          id: "mc-3",
          title: "Group",
          team1: { coach: { name: "C1" }, players: [{ id: "p1", name: "A" }] },
          team2: { coach: { name: "C2" }, players: [{ id: "p3", name: "C" }] },
        })

        const updated = yield* MatchCardAggregate.update({ id: "mc-3" }, ({ state }) => {
          const team1 = { ...state.team1 } as Record<string, unknown>
          delete team1.coach
          return { ...state, team1 } as typeof state
        })
        expect(updated.team1.coach).toBeUndefined()
        expect(updated.team1.players).toHaveLength(1)

        const fetched = yield* MatchCardAggregate.get({ id: "mc-3" })
        expect(fetched.team1.coach).toBeUndefined()
        expect(fetched.team1.players.map((p) => p.id)).toEqual(["p1"])
        expect(fetched.team2.coach?.name).toBe("C2")

        // Exactly one coach row remains (team1's was deleted); get() above proves
        // it is team2's. The discriminator is zero-padded in the SK, so assert on
        // count rather than a brittle suffix.
        const coaches = coachSks(yield* rawSks(matchPk("mc-3")))
        expect(coaches).toHaveLength(1)
      }).pipe(provideAgg),
    )
  })
})

// ---------------------------------------------------------------------------
// indexPolicy v1.7.1 integration tests — comprehensive multi-writer
// round-trip + per-half cascade + truncation coverage against DynamoDB Local.
//
// Asserts the v1.7.1 model end-to-end (closes #41):
//   1. Per-half evaluation gate — untouched halves stay untouched.
//   2. Per-half outcome — sparse drops the half; preserve noops or cascades.
//   3. Per-half cascade — Entity.remove drops only the half(s) containing
//      the named composite.
//   4. Set/remove asymmetry — set+remove truncates; remove alone REMOVEs.
// ---------------------------------------------------------------------------

// Hybrid telemetry-style fixture mirroring the consolidated comment in
// issue #41: byCurrentAlert has accountId on pk (preserve, enrichment-owned)
// and [alertState, timestamp] on sk (sparse, telemetry-owned). Both halves
// are touched independently by their respective writers.
class HybridDevice extends Schema.Class<HybridDevice>("HybridDevice")({
  channel: Schema.String,
  deviceId: Schema.String,
  // Enrichment-owned (preserve on pk).
  accountId: Schema.optional(Schema.String),
  // Telemetry-owned (sparse on sk).
  alertState: Schema.optional(Schema.Literals(["active", "cleared"])),
  timestamp: Schema.optional(Schema.String),
  // Stamp-style attribute — not in any GSI; touched by neither writer above.
  published: Schema.optional(Schema.String),
  label: Schema.optional(Schema.String),
}) {}

const ipSchema = DynamoSchema.make({ name: "ip-test", version: 1 })
const ipTableName = `ip-test-${Date.now()}`

const HybridDevices = Entity.make({
  model: HybridDevice,
  entityType: "HybridDevice",
  primaryKey: {
    pk: { field: "pk", composite: ["channel", "deviceId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    // The motivating multi-writer GSI from issue #41: pk preserve
    // (enrichment-owned), sk sparse (telemetry-owned).
    byCurrentAlert: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["accountId"] },
      sk: { field: "gsi1sk", composite: ["alertState", "timestamp"] },
      indexPolicy: { pk: "preserve", sk: "sparse" },
    },
    // Both-preserve GSI for testing per-half cascade override.
    byBothPreserve: {
      name: "gsi2",
      pk: { field: "gsi2pk", composite: ["accountId"] },
      sk: { field: "gsi2sk", composite: ["timestamp"] },
      indexPolicy: { pk: "preserve", sk: "preserve" },
    },
  },
  timestamps: true,
})

const IpTable = Table.make({ schema: ipSchema, entities: { HybridDevices } })
const IpTestLayer = Layer.mergeAll(ClientLayer, IpTable.layer({ name: ipTableName }))
const provideIp = Effect.provide(IpTestLayer)

// Hierarchical asset fixture for set/remove asymmetry + truncation tests.
class HierAsset extends Schema.Class<HierAsset>("HierAsset")({
  assetId: Schema.String,
  region: Schema.optional(Schema.String),
  country: Schema.optional(Schema.String),
  city: Schema.optional(Schema.String),
  site: Schema.optional(Schema.String),
}) {}

const hierTableName = `ip-hier-${Date.now()}`

const HierAssets = Entity.make({
  model: HierAsset,
  entityType: "HierAsset",
  primaryKey: {
    pk: { field: "pk", composite: ["assetId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byLocation: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["region"] },
      sk: { field: "gsi1sk", composite: ["country", "city", "site"] },
      indexPolicy: { pk: "preserve", sk: "preserve" },
    },
  },
  timestamps: true,
})
const HierTable = Table.make({ schema: ipSchema, entities: { HierAssets } })
const HierTestLayer = Layer.mergeAll(ClientLayer, HierTable.layer({ name: hierTableName }))
const provideHier = Effect.provide(HierTestLayer)

describeConnected("indexPolicy v1.7.1 integration tests (closes #41)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: ipTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(IpTable),
        })
      }).pipe(provideIp, Effect.scoped),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: hierTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(HierTable),
        })
      }).pipe(provideHier, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: ipTableName })
      }).pipe(
        provideIp,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: hierTableName })
      }).pipe(
        provideHier,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // ----- Scenario 1: Stamp doesn't disturb GSI -----
  it.effect("scenario 1 — stamp writer doesn't disturb either GSI half", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { HybridDevices },
        tables: { IpTable },
      })

      // Pre-seed item with all composites; both GSIs visible.
      yield* db.entities.HybridDevices.put({
        channel: "c-s1",
        deviceId: "d-s1",
        accountId: "acme",
        alertState: "active",
        timestamp: "2026-04-30T10:00:00Z",
      })
      const beforeAlert = yield* db.entities.HybridDevices.byCurrentAlert({
        accountId: "acme",
      }).collect()
      expect(beforeAlert.some((d) => d.deviceId === "d-s1")).toBe(true)

      // Stamp writer touches only `published` — no GSI composite touched.
      yield* db.entities.HybridDevices.update({ channel: "c-s1", deviceId: "d-s1" }).set({
        published: "2026-04-30",
      })

      // v1.7.1 critical: BOTH GSIs unchanged. (v1.7.0 would have REMOVE'd
      // gsi1sk because sparse fired on the untouched sk half.)
      const afterAlert = yield* db.entities.HybridDevices.byCurrentAlert({
        accountId: "acme",
      }).collect()
      expect(afterAlert.some((d) => d.deviceId === "d-s1")).toBe(true)

      // Verify raw stored item — both GSI key attrs still present.
      const item = yield* HybridDevices.get({
        channel: "c-s1",
        deviceId: "d-s1",
      }).pipe(Entity.asItem)
      expect(item.gsi1pk).toBeDefined()
      expect(item.gsi1sk).toBeDefined()
    }).pipe(provideIp),
  )

  // ----- Scenario 2: Enrichment writer SETs pk, leaves sk alone -----
  it.effect("scenario 2 — enrichment writer SETs pk, sk untouched", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { HybridDevices },
        tables: { IpTable },
      })

      yield* db.entities.HybridDevices.put({
        channel: "c-s2",
        deviceId: "d-s2",
        accountId: "acme",
        alertState: "active",
        timestamp: "2026-04-30T10:00:00Z",
      })

      // Enrichment moves accountId. SK untouched.
      yield* db.entities.HybridDevices.update({ channel: "c-s2", deviceId: "d-s2" }).set({
        accountId: "newAcct",
      })

      // Visible under newAcct (pk SET).
      const newAcct = yield* db.entities.HybridDevices.byCurrentAlert({
        accountId: "newAcct",
      }).collect()
      expect(newAcct.some((d) => d.deviceId === "d-s2")).toBe(true)
      // No longer visible under acme.
      const acme = yield* db.entities.HybridDevices.byCurrentAlert({
        accountId: "acme",
      }).collect()
      expect(acme.some((d) => d.deviceId === "d-s2")).toBe(false)

      // SK unchanged — verify stored gsi1sk hasn't moved.
      const item = yield* HybridDevices.get({
        channel: "c-s2",
        deviceId: "d-s2",
      }).pipe(Entity.asItem)
      expect(item.gsi1sk).toBeDefined()
    }).pipe(provideIp),
  )

  // ----- Scenario 3: Telemetry writer SETs sk, leaves pk alone -----
  it.effect("scenario 3 — telemetry writer SETs sk, pk untouched", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { HybridDevices },
        tables: { IpTable },
      })

      yield* db.entities.HybridDevices.put({
        channel: "c-s3",
        deviceId: "d-s3",
        accountId: "acme",
        alertState: "active",
        timestamp: "2026-04-30T10:00:00Z",
      })

      // Telemetry tick: new alertState + timestamp. PK untouched.
      yield* db.entities.HybridDevices.update({ channel: "c-s3", deviceId: "d-s3" }).set({
        alertState: "cleared",
        timestamp: "2026-04-30T11:00:00Z",
      })

      // Still visible under acme (pk preserved).
      const acme = yield* db.entities.HybridDevices.byCurrentAlert({
        accountId: "acme",
      }).collect()
      expect(acme.some((d) => d.deviceId === "d-s3")).toBe(true)

      // Stored item reflects the new sk.
      const item = yield* HybridDevices.get({
        channel: "c-s3",
        deviceId: "d-s3",
      }).pipe(Entity.asItem)
      expect((item.gsi1sk as string).includes("cleared")).toBe(true)
    }).pipe(provideIp),
  )

  // ----- Scenario 4: Telemetry "no alert" event drops sk -----
  it.effect(
    "scenario 4 — telemetry 'no alert' (timestamp only, alertState undefined) → REMOVE sk only",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { HybridDevices },
          tables: { IpTable },
        })

        yield* db.entities.HybridDevices.put({
          channel: "c-s4",
          deviceId: "d-s4",
          accountId: "acme",
          alertState: "active",
          timestamp: "2026-04-30T10:00:00Z",
        })

        // Telemetry tick with alertState explicitly set to undefined →
        // sk touched (alertState in payload), can't compose (hole at sk[0]) →
        // sparse → REMOVE sk.
        yield* db.entities.HybridDevices.update({ channel: "c-s4", deviceId: "d-s4" }).set({
          alertState: undefined,
          timestamp: "2026-04-30T11:00:00Z",
        })

        // Item invisible in GSI (DDB projection rule needs both keys).
        const acme = yield* db.entities.HybridDevices.byCurrentAlert({
          accountId: "acme",
        }).collect()
        expect(acme.some((d) => d.deviceId === "d-s4")).toBe(false)

        // But pk preserved on the underlying item — verify raw stored attrs.
        const item = yield* HybridDevices.get({
          channel: "c-s4",
          deviceId: "d-s4",
        }).pipe(Entity.asItem)
        expect(item.gsi1pk).toBeDefined() // preserved
        expect(item.gsi1sk).toBeUndefined() // REMOVE'd
      }).pipe(provideIp),
  )

  // ----- Scenario 5: Rejoin without enrichment re-fire (closes v1.7.0 bug-1) -----
  it.effect(
    "scenario 5 — rejoin: telemetry SETs sk after a drop; pk preserved → re-visible without enrichment re-fire",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { HybridDevices },
          tables: { IpTable },
        })

        yield* db.entities.HybridDevices.put({
          channel: "c-s5",
          deviceId: "d-s5",
          accountId: "acme",
          alertState: "active",
          timestamp: "2026-04-30T10:00:00Z",
        })

        // Drop sk first.
        yield* db.entities.HybridDevices.update({ channel: "c-s5", deviceId: "d-s5" }).set({
          alertState: undefined,
          timestamp: "2026-04-30T11:00:00Z",
        })
        const dropped = yield* db.entities.HybridDevices.byCurrentAlert({
          accountId: "acme",
        }).collect()
        expect(dropped.some((d) => d.deviceId === "d-s5")).toBe(false)

        // Telemetry rejoins — sets new alertState + timestamp.
        yield* db.entities.HybridDevices.update({ channel: "c-s5", deviceId: "d-s5" }).set({
          alertState: "active",
          timestamp: "2026-04-30T12:00:00Z",
        })

        // v1.7.1 critical assertion: item re-visible under PRESERVED pk
        // (acme), with NEW sk. No enrichment writer had to re-fire.
        // (v1.7.0 would have lost gsi1pk on the drop and the rejoin alone
        // wouldn't have re-indexed the item until enrichment re-fired.)
        const rejoined = yield* db.entities.HybridDevices.byCurrentAlert({
          accountId: "acme",
        }).collect()
        expect(rejoined.some((d) => d.deviceId === "d-s5")).toBe(true)
      }).pipe(provideIp),
  )

  // ----- Scenario 6: Explicit clear via undefined ----- (already exercised by 4)
  it.effect("scenario 6 — explicit clear via undefined produces same outcome as omission", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { HybridDevices },
        tables: { IpTable },
      })

      yield* db.entities.HybridDevices.put({
        channel: "c-s6",
        deviceId: "d-s6",
        accountId: "acme",
        alertState: "active",
        timestamp: "2026-04-30T10:00:00Z",
      })

      // Explicit `alertState: undefined` — sk touched (in operator true on
      // undefined), can't compose → sparse → REMOVE sk.
      yield* db.entities.HybridDevices.update({ channel: "c-s6", deviceId: "d-s6" }).set({
        alertState: undefined,
        timestamp: "2026-04-30T11:00:00Z",
      })
      const item = yield* HybridDevices.get({
        channel: "c-s6",
        deviceId: "d-s6",
      }).pipe(Entity.asItem)
      expect(item.gsi1sk).toBeUndefined()
      expect(item.gsi1pk).toBeDefined() // preserved
    }).pipe(provideIp),
  )

  // ----- Scenario 7: Entity.remove on sparse-half composite — per-half cascade -----
  it.effect(
    "scenario 7 — Entity.remove(['alertState']) drops sk only (per-half, NOT GSI-wide)",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { HybridDevices },
          tables: { IpTable },
        })

        yield* db.entities.HybridDevices.put({
          channel: "c-s7",
          deviceId: "d-s7",
          accountId: "acme",
          alertState: "active",
          timestamp: "2026-04-30T10:00:00Z",
        })

        // Per-half cascade: alertState in sk only → REMOVE gsi1sk only.
        yield* db.entities.HybridDevices.update({ channel: "c-s7", deviceId: "d-s7" }).remove([
          "alertState",
        ])

        const item = yield* HybridDevices.get({
          channel: "c-s7",
          deviceId: "d-s7",
        }).pipe(Entity.asItem)
        // v1.7.1 critical: gsi1pk preserved (per-half cascade — not GSI-wide).
        expect(item.gsi1pk).toBeDefined()
        // sk REMOVE'd via cascade override.
        expect(item.gsi1sk).toBeUndefined()
        // The byBothPreserve gsi2 has accountId on pk (untouched, no cascade
        // — alertState not in gsi2's composites) → both halves untouched.
        expect(item.gsi2pk).toBeDefined()
        expect(item.gsi2sk).toBeDefined()
      }).pipe(provideIp),
  )

  // ----- Scenario 8: Entity.remove on preserve-half composite — cascade override -----
  it.effect(
    "scenario 8 — Entity.remove(['accountId']) under preserve → REMOVE pk via cascade override (per-half)",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { HybridDevices },
          tables: { IpTable },
        })

        yield* db.entities.HybridDevices.put({
          channel: "c-s8",
          deviceId: "d-s8",
          accountId: "acme",
          alertState: "active",
          timestamp: "2026-04-30T10:00:00Z",
        })

        // accountId is the only pk composite for both byCurrentAlert and
        // byBothPreserve. Per-half cascade fires on PK halves only — sk
        // untouched (alertState/timestamp not in removedSet, not in payload).
        yield* db.entities.HybridDevices.update({ channel: "c-s8", deviceId: "d-s8" }).remove([
          "accountId",
        ])

        const item = yield* HybridDevices.get({
          channel: "c-s8",
          deviceId: "d-s8",
        }).pipe(Entity.asItem)
        // Both PK halves REMOVE'd (cascade override under preserve fires on
        // both — accountId is in both PKs).
        expect(item.gsi1pk).toBeUndefined()
        expect(item.gsi2pk).toBeUndefined()
        // v1.7.1 critical: SK halves preserved (they don't contain
        // accountId; per-half gate skips them entirely).
        expect(item.gsi1sk).toBeDefined()
        expect(item.gsi2sk).toBeDefined()
      }).pipe(provideIp),
  )

  // ----- Scenario 9: Hierarchical truncation via set+remove -----
  it.effect(
    "scenario 9 — hierarchical truncation: set surviving + remove leaf → SET sk truncated",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { HierAssets },
          tables: { HierTable },
        })

        yield* db.entities.HierAssets.put({
          assetId: "rack-9",
          region: "americas",
          country: "us",
          city: "sf",
          site: "datacenter-1",
        })

        // Demote — drop site, keep country+city via set; structural rule
        // composes the leading prefix [country, city]. SET sk truncated.
        // PK untouched (region not in payload, not in removedSet).
        yield* db.entities.HierAssets.update({ assetId: "rack-9" })
          .set({ country: "us", city: "sf" })
          .remove(["site"])

        const item = yield* HierAssets.get({ assetId: "rack-9" }).pipe(Entity.asItem)
        // PK preserved (region untouched).
        expect(item.gsi1pk).toBeDefined()
        // SK SET to truncated leading prefix.
        expect(item.gsi1sk as string).toBe("$ip-test#v1#hierasset#country_us#city_sf")

        // begins_with query at city level still finds the asset.
        const atRegion = yield* db.entities.HierAssets.byLocation({
          region: "americas",
        }).collect()
        expect(atRegion.some((a) => a.assetId === "rack-9")).toBe(true)
      }).pipe(provideHier),
  )

  // ----- Scenario 10: Hole pattern + sparse → REMOVE sk -----
  it.effect("scenario 10 — hole pattern under sparse → REMOVE sk only (NOT GSI-wide)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { HybridDevices },
        tables: { IpTable },
      })

      yield* db.entities.HybridDevices.put({
        channel: "c-s10",
        deviceId: "d-s10",
        accountId: "acme",
        alertState: "active",
        timestamp: "2026-04-30T10:00:00Z",
      })

      // Hole on sk: pass timestamp only; alertState absent from payload.
      // Wait — under v1.7.1, omitted means untouched per the gate. To
      // exercise hole-under-sparse here we have to TOUCH sk by including
      // alertState as undefined (or include another sk composite). We use
      // `timestamp` as the touch signal; alertState is absent (stored value
      // exists, but a fresh ingest `set({ timestamp: T })` only mentions
      // timestamp). That makes sk touched (timestamp in payload), but
      // alertState... is in stored attrs as "active" — so the merged record
      // has alertState present.
      //
      // To exercise the actual hole-under-sparse semantic, the writer must
      // touch sk AND signal alertState as absent. Use `alertState: undefined`
      // explicitly to force the hole.
      yield* db.entities.HybridDevices.update({ channel: "c-s10", deviceId: "d-s10" }).set({
        alertState: undefined,
        timestamp: "2026-04-30T11:00:00Z",
      })

      const item = yield* HybridDevices.get({
        channel: "c-s10",
        deviceId: "d-s10",
      }).pipe(Entity.asItem)
      // sk REMOVE'd via sparse + can't-compose (hole at sk[0]).
      expect(item.gsi1sk).toBeUndefined()
      // pk preserved (untouched).
      expect(item.gsi1pk).toBeDefined()
    }).pipe(provideIp),
  )

  // ----- Scenario 11: Hole pattern + preserve, no removedSet → noop -----
  it.effect(
    "scenario 11 — hole pattern under preserve (no removedSet) → noop sk (stored value retained)",
    () =>
      Effect.gen(function* () {
        // Use HierAssets with byLocation sk = [country, city, site], both
        // preserve. Hole = country present, city absent, site present.
        const dbHier = yield* DynamoClient.make({
          entities: { HierAssets },
          tables: { HierTable },
        })

        yield* dbHier.entities.HierAssets.put({
          assetId: "rack-11",
          region: "americas",
          country: "us",
          city: "sf",
          site: "datacenter-1",
        })
        const beforeStored = yield* HierAssets.get({ assetId: "rack-11" }).pipe(Entity.asItem)
        const beforeSk = beforeStored.gsi1sk

        // Touch sk with a hole pattern under preserve, no removedSet.
        // city: undefined, site: 'datacenter-2' → hole at sk[1].
        yield* dbHier.entities.HierAssets.update({ assetId: "rack-11" }).set({
          country: "us",
          city: undefined,
          site: "datacenter-2",
        })

        const item = yield* HierAssets.get({ assetId: "rack-11" }).pipe(Entity.asItem)
        // v1.7.1: preserve + can't-compose + no cascade → noop. The stored
        // gsi1sk is left at its previous value (no SET, no REMOVE).
        expect(item.gsi1sk).toBe(beforeSk)
      }).pipe(provideHier),
  )

  // ----- Scenario 12: Hole pattern + preserve + removedSet → REMOVE via cascade override -----
  it.effect(
    "scenario 12 — hole pattern under preserve + removedSet → REMOVE sk via cascade override",
    () =>
      Effect.gen(function* () {
        const dbHier = yield* DynamoClient.make({
          entities: { HierAssets },
          tables: { HierTable },
        })

        yield* dbHier.entities.HierAssets.put({
          assetId: "rack-12",
          region: "americas",
          country: "us",
          city: "sf",
          site: "datacenter-1",
        })

        // Touch sk via removedSet on city, set site to a new value → hole
        // pattern (country present, city absent via removedSet, site
        // present). Preserve + cascade override → REMOVE sk.
        yield* dbHier.entities.HierAssets.update({ assetId: "rack-12" })
          .set({ site: "datacenter-2" })
          .remove(["city"])

        const item = yield* HierAssets.get({ assetId: "rack-12" }).pipe(Entity.asItem)
        expect(item.gsi1sk).toBeUndefined()
        // pk preserved.
        expect(item.gsi1pk).toBeDefined()
      }).pipe(provideHier),
  )

  // ----- Scenario 13: Multi-writer concurrent updates -----
  it.effect(
    "scenario 13 — multi-writer sequential updates produce expected combined GSI state",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { HybridDevices },
          tables: { IpTable },
        })

        // Initial state — full composites set on put.
        yield* db.entities.HybridDevices.put({
          channel: "c-s13",
          deviceId: "d-s13",
          accountId: "acme",
          alertState: "active",
          timestamp: "2026-04-30T10:00:00Z",
        })

        // Enrichment writer fires.
        yield* db.entities.HybridDevices.update({ channel: "c-s13", deviceId: "d-s13" }).set({
          accountId: "newAcct",
        })
        // Telemetry writer fires.
        yield* db.entities.HybridDevices.update({ channel: "c-s13", deviceId: "d-s13" }).set({
          alertState: "cleared",
          timestamp: "2026-04-30T11:00:00Z",
        })

        // Both writes preserved end-to-end. Final GSI state under newAcct +
        // cleared.
        const final = yield* db.entities.HybridDevices.byCurrentAlert({
          accountId: "newAcct",
        }).collect()
        expect(final.some((d) => d.deviceId === "d-s13")).toBe(true)
        // Stale account still empty.
        const stale = yield* db.entities.HybridDevices.byCurrentAlert({
          accountId: "acme",
        }).collect()
        expect(stale.some((d) => d.deviceId === "d-s13")).toBe(false)
      }).pipe(provideIp),
  )

  // ----- Scenario 14: DDB projection invariant (documented, not library behavior) -----
  it.effect(
    "scenario 14 — DDB projection invariant: item with only one GSI key attr is invisible",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { HybridDevices },
          tables: { IpTable },
        })

        // Put with full composites — visible.
        yield* db.entities.HybridDevices.put({
          channel: "c-s14",
          deviceId: "d-s14",
          accountId: "acme",
          alertState: "active",
          timestamp: "2026-04-30T10:00:00Z",
        })
        const visible = yield* db.entities.HybridDevices.byCurrentAlert({
          accountId: "acme",
        }).collect()
        expect(visible.some((d) => d.deviceId === "d-s14")).toBe(true)

        // Drop sk only — gsi1pk persists, gsi1sk REMOVE'd.
        yield* db.entities.HybridDevices.update({ channel: "c-s14", deviceId: "d-s14" }).set({
          alertState: undefined,
          timestamp: "2026-04-30T11:00:00Z",
        })

        // DDB projection rule: GSI query returns nothing for items missing
        // either of the GSI's key attrs. This is NOT library behavior — it's
        // the DDB invariant the per-half cascade relies on. We document it
        // here so readers see the round trip.
        const invisible = yield* db.entities.HybridDevices.byCurrentAlert({
          accountId: "acme",
        }).collect()
        expect(invisible.some((d) => d.deviceId === "d-s14")).toBe(false)

        // Raw item still has gsi1pk — confirming the per-half persistence.
        const item = yield* HybridDevices.get({
          channel: "c-s14",
          deviceId: "d-s14",
        }).pipe(Entity.asItem)
        expect(item.gsi1pk).toBeDefined()
        expect(item.gsi1sk).toBeUndefined()
      }).pipe(provideIp),
  )
})

// ===========================================================================
// Empty-composite-half GSI shape — closes #46 (v1.7.3 skip-predicate reframe)
// ===========================================================================
//
// The bug (#46): GSI halves declared with `composite: []` had every
// `.some(...)` clause in the touched-predicate trivially return false, so
// `Entity.update()` skipped composing the half entirely. Items written via
// .put() (which uses the separate composeAllKeys/composeIndexKeys path) had
// the half correctly populated, but any subsequent .update() that touched
// the OTHER half left the empty-composite half untouched — and worse, an
// .update() that bound a previously-sparse GSI for the first time wrote
// only the PK half, leaving the SK half missing → invisible to the GSI.
//
// v1.7.3 reframes the gate as a skip-predicate keyed on the gate's actual
// purpose (multi-writer protection). The leading `length > 0` guard
// short-circuits empty-composite halves to "always evaluated," and
// classifyHalf (which already handled empty composites correctly as
// { kind: 'set', length: 0 }) is finally reached.
//
// These tests exercise the v1.7.3 fix end-to-end against DDB Local. They
// reproduce the #46 scenario exactly: a Vehicle entity with a sparse
// byDeviceBinding GSI whose SK composite is empty.
// ===========================================================================

class VehicleConnected extends Schema.Class<VehicleConnected>("VehicleConnected")({
  vehicleId: Schema.String,
  deviceBinding: Schema.optional(Schema.String),
  // Companion non-PK composites for the mixed-shape test.
  tenantId: Schema.optional(Schema.String),
  status: Schema.optional(Schema.String),
  // Plain payload field — used to drive .update() calls that don't touch
  // any GSI composite directly.
  label: Schema.optional(Schema.String),
}) {}

const vehicleSchema = DynamoSchema.make({ name: "vehicle-empty", version: 1 })
const vehicleTableName = `vehicle-empty-${Date.now()}`

// The exact #46 reproducer entity.
const VehiclesByDevice = Entity.make({
  model: VehicleConnected,
  entityType: "VehicleByDevice",
  primaryKey: {
    pk: { field: "pk", composite: ["vehicleId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byDeviceBinding: {
      name: "gsi3",
      pk: { field: "gsi3pk", composite: ["deviceBinding"] },
      sk: { field: "gsi3sk", composite: [] },
    },
  },
  timestamps: true,
})

// Mixed entity: ONE empty-SK GSI (byDeviceBinding) + ONE multi-writer GSI
// with non-PK composites (byTenant). Used to prove the empty-composite-
// half fix coexists with the v1.7.1 multi-writer protection (#41).
const VehiclesMixed = Entity.make({
  model: VehicleConnected,
  entityType: "VehicleMixed",
  primaryKey: {
    pk: { field: "pk", composite: ["vehicleId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byDeviceBinding: {
      name: "gsi3",
      pk: { field: "gsi3pk", composite: ["deviceBinding"] },
      sk: { field: "gsi3sk", composite: [] },
    },
    byTenant: {
      name: "gsi4",
      pk: { field: "gsi4pk", composite: ["tenantId"] },
      sk: { field: "gsi4sk", composite: ["status"] },
      indexPolicy: { pk: "preserve", sk: "preserve" },
    },
  },
  timestamps: true,
})

const VehicleTable = Table.make({
  schema: vehicleSchema,
  entities: { VehiclesByDevice, VehiclesMixed },
})
const VehicleTestLayer = Layer.mergeAll(ClientLayer, VehicleTable.layer({ name: vehicleTableName }))
const provideVehicle = Effect.provide(VehicleTestLayer)

describeConnected("Empty-composite-half GSI shape (closes #46)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: vehicleTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(VehicleTable),
        })
      }).pipe(provideVehicle, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: vehicleTableName })
      }).pipe(
        provideVehicle,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // ----- Scenario 1: #46 reproducer end-to-end -----
  it.effect(
    "scenario 1 — put without deviceBinding, update binds it, byDeviceBinding query returns the item",
    () =>
      Effect.gen(function* () {
        // The exact #46 reproducer. Pre-v1.7.3: vehicle invisible after the
        // update because gsi3sk was never written.
        const db = yield* DynamoClient.make({
          entities: { VehiclesByDevice },
          tables: { VehicleTable },
        })

        // Step 1: put without deviceBinding → sparse, no gsi3 keys composed.
        yield* db.entities.VehiclesByDevice.put({
          vehicleId: "veh-46-1",
          // deviceBinding intentionally omitted
        })

        // Step 2: bind the device via .update().
        yield* db.entities.VehiclesByDevice.update({ vehicleId: "veh-46-1" }).set({
          deviceBinding: "cloud#dev-46-1",
        })

        // v1.7.3 critical: byDeviceBinding query MUST return the vehicle.
        // Pre-v1.7.3 this returned 0 items because gsi3sk was never written.
        const rows = yield* db.entities.VehiclesByDevice.byDeviceBinding({
          deviceBinding: "cloud#dev-46-1",
        }).collect()
        expect(rows).toHaveLength(1)
        expect(rows[0]!.vehicleId).toBe("veh-46-1")

        // Verify the raw item — both halves of gsi3 must be present.
        const raw = yield* (yield* DynamoClient).getItem({
          TableName: vehicleTableName,
          Key: {
            pk: { S: "$vehicle-empty#v1#vehiclebydevice#vehicleid_veh-46-1" },
            sk: { S: "$vehicle-empty#v1#vehiclebydevice" },
          },
        })
        expect(raw.Item).toBeDefined()
        expect(raw.Item!.gsi3pk?.S).toBe(
          "$vehicle-empty#v1#vehiclebydevice#devicebinding_cloud#dev-46-1",
        )
        // The empty-composite half — gsi3sk MUST be the constant entity prefix.
        expect(raw.Item!.gsi3sk?.S).toBe("$vehicle-empty#v1#vehiclebydevice")
      }).pipe(provideVehicle),
  )

  // ----- Scenario 2: Idempotent re-update -----
  it.effect("scenario 2 — multiple sequential updates keep gsi3 keys consistent", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { VehiclesByDevice },
        tables: { VehicleTable },
      })

      yield* db.entities.VehiclesByDevice.put({
        vehicleId: "veh-46-2",
        deviceBinding: "cloud#dev-initial",
      })

      // Three updates: change deviceBinding, change a label, change
      // deviceBinding again. Each one must keep gsi3pk consistent with the
      // CURRENT deviceBinding and gsi3sk consistent with the constant
      // entity prefix.
      yield* db.entities.VehiclesByDevice.update({ vehicleId: "veh-46-2" }).set({
        deviceBinding: "cloud#dev-second",
      })
      yield* db.entities.VehiclesByDevice.update({ vehicleId: "veh-46-2" }).set({
        label: "stamped",
      })
      yield* db.entities.VehiclesByDevice.update({ vehicleId: "veh-46-2" }).set({
        deviceBinding: "cloud#dev-third",
      })

      // Final state — vehicle visible under cloud#dev-third only.
      const third = yield* db.entities.VehiclesByDevice.byDeviceBinding({
        deviceBinding: "cloud#dev-third",
      }).collect()
      expect(third).toHaveLength(1)
      expect(third[0]!.vehicleId).toBe("veh-46-2")
      expect(third[0]!.label).toBe("stamped")

      // Older bindings — no rows.
      const initial = yield* db.entities.VehiclesByDevice.byDeviceBinding({
        deviceBinding: "cloud#dev-initial",
      }).collect()
      expect(initial).toHaveLength(0)
      const second = yield* db.entities.VehiclesByDevice.byDeviceBinding({
        deviceBinding: "cloud#dev-second",
      }).collect()
      expect(second).toHaveLength(0)

      // gsi3sk must remain the constant entity prefix across all updates.
      const raw = yield* (yield* DynamoClient).getItem({
        TableName: vehicleTableName,
        Key: {
          pk: { S: "$vehicle-empty#v1#vehiclebydevice#vehicleid_veh-46-2" },
          sk: { S: "$vehicle-empty#v1#vehiclebydevice" },
        },
      })
      expect(raw.Item!.gsi3sk?.S).toBe("$vehicle-empty#v1#vehiclebydevice")
    }).pipe(provideVehicle),
  )

  // ----- Scenario 3: Mixed entity (#46 fix + #41 multi-writer fix coexist) -----
  it.effect(
    "scenario 3 — mixed entity: empty-SK GSI fixes; multi-writer GSI on same entity untouched by stamps",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { VehiclesMixed },
          tables: { VehicleTable },
        })

        // Seed: full vehicle with deviceBinding + tenantId + status.
        yield* db.entities.VehiclesMixed.put({
          vehicleId: "veh-46-3",
          deviceBinding: "cloud#dev-mixed",
          tenantId: "tenant-1",
          status: "active",
        })

        // Capture baseline byTenant key composition.
        const baseline = yield* (yield* DynamoClient).getItem({
          TableName: vehicleTableName,
          Key: {
            pk: { S: "$vehicle-empty#v1#vehiclemixed#vehicleid_veh-46-3" },
            sk: { S: "$vehicle-empty#v1#vehiclemixed" },
          },
        })
        const baselineGsi4pk = baseline.Item?.gsi4pk?.S
        const baselineGsi4sk = baseline.Item?.gsi4sk?.S
        expect(baselineGsi4pk).toBeDefined()
        expect(baselineGsi4sk).toBeDefined()

        // Stamp update: only label changes. Neither byDeviceBinding's
        // composites (deviceBinding) nor byTenant's composites (tenantId,
        // status) are in the payload.
        // - byDeviceBinding.pk: skipped (multi-writer protection — composite
        //   absent). gsi3pk preserved.
        // - byDeviceBinding.sk: empty composite → ALWAYS evaluated → constant
        //   prefix re-SET. gsi3sk preserved with the same value.
        // - byTenant.pk + sk: skipped (multi-writer protection). gsi4pk +
        //   gsi4sk preserved untouched. v1.7.1 #41 fix preserved.
        yield* db.entities.VehiclesMixed.update({ vehicleId: "veh-46-3" }).set({
          label: "stamped",
        })

        const after = yield* (yield* DynamoClient).getItem({
          TableName: vehicleTableName,
          Key: {
            pk: { S: "$vehicle-empty#v1#vehiclemixed#vehicleid_veh-46-3" },
            sk: { S: "$vehicle-empty#v1#vehiclemixed" },
          },
        })
        // byTenant unchanged — multi-writer fix preserved across stamp.
        expect(after.Item?.gsi4pk?.S).toBe(baselineGsi4pk)
        expect(after.Item?.gsi4sk?.S).toBe(baselineGsi4sk)
        // byDeviceBinding still visible under cloud#dev-mixed.
        const byDev = yield* db.entities.VehiclesMixed.byDeviceBinding({
          deviceBinding: "cloud#dev-mixed",
        }).collect()
        expect(byDev.some((v) => v.vehicleId === "veh-46-3")).toBe(true)
        // byTenant still visible.
        const byTen = yield* db.entities.VehiclesMixed.byTenant({
          tenantId: "tenant-1",
        }).collect()
        expect(byTen.some((v) => v.vehicleId === "veh-46-3")).toBe(true)

        // gsi3sk constant prefix preserved.
        expect(after.Item?.gsi3sk?.S).toBe("$vehicle-empty#v1#vehiclemixed")
      }).pipe(provideVehicle),
  )

  // ----- Scenario 4: Entity.remove on the empty-composite-half GSI's PK -----
  it.effect(
    "scenario 4 — Entity.remove(['deviceBinding']) on empty-SK GSI: PK REMOVE'd via cascade, SK still SET (constant prefix)",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { VehiclesByDevice },
          tables: { VehicleTable },
        })

        // Seed: vehicle with deviceBinding.
        yield* db.entities.VehiclesByDevice.put({
          vehicleId: "veh-46-4",
          deviceBinding: "cloud#dev-to-remove",
        })

        // Item visible.
        const before = yield* db.entities.VehiclesByDevice.byDeviceBinding({
          deviceBinding: "cloud#dev-to-remove",
        }).collect()
        expect(before).toHaveLength(1)

        // Remove the deviceBinding attribute. PK half touched via
        // removedSet, can't compose without deviceBinding → preserve +
        // cascade override → REMOVE gsi3pk. SK half (empty composite) →
        // always evaluated → constant prefix re-SET.
        yield* db.entities.VehiclesByDevice.update({ vehicleId: "veh-46-4" }).remove([
          "deviceBinding",
        ])

        // Item invisible (DDB needs both keys for projection).
        const after = yield* db.entities.VehiclesByDevice.byDeviceBinding({
          deviceBinding: "cloud#dev-to-remove",
        }).collect()
        expect(after).toHaveLength(0)

        // Raw item: gsi3pk REMOVE'd, gsi3sk still present (constant prefix).
        const raw = yield* (yield* DynamoClient).getItem({
          TableName: vehicleTableName,
          Key: {
            pk: { S: "$vehicle-empty#v1#vehiclebydevice#vehicleid_veh-46-4" },
            sk: { S: "$vehicle-empty#v1#vehiclebydevice" },
          },
        })
        expect(raw.Item).toBeDefined()
        expect(raw.Item!.gsi3pk).toBeUndefined()
        expect(raw.Item!.gsi3sk?.S).toBe("$vehicle-empty#v1#vehiclebydevice")
      }).pipe(provideVehicle),
  )
})

// ---------------------------------------------------------------------------
// TTL attribute name override (closes #51)
//
// Consumers may have pre-existing tables whose `TimeToLiveSpecification.AttributeName`
// is not `_ttl` (the library default). This describeConnected block verifies the
// end-to-end behaviour with `TableConfig.ttlAttributeName: "ttl"` across all
// three lifecycle features that write TTL — `timeSeries: { ttl }`,
// `softDelete: { ttl }`, and `versioned: { retain, ttl }` — and the restore
// path that strips the configured name.
// ---------------------------------------------------------------------------

class TtlEvent extends Schema.Class<TtlEvent>("TtlEvent")({
  channel: Schema.String,
  deviceId: Schema.String,
  timestamp: Schema.DateTimeUtc,
  reading: Schema.optional(Schema.Number),
}) {}

const TtlEventAppendInput = Schema.Struct({
  channel: Schema.String,
  deviceId: Schema.String,
  timestamp: Schema.DateTimeUtc,
  reading: Schema.optional(Schema.Number),
})

class TtlSoftItem extends Schema.Class<TtlSoftItem>("TtlSoftItem")({
  itemId: Schema.String,
  label: Schema.String,
}) {}

class TtlRetainItem extends Schema.Class<TtlRetainItem>("TtlRetainItem")({
  itemId: Schema.String,
  payload: Schema.String,
}) {}

class TtlUniqueItem extends Schema.Class<TtlUniqueItem>("TtlUniqueItem")({
  itemId: Schema.String,
  reservationCode: Schema.String,
}) {}

const ttlSchema = DynamoSchema.make({ name: "ttl-attr-test", version: 1 })
const ttlTableName = `ttl-attr-test-${Date.now()}`

const TtlEvents = Entity.make({
  model: TtlEvent,
  entityType: "TtlEvent",
  primaryKey: {
    pk: { field: "pk", composite: ["channel", "deviceId"] },
    sk: { field: "sk", composite: [] },
  },
  timestamps: true,
  timeSeries: {
    orderBy: "timestamp",
    ttl: Duration.days(7),
    appendInput: TtlEventAppendInput,
  },
})

const TtlSoftItems = Entity.make({
  model: TtlSoftItem,
  entityType: "TtlSoftItem",
  primaryKey: {
    pk: { field: "pk", composite: ["itemId"] },
    sk: { field: "sk", composite: [] },
  },
  timestamps: true,
  versioned: true,
  softDelete: { ttl: Duration.days(30) },
})

const TtlRetainItems = Entity.make({
  model: TtlRetainItem,
  entityType: "TtlRetainItem",
  primaryKey: {
    pk: { field: "pk", composite: ["itemId"] },
    sk: { field: "sk", composite: [] },
  },
  timestamps: true,
  versioned: { retain: true, ttl: Duration.days(90) },
})

// Unique constraint with a TTL — the sentinel auto-expires, freeing the
// reservation (#58: unique.ttl wired up). String form exercises Duration|string.
const TtlUniqueItems = Entity.make({
  model: TtlUniqueItem,
  entityType: "TtlUniqueItem",
  primaryKey: {
    pk: { field: "pk", composite: ["itemId"] },
    sk: { field: "sk", composite: [] },
  },
  timestamps: true,
  unique: { byCode: { fields: ["reservationCode"], ttl: "30 minutes" } },
})

const TtlTable = Table.make({
  schema: ttlSchema,
  entities: { TtlEvents, TtlSoftItems, TtlRetainItems, TtlUniqueItems },
})
const TtlTestLayer = Layer.mergeAll(
  ClientLayer,
  TtlTable.layer({ name: ttlTableName, ttlAttributeName: "ttl" }),
)
const provideTtl = Effect.provide(TtlTestLayer)

describeConnected("TableConfig.ttlAttributeName override (closes #51)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: ttlTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(TtlTable),
        })
      }).pipe(provideTtl, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: ttlTableName })
      }).pipe(
        provideTtl,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("timeSeries event writes TTL to the configured attribute name", () =>
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.millis(FROZEN_MS))
      const db = yield* DynamoClient.make({
        entities: { TtlEvents },
        tables: { TtlTable },
      })

      yield* db.entities.TtlEvents.append({
        channel: "c-cfg-1",
        deviceId: "d-1",
        timestamp: DateTime.makeUnsafe("2026-04-22T10:00:00.000Z"),
      })

      const raw = yield* (yield* DynamoClient).query({
        TableName: ttlTableName,
        KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :skPrefix)",
        ExpressionAttributeNames: { "#pk": "pk", "#sk": "sk" },
        ExpressionAttributeValues: {
          ":pk": { S: "$ttl-attr-test#v1#ttlevent#channel_c-cfg-1#deviceid_d-1" },
          ":skPrefix": { S: "$ttl-attr-test#v1#ttlevent#e#" },
        },
      })
      expect(raw.Items).toBeDefined()
      expect(raw.Items!.length).toBe(1)
      const event = raw.Items![0]!
      // Configured attribute "ttl" carries the epoch-seconds expiry.
      expect(event.ttl?.N).toBeDefined()
      const ttlVal = Number(event.ttl!.N)
      // Clock-backed TTL is deterministic under TestClock: exactly frozen + 7 days.
      expect(ttlVal).toBe(FROZEN_SECONDS + 7 * 86400)
      // Library default "_ttl" must NOT be written when override is in effect.
      expect(event._ttl).toBeUndefined()
    }).pipe(provideTtl),
  )

  it.effect("soft-deleted item writes TTL to the configured attribute name", () =>
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.millis(FROZEN_MS))
      const db = yield* DynamoClient.make({
        entities: { TtlSoftItems },
        tables: { TtlTable },
      })

      yield* db.entities.TtlSoftItems.put({ itemId: "sd-1", label: "to be deleted" })
      yield* db.entities.TtlSoftItems.delete({ itemId: "sd-1" })

      // Read the soft-deleted record raw to assert the attribute name.
      const raw = yield* (yield* DynamoClient).query({
        TableName: ttlTableName,
        KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :skPrefix)",
        ExpressionAttributeNames: { "#pk": "pk", "#sk": "sk" },
        ExpressionAttributeValues: {
          ":pk": { S: "$ttl-attr-test#v1#ttlsoftitem#itemid_sd-1" },
          ":skPrefix": { S: "$ttl-attr-test#v1#ttlsoftitem#deleted#" },
        },
      })
      expect(raw.Items).toBeDefined()
      expect(raw.Items!.length).toBe(1)
      const deleted = raw.Items![0]!
      expect(deleted.ttl?.N).toBeDefined()
      const ttlVal = Number(deleted.ttl!.N)
      // Clock-backed TTL is deterministic under TestClock: exactly frozen + 30 days.
      expect(ttlVal).toBe(FROZEN_SECONDS + 30 * 86400)
      expect(deleted._ttl).toBeUndefined()
    }).pipe(provideTtl),
  )

  it.effect("restore strips the configured TTL attribute from the resurrected item", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { TtlSoftItems },
        tables: { TtlTable },
      })

      yield* db.entities.TtlSoftItems.put({ itemId: "sd-2", label: "round-trip" })
      yield* db.entities.TtlSoftItems.delete({ itemId: "sd-2" })
      yield* db.entities.TtlSoftItems.restore({ itemId: "sd-2" })

      const raw = yield* (yield* DynamoClient).getItem({
        TableName: ttlTableName,
        Key: {
          pk: { S: "$ttl-attr-test#v1#ttlsoftitem#itemid_sd-2" },
          sk: { S: "$ttl-attr-test#v1#ttlsoftitem" },
        },
      })
      expect(raw.Item).toBeDefined()
      // Restored item should not carry the TTL — restore strips the configured name.
      expect(raw.Item!.ttl).toBeUndefined()
      expect(raw.Item!._ttl).toBeUndefined()
    }).pipe(provideTtl),
  )

  it.effect("versioned snapshot writes TTL to the configured attribute name", () =>
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.millis(FROZEN_MS))
      const db = yield* DynamoClient.make({
        entities: { TtlRetainItems },
        tables: { TtlTable },
      })

      // First put produces a v1 snapshot under the retain config (which has ttl).
      yield* db.entities.TtlRetainItems.put({ itemId: "rt-1", payload: "initial" })

      const raw = yield* (yield* DynamoClient).query({
        TableName: ttlTableName,
        KeyConditionExpression: "#pk = :pk AND begins_with(#sk, :skPrefix)",
        ExpressionAttributeNames: { "#pk": "pk", "#sk": "sk" },
        ExpressionAttributeValues: {
          ":pk": { S: "$ttl-attr-test#v1#ttlretainitem#itemid_rt-1" },
          ":skPrefix": { S: "$ttl-attr-test#v1#ttlretainitem#v#" },
        },
      })
      expect(raw.Items).toBeDefined()
      expect(raw.Items!.length).toBe(1)
      const snapshot = raw.Items![0]!
      expect(snapshot.ttl?.N).toBeDefined()
      const ttlVal = Number(snapshot.ttl!.N)
      // Clock-backed TTL is deterministic under TestClock: exactly frozen + 90 days.
      expect(ttlVal).toBe(FROZEN_SECONDS + 90 * 86400)
      expect(snapshot._ttl).toBeUndefined()
    }).pipe(provideTtl),
  )

  it.effect("unique sentinel carries the configured TTL (closes #58)", () =>
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.millis(FROZEN_MS))
      const db = yield* DynamoClient.make({
        entities: { TtlUniqueItems },
        tables: { TtlTable },
      })

      // Write an item with a unique constraint that declares `ttl: "30 minutes"`.
      yield* db.entities.TtlUniqueItems.put({ itemId: "u-1", reservationCode: "RES-7" })

      // Read the unique sentinel item directly. Key format (lowercased casing):
      //   pk = "<schema>#<entity>.<constraint>#<value>", sk = "<schema>#<entity>.<constraint>"
      const sentinel = yield* (yield* DynamoClient).getItem({
        TableName: ttlTableName,
        Key: {
          pk: { S: "$ttl-attr-test#v1#ttluniqueitem.bycode#res-7" },
          sk: { S: "$ttl-attr-test#v1#ttluniqueitem.bycode" },
        },
      })
      expect(sentinel.Item).toBeDefined()
      // The configured TTL attribute ("ttl") carries the epoch-seconds expiry,
      // deterministic under TestClock: exactly frozen + 30 minutes.
      expect(sentinel.Item!.ttl?.N).toBeDefined()
      expect(Number(sentinel.Item!.ttl!.N)).toBe(FROZEN_SECONDS + 30 * 60)
      // Library default "_ttl" must NOT be written when the override is in effect.
      expect(sentinel.Item!._ttl).toBeUndefined()
    }).pipe(provideTtl),
  )
})

// ---------------------------------------------------------------------------
// generatedId — auto-generated UUID primary keys via the Crypto service (#57)
// ---------------------------------------------------------------------------

const genIdSchema = DynamoSchema.make({ name: "genid-test", version: 1 })
const genIdTableName = `genid-test-${Date.now()}`

class GenWidget extends Schema.Class<GenWidget>("GenWidget")({
  widgetId: Schema.String,
  owner: Schema.String,
  label: Schema.NonEmptyString,
}) {}

// The generated `widgetId` composes into BOTH the primary key AND the
// `byOwner` GSI sort-key — so we can assert the item is queryable by primary
// key and by the GSI the generated id participates in.
const GenWidgets = Entity.make({
  model: GenWidget,
  entityType: "GenWidget",
  primaryKey: {
    pk: { field: "pk", composite: ["widgetId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byOwner: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["owner"] },
      sk: { field: "gsi1sk", composite: ["widgetId"] },
    },
  },
  generatedId: { field: "widgetId" },
  timestamps: true,
})

const GenIdTable = Table.make({ schema: genIdSchema, entities: { GenWidgets } })
const GenIdTestLayer = Layer.mergeAll(ClientLayer, GenIdTable.layer({ name: genIdTableName }))
const provideGenId = Effect.provide(GenIdTestLayer)

describeConnected("generatedId integration tests (closes #57)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: genIdTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(GenIdTable),
        })
      }).pipe(provideGenId, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: genIdTableName })
      }).pipe(
        provideGenId,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // UUID-validation schema (issue requests Schema.isUUID for the format check).
  const UUID = Schema.String.check(Schema.isUUID())

  it.effect("put WITHOUT id → read back → id present, valid UUID, queryable by PK + GSI", () =>
    // The typed client requires NO Crypto layer (R = never end-to-end): the
    // default Crypto is bundled by DynamoClient.make. GenIdTestLayer provides
    // only DynamoClient + TableConfig.
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { GenWidgets },
        tables: { GenIdTable },
      })

      // No widgetId supplied — the library fills it from the bundled Crypto.
      const created = yield* db.entities.GenWidgets.put({ owner: "alice", label: "Sprocket" })

      // id present and a valid UUID.
      expect(typeof created.widgetId).toBe("string")
      const isUuid = yield* Schema.decodeUnknownEffect(UUID)(created.widgetId).pipe(
        Effect.as(true),
        Effect.catch(() => Effect.succeed(false)),
      )
      expect(isUuid).toBe(true)

      // Queryable by primary key (the generated id composed into the PK).
      const fetched = yield* db.entities.GenWidgets.get({ widgetId: created.widgetId })
      expect(fetched.widgetId).toBe(created.widgetId)
      expect(fetched.label).toBe("Sprocket")

      // Queryable by the GSI the generated id composes into.
      const byOwner = yield* db.entities.GenWidgets.byOwner({ owner: "alice" }).collect()
      expect(byOwner.map((w) => w.widgetId)).toContain(created.widgetId)
    }).pipe(provideGenId),
  )

  // #120 — the multi-item write paths gated on the entity's CONFIGURATION, so
  // a `generatedId` entity was barred from `transactWrite` / `Batch.write`
  // outright, citing a `Crypto` dependency that a supplied id removes. Proven
  // end-to-end here: the item lands, decodes, and is queryable by the primary
  // key and by the GSI its id composes into — same as a plain `put`.
  it.effect("transactWrite accepts a supplied generated id and writes a well-formed item", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { GenWidgets },
        tables: { GenIdTable },
      })

      yield* Transaction.transactWrite([
        db.entities.GenWidgets.put({
          widgetId: "txn-widget-1",
          owner: "carol",
          label: "Flywheel",
        }),
      ])

      const fetched = yield* db.entities.GenWidgets.get({ widgetId: "txn-widget-1" })
      expect(fetched.label).toBe("Flywheel")

      const byOwner = yield* db.entities.GenWidgets.byOwner({ owner: "carol" }).collect()
      expect(byOwner.map((w) => w.widgetId)).toContain("txn-widget-1")
    }).pipe(provideGenId),
  )

  it.effect("Batch.write accepts a supplied generated id", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { GenWidgets },
        tables: { GenIdTable },
      })

      yield* Batch.write([
        db.entities.GenWidgets.put({ widgetId: "batch-widget-1", owner: "dan", label: "Gear" }),
      ])

      const fetched = yield* db.entities.GenWidgets.get({ widgetId: "batch-widget-1" })
      expect(fetched.label).toBe("Gear")
    }).pipe(provideGenId),
  )

  it.effect("transactWrite still refuses an OMITTED generated id", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { GenWidgets },
        tables: { GenIdTable },
      })

      const error = yield* Transaction.transactWrite([
        db.entities.GenWidgets.put({ owner: "erin", label: "Ratchet" } as never),
      ]).pipe(Effect.flip)

      expect(error._tag).toBe("ValidationError")
      expect(String((error as { cause: unknown }).cause)).toContain("omitted generated id")
    }).pipe(provideGenId),
  )

  it.effect("caller-supplied id is respected over auto-generation", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { GenWidgets },
        tables: { GenIdTable },
      })
      const created = yield* db.entities.GenWidgets.put({
        widgetId: "explicit-widget-1",
        owner: "bob",
        label: "Cog",
      })
      expect(created.widgetId).toBe("explicit-widget-1")
      const fetched = yield* db.entities.GenWidgets.get({ widgetId: "explicit-widget-1" })
      expect(fetched.owner).toBe("bob")
    }).pipe(provideGenId),
  )
})

// ---------------------------------------------------------------------------
// #69 — pure @effect-dynamodb/schema authoring bound via DynamoClient.make
//
// Entities authored with the AWS-free `@effect-dynamodb/schema` `Entity.make`
// (the headline of the schema/runtime split) must round-trip against real
// DynamoDB once bound. DynamoClient.make promotes each pure definition to a full
// runtime entity (a thin op-attach over its retained `_data`). This validates
// marshalling/decoding end-to-end — the deferred-decode crash class only
// surfaces on a non-empty read, so every read here returns >= 1 item.
// ---------------------------------------------------------------------------

class PureUser extends Schema.Class<PureUser>("PureUser")({
  orgId: Schema.String,
  userId: Schema.String,
  email: Schema.String,
  name: Schema.String,
}) {}

class PureTeam extends Schema.Class<PureTeam>("PureTeam")({
  orgId: Schema.String,
  teamId: Schema.String,
  label: Schema.String,
}) {}

const PureUsers = PureEntity.make({
  model: PureUser,
  entityType: "PureUser",
  primaryKey: {
    pk: { field: "pk", composite: ["orgId"] },
    sk: { field: "sk", composite: ["userId"] },
  },
  indexes: {
    usersByOrg: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["orgId"] },
      sk: { field: "gsi1sk", composite: ["userId"] },
      collection: "pureMembers",
    },
  },
})

const PureTeams = PureEntity.make({
  model: PureTeam,
  entityType: "PureTeam",
  primaryKey: {
    pk: { field: "pk", composite: ["orgId"] },
    sk: { field: "sk", composite: ["teamId"] },
  },
  indexes: {
    teamsByOrg: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["orgId"] },
      sk: { field: "gsi1sk", composite: ["teamId"] },
      collection: "pureMembers",
    },
  },
})

// Pure entity WITH a ref to another pure entity. Write-time ref hydration calls
// `.get()` on the ref target, so binding must promote the target too (#69).
class PureProject extends Schema.Class<PureProject>("PureProject")({
  projectId: Schema.String,
  projectName: Schema.String,
}) {}

class PureTask extends Schema.Class<PureTask>("PureTask")({
  taskId: Schema.String,
  title: Schema.String,
  project: DynamoModel.ref(PureProject),
}) {}

const PureProjects = PureEntity.make({
  model: DynamoModel.configure(PureProject, { projectId: { identifier: true } }),
  entityType: "PureProject",
  primaryKey: {
    pk: { field: "pk", composite: ["projectId"] },
    sk: { field: "sk", composite: [] },
  },
})

const PureTasks = PureEntity.make({
  model: PureTask,
  entityType: "PureTask",
  primaryKey: {
    pk: { field: "pk", composite: ["taskId"] },
    sk: { field: "sk", composite: [] },
  },
  refs: { project: { entity: PureProjects } },
})

const PureTable = Table.make({
  schema: AppSchema,
  entities: { PureUsers, PureTeams, PureProjects, PureTasks },
})
const pureTableName = "edd-pure-authoring-connected"
const PureTestLayer = Layer.mergeAll(ClientLayer, PureTable.layer({ name: pureTableName }))
const providePure = Effect.provide(PureTestLayer)

describeConnected("#69 — pure schema authoring → DynamoClient.make → real DynamoDB", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client
          .createTable({
            TableName: pureTableName,
            BillingMode: "PAY_PER_REQUEST",
            ...Table.definition(PureTable),
          })
          .pipe(Effect.catch(() => Effect.void))
      }).pipe(providePure, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: pureTableName })
      }).pipe(
        providePure,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("round-trips put/get/query/scan/collection/update/delete", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { PureUsers, PureTeams },
        tables: { PureTable },
      })

      yield* db.entities.PureUsers.put({
        orgId: "acme",
        userId: "u1",
        email: "a@x.io",
        name: "Ann",
      })
      yield* db.entities.PureUsers.put({
        orgId: "acme",
        userId: "u2",
        email: "b@x.io",
        name: "Bob",
      })
      yield* db.entities.PureTeams.put({ orgId: "acme", teamId: "t1", label: "Eng" })

      const ann = yield* db.entities.PureUsers.get({ orgId: "acme", userId: "u1" })
      expect(ann.name).toBe("Ann")

      const usersByOrg = yield* db.entities.PureUsers.usersByOrg({ orgId: "acme" }).collect()
      expect(usersByOrg.map((u) => u.userId).sort()).toEqual(["u1", "u2"])

      const scanned = yield* db.entities.PureUsers.scan().collect()
      expect(scanned.length).toBeGreaterThanOrEqual(2)

      const grouped = (yield* db.collections.pureMembers!({ orgId: "acme" }).collect()) as {
        PureUsers: PureUser[]
        PureTeams: PureTeam[]
      }
      expect(grouped.PureUsers.map((u) => u.userId).sort()).toEqual(["u1", "u2"])
      expect(grouped.PureTeams.map((t) => t.teamId)).toEqual(["t1"])

      yield* db.entities.PureUsers.update({ orgId: "acme", userId: "u1" }).set({ name: "Annie" })
      const updated = yield* db.entities.PureUsers.get({ orgId: "acme", userId: "u1" })
      expect(updated.name).toBe("Annie")

      yield* db.entities.PureUsers.delete({ orgId: "acme", userId: "u2" })
      const remaining = yield* db.entities.PureUsers.usersByOrg({ orgId: "acme" }).collect()
      expect(remaining.map((u) => u.userId)).toEqual(["u1"])
    }).pipe(providePure),
  )

  it.effect("pure entity with refs hydrates on write (ref target promoted)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { PureUsers, PureTeams, PureProjects, PureTasks },
        tables: { PureTable },
      })

      yield* db.entities.PureProjects.put({ projectId: "p1", projectName: "Apollo" })

      // Write a task by ref id — hydration calls PureProjects.get(p1) to
      // denormalise. Before #69's ref-target promotion this threw
      // "ref.refEntity.get is not a function".
      const task = yield* db.entities.PureTasks.put({
        taskId: "t1",
        title: "Land",
        projectId: "p1",
      })
      expect(task.project.projectId).toBe("p1")
      expect(task.project.projectName).toBe("Apollo")

      const fetched = yield* db.entities.PureTasks.get({ taskId: "t1" })
      expect(fetched.project.projectName).toBe("Apollo")
    }).pipe(providePure),
  )
})

// ===========================================================================
// #71 + #72 — pure-authored aggregate edges + transform (Pattern B) date fields
// ===========================================================================
//
// #71: aggregate edges authored from PURE `@effect-dynamodb/schema` definitions
//      carry no runtime `.get`; hydration must promote them (was a TypeError).
// #72: a `Schema.DateTimeUtcFromString` field on the aggregate root, on a
//      hydrated aggregate edge, and on a plain Entity ref target must decode
//      exactly once (was "Expected string, got DateTime.Utc" / corrupt write).
// Option A: a `Schema.DateTimeUtc` (Pattern A self-date) field INSIDE a ref /
//      edge target must also round-trip — `substituteSchemaDeep` recurses into
//      the nested target model while preserving its class instance identity.

class EddAuthor extends Schema.Class<EddAuthor>("EddAuthor")({
  id: Schema.String.pipe(DynamoModel.identifier),
  name: Schema.String,
  dateOfBirth: Schema.DateTimeUtcFromString, // Pattern B (transform)
  joinedAt: Schema.DateTimeUtc, // Pattern A self-date nested in a ref target (Option A)
}) {}
class EddCoach extends Schema.Class<EddCoach>("EddCoach")({
  id: Schema.String.pipe(DynamoModel.identifier),
  name: Schema.String,
  dateOfBirth: Schema.DateTimeUtcFromString, // Pattern B
  joinedAt: Schema.DateTimeUtc, // Pattern A self-date nested in an edge target (Option A)
  retiredAt: Schema.optional(Schema.DateTimeUtc), // optional Pattern A nested in an edge target
}) {}
class EddVenue extends Schema.Class<EddVenue>("EddVenue")({
  id: Schema.String.pipe(DynamoModel.identifier),
  name: Schema.String,
}) {}
class EddPlayer extends Schema.Class<EddPlayer>("EddPlayer")({
  id: Schema.String.pipe(DynamoModel.identifier),
  name: Schema.String,
}) {}
// Many-edge element: a wrapper class around a ref (mirrors a typical "sheet").
// The ref field name must match `deriveEntityFieldName(EddPlayers)` = "eddPlayer".
class EddPlayerSheet extends Schema.Class<EddPlayerSheet>("EddPlayerSheet")({
  eddPlayer: EddPlayer.pipe(DynamoModel.ref),
  shirtNumber: Schema.Number,
}) {}
class EddArticle extends Schema.Class<EddArticle>("EddArticle")({
  articleId: Schema.String,
  title: Schema.String,
  author: EddAuthor.pipe(DynamoModel.ref),
}) {}
class EddMatch extends Schema.Class<EddMatch>("EddMatch")({
  id: Schema.String,
  name: Schema.String,
  startDate: Schema.DateTimeUtcFromString, // Pattern B root field
  finishDate: Schema.optionalKey(Schema.DateTimeUtcFromString), // optional root date (#73 #1)
  venue: EddVenue.pipe(DynamoModel.ref),
  coach: EddCoach.pipe(DynamoModel.ref),
  // Optional MANY edge of a class — must keep its Array wrapper on assemble (#73 #2).
  players: Schema.optionalKey(Schema.Array(EddPlayerSheet)),
}) {}

const eddPk = {
  pk: { field: "pk", composite: ["id"] },
  sk: { field: "sk", composite: [] },
} as const
const EddAuthors = PureEntity.make({ model: EddAuthor, entityType: "EddAuthor", primaryKey: eddPk })
const EddCoaches = PureEntity.make({ model: EddCoach, entityType: "EddCoach", primaryKey: eddPk })
const EddVenues = PureEntity.make({ model: EddVenue, entityType: "EddVenue", primaryKey: eddPk })
const EddPlayers = PureEntity.make({ model: EddPlayer, entityType: "EddPlayer", primaryKey: eddPk })
const EddArticles = PureEntity.make({
  model: EddArticle,
  entityType: "EddArticle",
  primaryKey: { pk: { field: "pk", composite: ["articleId"] }, sk: { field: "sk", composite: [] } },
  refs: { author: { entity: EddAuthors } },
})
const EddTable = Table.make({
  schema: AppSchema,
  entities: { EddAuthors, EddCoaches, EddVenues, EddPlayers, EddArticles },
})
// Aggregate edges reference PURE definitions (no runtime `.get`) — #71.
const EddMatchAggregate = Aggregate.make(EddMatch, {
  table: EddTable,
  schema: AppSchema,
  pk: { field: "pk", composite: ["id"] },
  collection: { index: "lsi1", name: "eddmatch", sk: { field: "lsi1sk", composite: ["name"] } },
  root: { entityType: "EddMatchItem" },
  edges: {
    venue: Aggregate.one("venue", { entityType: "EddMatchVenue", entity: EddVenues }),
    coach: Aggregate.one("coach", { entityType: "EddMatchCoach", entity: EddCoaches }),
    players: Aggregate.many("players", { entityType: "EddMatchPlayer", entity: EddPlayers }),
  },
})
const eddTableName = "edd-71-72-connected"
const EddTestLayer = Layer.mergeAll(ClientLayer, EddTable.layer({ name: eddTableName }))
const provideEdd = Effect.provide(EddTestLayer)

describeConnected("#71/#72 — pure aggregate edges + Pattern B date fields", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client
          .createTable({
            TableName: eddTableName,
            BillingMode: "PAY_PER_REQUEST",
            KeySchema: [
              { AttributeName: "pk", KeyType: "HASH" },
              { AttributeName: "sk", KeyType: "RANGE" },
            ],
            AttributeDefinitions: [
              { AttributeName: "pk", AttributeType: "S" },
              { AttributeName: "sk", AttributeType: "S" },
              { AttributeName: "lsi1sk", AttributeType: "S" },
            ],
            LocalSecondaryIndexes: [
              {
                IndexName: "lsi1",
                KeySchema: [
                  { AttributeName: "pk", KeyType: "HASH" },
                  { AttributeName: "lsi1sk", KeyType: "RANGE" },
                ],
                Projection: { ProjectionType: "ALL" },
              },
            ],
          })
          .pipe(Effect.catch(() => Effect.void))
      }).pipe(provideEdd, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: eddTableName })
      }).pipe(
        provideEdd,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("#72 + Option A: plain Entity ref hydrates Pattern B + nested Pattern A dates", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EddAuthors, EddCoaches, EddVenues, EddArticles },
        tables: { EddTable },
      })
      yield* db.entities.EddAuthors.put({
        id: "alice",
        name: "Alice",
        dateOfBirth: "1990-05-01T00:00:00.000Z" as unknown as DateTime.Utc,
        joinedAt: DateTime.makeUnsafe("2015-06-15T00:00:00.000Z"),
      })
      yield* db.entities.EddArticles.put({ articleId: "a1", title: "T", authorId: "alice" })
      const got = yield* db.entities.EddArticles.get({ articleId: "a1" })
      expect(got.author.name).toBe("Alice")
      // #72: Pattern B transform field in the ref target round-trips.
      expect(DateTime.isDateTime(got.author.dateOfBirth)).toBe(true)
      expect(DateTime.toEpochMillis(got.author.dateOfBirth)).toBe(
        DateTime.toEpochMillis(DateTime.makeUnsafe("1990-05-01T00:00:00.000Z")),
      )
      // Option A: Pattern A self-date field nested in the ref target round-trips.
      expect(DateTime.isDateTime(got.author.joinedAt)).toBe(true)
      expect(DateTime.toEpochMillis(got.author.joinedAt)).toBe(
        DateTime.toEpochMillis(DateTime.makeUnsafe("2015-06-15T00:00:00.000Z")),
      )
    }).pipe(provideEdd),
  )

  it.effect(
    "#71/#72 + Option A: aggregate create + get with pure edges, Pattern B + nested Pattern A",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { EddAuthors, EddCoaches, EddVenues, EddPlayers, EddArticles },
          aggregates: { EddMatchAggregate },
          tables: { EddTable },
        })
        yield* db.entities.EddVenues.put({ id: "mcg", name: "MCG" })
        yield* db.entities.EddPlayers.put({ id: "p1", name: "Smith" })
        yield* db.entities.EddPlayers.put({ id: "p2", name: "Khawaja" })
        yield* db.entities.EddCoaches.put({
          id: "mcd",
          name: "McDonald",
          dateOfBirth: "1980-01-02T00:00:00.000Z" as unknown as DateTime.Utc,
          joinedAt: DateTime.makeUnsafe("2010-03-20T00:00:00.000Z"),
          retiredAt: DateTime.makeUnsafe("2024-09-01T00:00:00.000Z"),
        })

        // #71: pure coach/venue edges must promote at hydrate (was a TypeError).
        // #73 #1: `finishDate` (optionalKey root date) is OMITTED — must not be
        //   treated as required during create. #73 #2: `players` is an optional
        //   MANY edge whose Array wrapper must survive substitution.
        const created = yield* db.aggregates.EddMatchAggregate.create({
          id: "m1",
          name: "M1",
          startDate: "2025-03-01T10:00:00.000Z",
          venueId: "mcg",
          coachId: "mcd",
          players: [
            { eddPlayerId: "p1", shirtNumber: 7 },
            { eddPlayerId: "p2", shirtNumber: 10 },
          ],
        } as unknown as Parameters<typeof db.aggregates.EddMatchAggregate.create>[0])
        expect((created as EddMatch).coach.name).toBe("McDonald")
        expect(DateTime.isDateTime((created as EddMatch).startDate)).toBe(true)

        // #72: assemble decodes root startDate and the coach edge date exactly once.
        const fetched = (yield* db.aggregates.EddMatchAggregate.get({ id: "m1" })) as EddMatch
        expect(fetched.name).toBe("M1")
        expect(DateTime.isDateTime(fetched.startDate)).toBe(true)
        expect(DateTime.toEpochMillis(fetched.startDate)).toBe(
          DateTime.toEpochMillis(DateTime.makeUnsafe("2025-03-01T10:00:00.000Z")),
        )
        expect(fetched.coach.name).toBe("McDonald")
        expect(DateTime.isDateTime(fetched.coach.dateOfBirth)).toBe(true)
        expect(DateTime.toEpochMillis(fetched.coach.dateOfBirth)).toBe(
          DateTime.toEpochMillis(DateTime.makeUnsafe("1980-01-02T00:00:00.000Z")),
        )
        // Option A: Pattern A self-date nested in the edge target round-trips.
        expect(DateTime.isDateTime(fetched.coach.joinedAt)).toBe(true)
        expect(DateTime.toEpochMillis(fetched.coach.joinedAt)).toBe(
          DateTime.toEpochMillis(DateTime.makeUnsafe("2010-03-20T00:00:00.000Z")),
        )
        // Option A: an OPTIONAL Pattern A self-date nested in the edge target.
        expect(DateTime.isDateTime(fetched.coach.retiredAt as DateTime.Utc)).toBe(true)
        expect(fetched.venue.name).toBe("MCG")
        // #73 #1: omitted optionalKey root date stays absent (not "Missing key").
        expect(fetched.finishDate).toBeUndefined()
        // #73 #2: the optional many edge keeps its Array wrapper and hydrates.
        expect(Array.isArray(fetched.players)).toBe(true)
        expect((fetched.players ?? []).length).toBe(2)
        expect((fetched.players ?? []).map((p) => p.eddPlayer.name).sort()).toEqual([
          "Khawaja",
          "Smith",
        ])

        // #72 update: mutating a non-date field must not trip the Pattern B root
        // re-decode (the mutated state carries a domain `DateTime` for startDate).
        const updated = (yield* db.aggregates.EddMatchAggregate.update({ id: "m1" }, (c: any) => ({
          ...c.state,
          name: "M1-renamed",
        }))) as EddMatch
        expect(updated.name).toBe("M1-renamed")
        expect(DateTime.toEpochMillis(updated.startDate)).toBe(
          DateTime.toEpochMillis(DateTime.makeUnsafe("2025-03-01T10:00:00.000Z")),
        )
        expect(DateTime.isDateTime(updated.coach.joinedAt)).toBe(true)
      }).pipe(provideEdd),
  )
})

// ---------------------------------------------------------------------------
// Vector search integration (closes #78)
//
// DynamoDB Local does NOT implement vector search — CreateTable silently
// discards `VectorIndexes` and SearchVectors fails with
// UnknownOperationException. These tests therefore run through
// `VectorSearchEmulation.layer`, which replaces `searchVectors` with a
// Scan + brute-force implementation while every other operation (including
// every write in this suite) hits real DynamoDB Local.
//
// That split is exactly what makes the suite valuable: the WRITE half — key
// composition, embedding, sparse partition attributes, lifecycle stripping —
// is exercised against a real engine, and only the ANN ranking is simulated.
// ---------------------------------------------------------------------------

const vecSchema = DynamoSchema.make({ name: "vecapp", version: 1 })
const vecTableName = "connected-test-vec"
const VEC_DIMENSIONS = 4

class VecDoc extends Schema.Class<VecDoc>("VecDoc")({
  docId: Schema.String,
  tenantId: Schema.String,
  title: Schema.String,
  body: Schema.String,
  category: Schema.String,
}) {}

const VecDocs = Entity.make({
  model: VecDoc,
  entityType: "vecdoc",
  primaryKey: {
    pk: { field: "pk", composite: ["tenantId", "docId"] },
    sk: { field: "sk", composite: [] },
  },
  vectorIndexes: {
    byBody: {
      name: "vec1",
      dimensions: VEC_DIMENSIONS,
      distance: "cosine",
      source: { fields: ["title", "body"] },
      partition: ["tenantId"],
      filters: ["category"],
    },
  },
})

const VecTable = Table.make({ schema: vecSchema, entities: { VecDocs } })

/**
 * Deterministic 4-dimension embedder keyed on the axis word in the text, so
 * ranking assertions are exact rather than probabilistic.
 */
const vecAxis: Record<string, ReadonlyArray<number>> = {
  north: [1, 0, 0, 0],
  east: [0, 1, 0, 0],
  south: [0, 0, 1, 0],
  west: [0, 0, 0, 1],
}
const AxisEmbedder = Layer.succeed(Embedder, {
  dimensions: VEC_DIMENSIONS,
  embed: (text: string) => {
    const axis = Object.keys(vecAxis).find((key) => text.toLowerCase().includes(key))
    return Effect.succeed(axis ? vecAxis[axis]! : [1, 1, 1, 1])
  },
})

const VecTestLayer = Layer.mergeAll(
  VectorSearchEmulation.layer(ClientLayer, { tables: { VecTable } }),
  VecTable.layer({ name: vecTableName }),
  AxisEmbedder,
)
const provideVec = Effect.provide(VecTestLayer)

describeConnected("vector search integration (closes #78)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { VecDocs },
          tables: { VecTable },
        })
        // DynamoDB Local accepts and discards VectorIndexes — create() must not
        // fail because of them.
        yield* db.tables.VecTable.create()
      }).pipe(provideVec, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: vecTableName })
      }).pipe(
        provideVec,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("writes embed + partition attributes and searches them back, ranked", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      yield* db.entities.VecDocs.put({
        docId: "d-north",
        tenantId: "t-1",
        title: "North",
        body: "north pole",
        category: "geo",
      })
      yield* db.entities.VecDocs.put({
        docId: "d-east",
        tenantId: "t-1",
        title: "East",
        body: "east coast",
        category: "geo",
      })

      const hits = yield* db.entities.VecDocs.byBody("north star")
        .partition({ tenantId: "t-1" })
        .collect()

      expect(hits.map((h) => h.item.docId)).toEqual(["d-north", "d-east"])
      // Cosine: identical → distance 0 → similarity 1; orthogonal → 1 → 0.5.
      expect(hits[0]!.similarity).toBeCloseTo(1, 5)
      expect(hits[1]!.similarity).toBeCloseTo(0.5, 5)
      // The decoded item is a full domain record — no __edd_* leakage.
      expect(hits[0]!.item.title).toBe("North")
      expect(Object.keys(hits[0]!.item)).not.toContain("__edd_v_vec1__")
      expect(Object.keys(hits[0]!.item)).not.toContain("__edd_vp_vec1__")
    }).pipe(provideVec),
  )

  it.effect("the composed partition attribute scopes results per tenant", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      yield* db.entities.VecDocs.put({
        docId: "d-shared",
        tenantId: "t-2",
        title: "South",
        body: "south bank",
        category: "geo",
      })

      const t2 = yield* db.entities.VecDocs.byBody("south wind")
        .partition({ tenantId: "t-2" })
        .collect()
      expect(t2.map((h) => h.item.docId)).toEqual(["d-shared"])

      const t1 = yield* db.entities.VecDocs.byBody("south wind")
        .partition({ tenantId: "t-1" })
        .collect()
      expect(t1.map((h) => h.item.docId)).not.toContain("d-shared")
    }).pipe(provideVec),
  )

  it.effect("inline filters and topK narrow the result set", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      yield* db.entities.VecDocs.put({
        docId: "d-west-geo",
        tenantId: "t-3",
        title: "West",
        body: "west end",
        category: "geo",
      })
      yield* db.entities.VecDocs.put({
        docId: "d-west-lit",
        tenantId: "t-3",
        title: "West",
        body: "west wing",
        category: "literature",
      })

      const filtered = yield* db.entities.VecDocs.byBody("west side")
        .partition({ tenantId: "t-3" })
        .filter({ category: "literature" })
        .collect()
      expect(filtered.map((h) => h.item.docId)).toEqual(["d-west-lit"])

      const limited = yield* db.entities.VecDocs.byBody("west side")
        .partition({ tenantId: "t-3" })
        .topK(1)
        .collect()
      expect(limited).toHaveLength(1)
    }).pipe(provideVec),
  )

  it.effect("update re-embeds only when a source field is in the payload", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      // The title carries no axis word, so the embedding is decided purely by
      // `body` — which is what makes the "partial source ⇒ read-and-merge"
      // behaviour observable below.
      yield* db.entities.VecDocs.put({
        docId: "d-move",
        tenantId: "t-4",
        title: "Marker",
        body: "north pole",
        category: "geo",
      })

      // Touching a non-source field must leave the vector alone.
      yield* db.entities.VecDocs.update({ tenantId: "t-4", docId: "d-move" }).set({
        category: "science",
      })
      const stillNorth = yield* db.entities.VecDocs.byBody("north star")
        .partition({ tenantId: "t-4" })
        .collect()
      expect(stillNorth[0]!.similarity).toBeCloseTo(1, 5)

      // Touching a source field re-embeds — the doc moves to the east axis.
      yield* db.entities.VecDocs.update({ tenantId: "t-4", docId: "d-move" }).set({
        body: "east coast",
      })
      const nowEast = yield* db.entities.VecDocs.byBody("east wind")
        .partition({ tenantId: "t-4" })
        .collect()
      expect(nowEast[0]!.similarity).toBeCloseTo(1, 5)
    }).pipe(provideVec),
  )

  it.effect(".withVector stores a pre-computed embedding verbatim", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      yield* db.entities.VecDocs.put({
        docId: "d-manual",
        tenantId: "t-5",
        title: "Anything",
        body: "anything at all",
        category: "geo",
      }).withVector("byBody", [0, 0, 0, 1])

      const hits = yield* db.entities.VecDocs.byBody([0, 0, 0, 1])
        .partition({ tenantId: "t-5" })
        .collect()
      expect(hits[0]!.item.docId).toBe("d-manual")
      expect(hits[0]!.similarity).toBeCloseTo(1, 5)
    }).pipe(provideVec),
  )

  it.effect("deleting an item removes it from the index (sparse semantics)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      yield* db.entities.VecDocs.put({
        docId: "d-gone",
        tenantId: "t-6",
        title: "South",
        body: "south side",
        category: "geo",
      })
      yield* db.entities.VecDocs.delete({ tenantId: "t-6", docId: "d-gone" })

      const hits = yield* db.entities.VecDocs.byBody("south side")
        .partition({ tenantId: "t-6" })
        .collect()
      expect(hits).toHaveLength(0)
    }).pipe(provideVec),
  )

  it.effect("select() returns a projected item without library-managed attributes", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      yield* db.entities.VecDocs.put({
        docId: "d-proj",
        tenantId: "t-7",
        title: "East",
        body: "east gate",
        category: "geo",
      })

      const hits = yield* db.entities.VecDocs.byBody("east gate")
        .partition({ tenantId: "t-7" })
        .select(["docId", "title"])
        .collect()
      expect(hits[0]!.item).toEqual({ docId: "d-proj", title: "East" })
    }).pipe(provideVec),
  )

  it.effect("reembed rewrites stale vectors in place", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      // Write a deliberately wrong embedding, then let reembed correct it from
      // the declared source fields.
      yield* db.entities.VecDocs.put({
        docId: "d-stale",
        tenantId: "t-8",
        title: "West",
        body: "west gate",
        category: "geo",
      }).withVector("byBody", [1, 0, 0, 0])

      const before = yield* db.entities.VecDocs.byBody("west gate")
        .partition({ tenantId: "t-8" })
        .collect()
      expect(before[0]!.similarity).toBeCloseTo(0.5, 5)

      yield* db.entities.VecDocs.reembed({ concurrency: 2 })

      const after = yield* db.entities.VecDocs.byBody("west gate")
        .partition({ tenantId: "t-8" })
        .collect()
      expect(after[0]!.similarity).toBeCloseTo(1, 5)
    }).pipe(provideVec),
  )

  it.effect("waitForVectorIndex resolves immediately where the engine has no vector indexes", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })
      // DynamoDB Local reports no VectorIndexes at all; the poller treats that
      // as "nothing to wait for" rather than blocking for the full timeout.
      yield* db.tables.VecTable.waitForVectorIndex("vec1", { timeout: Duration.seconds(5) })
    }).pipe(provideVec),
  )

  it.effect("upsert makes an item searchable and refreshes a stale vector (closes B1)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      // First upsert creates the item. Before the fix this wrote no vector at
      // all, leaving the item permanently invisible to search.
      yield* db.entities.VecDocs.upsert({
        docId: "d-upsert",
        tenantId: "t-9",
        title: "Marker",
        body: "north pole",
        category: "geo",
      })
      const created = yield* db.entities.VecDocs.byBody("north star")
        .partition({ tenantId: "t-9" })
        .collect()
      expect(created.map((h) => h.item.docId)).toEqual(["d-upsert"])
      expect(created[0]!.similarity).toBeCloseTo(1, 5)

      // Second upsert overwrites the source — the vector must follow, not
      // linger on the previous description.
      yield* db.entities.VecDocs.upsert({
        docId: "d-upsert",
        tenantId: "t-9",
        title: "Marker",
        body: "east coast",
        category: "geo",
      })
      const refreshed = yield* db.entities.VecDocs.byBody("east wind")
        .partition({ tenantId: "t-9" })
        .collect()
      expect(refreshed[0]!.similarity).toBeCloseTo(1, 5)
      const stale = yield* db.entities.VecDocs.byBody("north star")
        .partition({ tenantId: "t-9" })
        .collect()
      expect(stale[0]!.similarity).toBeCloseTo(0.5, 5)
    }).pipe(provideVec),
  )

  it.effect(".withVector on update replaces the stored embedding (closes gap 3)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      yield* db.entities.VecDocs.put({
        docId: "d-wv",
        tenantId: "t-10",
        title: "Marker",
        body: "north pole",
        category: "geo",
      })
      // `category` is not a source field, so only the explicit vector can move
      // the item onto the west axis.
      yield* db.entities.VecDocs.update({ tenantId: "t-10", docId: "d-wv" })
        .set({ category: "science" })
        .withVector("byBody", [0, 0, 0, 1])

      const hits = yield* db.entities.VecDocs.byBody([0, 0, 0, 1])
        .partition({ tenantId: "t-10" })
        .collect()
      expect(hits[0]!.item.docId).toBe("d-wv")
      expect(hits[0]!.similarity).toBeCloseTo(1, 5)
    }).pipe(provideVec),
  )

  it.effect("reembed skips items deleted after the scan page (closes S6)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { VecDocs }, tables: { VecTable } })

      yield* db.entities.VecDocs.put({
        docId: "d-ghost",
        tenantId: "t-12",
        title: "Marker",
        body: "north pole",
        category: "geo",
      })
      yield* db.entities.VecDocs.delete({ tenantId: "t-12", docId: "d-ghost" })

      // The item is gone before reembed runs, so the conditional write must
      // decline rather than resurrect it as a key + vector fragment.
      yield* db.entities.VecDocs.reembed({ concurrency: 2 })

      const hits = yield* db.entities.VecDocs.byBody("north star")
        .partition({ tenantId: "t-12" })
        .collect()
      expect(hits).toHaveLength(0)
    }).pipe(provideVec),
  )
})

// ---------------------------------------------------------------------------
// Vector search — two entity types sharing one physical index (closes #78)
//
// The headline claim of the partition design is that the composed
// `__edd_vp_*` value carries the entity type, so a shared physical vector
// index scopes to one entity for free. That claim is only worth anything if it
// holds against a real engine with both entity types' items interleaved in the
// same table, which is what this suite exercises.
// ---------------------------------------------------------------------------

const sharedVecTableName = "connected-test-vec-shared"

class VecNote extends Schema.Class<VecNote>("VecNote")({
  noteId: Schema.String,
  tenantId: Schema.String,
  body: Schema.String,
  kind: Schema.String,
}) {}

const SharedVecDocs = Entity.make({
  model: VecDoc,
  entityType: "svecdoc",
  primaryKey: {
    pk: { field: "pk", composite: ["tenantId", "docId"] },
    sk: { field: "sk", composite: [] },
  },
  vectorIndexes: {
    byBody: {
      name: "vec1",
      dimensions: VEC_DIMENSIONS,
      distance: "cosine",
      source: { fields: ["body"] },
      partition: ["tenantId"],
      filters: ["category"],
    },
  },
})

const SharedVecNotes = Entity.make({
  model: VecNote,
  entityType: "svecnote",
  primaryKey: {
    pk: { field: "pk", composite: ["tenantId", "noteId"] },
    sk: { field: "sk", composite: [] },
  },
  vectorIndexes: {
    byBody: {
      name: "vec1",
      dimensions: VEC_DIMENSIONS,
      distance: "cosine",
      source: { fields: ["body"] },
      partition: ["tenantId"],
      // A DIFFERENT filter attribute — the merged SearchSchema must union both.
      filters: ["kind"],
    },
  },
})

const SharedVecTable = Table.make({
  schema: vecSchema,
  entities: { SharedVecDocs, SharedVecNotes },
})

const SharedVecTestLayer = Layer.mergeAll(
  VectorSearchEmulation.layer(ClientLayer, { tables: { SharedVecTable } }),
  SharedVecTable.layer({ name: sharedVecTableName }),
  AxisEmbedder,
)
const provideSharedVec = Effect.provide(SharedVecTestLayer)

describeConnected("vector search — shared physical index (closes #78)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { SharedVecDocs, SharedVecNotes },
          tables: { SharedVecTable },
        })
        yield* db.tables.SharedVecTable.create()
      }).pipe(provideSharedVec, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: sharedVecTableName })
      }).pipe(
        provideSharedVec,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("the merged VectorIndex unions filters from both sharers", () => {
    const definition = Table.definition(SharedVecTable as unknown as Table.Table)
    const searchSchema = definition.VectorIndexes?.[0]?.SearchSchema ?? []
    expect(searchSchema.map((element) => element.AttributeName)).toEqual([
      "__edd_vp_vec1__",
      "category",
      "kind",
    ])
    return Effect.void
  })

  it.effect("a search returns only the queried entity type, with no user filter", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SharedVecDocs, SharedVecNotes },
        tables: { SharedVecTable },
      })

      // Identical embeddings (same axis word), same tenant, same physical index
      // — only the composed partition value separates them.
      yield* db.entities.SharedVecDocs.put({
        docId: "shared-doc",
        tenantId: "t-1",
        title: "Doc",
        body: "north pole",
        category: "geo",
      })
      yield* db.entities.SharedVecNotes.put({
        noteId: "shared-note",
        tenantId: "t-1",
        body: "north pole",
        kind: "memo",
      })

      const docs = yield* db.entities.SharedVecDocs.byBody("north star")
        .partition({ tenantId: "t-1" })
        .collect()
      expect(docs.map((h) => h.item.docId)).toEqual(["shared-doc"])

      const notes = yield* db.entities.SharedVecNotes.byBody("north star")
        .partition({ tenantId: "t-1" })
        .collect()
      expect(notes.map((h) => h.item.noteId)).toEqual(["shared-note"])
    }).pipe(provideSharedVec),
  )

  it.effect("each sharer can filter on its own declared INLINE_FILTER attribute", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SharedVecDocs, SharedVecNotes },
        tables: { SharedVecTable },
      })

      yield* db.entities.SharedVecNotes.put({
        noteId: "memo-note",
        tenantId: "t-2",
        body: "east coast",
        kind: "memo",
      })
      yield* db.entities.SharedVecNotes.put({
        noteId: "draft-note",
        tenantId: "t-2",
        body: "east coast",
        kind: "draft",
      })

      // `kind` is declared only by SharedVecNotes — the union merge is what
      // keeps it in the physical index's SearchSchema.
      const drafts = yield* db.entities.SharedVecNotes.byBody("east wind")
        .partition({ tenantId: "t-2" })
        .filter({ kind: "draft" })
        .collect()
      expect(drafts.map((h) => h.item.noteId)).toEqual(["draft-note"])
    }).pipe(provideSharedVec),
  )
})

// ---------------------------------------------------------------------------
// Vector search — clearing the embedding source (closes #78)
//
// The source fields are optional here on purpose: clearing a REQUIRED field
// would leave an item that cannot decode, which is a different failure. What is
// under test is the sparse-index story — an item whose source text is gone must
// leave the index rather than answer to a description it no longer has.
// ---------------------------------------------------------------------------

const clearVecTableName = "connected-test-vec-clear"

class VecDraft extends Schema.Class<VecDraft>("VecDraft")({
  draftId: Schema.String,
  tenantId: Schema.String,
  body: Schema.optional(Schema.String),
}) {}

const VecDrafts = Entity.make({
  model: VecDraft,
  entityType: "vecdraft",
  primaryKey: {
    pk: { field: "pk", composite: ["tenantId", "draftId"] },
    sk: { field: "sk", composite: [] },
  },
  vectorIndexes: {
    byBody: {
      name: "vec1",
      dimensions: VEC_DIMENSIONS,
      distance: "cosine",
      source: { fields: ["body"] },
      partition: ["tenantId"],
    },
  },
})

const ClearVecTable = Table.make({ schema: vecSchema, entities: { VecDrafts } })
const ClearVecTestLayer = Layer.mergeAll(
  VectorSearchEmulation.layer(ClientLayer, { tables: { ClearVecTable } }),
  ClearVecTable.layer({ name: clearVecTableName }),
  AxisEmbedder,
)
const provideClearVec = Effect.provide(ClearVecTestLayer)

describeConnected("vector search — clearing the embedding source (closes #78)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { VecDrafts },
          tables: { ClearVecTable },
        })
        yield* db.tables.ClearVecTable.create()
      }).pipe(provideClearVec, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: clearVecTableName })
      }).pipe(
        provideClearVec,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("remove() of the last source field drops the item out of the index", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { VecDrafts },
        tables: { ClearVecTable },
      })

      yield* db.entities.VecDrafts.put({
        draftId: "d-clear",
        tenantId: "t-1",
        body: "south side",
      })
      const before = yield* db.entities.VecDrafts.byBody("south wind")
        .partition({ tenantId: "t-1" })
        .collect()
      expect(before.map((h) => h.item.draftId)).toEqual(["d-clear"])

      yield* db.entities.VecDrafts.update({ tenantId: "t-1", draftId: "d-clear" }).remove(["body"])

      const after = yield* db.entities.VecDrafts.byBody("south wind")
        .partition({ tenantId: "t-1" })
        .collect()
      expect(after).toHaveLength(0)
      // The item itself survives — only its index entry is gone.
      const stillThere = yield* db.entities.VecDrafts.get({ tenantId: "t-1", draftId: "d-clear" })
      expect(stillThere.draftId).toBe("d-clear")
      expect(stillThere.body).toBeUndefined()
    }).pipe(provideClearVec),
  )

  it.effect("a null set() of the last source field drops it out of the index too", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { VecDrafts },
        tables: { ClearVecTable },
      })

      yield* db.entities.VecDrafts.put({
        draftId: "d-null",
        tenantId: "t-2",
        body: "east coast",
      })
      yield* db.entities.VecDrafts.update({ tenantId: "t-2", draftId: "d-null" }).set({
        body: undefined,
      })

      const after = yield* db.entities.VecDrafts.byBody("east wind")
        .partition({ tenantId: "t-2" })
        .collect()
      expect(after).toHaveLength(0)
    }).pipe(provideClearVec),
  )

  it.effect("re-supplying the source field puts the item back in the index", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { VecDrafts },
        tables: { ClearVecTable },
      })

      yield* db.entities.VecDrafts.put({ draftId: "d-back", tenantId: "t-3", body: "north pole" })
      yield* db.entities.VecDrafts.update({ tenantId: "t-3", draftId: "d-back" }).remove(["body"])
      expect(
        yield* db.entities.VecDrafts.byBody("north star").partition({ tenantId: "t-3" }).collect(),
      ).toHaveLength(0)

      yield* db.entities.VecDrafts.update({ tenantId: "t-3", draftId: "d-back" }).set({
        body: "west end",
      })
      const back = yield* db.entities.VecDrafts.byBody("west gate")
        .partition({ tenantId: "t-3" })
        .collect()
      expect(back.map((h) => h.item.draftId)).toEqual(["d-back"])
      expect(back[0]!.similarity).toBeCloseTo(1, 5)
    }).pipe(provideClearVec),
  )
})

// ---------------------------------------------------------------------------
// Vector search — soft-delete round trip (closes #78)
// ---------------------------------------------------------------------------

const softVecTableName = "connected-test-vec-soft"

const SoftVecDocs = Entity.make({
  model: VecDoc,
  entityType: "softvecdoc",
  primaryKey: {
    pk: { field: "pk", composite: ["tenantId", "docId"] },
    sk: { field: "sk", composite: [] },
  },
  softDelete: true,
  vectorIndexes: {
    byBody: {
      name: "vec1",
      dimensions: VEC_DIMENSIONS,
      distance: "cosine",
      source: { fields: ["body"] },
      partition: ["tenantId"],
    },
  },
})

const SoftVecTable = Table.make({ schema: vecSchema, entities: { SoftVecDocs } })
const SoftVecTestLayer = Layer.mergeAll(
  VectorSearchEmulation.layer(ClientLayer, { tables: { SoftVecTable } }),
  SoftVecTable.layer({ name: softVecTableName }),
  AxisEmbedder,
)
const provideSoftVec = Effect.provide(SoftVecTestLayer)

describeConnected("vector search — soft delete round trip (closes #78)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { SoftVecDocs },
          tables: { SoftVecTable },
        })
        yield* db.tables.SoftVecTable.create()
      }).pipe(provideSoftVec, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: softVecTableName })
      }).pipe(
        provideSoftVec,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("soft delete removes the item from search; restore brings it back", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SoftVecDocs },
        tables: { SoftVecTable },
      })

      yield* db.entities.SoftVecDocs.put({
        docId: "d-soft",
        tenantId: "t-1",
        title: "Doc",
        body: "west end",
        category: "geo",
      })
      const before = yield* db.entities.SoftVecDocs.byBody("west gate")
        .partition({ tenantId: "t-1" })
        .collect()
      expect(before.map((h) => h.item.docId)).toEqual(["d-soft"])

      // The tombstone strips the indexed attributes and stashes the embedding.
      yield* db.entities.SoftVecDocs.delete({ tenantId: "t-1", docId: "d-soft" })
      const during = yield* db.entities.SoftVecDocs.byBody("west gate")
        .partition({ tenantId: "t-1" })
        .collect()
      expect(during).toHaveLength(0)

      // Restore un-stashes it — no Embedder round trip, same vector as before.
      yield* db.entities.SoftVecDocs.restore({ tenantId: "t-1", docId: "d-soft" })
      const after = yield* db.entities.SoftVecDocs.byBody("west gate")
        .partition({ tenantId: "t-1" })
        .collect()
      expect(after.map((h) => h.item.docId)).toEqual(["d-soft"])
      expect(after[0]!.similarity).toBeCloseTo(before[0]!.similarity, 10)
    }).pipe(provideSoftVec),
  )
})

// ---------------------------------------------------------------------------
// EventStore — codec symmetry + real cancellation mapping (closes #81)
// ---------------------------------------------------------------------------
//
// EventStore previously had ZERO connected coverage: every assertion ran
// against a stubbed client with a hand-crafted `CancellationReasons` array.
// These tests exercise the real DynamoDB wire format and the real
// `TransactionCanceledException` shapes DynamoDB emits.

const EsSchema = DynamoSchema.make({ name: "es-connected", version: 1 })

class EsGoalScored extends Schema.Class<EsGoalScored>("EsGoalScored")({
  scorer: Schema.String,
  occurredAt: Schema.DateTimeUtcFromString,
}) {}

class EsMatchAbandoned extends Schema.TaggedClass<EsMatchAbandoned>()("EsMatchAbandoned", {
  reason: Schema.String,
  abandonedAt: Schema.DateTimeUtcFromString,
}) {}

const EsMetadata = Schema.Struct({
  correlationId: Schema.String,
  recordedAt: Schema.DateTimeUtcFromString,
})

const EsTable = Table.make({ schema: EsSchema })
const esTableName = `es-events-${Date.now()}`
const EsStream = EventStore.makeStream({
  table: EsTable,
  streamName: "EsMatch",
  events: [EsGoalScored, EsMatchAbandoned],
  streamId: { composite: ["matchId"] },
  metadata: EsMetadata,
})
const EsTestLayer = Layer.mergeAll(ClientLayer, EsTable.layer({ name: esTableName }))
const provideEs = Effect.provide(EsTestLayer)

// A second table whose partition key is declared as a NUMBER. EventStore always
// writes a string `pk`, so every Put in the transaction is cancelled by real
// DynamoDB with `Code: "ValidationError"` — a genuine, non-ConditionalCheckFailed
// cancellation reason produced by the service rather than hand-crafted in a mock.
const EsBadKeyTable = Table.make({ schema: EsSchema })
const esBadKeyTableName = `es-badkey-${Date.now()}`
const EsBadKeyStream = EventStore.makeStream({
  table: EsBadKeyTable,
  streamName: "EsBadKey",
  events: [EsGoalScored],
  streamId: { composite: ["matchId"] },
})
const EsBadKeyLayer = Layer.mergeAll(ClientLayer, EsBadKeyTable.layer({ name: esBadKeyTableName }))
const provideEsBadKey = Effect.provide(EsBadKeyLayer)

const esStreamPk = (matchId: string) => DynamoSchema.composeKey(EsSchema, "esmatch", [matchId])

describeConnected("EventStore connected tests (closes #81)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: esTableName,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
        })
        yield* client.createTable({
          TableName: esBadKeyTableName,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          // Deliberate mismatch: EventStore writes `pk` as a string.
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "N" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
        })
      }).pipe(provideEs, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: esTableName }).pipe(Effect.catch(() => Effect.void))
        yield* client
          .deleteTable({ TableName: esBadKeyTableName })
          .pipe(Effect.catch(() => Effect.void))
      }).pipe(provideEs, Effect.scoped),
    )
  }, 20000)

  // -------------------------------------------------------------------------
  // Codec symmetry against real DynamoDB
  // -------------------------------------------------------------------------

  it.effect("transforming event + metadata schemas round-trip append → read", () =>
    Effect.gen(function* () {
      const occurredAt = DateTime.makeUnsafe("2026-05-04T09:30:00.000Z")
      const recordedAt = DateTime.makeUnsafe("2026-05-04T09:31:00.000Z")

      yield* EsStream.append(
        { matchId: "rt-1" },
        [
          new EsGoalScored({ scorer: "Kane", occurredAt }),
          new EsMatchAbandoned({ reason: "rain", abandonedAt: occurredAt }),
        ],
        0,
        { metadata: { correlationId: "corr-rt-1", recordedAt } },
      )

      // 1. The bytes actually stored are the ENCODED (wire) form — ISO strings,
      //    not a marshalled `DateTime.Utc` instance.
      const client = yield* DynamoClient
      const raw = yield* client.query({
        TableName: esTableName,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: esStreamPk("rt-1") } },
      })
      const stored = (raw.Items ?? []).map((i) => fromAttributeMap(i))
      expect(stored).toHaveLength(2)
      expect(stored[0]!.data).toEqual({
        _tag: "EsGoalScored",
        scorer: "Kane",
        occurredAt: "2026-05-04T09:30:00.000Z",
      })
      expect(stored[0]!.metadata).toEqual({
        correlationId: "corr-rt-1",
        recordedAt: "2026-05-04T09:31:00.000Z",
      })
      expect(stored[1]!.data).toEqual({
        _tag: "EsMatchAbandoned",
        reason: "rain",
        abandonedAt: "2026-05-04T09:30:00.000Z",
      })

      // 2. The read path decodes back to the domain types.
      const events = yield* EsStream.read({ matchId: "rt-1" })
      expect(events.map((e) => e.version)).toEqual([1, 2])
      expect(events.map((e) => e.eventType)).toEqual(["EsGoalScored", "EsMatchAbandoned"])

      const goal = events[0]!.data as EsGoalScored
      expect(goal).toBeInstanceOf(EsGoalScored)
      expect(DateTime.isDateTime(goal.occurredAt)).toBe(true)
      expect(DateTime.toEpochMillis(goal.occurredAt)).toBe(DateTime.toEpochMillis(occurredAt))

      const abandoned = events[1]!.data as EsMatchAbandoned
      expect(abandoned).toBeInstanceOf(EsMatchAbandoned)
      expect(abandoned._tag).toBe("EsMatchAbandoned")
      expect(DateTime.toEpochMillis(abandoned.abandonedAt)).toBe(DateTime.toEpochMillis(occurredAt))

      // 3. Metadata is DECODED on read, not returned as a raw attribute map.
      const metadata = events[0]!.metadata
      expect(metadata?.correlationId).toBe("corr-rt-1")
      expect(DateTime.isDateTime(metadata?.recordedAt as DateTime.Utc)).toBe(true)
      expect(DateTime.toEpochMillis(metadata?.recordedAt as DateTime.Utc)).toBe(
        DateTime.toEpochMillis(recordedAt),
      )
    }).pipe(provideEs),
  )

  // -------------------------------------------------------------------------
  // Optimistic concurrency against real DynamoDB
  // -------------------------------------------------------------------------

  it.effect(
    "two concurrent appends at the same expectedVersion → 1 success, 1 VersionConflict",
    () =>
      Effect.gen(function* () {
        const occurredAt = DateTime.makeUnsafe("2026-05-04T10:00:00.000Z")
        const attempt = (scorer: string) =>
          EsStream.append({ matchId: "cc-1" }, [new EsGoalScored({ scorer, occurredAt })], 0).pipe(
            Effect.map(() => "ok" as const),
            Effect.catch((error) => Effect.succeed(error._tag)),
          )

        const outcomes = yield* Effect.all([attempt("Kane"), attempt("Son")], {
          concurrency: "unbounded",
        })

        expect(outcomes.filter((o) => o === "ok")).toHaveLength(1)
        expect(outcomes.filter((o) => o === "VersionConflict")).toHaveLength(1)

        // Exactly one event landed — the loser wrote nothing.
        const events = yield* EsStream.read({ matchId: "cc-1" })
        expect(events).toHaveLength(1)
        expect(yield* EsStream.currentVersion({ matchId: "cc-1" })).toBe(1)
      }).pipe(provideEs),
  )

  it.effect("real mixed-reason cancellation [ConditionalCheckFailed, None] → VersionConflict", () =>
    Effect.gen(function* () {
      const occurredAt = DateTime.makeUnsafe("2026-05-04T11:00:00.000Z")
      yield* EsStream.append(
        { matchId: "mx-1" },
        [new EsGoalScored({ scorer: "Kane", occurredAt })],
        0,
      )

      // Two events at a stale expectedVersion: the first Put collides with the
      // existing v1 (ConditionalCheckFailed), the second targets a free v2 and
      // is reported by DynamoDB as `Code: "None"`. That heterogeneous reasons
      // array is exactly what a hand-crafted mock never produces.
      const error = yield* EsStream.append(
        { matchId: "mx-1" },
        [
          new EsGoalScored({ scorer: "Son", occurredAt }),
          new EsGoalScored({ scorer: "Maddison", occurredAt }),
        ],
        0,
      ).pipe(Effect.flip)

      expect(error._tag).toBe("VersionConflict")
      expect((error as { expectedVersion: number }).expectedVersion).toBe(0)

      // Nothing was written — the whole transaction rolled back.
      const events = yield* EsStream.read({ matchId: "mx-1" })
      expect(events).toHaveLength(1)
      expect((events[0]!.data as EsGoalScored).scorer).toBe("Kane")
    }).pipe(provideEs),
  )

  it.effect("non-conditional cancellation reason → TransactionCancelled", () =>
    Effect.gen(function* () {
      const occurredAt = DateTime.makeUnsafe("2026-05-04T12:00:00.000Z")
      const error = yield* EsBadKeyStream.append(
        { matchId: "bad-1" },
        [new EsGoalScored({ scorer: "Kane", occurredAt })],
        0,
      ).pipe(Effect.flip)

      expect(error._tag).toBe("TransactionCancelled")
      const cancelled = error as { operation: string; reasons: ReadonlyArray<{ code?: string }> }
      expect(cancelled.operation).toBe("TransactWriteItems")
      // DynamoDB cancels the item with a ValidationError, not a conditional
      // check failure — so it must NOT be flattened into a VersionConflict.
      expect(cancelled.reasons.map((r) => r.code)).toContain("ValidationError")
      expect(cancelled.reasons.map((r) => r.code)).not.toContain("ConditionalCheckFailed")
    }).pipe(provideEsBadKey),
  )
})

// ---------------------------------------------------------------------------
// EventStore append guards (closes #82)
// ---------------------------------------------------------------------------

class EsMatchStarted extends Schema.Class<EsMatchStarted>("EsMatchStarted")({
  venue: Schema.String,
}) {}

class EsInningsCompleted extends Schema.Class<EsInningsCompleted>("EsInningsCompleted")({
  innings: Schema.Number,
  runs: Schema.Number,
}) {}

const esGuardSchema = DynamoSchema.make({ name: "es-test", version: 1 })
const esGuardTableName = `es-test-${Date.now()}`
const EsGuardTable = Table.make({ schema: esGuardSchema })

const EsMatchEvents = EventStore.makeStream({
  table: EsGuardTable,
  streamName: "EsMatch",
  events: [EsMatchStarted, EsInningsCompleted],
  streamId: { composite: ["matchId"] },
})

const EsGuardTestLayer = Layer.mergeAll(ClientLayer, EsGuardTable.layer({ name: esGuardTableName }))
const provideEsGuard = Effect.provide(EsGuardTestLayer)

describeConnected("EventStore append guards (closes #82)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: esGuardTableName,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
        })
      }).pipe(provideEsGuard, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: esGuardTableName })
      }).pipe(
        provideEsGuard,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect(
    "sequential appends at the correct expectedVersion succeed through the contiguity check",
    () =>
      Effect.gen(function* () {
        const r1 = yield* EsMatchEvents.append(
          { matchId: "es-seq" },
          [new EsMatchStarted({ venue: "MCG" })],
          0,
        )
        expect(r1.version).toBe(1)

        // expectedVersion > 0 → the ConditionCheck on event v1 must pass
        const r2 = yield* EsMatchEvents.append(
          { matchId: "es-seq" },
          [
            new EsInningsCompleted({ innings: 1, runs: 250 }),
            new EsInningsCompleted({ innings: 2, runs: 180 }),
          ],
          1,
        )
        expect(r2.version).toBe(3)

        const events = yield* EsMatchEvents.read({ matchId: "es-seq" })
        expect(events.map((e) => e.version)).toEqual([1, 2, 3])
      }).pipe(provideEsGuard),
  )

  it.effect("append at an AHEAD expectedVersion fails with VersionConflict and writes no gap", () =>
    Effect.gen(function* () {
      yield* EsMatchEvents.append(
        { matchId: "es-ahead" },
        [new EsMatchStarted({ venue: "SCG" })],
        0,
      )

      // Stream head is at version 1; expectedVersion 10 would silently write
      // version 11 without the contiguity ConditionCheck.
      const error = yield* EsMatchEvents.append(
        { matchId: "es-ahead" },
        [new EsInningsCompleted({ innings: 1, runs: 300 })],
        10,
      ).pipe(Effect.flip)

      expect(error._tag).toBe("VersionConflict")
      const conflict = error as VersionConflict
      expect(conflict.streamName).toBe("EsMatch")
      expect(conflict.streamId).toBe("es-ahead")
      expect(conflict.expectedVersion).toBe(10)

      // Nothing was written — the stream is still contiguous at version 1.
      const events = yield* EsMatchEvents.read({ matchId: "es-ahead" })
      expect(events.map((e) => e.version)).toEqual([1])
    }).pipe(provideEsGuard),
  )

  it.effect(
    "append at a positive expectedVersion on an EMPTY stream fails with VersionConflict",
    () =>
      Effect.gen(function* () {
        const error = yield* EsMatchEvents.append(
          { matchId: "es-empty" },
          [new EsMatchStarted({ venue: "Lord's" })],
          5,
        ).pipe(Effect.flip)

        expect(error._tag).toBe("VersionConflict")
        expect((error as VersionConflict).expectedVersion).toBe(5)

        const events = yield* EsMatchEvents.read({ matchId: "es-empty" })
        expect(events).toEqual([])
      }).pipe(provideEsGuard),
  )

  it.effect(
    "STALE expectedVersion still maps to VersionConflict with the contiguity check present",
    () =>
      Effect.gen(function* () {
        yield* EsMatchEvents.append(
          { matchId: "es-stale" },
          [new EsMatchStarted({ venue: "Eden Gardens" })],
          0,
        )
        yield* EsMatchEvents.append(
          { matchId: "es-stale" },
          [new EsInningsCompleted({ innings: 1, runs: 200 })],
          1,
        )

        // Stream head is at version 2. Appending again at expectedVersion 1:
        // the ConditionCheck (event v1 exists) passes, but the Put at v2
        // collides — the cancellation must still surface as VersionConflict.
        const staleAtOne = yield* EsMatchEvents.append(
          { matchId: "es-stale" },
          [new EsInningsCompleted({ innings: 1, runs: 999 })],
          1,
        ).pipe(Effect.flip)
        expect(staleAtOne._tag).toBe("VersionConflict")
        expect((staleAtOne as VersionConflict).expectedVersion).toBe(1)

        // Stale at expectedVersion 0 (no ConditionCheck item) also conflicts.
        const staleAtZero = yield* EsMatchEvents.append(
          { matchId: "es-stale" },
          [new EsMatchStarted({ venue: "Duplicate" })],
          0,
        ).pipe(Effect.flip)
        expect(staleAtZero._tag).toBe("VersionConflict")

        const events = yield* EsMatchEvents.read({ matchId: "es-stale" })
        expect(events.map((e) => e.version)).toEqual([1, 2])
      }).pipe(provideEsGuard),
  )
})

// ---------------------------------------------------------------------------
// EventStore snapshots + commandHandler retry (closes #84)
// ---------------------------------------------------------------------------

const esSnapSchema = DynamoSchema.make({ name: "es-snap", version: 1 })
const esSnapTableName = `es-snap-${Date.now()}`
const EsSnapTable = Table.make({ schema: esSnapSchema })

// TaggedClass, not Class: the two events are structurally identical, so an
// untagged union would decode every `Withdrew` back as a `Deposited`.
class Deposited extends Schema.TaggedClass<Deposited>()("Deposited", {
  amount: Schema.Number,
}) {}

class Withdrew extends Schema.TaggedClass<Withdrew>()("Withdrew", {
  amount: Schema.Number,
}) {}

type LedgerEvent = Deposited | Withdrew

interface LedgerState {
  readonly balance: number
  readonly txCount: number
}

/**
 * Deliberately *transforming*: the balance is stored as a `"cents:<n>"` string,
 * so a snapshot that skipped `Schema.encodeUnknownEffect` / `decodeUnknownEffect`
 * would fail to round-trip against real DynamoDB.
 */
const BalanceFromCents = Schema.String.pipe(
  Schema.decodeTo(Schema.Number, {
    decode: SchemaGetter.transform((s: string) => Number(s.replace("cents:", ""))),
    encode: SchemaGetter.transform((n: number) => `cents:${n}`),
  }),
)

const LedgerStateSchema = Schema.Struct({
  balance: BalanceFromCents,
  txCount: Schema.Number,
})

class InsufficientFunds extends Data.TaggedError("InsufficientFunds")<{
  readonly balance: number
}> {}

type LedgerCommand =
  | { readonly _tag: "Deposit"; readonly amount: number }
  | { readonly _tag: "Withdraw"; readonly amount: number }

const ledgerDecider: EventStore.Decider<
  LedgerState,
  LedgerCommand,
  LedgerEvent,
  InsufficientFunds
> = {
  initialState: { balance: 0, txCount: 0 },
  decide: (command, state) =>
    Effect.gen(function* () {
      if (command._tag === "Deposit") return [new Deposited({ amount: command.amount })]
      if (state.balance < command.amount) {
        return yield* new InsufficientFunds({ balance: state.balance })
      }
      return [new Withdrew({ amount: command.amount })]
    }),
  evolve: (state, event) =>
    event instanceof Deposited
      ? { balance: state.balance + event.amount, txCount: state.txCount + 1 }
      : { balance: state.balance - event.amount, txCount: state.txCount + 1 },
}

/** No snapshots — the pre-#84 baseline. */
const PlainLedger = EventStore.makeStream({
  table: EsSnapTable,
  streamName: "Plain",
  events: [Deposited, Withdrew],
  streamId: { composite: ["accountId"] },
})

/** Snapshots enabled, auto-snapshot every 3 events. */
const SnapLedger = EventStore.makeStream({
  table: EsSnapTable,
  streamName: "Snap",
  events: [Deposited, Withdrew],
  streamId: { composite: ["accountId"] },
  snapshot: { schema: LedgerStateSchema, every: 3 },
})

const EsSnapTestLayer = Layer.mergeAll(ClientLayer, EsSnapTable.layer({ name: esSnapTableName }))
const provideEsSnap = Effect.provide(EsSnapTestLayer)

describeConnected("EventStore snapshots + retry (closes #84)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: esSnapTableName,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
        })
      }).pipe(provideEsSnap, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: esSnapTableName })
      }).pipe(
        provideEsSnap,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("snapshot round-trips through a transforming state schema", () =>
    Effect.gen(function* () {
      const client = yield* DynamoClient

      yield* SnapLedger.writeSnapshot({ accountId: "rt-1" }, { balance: 4200, txCount: 7 }, 7)

      const read = yield* SnapLedger.readSnapshot({ accountId: "rt-1" })
      expect(Option.isSome(read)).toBe(true)
      const snapshot = Option.getOrThrow(read)
      expect(snapshot.state).toEqual({ balance: 4200, txCount: 7 })
      expect(snapshot.asOfVersion).toBe(7)
      expect(typeof snapshot.timestamp).toBe("string")

      // The *stored* form is the encoded one — proves encode-on-write happened.
      const raw = yield* client.getItem({
        TableName: esSnapTableName,
        Key: {
          pk: { S: "$es-snap#v1#snap#rt-1" },
          sk: { S: "$es-snap#v1#snap.snapshot" },
        },
        ConsistentRead: true,
      })
      expect(raw.Item?.state?.M?.balance?.S).toBe("cents:4200")
      expect(raw.Item?.__edd_e__?.S).toBe("snap.snapshot")
    }).pipe(provideEsSnap),
  )

  it.effect("readSnapshot returns None when no snapshot has been written", () =>
    Effect.gen(function* () {
      const result = yield* SnapLedger.readSnapshot({ accountId: "absent-1" })
      expect(Option.isNone(result)).toBe(true)
    }).pipe(provideEsSnap),
  )

  it.effect("snapshot writes are monotonic — an older asOfVersion is a no-op", () =>
    Effect.gen(function* () {
      yield* SnapLedger.writeSnapshot({ accountId: "mono-1" }, { balance: 500, txCount: 5 }, 5)
      // Losing the race must not regress the cache, and must not fail.
      yield* SnapLedger.writeSnapshot({ accountId: "mono-1" }, { balance: 100, txCount: 1 }, 1)

      const snapshot = Option.getOrThrow(yield* SnapLedger.readSnapshot({ accountId: "mono-1" }))
      expect(snapshot.asOfVersion).toBe(5)
      expect(snapshot.state).toEqual({ balance: 500, txCount: 5 })

      // A strictly newer version does overwrite.
      yield* SnapLedger.writeSnapshot({ accountId: "mono-1" }, { balance: 900, txCount: 9 }, 9)
      const newer = Option.getOrThrow(yield* SnapLedger.readSnapshot({ accountId: "mono-1" }))
      expect(newer.asOfVersion).toBe(9)
    }).pipe(provideEsSnap),
  )

  it.effect("the snapshot item is invisible to read / readFrom / currentVersion", () =>
    Effect.gen(function* () {
      yield* SnapLedger.append(
        { accountId: "iso-1" },
        [
          new Deposited({ amount: 10 }),
          new Deposited({ amount: 20 }),
          new Deposited({ amount: 30 }),
        ],
        0,
      )
      // The snapshot SK sorts AFTER every event SK in the same partition, so a
      // `Limit`-bearing reverse query would hit it first without SK hardening.
      yield* SnapLedger.writeSnapshot({ accountId: "iso-1" }, { balance: 30, txCount: 2 }, 2)

      const all = yield* SnapLedger.read({ accountId: "iso-1" })
      expect(all.map((e) => e.version)).toEqual([1, 2, 3])

      const delta = yield* SnapLedger.readFrom({ accountId: "iso-1" }, 2)
      expect(delta.map((e) => e.version)).toEqual([3])

      // Without the begins_with bound this returns 0 (the snapshot is evaluated
      // first, then filtered out by __edd_e__, leaving an empty page).
      expect(yield* SnapLedger.currentVersion({ accountId: "iso-1" })).toBe(3)

      // ...and events are invisible to readSnapshot.
      const snapshot = Option.getOrThrow(yield* SnapLedger.readSnapshot({ accountId: "iso-1" }))
      expect(snapshot.asOfVersion).toBe(2)
    }).pipe(provideEsSnap),
  )

  it.effect("snapshot-aware handler agrees with a full replay and auto-snapshots", () =>
    Effect.gen(function* () {
      const handle = EventStore.commandHandler(ledgerDecider, SnapLedger)
      const key = { accountId: "auto-1" }

      // every: 3 — v1, v2 stay below the threshold.
      yield* handle(key, { _tag: "Deposit", amount: 100 })
      yield* handle(key, { _tag: "Deposit", amount: 50 })
      expect(Option.isNone(yield* SnapLedger.readSnapshot(key))).toBe(true)

      // v3 crosses it.
      const r3 = yield* handle(key, { _tag: "Withdraw", amount: 30 })
      expect(r3.version).toBe(3)
      expect(r3.state).toEqual({ balance: 120, txCount: 3 })

      const snapshot = Option.getOrThrow(yield* SnapLedger.readSnapshot(key))
      expect(snapshot.asOfVersion).toBe(3)
      expect(snapshot.state).toEqual({ balance: 120, txCount: 3 })

      // The next command reads the snapshot and folds only the delta — the
      // result must be identical to a full replay of the same events.
      const r4 = yield* handle(key, { _tag: "Deposit", amount: 80 })
      expect(r4.version).toBe(4)
      expect(r4.state).toEqual({ balance: 200, txCount: 4 })

      const replayed = EventStore.fold(ledgerDecider, yield* SnapLedger.read(key))
      expect(replayed).toEqual(r4.state)

      // Still at v3 — only 1 event since the last snapshot.
      expect(Option.getOrThrow(yield* SnapLedger.readSnapshot(key)).asOfVersion).toBe(3)

      // v6 crosses the threshold again.
      yield* handle(key, { _tag: "Deposit", amount: 1 })
      const r6 = yield* handle(key, { _tag: "Deposit", amount: 1 })
      expect(r6.version).toBe(6)
      expect(Option.getOrThrow(yield* SnapLedger.readSnapshot(key)).asOfVersion).toBe(6)
    }).pipe(provideEsSnap),
  )

  it.effect("a snapshot-aware handler and a plain handler reach the same state", () =>
    Effect.gen(function* () {
      const snapHandle = EventStore.commandHandler(ledgerDecider, SnapLedger)
      const plainHandle = EventStore.commandHandler(ledgerDecider, PlainLedger)
      const commands: ReadonlyArray<LedgerCommand> = [
        { _tag: "Deposit", amount: 10 },
        { _tag: "Deposit", amount: 20 },
        { _tag: "Withdraw", amount: 5 },
        { _tag: "Deposit", amount: 7 },
        { _tag: "Withdraw", amount: 2 },
      ]

      let snapState: LedgerState = ledgerDecider.initialState
      let plainState: LedgerState = ledgerDecider.initialState
      for (const command of commands) {
        snapState = (yield* snapHandle({ accountId: "parity-1" }, command)).state
        plainState = (yield* plainHandle({ accountId: "parity-1" }, command)).state
      }

      expect(snapState).toEqual(plainState)
      expect(snapState).toEqual({ balance: 30, txCount: 5 })
    }).pipe(provideEsSnap),
  )

  it.effect("domain errors from the decider still fail the handler", () =>
    Effect.gen(function* () {
      const handle = EventStore.commandHandler(ledgerDecider, SnapLedger, { retry: 3 })
      const error = yield* handle({ accountId: "domain-1" }, { _tag: "Withdraw", amount: 10 }).pipe(
        Effect.flip,
      )
      expect(error._tag).toBe("InsufficientFunds")
    }).pipe(provideEsSnap),
  )

  it.effect("without retry, a real concurrent conflict surfaces as VersionConflict", () =>
    Effect.gen(function* () {
      const handle = EventStore.commandHandler(ledgerDecider, PlainLedger)
      const key = { accountId: "conflict-1" }

      const results = yield* Effect.all(
        [
          handle(key, { _tag: "Deposit", amount: 10 }).pipe(Effect.result),
          handle(key, { _tag: "Deposit", amount: 20 }).pipe(Effect.result),
        ],
        { concurrency: 2 },
      )

      const failures = results.filter((r) => r._tag === "Failure")
      expect(failures).toHaveLength(1)
      expect((failures[0] as { failure: { _tag: string } }).failure._tag).toBe("VersionConflict")

      // Only the winner's event landed.
      expect(yield* PlainLedger.currentVersion(key)).toBe(1)
    }).pipe(provideEsSnap),
  )

  it.effect("retry resolves a real concurrent VersionConflict by re-deciding", () =>
    Effect.gen(function* () {
      const handle = EventStore.commandHandler(ledgerDecider, PlainLedger, { retry: 5 })
      const key = { accountId: "retry-1" }

      yield* Effect.all(
        [
          handle(key, { _tag: "Deposit", amount: 10 }),
          handle(key, { _tag: "Deposit", amount: 20 }),
          handle(key, { _tag: "Deposit", amount: 30 }),
        ],
        { concurrency: 3 },
      )

      // All three commands landed exactly once, at consecutive versions — a
      // blind re-append would have produced duplicates or lost an event.
      const events = yield* PlainLedger.read(key)
      expect(events.map((e) => e.version)).toEqual([1, 2, 3])
      expect(EventStore.fold(ledgerDecider, events)).toEqual({ balance: 60, txCount: 3 })
      expect(yield* PlainLedger.currentVersion(key)).toBe(3)
    }).pipe(provideEsSnap),
  )

  it.effect("retry composes with the snapshot-aware read path", () =>
    Effect.gen(function* () {
      const handle = EventStore.commandHandler(ledgerDecider, SnapLedger, { retry: 5 })
      const key = { accountId: "retry-snap-1" }

      yield* Effect.all(
        [
          handle(key, { _tag: "Deposit", amount: 100 }),
          handle(key, { _tag: "Deposit", amount: 200 }),
          handle(key, { _tag: "Deposit", amount: 300 }),
          handle(key, { _tag: "Deposit", amount: 400 }),
        ],
        { concurrency: 4 },
      )

      const events = yield* SnapLedger.read(key)
      expect(events.map((e) => e.version)).toEqual([1, 2, 3, 4])
      expect(EventStore.fold(ledgerDecider, events)).toEqual({ balance: 1000, txCount: 4 })

      // The auto-snapshot (every: 3) fired and is consistent with the stream.
      const snapshot = Option.getOrThrow(yield* SnapLedger.readSnapshot(key))
      expect(snapshot.asOfVersion).toBeGreaterThanOrEqual(3)
      const upTo = events.filter((e) => e.version <= snapshot.asOfVersion)
      expect(EventStore.fold(ledgerDecider, upTo)).toEqual(snapshot.state)
    }).pipe(provideEsSnap),
  )

  it.effect("bind carries the snapshot primitives with R = never", () =>
    Effect.gen(function* () {
      const bound = yield* EventStore.bind(SnapLedger)
      expect(bound.snapshotConfig).toEqual({ mode: "after-append", every: 3 })

      const program: Effect.Effect<number, unknown, never> = Effect.gen(function* () {
        yield* bound.writeSnapshot({ accountId: "bind-1" }, { balance: 11, txCount: 1 }, 1)
        const snapshot = yield* bound.readSnapshot({ accountId: "bind-1" })
        return Option.getOrThrow(snapshot).state.balance
      })

      expect(yield* program).toBe(11)
    }).pipe(provideEsSnap),
  )

  it.effect("data-last commandHandler works against a live stream", () =>
    Effect.gen(function* () {
      const handle = PlainLedger.pipe(EventStore.commandHandler(ledgerDecider, { retry: 2 }))
      const result = yield* handle({ accountId: "datalast-1" }, { _tag: "Deposit", amount: 42 })
      expect(result.version).toBe(1)
      expect(result.state).toEqual({ balance: 42, txCount: 1 })
    }).pipe(provideEsSnap),
  )
})

// ---------------------------------------------------------------------------
// EventStore — additional transaction items + command idempotency (#85)
// ---------------------------------------------------------------------------

const esIdemSchema = DynamoSchema.make({ name: "es-idem", version: 1 })
const esIdemTableName = `es-idem-${Date.now()}`

/** Side record written atomically with events, to exercise `additionalItems`. */
class EsWatermark extends Schema.Class<EsWatermark>("EsWatermark")({
  writerId: Schema.String,
  lastSeq: Schema.Number,
}) {}

const EsWatermarks = Entity.make({
  model: EsWatermark,
  entityType: "EsWatermark",
  primaryKey: {
    pk: { field: "pk", composite: ["writerId"] },
    sk: { field: "sk", composite: [] },
  },
})

/**
 * A read model authored with the PURE, AWS-free `@effect-dynamodb/schema`
 * `Entity.make` — the #100 shape. A pure definition carries no CRUD ops, so the
 * only write descriptor its author can hold is the bound builder returned by
 * `db.entities.EsStatusProjection.put(...)`.
 */
const EsStatusRecord = Schema.Struct({
  matchId: Schema.String,
  state: Schema.String,
})

const EsStatusProjection = PureEntity.make({
  model: DynamoModel.configure(EsStatusRecord, { matchId: { identifier: true } }),
  entityType: "EsStatus",
  primaryKey: {
    pk: { field: "pk", composite: ["matchId"] },
    sk: { field: "sk", composite: [] },
  },
  // Timestamps + version make the upsert-vs-put divergence observable: a real
  // upsert preserves `createdAt` and increments `version`; a Put resets both.
  timestamps: true,
  versioned: true,
})

const EsIdemTable = Table.make({
  schema: esIdemSchema,
  entities: { EsWatermarks, EsStatusProjection },
})

class EsIdemMatchStarted extends Schema.Class<EsIdemMatchStarted>("EsIdemMatchStarted")({
  venue: Schema.String,
}) {}

class EsIdemInningsCompleted extends Schema.Class<EsIdemInningsCompleted>("EsIdemInningsCompleted")(
  {
    innings: Schema.Number,
    runs: Schema.Number,
  },
) {}

type EsIdemMatchEvent = EsIdemMatchStarted | EsIdemInningsCompleted

const EsIdemMatchEvents = EventStore.makeStream({
  table: EsIdemTable,
  streamName: "EsMatch",
  events: [EsIdemMatchStarted, EsIdemInningsCompleted],
  streamId: { composite: ["matchId"] },
})

interface EsIdemMatchState {
  readonly status: "pending" | "in-progress"
  readonly innings: number
}

type EsIdemMatchCommand =
  | { readonly _tag: "Start"; readonly venue: string }
  | { readonly _tag: "CompleteInnings"; readonly innings: number; readonly runs: number }

const esIdemDecider: EventStore.Decider<EsIdemMatchState, EsIdemMatchCommand, EsIdemMatchEvent> = {
  initialState: { status: "pending", innings: 0 },
  decide: (command) =>
    Effect.succeed(
      command._tag === "Start"
        ? [new EsIdemMatchStarted({ venue: command.venue })]
        : [new EsIdemInningsCompleted({ innings: command.innings, runs: command.runs })],
    ),
  evolve: (state, event) =>
    event instanceof EsIdemMatchStarted
      ? { ...state, status: "in-progress" as const }
      : { ...state, innings: state.innings + 1 },
}

const EsIdemTestLayer = Layer.mergeAll(ClientLayer, EsIdemTable.layer({ name: esIdemTableName }))
const provideEsIdem = Effect.provide(EsIdemTestLayer)

describeConnected("EventStore — additionalItems + idempotency (closes #85)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: esIdemTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(EsIdemTable),
        })
      }).pipe(provideEsIdem, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: esIdemTableName })
      }).pipe(
        provideEsIdem,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("commits events and an additional item atomically", () =>
    Effect.gen(function* () {
      yield* EsIdemMatchEvents.append(
        { matchId: "atomic-1" },
        [new EsIdemMatchStarted({ venue: "MCG" })],
        0,
        { additionalItems: [EsWatermarks.put({ writerId: "w-atomic-1", lastSeq: 1 })] },
      )

      const events = yield* EsIdemMatchEvents.read({ matchId: "atomic-1" })
      expect(events).toHaveLength(1)
      expect(events[0]!.eventType).toBe("EsIdemMatchStarted")

      const watermark = yield* EsWatermarks.get({ writerId: "w-atomic-1" })
      expect(watermark.lastSeq).toBe(1)
    }).pipe(provideEsIdem),
  )

  it.effect("a user-item condition failure is NOT reported as VersionConflict", () =>
    Effect.gen(function* () {
      yield* EsWatermarks.put({ writerId: "w-cond-1", lastSeq: 100 })

      const error = yield* EsIdemMatchEvents.append(
        { matchId: "cond-1" },
        [new EsIdemMatchStarted({ venue: "SCG" })],
        0,
        {
          additionalItems: [
            // Watermark is at 100 — this condition cannot hold.
            Transaction.check(
              EsWatermarks.get({ writerId: "w-cond-1" }),
              Expression.condition({ lt: { lastSeq: 50 } }),
            ),
          ],
        },
      ).pipe(Effect.flip)

      expect(error._tag).toBe("AdditionalItemConditionFailed")
      const failure = error as AdditionalItemConditionFailed
      expect(failure.streamName).toBe("EsMatch")
      expect(failure.streamId).toBe("cond-1")
      expect(failure.indices).toEqual([0])

      // All-or-nothing: the event must not have been written
      const events = yield* EsIdemMatchEvents.read({ matchId: "cond-1" })
      expect(events).toHaveLength(0)
    }).pipe(provideEsIdem),
  )

  it.effect("a version conflict is still mapped correctly with additional items present", () =>
    Effect.gen(function* () {
      // Establish v1 on the stream
      yield* EsIdemMatchEvents.append(
        { matchId: "vc-1" },
        [new EsIdemMatchStarted({ venue: "WACA" })],
        0,
      )
      yield* EsWatermarks.put({ writerId: "w-vc-1", lastSeq: 0 })

      // Append again at the now-stale expectedVersion 0, with a satisfiable
      // additional-item condition, so only the event put's guard can fail.
      const error = yield* EsIdemMatchEvents.append(
        { matchId: "vc-1" },
        [new EsIdemInningsCompleted({ innings: 1, runs: 250 })],
        0,
        {
          additionalItems: [
            Transaction.check(
              EsWatermarks.get({ writerId: "w-vc-1" }),
              Expression.condition({ eq: { lastSeq: 0 } }),
            ),
          ],
        },
      ).pipe(Effect.flip)

      expect(error._tag).toBe("VersionConflict")
      expect((error as VersionConflict).expectedVersion).toBe(0)
    }).pipe(provideEsIdem),
  )

  it.effect("rejects a replayed commandId with DuplicateCommand", () =>
    Effect.gen(function* () {
      const handle = EventStore.commandHandler(esIdemDecider, EsIdemMatchEvents, {
        idempotency: {},
      })

      const first = yield* handle(
        { matchId: "idem-1" },
        { _tag: "Start", venue: "Lords" },
        { commandId: "cmd-idem-1" },
      )
      expect(first.version).toBe(1)

      const error = yield* handle(
        { matchId: "idem-1" },
        { _tag: "Start", venue: "Lords" },
        { commandId: "cmd-idem-1" },
      ).pipe(Effect.flip)

      expect(error._tag).toBe("DuplicateCommand")
      expect((error as DuplicateCommand).commandId).toBe("cmd-idem-1")

      // The replay must not have appended a second event
      const events = yield* EsIdemMatchEvents.read({ matchId: "idem-1" })
      expect(events).toHaveLength(1)
    }).pipe(provideEsIdem),
  )

  it.effect("distinct commandIds each append, and sentinels are invisible to read", () =>
    Effect.gen(function* () {
      const handle = EventStore.commandHandler(esIdemDecider, EsIdemMatchEvents, {
        idempotency: { ttl: Duration.days(1) },
      })

      yield* handle(
        { matchId: "idem-2" },
        { _tag: "Start", venue: "Eden" },
        { commandId: "cmd-idem-2a" },
      )
      yield* handle(
        { matchId: "idem-2" },
        { _tag: "CompleteInnings", innings: 1, runs: 300 },
        { commandId: "cmd-idem-2b" },
      )

      const events = yield* EsIdemMatchEvents.read({ matchId: "idem-2" })
      expect(events.map((e) => e.eventType)).toEqual([
        "EsIdemMatchStarted",
        "EsIdemInningsCompleted",
      ])
      expect(yield* EsIdemMatchEvents.currentVersion({ matchId: "idem-2" })).toBe(2)

      // Sentinels live in the same partition but under a different entity type —
      // a raw partition query sees them, the typed read does not.
      const raw = yield* (yield* DynamoClient).query({
        TableName: esIdemTableName,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: "$es-idem#v1#esmatch#idem-2" } },
      })
      const entityTypes = (raw.Items ?? []).map((i) => i.__edd_e__?.S)
      expect(entityTypes.filter((t) => t === "esmatch.command")).toHaveLength(2)
      expect(entityTypes.filter((t) => t === "esmatch.event")).toHaveLength(2)
    }).pipe(provideEsIdem),
  )

  // -------------------------------------------------------------------------
  // #100 — bound-CRUD builders as multi-item write ops.
  //
  // The headline `additionalItems` use case: commit a read model atomically
  // with the events that produced it, where the read model was authored with
  // the pure `@effect-dynamodb/schema` Entity.make. Before the fix this failed
  // with ValidationError { entityType: "unknown" }.
  // -------------------------------------------------------------------------

  it.effect("commits a pure-authored read-model row atomically with the events (#100)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      yield* EsIdemMatchEvents.append(
        { matchId: "proj-1" },
        [new EsIdemMatchStarted({ venue: "Basin Reserve" })],
        0,
        {
          additionalItems: [
            db.entities.EsStatusProjection.put({ matchId: "proj-1", state: "IN_PROGRESS" }),
          ],
        },
      )

      const events = yield* EsIdemMatchEvents.read({ matchId: "proj-1" })
      expect(events.map((e) => e.eventType)).toEqual(["EsIdemMatchStarted"])

      const projection = yield* db.entities.EsStatusProjection.get({ matchId: "proj-1" })
      expect(projection.state).toBe("IN_PROGRESS")
    }).pipe(provideEsIdem),
  )

  it.effect("a failing condition on a bound additional item rolls the whole append back", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      yield* db.entities.EsStatusProjection.put({ matchId: "proj-2", state: "IN_PROGRESS" })

      // The projection is already IN_PROGRESS, so this condition cannot hold.
      // Before the fix the condition was dropped and the write silently applied.
      const error = yield* EsIdemMatchEvents.append(
        { matchId: "proj-2" },
        [new EsIdemMatchStarted({ venue: "Seddon Park" })],
        0,
        {
          additionalItems: [
            db.entities.EsStatusProjection.put({ matchId: "proj-2", state: "COMPLETE" }).condition({
              state: "PRE_MATCH",
            }),
          ],
        },
      ).pipe(Effect.flip)

      expect(error._tag).toBe("AdditionalItemConditionFailed")
      expect((error as AdditionalItemConditionFailed).indices).toEqual([0])

      // All-or-nothing: neither the event nor the projection update landed.
      expect(yield* EsIdemMatchEvents.read({ matchId: "proj-2" })).toHaveLength(0)
      const projection = yield* db.entities.EsStatusProjection.get({ matchId: "proj-2" })
      expect(projection.state).toBe("IN_PROGRESS")
    }).pipe(provideEsIdem),
  )

  it.effect("keeps a versioned read model in step across appends (#133)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })
      const client = yield* DynamoClient
      const stored = () =>
        client
          .getItem({
            TableName: esIdemTableName,
            Key: {
              pk: { S: "$es-idem#v1#esstatus#matchid_proj-3" },
              sk: { S: "$es-idem#v1#esstatus" },
            },
            ConsistentRead: true,
          })
          .pipe(Effect.map((r) => fromAttributeMap(r.Item!) as Record<string, any>))

      yield* EsIdemMatchEvents.append(
        { matchId: "proj-3" },
        [new EsIdemMatchStarted({ venue: "Eden Park" })],
        0,
        {
          additionalItems: [
            db.entities.EsStatusProjection.put({ matchId: "proj-3", state: "IN_PROGRESS" }),
          ],
        },
      )
      const first = yield* stored()
      // The second append replaces the existing versioned row — no condition
      // was set, so nothing may fail as one.
      yield* EsIdemMatchEvents.append(
        { matchId: "proj-3" },
        [new EsIdemInningsCompleted({ innings: 1, runs: 250 })],
        1,
        {
          additionalItems: [
            db.entities.EsStatusProjection.put({ matchId: "proj-3", state: "INNINGS_BREAK" }),
          ],
        },
      )
      const second = yield* stored()
      expect([second.state, second.version]).toEqual(["INNINGS_BREAK", 2])
      expect(second.createdAt).toBe(first.createdAt)
      expect(second.__edd_i__).toBe(first.__edd_i__)
      expect(yield* EsIdemMatchEvents.read({ matchId: "proj-3" })).toHaveLength(2)
    }).pipe(provideEsIdem),
  )

  it.effect("Transaction.transactWrite accepts bound builders from a pure entity (#100)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      yield* Transaction.transactWrite([
        db.entities.EsStatusProjection.put({ matchId: "tx-1", state: "PRE_MATCH" }),
        db.entities.EsStatusProjection.put({ matchId: "tx-2", state: "PRE_MATCH" }),
      ])

      expect((yield* db.entities.EsStatusProjection.get({ matchId: "tx-1" })).state).toBe(
        "PRE_MATCH",
      )
      expect((yield* db.entities.EsStatusProjection.get({ matchId: "tx-2" })).state).toBe(
        "PRE_MATCH",
      )

      yield* Transaction.transactWrite([db.entities.EsStatusProjection.delete({ matchId: "tx-2" })])
      const gone = yield* db.entities.EsStatusProjection.get({ matchId: "tx-2" }).pipe(Effect.flip)
      expect(gone._tag).toBe("ItemNotFound")
    }).pipe(provideEsIdem),
  )

  it.effect("transactWrite honours create()'s attribute_not_exists guard (#100)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      yield* Transaction.transactWrite([
        db.entities.EsStatusProjection.create({ matchId: "tx-create", state: "PRE_MATCH" }),
      ])

      // Second create on the same key must be rejected by real DynamoDB — before
      // the fix the guard was dropped and this silently overwrote the row.
      const error = yield* Transaction.transactWrite([
        db.entities.EsStatusProjection.create({ matchId: "tx-create", state: "COMPLETE" }),
      ]).pipe(Effect.flip)
      expect(error._tag).toBe("TransactionCancelled")

      const row = yield* db.entities.EsStatusProjection.get({ matchId: "tx-create" })
      expect(row.state).toBe("PRE_MATCH")
    }).pipe(provideEsIdem),
  )

  it.effect("Batch.write accepts bound builders from a pure entity (#100)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      yield* Batch.write([
        db.entities.EsStatusProjection.put({ matchId: "bw-1", state: "PRE_MATCH" }),
        db.entities.EsStatusProjection.put({ matchId: "bw-2", state: "PRE_MATCH" }),
      ])

      expect((yield* db.entities.EsStatusProjection.get({ matchId: "bw-1" })).state).toBe(
        "PRE_MATCH",
      )

      yield* Batch.write([db.entities.EsStatusProjection.delete({ matchId: "bw-1" })])
      const gone = yield* db.entities.EsStatusProjection.get({ matchId: "bw-1" }).pipe(Effect.flip)
      expect(gone._tag).toBe("ItemNotFound")
    }).pipe(provideEsIdem),
  )

  it.effect("Batch.write rejects a conditional op rather than dropping the condition", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      const error = yield* Batch.write([
        db.entities.EsStatusProjection.create({ matchId: "bw-cond", state: "PRE_MATCH" }),
      ]).pipe(Effect.flip)

      expect(error._tag).toBe("ValidationError")
      const gone = yield* db.entities.EsStatusProjection.get({ matchId: "bw-cond" }).pipe(
        Effect.flip,
      )
      expect(gone._tag).toBe("ItemNotFound")
    }).pipe(provideEsIdem),
  )

  // -------------------------------------------------------------------------
  // #100 review — `upsert` does NOT have Put semantics. Its whole contract is
  // `if_not_exists` on createdAt / immutable fields / the version counter, so
  // compiling it as a Put silently resets them. Proven end-to-end here: the
  // direct upsert preserves, the transact paths refuse, and the stored row is
  // left exactly as it was.
  // -------------------------------------------------------------------------

  it.effect("a direct upsert preserves createdAt and bumps version (the contract)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      const first = yield* db.entities.EsStatusProjection.upsert({
        matchId: "ups-1",
        state: "PRE_MATCH",
      }).asEffect()
      const second = yield* db.entities.EsStatusProjection.upsert({
        matchId: "ups-1",
        state: "IN_PROGRESS",
      }).asEffect()

      // `upsert` resolves to the model type; `createdAt` / `version` are added
      // by `timestamps` / `versioned` on the stored record, which the bound
      // builders do not surface — hence the widening view.
      type Stamped = { readonly createdAt: unknown; readonly version: number }
      const firstStamped = first as unknown as Stamped
      const secondStamped = second as unknown as Stamped

      expect(secondStamped.createdAt).toEqual(firstStamped.createdAt)
      expect(secondStamped.version).toBe(firstStamped.version + 1)
    }).pipe(provideEsIdem),
  )

  it.effect("transactWrite refuses an upsert and leaves the stored row untouched", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      const before = yield* db.entities.EsStatusProjection.upsert({
        matchId: "ups-tx",
        state: "PRE_MATCH",
      }).asEffect()

      const error = yield* Transaction.transactWrite([
        db.entities.EsStatusProjection.upsert({ matchId: "ups-tx", state: "IN_PROGRESS" }),
      ]).pipe(Effect.flip)
      expect(error._tag).toBe("ValidationError")

      const after = yield* db.entities.EsStatusProjection.get({ matchId: "ups-tx" }).pipe(
        Effect.map((r) => r as unknown as { state: string; createdAt: unknown; version: number }),
      )
      // Refused up front — no partial write, and createdAt/version intact.
      expect(after.state).toBe("PRE_MATCH")
      const beforeStamped = before as unknown as {
        readonly createdAt: unknown
        readonly version: number
      }
      expect(after.createdAt).toEqual(beforeStamped.createdAt)
      expect(after.version).toBe(beforeStamped.version)
    }).pipe(provideEsIdem),
  )

  it.effect("Batch.write and additionalItems refuse an upsert too", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { EsStatusProjection },
        tables: { EsIdemTable },
      })

      const batchError = yield* Batch.write([
        db.entities.EsStatusProjection.upsert({ matchId: "ups-bw", state: "PRE_MATCH" }),
      ]).pipe(Effect.flip)
      expect(batchError._tag).toBe("ValidationError")

      const appendError = yield* EsIdemMatchEvents.append(
        { matchId: "ups-es" },
        [new EsIdemMatchStarted({ venue: "MCG" })],
        0,
        {
          additionalItems: [
            db.entities.EsStatusProjection.upsert({ matchId: "ups-es", state: "PRE_MATCH" }),
          ],
        },
      ).pipe(Effect.flip)
      expect(appendError._tag).toBe("ValidationError")

      // All-or-nothing: the refused append wrote no events either.
      expect(yield* EsIdemMatchEvents.read({ matchId: "ups-es" })).toHaveLength(0)
    }).pipe(provideEsIdem),
  )
})

// ===========================================================================
// EventStore command path — consistent reads (#139), caller expectedVersion
// (#136), decision-derived additionalItems with fold-before-append (#137)
// ===========================================================================

const esCmdSchema = DynamoSchema.make({ name: "es-cmd", version: 1 })
const esCmdTableName = `es-cmd-${Date.now()}`

/** Inline projection of an account — written in the same transaction as its events. */
class EsCmdTally extends Schema.Class<EsCmdTally>("EsCmdTally")({
  accountId: Schema.String,
  balance: Schema.Number,
  txCount: Schema.Number,
}) {}

const EsCmdTallies = Entity.make({
  model: EsCmdTally,
  entityType: "EsCmdTally",
  primaryKey: {
    pk: { field: "pk", composite: ["accountId"] },
    sk: { field: "sk", composite: [] },
  },
})

const EsCmdTable = Table.make({ schema: esCmdSchema, entities: { EsCmdTallies } })

class EsCmdDeposited extends Schema.TaggedClass<EsCmdDeposited>()("EsCmdDeposited", {
  amount: Schema.Number,
}) {}

class EsCmdWithdrew extends Schema.TaggedClass<EsCmdWithdrew>()("EsCmdWithdrew", {
  amount: Schema.Number,
}) {}

type EsCmdEvent = EsCmdDeposited | EsCmdWithdrew

interface EsCmdState {
  readonly balance: number
  readonly txCount: number
}

type EsCmdCommand =
  | { readonly _tag: "Deposit"; readonly amount: number }
  | { readonly _tag: "Withdraw"; readonly amount: number }

class EsCmdInsufficientFunds extends Data.TaggedError("EsCmdInsufficientFunds") {}

const EsCmdAccounts = EventStore.makeStream({
  table: EsCmdTable,
  streamName: "Account",
  events: [EsCmdDeposited, EsCmdWithdrew],
  streamId: { composite: ["accountId"] },
})

/** Every `decide` call, by account — proves a stale If-Match never reaches it. */
const esCmdDecideLog: Array<string> = []

const esCmdDecider = (
  accountId: string,
): EventStore.Decider<EsCmdState, EsCmdCommand, EsCmdEvent, EsCmdInsufficientFunds> => ({
  initialState: { balance: 0, txCount: 0 },
  decide: (command, state) =>
    Effect.gen(function* () {
      esCmdDecideLog.push(accountId)
      if (command._tag === "Deposit") return [new EsCmdDeposited({ amount: command.amount })]
      if (state.balance < command.amount) return yield* new EsCmdInsufficientFunds()
      return [new EsCmdWithdrew({ amount: command.amount })]
    }),
  evolve: (state, event) =>
    event instanceof EsCmdDeposited
      ? { balance: state.balance + event.amount, txCount: state.txCount + 1 }
      : { balance: state.balance - event.amount, txCount: state.txCount + 1 },
})

/** Every `query` this suite issues: the partition it reads and its `ConsistentRead`. */
const esCmdQueryLog: Array<{ readonly pk: string | undefined; readonly consistent: boolean }> = []

const EsCmdClientLayer = Layer.effect(
  DynamoClient,
  Effect.map(
    DynamoClient,
    (client): DynamoClientService => ({
      ...client,
      query: (input) => {
        esCmdQueryLog.push({
          pk: input.ExpressionAttributeValues?.[":pk"]?.S,
          consistent: input.ConsistentRead === true,
        })
        return client.query(input)
      },
    }),
  ),
).pipe(Layer.provide(ClientLayer))

const EsCmdLayer = Layer.mergeAll(EsCmdClientLayer, EsCmdTable.layer({ name: esCmdTableName }))
const provideEsCmd = Effect.provide(EsCmdLayer)

/** The queries issued against one account's stream partition. */
const esCmdQueriesFor = (accountId: string) =>
  esCmdQueryLog.filter((entry) => entry.pk === `$es-cmd#v1#account#${accountId}`)

class EsCmdOwner extends Context.Service<EsCmdOwner, { readonly accountId: string }>()(
  "test/EsCmdOwner",
) {}

describeConnected("EventStore command path (#139, #136, #137)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: esCmdTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(EsCmdTable),
        })
      }).pipe(provideEsCmd, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: esCmdTableName })
      }).pipe(
        provideEsCmd,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // -------------------------------------------------------------------------
  // #139 — consistent reads
  // -------------------------------------------------------------------------

  it.effect("#139 consistentRead reads its own writes on every read primitive", () =>
    Effect.gen(function* () {
      const key = { accountId: "ryw-1" }
      yield* EsCmdAccounts.append(key, [new EsCmdDeposited({ amount: 10 })], 0)
      yield* EsCmdAccounts.append(key, [new EsCmdDeposited({ amount: 5 })], 1)

      const all = yield* EsCmdAccounts.read(key, { consistentRead: true })
      const after = yield* EsCmdAccounts.readFrom(key, 1, { consistentRead: true })
      const head = yield* EsCmdAccounts.currentVersion(key, { consistentRead: true })
      expect(all.map((e) => e.version)).toEqual([1, 2])
      expect(after.map((e) => e.data)).toEqual([new EsCmdDeposited({ amount: 5 })])
      expect(head).toBe(2)

      const bound = yield* EventStore.bind(EsCmdAccounts)
      expect(yield* bound.currentVersion(key, { consistentRead: true })).toBe(2)

      // Every one of those requests asked for a strongly consistent read.
      const queries = esCmdQueriesFor("ryw-1")
      expect(queries).toHaveLength(4)
      expect(queries.every((q) => q.consistent)).toBe(true)

      // The default is unchanged: eventually consistent.
      yield* EsCmdAccounts.read(key)
      expect(esCmdQueriesFor("ryw-1").at(-1)?.consistent).toBe(false)
    }).pipe(provideEsCmd),
  )

  it.effect(
    "#139 commandHandler loads consistently by default; consistentRead: false opts out",
    () =>
      Effect.gen(function* () {
        const key = { accountId: "ryw-2" }
        const consistent = EventStore.commandHandler(esCmdDecider("ryw-2"), EsCmdAccounts)
        const relaxed = EsCmdAccounts.pipe(
          EventStore.commandHandler(esCmdDecider("ryw-2"), { consistentRead: false }),
        )

        // Each command sees the previous one's event immediately.
        expect((yield* consistent(key, { _tag: "Deposit", amount: 7 })).version).toBe(1)
        expect((yield* consistent(key, { _tag: "Withdraw", amount: 2 })).state).toEqual({
          balance: 5,
          txCount: 2,
        })
        expect(esCmdQueriesFor("ryw-2").map((q) => q.consistent)).toEqual([true, true])

        yield* relaxed(key, { _tag: "Deposit", amount: 1 })
        expect(esCmdQueriesFor("ryw-2").at(-1)?.consistent).toBe(false)
      }).pipe(provideEsCmd),
  )

  // -------------------------------------------------------------------------
  // #136 — caller-supplied expectedVersion (If-Match)
  // -------------------------------------------------------------------------

  it.effect("#136 a stale expectedVersion fails before decide, with actualVersion, unretried", () =>
    Effect.gen(function* () {
      const key = { accountId: "ifm-1" }
      yield* EsCmdAccounts.append(
        key,
        [new EsCmdDeposited({ amount: 10 }), new EsCmdDeposited({ amount: 20 })],
        0,
      )
      const handle = EventStore.commandHandler(esCmdDecider("ifm-1"), EsCmdAccounts, {
        retry: 5,
      })

      const error = yield* handle(
        key,
        { _tag: "Withdraw", amount: 5 },
        { expectedVersion: 1 },
      ).pipe(Effect.flip)

      expect(error._tag).toBe("VersionConflict")
      const conflict = error as VersionConflict
      expect(conflict.streamName).toBe("Account")
      expect(conflict.streamId).toBe("ifm-1")
      expect(conflict.expectedVersion).toBe(1)
      expect(conflict.actualVersion).toBe(2)
      // `decide` never ran, the retry policy was not applied (one load), and
      // nothing was written.
      expect(esCmdDecideLog.filter((id) => id === "ifm-1")).toHaveLength(0)
      expect(esCmdQueriesFor("ifm-1")).toHaveLength(1)
      expect(yield* EsCmdAccounts.currentVersion(key, { consistentRead: true })).toBe(2)

      // The matching version goes through.
      const ok = yield* handle(key, { _tag: "Withdraw", amount: 5 }, { expectedVersion: 2 })
      expect(ok.version).toBe(3)
      expect(ok.state).toEqual({ balance: 25, txCount: 3 })
    }).pipe(provideEsCmd),
  )

  it.effect("#136 concurrent writers at the same expectedVersion — exactly one lands", () =>
    Effect.gen(function* () {
      const key = { accountId: "ifm-2" }
      yield* EsCmdAccounts.append(key, [new EsCmdDeposited({ amount: 100 })], 0)
      const handle = EventStore.commandHandler(esCmdDecider("ifm-2"), EsCmdAccounts, {
        retry: 5,
      })

      const results = yield* Effect.all(
        [1, 2, 3].map((amount) =>
          handle(key, { _tag: "Withdraw", amount }, { expectedVersion: 1 }).pipe(Effect.result),
        ),
        { concurrency: 3 },
      )

      const successes = results.filter((r) => r._tag === "Success")
      const failures = results.filter((r) => r._tag === "Failure")
      expect(successes).toHaveLength(1)
      expect(failures).toHaveLength(2)
      for (const failure of failures) {
        expect((failure as { failure: { _tag: string } }).failure._tag).toBe("VersionConflict")
      }
      // The retry policy never ran: exactly one load per command. A retried
      // loser — whether it lost at the append or at the pre-decide check —
      // would have reloaded the partition (decide counts cannot show this: a
      // retried loser fails pre-decide and never reaches decide).
      expect(esCmdQueriesFor("ifm-2")).toHaveLength(3)
      expect(yield* EsCmdAccounts.currentVersion(key, { consistentRead: true })).toBe(2)
    }).pipe(provideEsCmd),
  )

  it.effect(
    "#136 a redelivered idempotent command with its original expectedVersion is a DuplicateCommand",
    () =>
      Effect.gen(function* () {
        const key = { accountId: "ifm-3" }
        const handle = EventStore.commandHandler(esCmdDecider("ifm-3"), EsCmdAccounts, {
          idempotency: {},
          retry: 5,
        })

        // First delivery commits; its response is "lost".
        const first = yield* handle(
          key,
          { _tag: "Deposit", amount: 5 },
          { commandId: "cmd-1", expectedVersion: 0 },
        )
        expect(first.version).toBe(1)

        // The redelivery carries the same commandId and If-Match: already applied.
        const replay = yield* handle(
          key,
          { _tag: "Deposit", amount: 5 },
          { commandId: "cmd-1", expectedVersion: 0 },
        ).pipe(Effect.flip)
        expect(replay._tag).toBe("DuplicateCommand")
        expect((replay as DuplicateCommand).commandId).toBe("cmd-1")

        // A different command at the stale version lost a race.
        const stale = yield* handle(
          key,
          { _tag: "Deposit", amount: 5 },
          { commandId: "cmd-2", expectedVersion: 0 },
        ).pipe(Effect.flip)
        expect(stale._tag).toBe("VersionConflict")
        expect((stale as VersionConflict).actualVersion).toBe(1)

        // Neither reached decide, and nothing was written twice.
        expect(esCmdDecideLog.filter((id) => id === "ifm-3")).toHaveLength(1)
        expect(yield* EsCmdAccounts.currentVersion(key, { consistentRead: true })).toBe(1)
      }).pipe(provideEsCmd),
  )

  // -------------------------------------------------------------------------
  // #137 — decision-derived additionalItems
  // -------------------------------------------------------------------------

  it.effect("#137 a decision-derived projection commits atomically with the events", () =>
    Effect.gen(function* () {
      const accountId = "proj-1"
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsCmdAccounts)
      const project = ({ state }: EventStore.Decision<EsCmdState, EsCmdEvent>) => [
        EsCmdTallies.put({ accountId, balance: state.balance, txCount: state.txCount }),
      ]

      yield* handle({ accountId }, { _tag: "Deposit", amount: 50 }, { additionalItems: project })
      const last = yield* handle(
        { accountId },
        { _tag: "Withdraw", amount: 20 },
        { additionalItems: project },
      )

      const tally = yield* EsCmdTallies.get({ accountId })
      expect({ balance: tally.balance, txCount: tally.txCount }).toEqual(last.state)
      expect(last.state).toEqual({ balance: 30, txCount: 2 })
    }).pipe(provideEsCmd),
  )

  it.effect("#137 an Effect-form projection may read before writing", () =>
    Effect.gen(function* () {
      const accountId = "proj-2"
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsCmdAccounts)
      // Reads the current projection row (absent on the first command) and
      // guards the write on it — an optimistic projection.
      const project = ({ state, version }: EventStore.Decision<EsCmdState, EsCmdEvent>) =>
        Effect.gen(function* () {
          const current = yield* EsCmdTallies.get({ accountId }).asEffect().pipe(Effect.option)
          expect(Option.map(current, (t) => t.txCount)).toEqual(
            version === 0 ? Option.none() : Option.some(version),
          )
          return [EsCmdTallies.put({ accountId, balance: state.balance, txCount: state.txCount })]
        })

      yield* handle({ accountId }, { _tag: "Deposit", amount: 5 }, { additionalItems: project })
      yield* handle({ accountId }, { _tag: "Deposit", amount: 6 }, { additionalItems: project })

      const tally = yield* EsCmdTallies.get({ accountId })
      expect(tally.balance).toBe(11)
      expect(tally.txCount).toBe(2)
    }).pipe(provideEsCmd),
  )

  it.effect("#137 a failing projection condition leaves no events behind", () =>
    Effect.gen(function* () {
      const accountId = "proj-3"
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsCmdAccounts)
      yield* handle(
        { accountId },
        { _tag: "Deposit", amount: 40 },
        {
          additionalItems: ({ state }) => [
            EsCmdTallies.put({ accountId, balance: state.balance, txCount: state.txCount }),
          ],
        },
      )

      const error = yield* handle(
        { accountId },
        { _tag: "Withdraw", amount: 15 },
        {
          additionalItems: ({ state }) => [
            EsCmdTallies.put({ accountId, balance: state.balance, txCount: state.txCount }),
            // No guard row exists — this check cannot hold.
            Transaction.check(
              EsCmdTallies.get({ accountId: "proj-3-guard" }),
              Expression.condition({ attributeExists: "pk" }),
            ),
          ],
        },
      ).pipe(Effect.flip)

      expect(error._tag).toBe("AdditionalItemConditionFailed")
      expect((error as AdditionalItemConditionFailed).indices).toEqual([1])

      // All-or-nothing: neither the event nor the projection moved.
      const events = yield* EsCmdAccounts.read({ accountId }, { consistentRead: true })
      expect(events.map((e) => e.version)).toEqual([1])
      const tally = yield* EsCmdTallies.get({ accountId })
      expect(tally.balance).toBe(40)
      expect(tally.txCount).toBe(1)
    }).pipe(provideEsCmd),
  )

  it.effect("#137 retried commands re-derive the projection from the fresh decision", () =>
    Effect.gen(function* () {
      const accountId = "proj-4"
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsCmdAccounts, {
        retry: 10,
      })
      const project = ({ state }: EventStore.Decision<EsCmdState, EsCmdEvent>) => [
        EsCmdTallies.put({ accountId, balance: state.balance, txCount: state.txCount }),
      ]

      yield* Effect.all(
        [10, 20, 30, 40].map((amount) =>
          handle({ accountId }, { _tag: "Deposit", amount }, { additionalItems: project }),
        ),
        { concurrency: 4 },
      )

      // Each commit's projection is the fold through its own version, and the
      // last commit is the highest version, so the row equals the full fold —
      // a projection derived from a stale (pre-retry) decision would not.
      const events = yield* EsCmdAccounts.read({ accountId }, { consistentRead: true })
      expect(events.map((e) => e.version)).toEqual([1, 2, 3, 4])
      const tally = yield* EsCmdTallies.get({ accountId })
      expect({ balance: tally.balance, txCount: tally.txCount }).toEqual(
        EventStore.fold(esCmdDecider(accountId), events),
      )
      expect(tally.balance).toBe(100)
    }).pipe(provideEsCmd),
  )

  it.effect("#137 a bound handler needs only the projection's own services", () =>
    Effect.gen(function* () {
      const bound = yield* EventStore.bind(EsCmdAccounts)
      const handle = bound.pipe(EventStore.commandHandler(esCmdDecider("proj-5")))

      const program = handle(
        { accountId: "proj-5" },
        { _tag: "Deposit", amount: 9 },
        {
          additionalItems: ({ state }) =>
            Effect.gen(function* () {
              const { accountId } = yield* EsCmdOwner
              return [
                EsCmdTallies.put({ accountId, balance: state.balance, txCount: state.txCount }),
              ]
            }),
        },
      )
      // R is exactly the projection's service — the stream's are bound.
      const provided: Effect.Effect<unknown, unknown, never> = program.pipe(
        Effect.provideService(EsCmdOwner, { accountId: "proj-5" }),
      )
      yield* provided

      const tally = yield* EsCmdTallies.get({ accountId: "proj-5" })
      expect(tally.balance).toBe(9)
    }).pipe(provideEsCmd),
  )
})

// ===========================================================================
// EventStore inline snapshots + single-request state load (#138), and large
// commands as stepped commands (each one atomic append)
// ===========================================================================

const esLatSchema = DynamoSchema.make({ name: "es-lat", version: 1 })
const esLatTableName = `es-lat-${Date.now()}`

/** Inline projection carried by each step of a stepped command. */
class EsLatTally extends Schema.Class<EsLatTally>("EsLatTally")({
  accountId: Schema.String,
  balance: Schema.Number,
  txCount: Schema.Number,
}) {}

const EsLatTallies = Entity.make({
  model: EsLatTally,
  entityType: "EsLatTally",
  primaryKey: {
    pk: { field: "pk", composite: ["accountId"] },
    sk: { field: "sk", composite: [] },
  },
})

const EsLatTable = Table.make({ schema: esLatSchema, entities: { EsLatTallies } })

const EsLatState = Schema.Struct({ balance: Schema.Number, txCount: Schema.Number })

const esLatStream = (
  streamName: string,
  snapshot?: { readonly mode?: "inline" | "after-append"; readonly every?: number },
) =>
  EventStore.makeStream({
    table: EsLatTable,
    streamName,
    events: [EsCmdDeposited, EsCmdWithdrew],
    streamId: { composite: ["accountId"] },
    snapshot: { schema: EsLatState, ...snapshot },
  })

/** Inline snapshot on every append. */
const EsLedger = esLatStream("Ledger", { mode: "inline" })
/** Inline snapshot every 5 events. */
const EsBatched = esLatStream("Batched", { mode: "inline", every: 5 })
/** After-append snapshots every 3 events — later switched to inline. */
const EsLegacy = esLatStream("Legacy", { every: 3 })
const EsLegacyInline = esLatStream("Legacy", { mode: "inline" })
/** Stepped commands, with inline snapshots. */
const EsBulk = esLatStream("Bulk", { mode: "inline" })
/** No snapshot config at all. */
const EsPlain = EventStore.makeStream({
  table: EsLatTable,
  streamName: "Plain",
  events: [EsCmdDeposited, EsCmdWithdrew],
  streamId: { composite: ["accountId"] },
})

/** No snapshot config; events carry (large) metadata. */
const EsPlainMeta = EventStore.makeStream({
  table: EsLatTable,
  streamName: "PlainMeta",
  events: [EsCmdDeposited, EsCmdWithdrew],
  streamId: { composite: ["accountId"] },
  metadata: Schema.Struct({ note: Schema.String }),
})

/** A decider that deposits `amount` one unit at a time — a command of `amount` events. */
const esBulkDecider: EventStore.Decider<EsCmdState, number, EsCmdEvent> = {
  initialState: { balance: 0, txCount: 0 },
  decide: (count) =>
    Effect.succeed(Array.from({ length: count }, () => new EsCmdDeposited({ amount: 1 }))),
  evolve: (state, event) =>
    event instanceof EsCmdDeposited
      ? { balance: state.balance + event.amount, txCount: state.txCount + 1 }
      : { balance: state.balance - event.amount, txCount: state.txCount + 1 },
}

/** Every request this suite sends, with the partition it targets. */
const esLatLog: Array<{
  readonly op: "query" | "transactWriteItems" | "putItem" | "getItem"
  readonly pk: string | undefined
  readonly items?: ReadonlyArray<Record<string, unknown>>
  /** Items a `query` read (its `ScannedCount`), recorded when it returns. */
  scanned?: number | undefined
}> = []

/** The stream partition a transact item touches. */
const transactPk = (item: Record<string, any>): string | undefined => {
  const entry = item.Put?.Item ?? item.ConditionCheck?.Key ?? item.Delete?.Key ?? item.Update?.Key
  return entry?.pk?.S
}

const EsLatClientLayer = Layer.effect(
  DynamoClient,
  Effect.map(
    DynamoClient,
    (client): DynamoClientService => ({
      ...client,
      query: (input) => {
        const entry: (typeof esLatLog)[number] = {
          op: "query",
          pk: input.ExpressionAttributeValues?.[":pk"]?.S,
        }
        esLatLog.push(entry)
        return client.query(input).pipe(
          Effect.tap((output) =>
            Effect.sync(() => {
              entry.scanned = output.ScannedCount
            }),
          ),
        )
      },
      getItem: (input) => {
        esLatLog.push({ op: "getItem", pk: input.Key?.pk?.S })
        return client.getItem(input)
      },
      putItem: (input) => {
        esLatLog.push({ op: "putItem", pk: input.Item?.pk?.S })
        return client.putItem(input)
      },
      transactWriteItems: (input) => {
        const items = (input.TransactItems ?? []) as unknown as ReadonlyArray<
          Record<string, unknown>
        >
        esLatLog.push({
          op: "transactWriteItems",
          pk: items.map(transactPk).find((pk) => pk?.includes("#v1#") === true),
          items,
        })
        return client.transactWriteItems(input)
      },
    }),
  ),
).pipe(Layer.provide(ClientLayer))

const EsLatLayer = Layer.mergeAll(EsLatClientLayer, EsLatTable.layer({ name: esLatTableName }))
const provideEsLat = Effect.provide(EsLatLayer)

/** The requests sent to one stream's partition (`label` is the lower-cased stream name). */
const esLatRequests = (label: string, accountId: string) =>
  esLatLog.filter((entry) => entry.pk === `$es-lat#v1#${label}#${accountId}`)

/** A command's idempotency sentinel in the "Bulk" stream, read back raw. */
const esLatSentinel = (accountId: string, commandId: string) =>
  Effect.gen(function* () {
    const client = yield* DynamoClient
    const result = yield* client.getItem({
      TableName: esLatTableName,
      Key: toAttributeMap({
        pk: `$es-lat#v1#bulk#${accountId}`,
        sk: DynamoSchema.composeKey(esLatSchema, "bulk.command", [commandId]),
      }),
      ConsistentRead: true,
    })
    return result.Item === undefined ? undefined : fromAttributeMap(result.Item)
  })

const esLatDeposits = (n: number) =>
  Array.from({ length: n }, () => new EsCmdDeposited({ amount: 1 }))

/**
 * A `Schema.Class` state folded by an immutable `evolve` that spreads — the
 * fold yields a plain object the class encoder alone refuses.
 */
class EsLatClassState extends Schema.Class<EsLatClassState>("EsLatClassState")({
  balance: Schema.Number,
  txCount: Schema.Number,
}) {}

const esLatClassDecider: EventStore.Decider<EsLatClassState, number, EsCmdEvent> = {
  initialState: new EsLatClassState({ balance: 0, txCount: 0 }),
  decide: (amount) => Effect.succeed([new EsCmdDeposited({ amount })]),
  evolve: (state, event) => ({
    ...state,
    balance: state.balance + (event instanceof EsCmdDeposited ? event.amount : -event.amount),
    txCount: state.txCount + 1,
  }),
}

const esLatClassStream = (streamName: string, mode: "inline" | "after-append") =>
  EventStore.makeStream({
    table: EsLatTable,
    streamName,
    events: [EsCmdDeposited, EsCmdWithdrew],
    streamId: { composite: ["accountId"] },
    snapshot: { schema: EsLatClassState, mode, every: 1 },
  })

const EsClassInline = esLatClassStream("ClassInline", "inline")
const EsClassAfter = esLatClassStream("ClassAfter", "after-append")

describeConnected("EventStore inline snapshots and stepped commands (#138)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: esLatTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(EsLatTable),
        })
      }).pipe(provideEsLat, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: esLatTableName })
      }).pipe(
        provideEsLat,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // -------------------------------------------------------------------------
  // #138 — inline snapshots + readLatest
  // -------------------------------------------------------------------------

  it.effect("#138 an inline snapshot is current after every command", () =>
    Effect.gen(function* () {
      const accountId = "inline-1"
      const key = { accountId }
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsLedger)

      const commands: ReadonlyArray<EsCmdCommand> = [
        { _tag: "Deposit", amount: 40 },
        { _tag: "Withdraw", amount: 15 },
        { _tag: "Deposit", amount: 5 },
      ]
      for (const command of commands) {
        const result = yield* handle(key, command)
        const snapshot = Option.getOrThrow(yield* EsLedger.readSnapshot(key))
        expect(snapshot.asOfVersion).toBe(result.version)
        expect(snapshot.state).toEqual(result.state)
      }
      expect(Option.getOrThrow(yield* EsLedger.readSnapshot(key)).state).toEqual({
        balance: 30,
        txCount: 3,
      })

      // The snapshot rode in each append transaction: no separate write, and
      // each command was one load plus one transaction.
      const requests = esLatRequests("ledger", accountId)
      expect(requests.filter((r) => r.op === "putItem")).toHaveLength(0)
      expect(requests.filter((r) => r.op === "transactWriteItems")).toHaveLength(3)
      expect(requests.filter((r) => r.op === "query")).toHaveLength(3)
    }).pipe(provideEsLat),
  )

  it.effect("#138 readLatest loads state in one request in steady state", () =>
    Effect.gen(function* () {
      const accountId = "inline-2"
      const key = { accountId }
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsLedger)
      for (let i = 0; i < 6; i++) yield* handle(key, { _tag: "Deposit", amount: 1 })

      const before = esLatRequests("ledger", accountId).length
      const latest = yield* EsLedger.readLatest(key, { consistentRead: true })
      const after = esLatRequests("ledger", accountId).slice(before)
      expect(after.map((r) => r.op)).toEqual(["query"])
      expect(Option.getOrThrow(latest.snapshot).asOfVersion).toBe(6)
      expect(latest.events).toEqual([])
      expect(latest.version).toBe(6)

      // A command loads in one request too, then writes in one transaction.
      const mark = esLatRequests("ledger", accountId).length
      const result = yield* handle(key, { _tag: "Withdraw", amount: 2 })
      expect(result.state).toEqual({ balance: 4, txCount: 7 })
      expect(
        esLatRequests("ledger", accountId)
          .slice(mark)
          .map((r) => r.op),
      ).toEqual(["query", "transactWriteItems"])

      // Bound streams expose it with R = never.
      const bound = yield* EventStore.bind(EsLedger)
      expect((yield* bound.readLatest(key)).version).toBe(7)
    }).pipe(provideEsLat),
  )

  it.effect("#138 falls back when the snapshot is missing", () =>
    Effect.gen(function* () {
      const accountId = "missing-1"
      const key = { accountId }
      // Events written without any snapshot.
      yield* EsLedger.append(key, esLatDeposits(4), 0)

      const latest = yield* EsLedger.readLatest(key, { consistentRead: true })
      expect(Option.isNone(latest.snapshot)).toBe(true)
      expect(latest.events.map((e) => e.version)).toEqual([1, 2, 3, 4])
      expect(latest.version).toBe(4)

      // The handler folds the full stream, then writes the snapshot inline.
      const result = yield* EventStore.commandHandler(esCmdDecider(accountId), EsLedger)(key, {
        _tag: "Deposit",
        amount: 10,
      })
      expect(result).toMatchObject({ version: 5, state: { balance: 14, txCount: 5 } })
      const snapshot = Option.getOrThrow(yield* EsLedger.readSnapshot(key))
      expect(snapshot).toMatchObject({ asOfVersion: 5, state: { balance: 14, txCount: 5 } })
    }).pipe(provideEsLat),
  )

  it.effect("#138 a Schema.Class state from a spreading evolve snapshots in both modes", () =>
    Effect.gen(function* () {
      const key = { accountId: "class-1" }
      for (const stream of [EsClassInline, EsClassAfter]) {
        const handle = EventStore.commandHandler(esLatClassDecider, stream)
        yield* handle(key, 30)
        const result = yield* handle(key, 12)
        expect(result).toMatchObject({ version: 2, state: { balance: 42, txCount: 2 } })

        // The snapshot was written, and decodes back into the class.
        const snapshot = Option.getOrThrow(yield* stream.readSnapshot(key))
        expect(snapshot.asOfVersion).toBe(2)
        expect(snapshot.state).toBeInstanceOf(EsLatClassState)
        expect(snapshot.state).toMatchObject({ balance: 42, txCount: 2 })

        // The next command folds from it.
        const next = yield* handle(key, 8)
        expect(next).toMatchObject({ version: 3, state: { balance: 50, txCount: 3 } })
      }
    }).pipe(provideEsLat),
  )

  it.effect("#138 a lagging snapshot loads within the first page, or one page more", () =>
    Effect.gen(function* () {
      const accountId = "lag-1"
      const key = { accountId }
      // Snapshot at v2, head at v12: ten events behind, past `every: 5`.
      yield* EsBatched.append(key, esLatDeposits(2), 0)
      yield* EsBatched.writeSnapshot(key, { balance: 2, txCount: 2 }, 2)
      yield* EsBatched.append(key, esLatDeposits(10), 2)

      const mark = esLatRequests("batched", accountId).length
      const lagging = yield* EsBatched.readLatest(key, { consistentRead: true })
      expect(esLatRequests("batched", accountId).slice(mark)).toHaveLength(2)
      expect(Option.getOrThrow(lagging.snapshot).asOfVersion).toBe(2)
      expect(lagging.events.map((e) => e.version)).toEqual([3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
      expect(lagging.version).toBe(12)

      // The handler folds snapshot + delta, and — 13 - 2 >= 5 — snapshots inline.
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsBatched)
      const result = yield* handle(key, { _tag: "Deposit", amount: 1 })
      expect(result).toMatchObject({ version: 13, state: { balance: 13, txCount: 13 } })
      expect(Option.getOrThrow(yield* EsBatched.readSnapshot(key)).asOfVersion).toBe(13)

      // Lagging by less than `every`: still one request.
      yield* handle(key, { _tag: "Deposit", amount: 1 })
      yield* handle(key, { _tag: "Deposit", amount: 1 })
      const mark2 = esLatRequests("batched", accountId).length
      const within = yield* EsBatched.readLatest(key, { consistentRead: true })
      expect(esLatRequests("batched", accountId).slice(mark2)).toHaveLength(1)
      expect(within.events.map((e) => e.version)).toEqual([14, 15])
      expect(within.version).toBe(15)

      // Lagging by exactly `every`: the first page holds the snapshot and every
      // missing event, so it is still one request.
      const exact = { accountId: "lag-2" }
      yield* EsBatched.append(exact, esLatDeposits(3), 0)
      yield* EsBatched.writeSnapshot(exact, { balance: 3, txCount: 3 }, 3)
      yield* EsBatched.append(exact, esLatDeposits(5), 3)
      const mark3 = esLatRequests("batched", "lag-2").length
      const boundary = yield* EsBatched.readLatest(exact, { consistentRead: true })
      expect(esLatRequests("batched", "lag-2").slice(mark3)).toHaveLength(1)
      expect(boundary.events.map((e) => e.version)).toEqual([4, 5, 6, 7, 8])
      expect(boundary.version).toBe(8)
    }).pipe(provideEsLat),
  )

  it.effect("#138 a pre-existing after-append stream switched to inline", () =>
    Effect.gen(function* () {
      const accountId = "switch-1"
      const key = { accountId }
      const legacy = EventStore.commandHandler(esCmdDecider(accountId), EsLegacy)
      for (let i = 0; i < 4; i++) yield* legacy(key, { _tag: "Deposit", amount: 10 })
      // after-append, every 3: the snapshot was written by a separate put at v3.
      expect(Option.getOrThrow(yield* EsLegacy.readSnapshot(key)).asOfVersion).toBe(3)
      expect(esLatRequests("legacy", accountId).filter((r) => r.op === "putItem")).toHaveLength(1)

      // Same stream, now inline: the first command catches the lag up...
      const inline = EventStore.commandHandler(esCmdDecider(accountId), EsLegacyInline)
      const first = yield* inline(key, { _tag: "Withdraw", amount: 5 })
      expect(first).toMatchObject({ version: 5, state: { balance: 35, txCount: 5 } })
      expect(Option.getOrThrow(yield* EsLegacyInline.readSnapshot(key)).asOfVersion).toBe(5)

      // ...and from then on a command is one load and one transaction.
      const mark = esLatRequests("legacy", accountId).length
      const second = yield* inline(key, { _tag: "Deposit", amount: 1 })
      expect(second.state).toEqual({ balance: 36, txCount: 6 })
      expect(
        esLatRequests("legacy", accountId)
          .slice(mark)
          .map((r) => r.op),
      ).toEqual(["query", "transactWriteItems"])
    }).pipe(provideEsLat),
  )

  // -------------------------------------------------------------------------
  // verifySnapshot: false — the inline snapshot alone, with verified fallbacks
  // -------------------------------------------------------------------------

  /** The ops sent to a "ledger" partition since `mark`. */
  const ledgerOpsSince = (accountId: string, mark: number) =>
    esLatRequests("ledger", accountId)
      .slice(mark)
      .map((r) => r.op)
  const decisions = (accountId: string) => esCmdDecideLog.filter((id) => id === accountId).length

  it.effect("verifySnapshot: false loads a command's state with one GetItem", () =>
    Effect.gen(function* () {
      const accountId = "unverified-1"
      const key = { accountId }
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsLedger, {
        verifySnapshot: false,
      })
      // The first command finds no snapshot and falls back to the verified load.
      yield* handle(key, { _tag: "Deposit", amount: 10 })
      expect(ledgerOpsSince(accountId, 0)).toEqual(["getItem", "query", "transactWriteItems"])

      // From then on: one GetItem of the snapshot, one transaction.
      for (let i = 0; i < 3; i++) {
        const mark = esLatRequests("ledger", accountId).length
        yield* handle(key, { _tag: "Deposit", amount: 10 })
        expect(ledgerOpsSince(accountId, mark)).toEqual(["getItem", "transactWriteItems"])
      }
      const snapshot = Option.getOrThrow(yield* EsLedger.readSnapshot(key))
      expect(snapshot).toMatchObject({ asOfVersion: 4, state: { balance: 40, txCount: 4 } })

      // readLatest: the snapshot alone, its version unverified.
      const mark = esLatRequests("ledger", accountId).length
      const latest = yield* EsLedger.readLatest(key, {
        verifySnapshot: false,
        consistentRead: true,
      })
      expect(ledgerOpsSince(accountId, mark)).toEqual(["getItem"])
      expect(latest.events).toEqual([])
      expect(latest.version).toBe(4)
      expect(Option.getOrThrow(latest.snapshot).state).toEqual({ balance: 40, txCount: 4 })

      // Bound: the same single GetItem.
      const bound = yield* EventStore.bind(EsLedger)
      const boundMark = esLatRequests("ledger", accountId).length
      const result = yield* bound.pipe(
        EventStore.commandHandler(esCmdDecider(accountId), { verifySnapshot: false }),
      )(key, { _tag: "Withdraw", amount: 5 })
      expect(result).toMatchObject({ version: 5, state: { balance: 35, txCount: 5 } })
      expect(ledgerOpsSince(accountId, boundMark)).toEqual(["getItem", "transactWriteItems"])
    }).pipe(provideEsLat),
  )

  it.effect(
    "verifySnapshot: false re-decides once on a stale snapshot, without a retry policy",
    () =>
      Effect.gen(function* () {
        const accountId = "unverified-stale"
        const key = { accountId }
        const handle = EventStore.commandHandler(esCmdDecider(accountId), EsLedger, {
          verifySnapshot: false,
        })
        yield* handle(key, { _tag: "Deposit", amount: 10 })
        yield* handle(key, { _tag: "Deposit", amount: 10 })
        // Three events appended raw, without a snapshot: snapshot v2, head v5.
        yield* EsLedger.append(key, esLatDeposits(3), 2)

        const before = decisions(accountId)
        const mark = esLatRequests("ledger", accountId).length
        const result = yield* handle(key, { _tag: "Withdraw", amount: 4 })

        // Decided on the stale snapshot, conflicted, re-read verified (a snapshot
        // lagging past the verified read's first page costs one page more),
        // decided again.
        expect(decisions(accountId) - before).toBe(2)
        expect(ledgerOpsSince(accountId, mark)).toEqual([
          "getItem",
          "transactWriteItems",
          "query",
          "query",
          "transactWriteItems",
        ])
        expect(result).toMatchObject({ version: 6, state: { balance: 19, txCount: 6 } })
        // The successful inline append repaired the snapshot.
        const snapshot = Option.getOrThrow(yield* EsLedger.readSnapshot(key))
        expect(snapshot).toMatchObject({ asOfVersion: 6, state: { balance: 19, txCount: 6 } })
        expect((yield* EsLedger.read(key, { consistentRead: true })).map((e) => e.version)).toEqual(
          [1, 2, 3, 4, 5, 6],
        )
      }).pipe(provideEsLat),
  )

  it.effect("verifySnapshot: false surfaces a genuine race, without a retry policy", () =>
    Effect.gen(function* () {
      const accountId = "unverified-race"
      const key = { accountId }
      const bound = yield* EventStore.bind(EsLedger)
      const rival = EventStore.commandHandler(esCmdDecider(accountId), bound)
      yield* rival(key, { _tag: "Deposit", amount: 10 })

      // A decider that lets a rival handler commit between its load and its
      // append — the rival writes its inline snapshot, so the head is snapshotted.
      let raced = false
      const base = esCmdDecider(accountId)
      const racing: typeof base = {
        ...base,
        decide: (command, state) =>
          Effect.gen(function* () {
            if (!raced) {
              raced = true
              yield* rival(key, { _tag: "Deposit", amount: 1 }).pipe(Effect.orDie)
            }
            return yield* base.decide(command, state)
          }),
      }

      const before = decisions(accountId)
      const error = yield* EventStore.commandHandler(racing, EsLedger, { verifySnapshot: false })(
        key,
        { _tag: "Deposit", amount: 5 },
      ).pipe(Effect.flip)

      expect(error._tag).toBe("VersionConflict")
      // Our decide plus the rival's: no re-decision on a genuine race.
      expect(decisions(accountId) - before).toBe(2)
      const latest = yield* EsLedger.readLatest(key, { consistentRead: true })
      expect(latest.version).toBe(2)
      expect(Option.getOrThrow(latest.snapshot).state).toEqual({ balance: 11, txCount: 2 })
    }).pipe(provideEsLat),
  )

  it.effect("verifySnapshot: false re-reads a stale snapshot before the If-Match check", () =>
    Effect.gen(function* () {
      const accountId = "unverified-ifm"
      const key = { accountId }
      const handle = EventStore.commandHandler(esCmdDecider(accountId), EsLedger, {
        verifySnapshot: false,
      })
      yield* handle(key, { _tag: "Deposit", amount: 10 })
      yield* handle(key, { _tag: "Deposit", amount: 10 })
      yield* EsLedger.append(key, esLatDeposits(3), 2)

      // A genuine mismatch: the actual version is the verified head, not the snapshot's.
      const before = decisions(accountId)
      const error = yield* handle(key, { _tag: "Deposit", amount: 1 }, { expectedVersion: 3 }).pipe(
        Effect.flip,
      )
      expect(error._tag).toBe("VersionConflict")
      expect((error as VersionConflict).actualVersion).toBe(5)
      expect(decisions(accountId) - before).toBe(0)

      // The caller's version matches the verified head: decided once, applied.
      const mark = esLatRequests("ledger", accountId).length
      const result = yield* handle(key, { _tag: "Deposit", amount: 1 }, { expectedVersion: 5 })
      expect(result).toMatchObject({ version: 6, state: { balance: 24, txCount: 6 } })
      expect(decisions(accountId) - before).toBe(1)
      // The verified read of the lagging snapshot is two pages (see above).
      expect(ledgerOpsSince(accountId, mark)).toEqual([
        "getItem",
        "query",
        "query",
        "transactWriteItems",
      ])
    }).pipe(provideEsLat),
  )

  it.effect(
    "verifySnapshot: false confirms a domain error or no-op against the head, re-deciding on a stale snapshot",
    () =>
      Effect.gen(function* () {
        const accountId = "unverified-noappend"
        const key = { accountId }
        const base = esCmdDecider(accountId)
        // A withdrawal the balance cannot cover is refused by `base`, and a
        // no-op by `lenient`.
        const lenient: typeof base = {
          ...base,
          decide: (command, state) =>
            command._tag === "Withdraw" && state.balance < command.amount
              ? Effect.sync(() => {
                  esCmdDecideLog.push(accountId)
                  return []
                })
              : base.decide(command, state),
        }
        const handle = EventStore.commandHandler(base, EsLedger, { verifySnapshot: false })
        const lenientHandle = EventStore.commandHandler(lenient, EsLedger, {
          verifySnapshot: false,
        })
        yield* handle(key, { _tag: "Deposit", amount: 10 })

        // The snapshot is at the head: the refusal and the no-op stand, each
        // confirmed by one head check.
        let mark = esLatRequests("ledger", accountId).length
        const refused = yield* handle(key, { _tag: "Withdraw", amount: 100 }).pipe(Effect.flip)
        expect(refused._tag).toBe("EsCmdInsufficientFunds")
        expect(ledgerOpsSince(accountId, mark)).toEqual(["getItem", "query"])
        // The head check is bounded above the snapshot, so a current snapshot
        // costs a Query that reads no items — never the newest event.
        expect(esLatRequests("ledger", accountId)[mark + 1]!.scanned).toBe(0)
        mark = esLatRequests("ledger", accountId).length
        const noop = yield* lenientHandle(key, { _tag: "Withdraw", amount: 100 })
        expect(noop).toEqual({ state: { balance: 10, txCount: 1 }, version: 1, events: [] })
        expect(ledgerOpsSince(accountId, mark)).toEqual(["getItem", "query"])
        expect(esLatRequests("ledger", accountId)[mark + 1]!.scanned).toBe(0)

        // 500 deposited raw, without a snapshot: snapshot v1 (balance 10), head v2.
        yield* EsLedger.append(key, [new EsCmdDeposited({ amount: 500 })], 1)

        // The refusal made on the stale snapshot is decided again, and applied.
        let before = decisions(accountId)
        mark = esLatRequests("ledger", accountId).length
        const withdrawn = yield* handle(key, { _tag: "Withdraw", amount: 100 })
        expect(withdrawn).toMatchObject({ version: 3, state: { balance: 410, txCount: 3 } })
        expect(decisions(accountId) - before).toBe(2)
        expect(ledgerOpsSince(accountId, mark)).toEqual([
          "getItem",
          "query",
          "query",
          "transactWriteItems",
        ])
        // On a stale snapshot the head check reads exactly the head event.
        expect(esLatRequests("ledger", accountId)[mark + 1]!.scanned).toBe(1)

        // The same for a no-op made on a stale snapshot.
        yield* EsLedger.append(key, [new EsCmdDeposited({ amount: 500 })], 3)
        before = decisions(accountId)
        const applied = yield* lenientHandle(key, { _tag: "Withdraw", amount: 800 })
        expect(applied).toMatchObject({ version: 5, state: { balance: 110, txCount: 5 } })
        expect(decisions(accountId) - before).toBe(2)
        expect(Option.getOrThrow(yield* EsLedger.readSnapshot(key)).asOfVersion).toBe(5)
      }).pipe(provideEsLat),
  )

  it.effect(
    "verifySnapshot: false answers an If-Match at a lagging snapshot as the verified load does",
    () =>
      Effect.gen(function* () {
        const accountId = "unverified-ifm-lag"
        const key = { accountId }
        const base = esCmdDecider(accountId)
        const lenient: typeof base = {
          ...base,
          decide: (command, state) =>
            command._tag === "Withdraw" && state.balance < command.amount
              ? Effect.succeed([])
              : base.decide(command, state),
        }
        const unverified = EventStore.commandHandler(base, EsLedger, { verifySnapshot: false })
        const verified = EventStore.commandHandler(base, EsLedger)
        const lenientUnverified = EventStore.commandHandler(lenient, EsLedger, {
          verifySnapshot: false,
        })
        const lenientVerified = EventStore.commandHandler(lenient, EsLedger)
        yield* unverified(key, { _tag: "Deposit", amount: 10 })
        // Snapshot v1, head v2: the caller's If-Match is the snapshot's version.
        yield* EsLedger.append(key, [new EsCmdDeposited({ amount: 500 })], 1)

        const cases = [
          [unverified, verified, { _tag: "Deposit", amount: 1 }],
          [unverified, verified, { _tag: "Withdraw", amount: 100 }],
          [lenientUnverified, lenientVerified, { _tag: "Withdraw", amount: 100 }],
        ] as const
        for (const [lean, full, command] of cases) {
          const leanError = yield* lean(key, command, { expectedVersion: 1 }).pipe(Effect.flip)
          const fullError = yield* full(key, command, { expectedVersion: 1 }).pipe(Effect.flip)
          expect(leanError).toMatchObject({
            _tag: "VersionConflict",
            expectedVersion: 1,
            actualVersion: 2,
          })
          expect(leanError).toEqual(fullError)
        }
        expect(yield* EsLedger.currentVersion(key, { consistentRead: true })).toBe(2)
      }).pipe(provideEsLat),
  )

  it.effect(
    "verifySnapshot: false falls back to the verified load when the snapshot is missing",
    () =>
      Effect.gen(function* () {
        const accountId = "unverified-missing"
        const key = { accountId }
        // Events written before inline snapshots: no snapshot item at all.
        yield* EsLedger.append(key, esLatDeposits(4), 0)

        const latest = yield* EsLedger.readLatest(key, {
          verifySnapshot: false,
          consistentRead: true,
        })
        expect(Option.isNone(latest.snapshot)).toBe(true)
        expect(latest.version).toBe(4)

        const before = decisions(accountId)
        const mark = esLatRequests("ledger", accountId).length
        const result = yield* EventStore.commandHandler(esCmdDecider(accountId), EsLedger, {
          verifySnapshot: false,
        })(key, { _tag: "Deposit", amount: 6 })
        expect(result).toMatchObject({ version: 5, state: { balance: 10, txCount: 5 } })
        expect(decisions(accountId) - before).toBe(1)
        // No snapshot: the verified read pages to the start of the stream.
        expect(ledgerOpsSince(accountId, mark)).toEqual([
          "getItem",
          "query",
          "query",
          "transactWriteItems",
        ])
        expect(Option.getOrThrow(yield* EsLedger.readSnapshot(key)).asOfVersion).toBe(5)
      }).pipe(provideEsLat),
  )

  it("verifySnapshot: false is refused unless the stream is inline without `every` (EDD-9068)", () => {
    expect(() =>
      EventStore.commandHandler(esCmdDecider("x"), EsBatched, { verifySnapshot: false }),
    ).toThrow(/EDD-9068/)
    expect(() =>
      EsLegacy.pipe(EventStore.commandHandler(esCmdDecider("x"), { verifySnapshot: false })),
    ).toThrow(/EDD-9068/)
  })

  // -------------------------------------------------------------------------
  // Large commands — stepped commands, one atomic append each
  // -------------------------------------------------------------------------

  it.effect("an append over the item or size limit fails before anything is written", () =>
    Effect.gen(function* () {
      const key = { accountId: "large-1" }
      const error = yield* EsPlain.append(key, esLatDeposits(250), 0).pipe(Effect.flip)
      expect(error._tag).toBe("AppendTooLarge")
      expect((error as AppendTooLarge).count).toBe(250)
      expect((error as AppendTooLarge).limit).toBe(100)
      expect(yield* EsPlain.currentVersion(key, { consistentRead: true })).toBe(0)

      // 30 events of ~300 KB: ~9 MB, well under 100 items but over 4 MB.
      const metadata = { note: "x".repeat(300_000) }
      const events = Array.from({ length: 30 }, () => new EsCmdDeposited({ amount: 1 }))
      const refused = yield* EsPlainMeta.append(key, events, 0, { metadata }).pipe(Effect.flip)
      expect(refused._tag).toBe("ValidationError")
      expect(yield* EsPlainMeta.currentVersion(key, { consistentRead: true })).toBe(0)
      expect(
        esLatLog.filter((r) => r.op === "transactWriteItems" && r.pk?.endsWith("#large-1")),
      ).toEqual([])
    }).pipe(provideEsLat),
  )

  it.effect(
    "a large command runs as stepped commands chained by expectedVersion, each atomic",
    () =>
      Effect.gen(function* () {
        const accountId = "step-1"
        const key = { accountId }
        const handle = EventStore.commandHandler(esBulkDecider, EsBulk, { idempotency: {} })
        const project = ({ state }: EventStore.Decision<EsCmdState, EsCmdEvent>) => [
          EsLatTallies.put({ accountId, balance: state.balance, txCount: state.txCount }),
        ]

        // One decision of 250 events cannot be one atomic append.
        const tooLarge = yield* handle(key, 250, {
          commandId: "bulk-1",
          additionalItems: project,
        }).pipe(Effect.flip)
        expect(tooLarge._tag).toBe("AppendTooLarge")
        expect((tooLarge as AppendTooLarge).count).toBeGreaterThan(100)

        // The application plans fixed-size steps; each is an ordinary command
        // with its own commandId, appended after the version the last returned.
        let version = 0
        for (const [n, size] of [90, 90, 70].entries()) {
          const step = yield* handle(key, size, {
            commandId: `bulk-1#step-${n}`,
            expectedVersion: version,
            additionalItems: project,
          })
          version = step.version
          // Every step leaves a real, consistent state: events, snapshot and
          // projection all at the same version.
          const snapshot = Option.getOrThrow(yield* EsBulk.readSnapshot(key))
          expect(snapshot).toMatchObject({ asOfVersion: version, state: step.state })
          const tally = yield* EsLatTallies.get({ accountId })
          expect({ balance: tally.balance, txCount: tally.txCount }).toEqual(step.state)
          expect(yield* esLatSentinel(accountId, `bulk-1#step-${n}`)).toMatchObject({ version })
        }
        expect(version).toBe(250)
        const transacts = esLatRequests("bulk", accountId).filter(
          (r) => r.op === "transactWriteItems",
        )
        expect(transacts.map((t) => t.items?.length)).toEqual([93, 94, 74])

        // A redelivered step — its response lost — is a duplicate, not a conflict.
        const replay = yield* handle(key, 70, {
          commandId: "bulk-1#step-2",
          expectedVersion: 180,
          additionalItems: project,
        }).pipe(Effect.flip)
        expect(replay._tag).toBe("DuplicateCommand")
        expect(yield* EsBulk.currentVersion(key, { consistentRead: true })).toBe(250)
      }).pipe(provideEsLat),
  )

  it.effect(
    "a stepped command that fails partway stops at a consistent state, and re-issuing continues",
    () =>
      Effect.gen(function* () {
        const accountId = "step-2"
        const key = { accountId }
        const handle = EventStore.commandHandler(esBulkDecider, EsBulk, { idempotency: {} })

        const first = yield* handle(key, 90, { commandId: "bulk-2#step-0", expectedVersion: 0 })
        expect(first.version).toBe(90)
        // Another writer appends between the steps.
        yield* EsBulk.append(key, [new EsCmdWithdrew({ amount: 7 })], 90)

        const conflict = yield* handle(key, 90, {
          commandId: "bulk-2#step-1",
          expectedVersion: first.version,
        }).pipe(Effect.flip)
        expect(conflict._tag).toBe("VersionConflict")
        expect((conflict as VersionConflict).actualVersion).toBe(91)
        // Nothing of step 1 was written: the stream is at the other writer's
        // event, and the snapshot is still step 0's, consistent at its version.
        expect(yield* EsBulk.currentVersion(key, { consistentRead: true })).toBe(91)
        expect(Option.getOrThrow(yield* EsBulk.readSnapshot(key))).toMatchObject({
          asOfVersion: 90,
          state: { balance: 90, txCount: 90 },
        })
        expect(yield* esLatSentinel(accountId, "bulk-2#step-1")).toBeUndefined()

        // Re-issuing from the real state continues the plan.
        const resumed = yield* handle(key, 90, {
          commandId: "bulk-2#step-1",
          expectedVersion: 91,
        })
        expect(resumed).toMatchObject({ version: 181, state: { balance: 173, txCount: 181 } })
      }).pipe(provideEsLat),
  )

  it.effect(
    "a redelivered stepped command skips its committed steps and resumes at the first one not committed",
    () =>
      Effect.gen(function* () {
        const accountId = "step-3"
        const key = { accountId }
        // Step 1's decide fails once, after step 0 committed.
        let failOnce = true
        const flaky: typeof esBulkDecider = {
          ...esBulkDecider,
          decide: (count, state) =>
            state.txCount === 40 && failOnce
              ? Effect.sync(() => {
                  failOnce = false
                  throw new Error("boom")
                })
              : esBulkDecider.decide(count, state),
        }
        const handle = EventStore.commandHandler(flaky, EsBulk, { idempotency: {} })

        // The tutorial's helper: a step committed by an earlier delivery is a
        // DuplicateCommand (the stale If-Match makes the handler consult its
        // sentinel before deciding), so it is skipped with the If-Match kept
        // stale; the first step not committed is a VersionConflict carrying the
        // head, and runs from there.
        const stepped = (
          sizes: ReadonlyArray<number>,
          commandId: string,
          expectedVersion: number,
        ) =>
          Effect.gen(function* () {
            let version = expectedVersion
            let skipping = false
            for (const [n, size] of sizes.entries()) {
              const run = (expected: number) =>
                handle(key, size, {
                  commandId: `${commandId}#step-${n}`,
                  expectedVersion: expected,
                })
              const outcome = yield* run(version).pipe(
                Effect.map((done) => done.version),
                Effect.catchTag("DuplicateCommand", () => Effect.succeed(undefined)),
                Effect.catchTag("VersionConflict", (conflict) =>
                  skipping && conflict.actualVersion !== undefined
                    ? run(conflict.actualVersion).pipe(Effect.map((done) => done.version))
                    : Effect.fail(conflict),
                ),
              )
              skipping = outcome === undefined
              if (outcome !== undefined) version = outcome
            }
            return skipping ? yield* EsBulk.currentVersion(key, { consistentRead: true }) : version
          })

        const failed = yield* stepped([40, 40, 40], "bulk-3", 0).pipe(Effect.exit)
        expect(failed._tag).toBe("Failure")
        expect(yield* EsBulk.currentVersion(key, { consistentRead: true })).toBe(40)

        // Redelivered with its original commandId and If-Match: step 0 is
        // skipped, steps 1 and 2 run.
        const before = esLatRequests("bulk", accountId).filter(
          (r) => r.op === "transactWriteItems",
        ).length
        expect(yield* stepped([40, 40, 40], "bulk-3", 0)).toBe(120)
        expect(
          esLatRequests("bulk", accountId).filter((r) => r.op === "transactWriteItems").length -
            before,
        ).toBe(2)
        for (const n of [0, 1, 2]) {
          expect(yield* esLatSentinel(accountId, `bulk-3#step-${n}`)).toMatchObject({
            version: 40 * (n + 1),
          })
        }

        // Redelivered again: every step is skipped, nothing is written.
        expect(yield* stepped([40, 40, 40], "bulk-3", 0)).toBe(120)
        expect(
          esLatRequests("bulk", accountId).filter((r) => r.op === "transactWriteItems").length -
            before,
        ).toBe(2)
        expect(Option.getOrThrow(yield* EsBulk.readSnapshot(key))).toMatchObject({
          asOfVersion: 120,
          state: { balance: 120, txCount: 120 },
        })
      }).pipe(provideEsLat),
  )
})

// ===========================================================================
// EventStore stream indexes — sub-streams by derived key (#140)
// ===========================================================================

const esIdxSchema = DynamoSchema.make({ name: "es-idx", version: 1 })
const esIdxTableName = `es-idx-${Date.now()}`
const EsIdxTable = Table.make({ schema: esIdxSchema, entities: {} })

class EsIdxRecorded extends Schema.TaggedClass<EsIdxRecorded>()("EsIdxRecorded", {
  section: Schema.Number,
  item: Schema.Number,
}) {}

class EsIdxNoted extends Schema.TaggedClass<EsIdxNoted>()("EsIdxNoted", {
  day: Schema.String,
}) {}

type EsIdxEvent = EsIdxRecorded | EsIdxNoted

const esIdxPad = (n: number) => String(n).padStart(4, "0")

const esIdxEntryKey = (event: EsIdxEvent, version: number) =>
  event._tag === "EsIdxRecorded"
    ? `ENTRY#${event.section}-${esIdxPad(event.item)}-${esIdxPad(version)}`
    : undefined

/**
 * Recorded entries on an LSI (`byEntry`), notes on a GSI (`byDay`) — each
 * index sparse with respect to the other event type. Inline snapshots and
 * idempotency sentinels share the partition and must stay out of both.
 */
const EsIdxLedger = EventStore.makeStream({
  table: EsIdxTable,
  streamName: "Ledger",
  events: [EsIdxRecorded, EsIdxNoted],
  streamId: { composite: ["ledgerId"] },
  snapshot: { schema: Schema.Struct({ entries: Schema.Number }), mode: "inline" },
  indexes: {
    byEntry: { index: "lsi1", sk: "lsi1sk", key: esIdxEntryKey },
    byDay: {
      type: "gsi",
      index: "gsi1",
      pk: "gsi1pk",
      sk: "gsi1sk",
      key: (event) => (event._tag === "EsIdxNoted" ? `DAY#${event.day}` : undefined),
    },
  },
})

/** A second stream on the same LSI (an identical definition, deduplicated). */
const EsIdxJournal = EventStore.makeStream({
  table: EsIdxTable,
  streamName: "Journal",
  events: [EsIdxRecorded, EsIdxNoted],
  streamId: { composite: ["ledgerId"] },
  indexes: { byEntry: { index: "lsi1", sk: "lsi1sk", key: esIdxEntryKey } },
})

const esIdxDecider: EventStore.Decider<{ readonly entries: number }, number, EsIdxEvent> = {
  initialState: { entries: 0 },
  decide: (count, state) =>
    Effect.succeed(
      Array.from(
        { length: count },
        (_, i) => new EsIdxRecorded({ section: 1, item: state.entries + i + 1 }),
      ),
    ),
  evolve: (state, event) =>
    event._tag === "EsIdxRecorded" ? { entries: state.entries + 1 } : state,
}

const EsIdxLayer = Layer.mergeAll(ClientLayer, EsIdxTable.layer({ name: esIdxTableName }))
const provideEsIdx = Effect.provide(EsIdxLayer)

/** Every item of one physical index for a stream partition, raw (no filter). */
const esIdxRawIndex = (index: string, pkField: string, pk: string) =>
  Effect.gen(function* () {
    const client = yield* DynamoClient
    const result = yield* client.query({
      TableName: esIdxTableName,
      IndexName: index,
      KeyConditionExpression: "#pk = :pk",
      ExpressionAttributeNames: { "#pk": pkField },
      ExpressionAttributeValues: toAttributeMap({ ":pk": pk }),
    })
    return (result.Items ?? []).map((item) => fromAttributeMap(item))
  })

describeConnected("EventStore stream indexes (#140)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        const fragments = EventStore.indexDefinitions(EsIdxLedger, EsIdxJournal)
        yield* client.createTable({
          TableName: esIdxTableName,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
            ...fragments.AttributeDefinitions,
          ],
          LocalSecondaryIndexes: fragments.LocalSecondaryIndexes,
          GlobalSecondaryIndexes: fragments.GlobalSecondaryIndexes,
        })
      }).pipe(provideEsIdx, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: esIdxTableName })
      }).pipe(
        provideEsIdx,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("#140 indexDefinitions creates the LSI and GSI with projection ALL", () =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const described = yield* client.describeTable({ TableName: esIdxTableName })
      const lsis = described.Table?.LocalSecondaryIndexes ?? []
      const gsis = described.Table?.GlobalSecondaryIndexes ?? []
      expect(lsis.map((index) => index.IndexName)).toEqual(["lsi1"])
      expect(gsis.map((index) => index.IndexName)).toEqual(["gsi1"])
      expect(lsis[0]!.KeySchema).toEqual([
        { AttributeName: "pk", KeyType: "HASH" },
        { AttributeName: "lsi1sk", KeyType: "RANGE" },
      ])
      expect(gsis[0]!.KeySchema).toEqual([
        { AttributeName: "gsi1pk", KeyType: "HASH" },
        { AttributeName: "gsi1sk", KeyType: "RANGE" },
      ])
      expect(lsis[0]!.Projection?.ProjectionType).toBe("ALL")
      expect(gsis[0]!.Projection?.ProjectionType).toBe("ALL")
    }).pipe(provideEsIdx),
  )

  it.effect("#140 a sparse LSI sub-stream reads in derived-key order", () =>
    Effect.gen(function* () {
      const key = { ledgerId: "sparse-1" }
      yield* EsIdxLedger.append(
        key,
        [
          new EsIdxRecorded({ section: 2, item: 1 }),
          new EsIdxNoted({ day: "2026-10-01" }),
          new EsIdxRecorded({ section: 1, item: 2 }),
          new EsIdxRecorded({ section: 1, item: 1 }),
        ],
        0,
        { idempotency: { commandId: "cmd-1" }, snapshot: { entries: 3 } },
      )

      // Index order, not stream order; the note is not in the index.
      const entries = yield* EsIdxLedger.readIndex("byEntry", key)
      expect(entries.map((event) => event.version)).toEqual([4, 3, 1])
      expect(entries.map((event) => event.data)).toEqual([
        new EsIdxRecorded({ section: 1, item: 1 }),
        new EsIdxRecorded({ section: 1, item: 2 }),
        new EsIdxRecorded({ section: 2, item: 1 }),
      ])

      // The physical index holds exactly the three entries: no note, no
      // snapshot, no idempotency sentinel.
      const pk = "$es-idx#v1#ledger#sparse-1"
      const raw = yield* esIdxRawIndex("lsi1", "pk", pk)
      expect(raw.map((item) => item.__edd_e__)).toEqual([
        "ledger.event",
        "ledger.event",
        "ledger.event",
      ])
      expect(raw.map((item) => item.lsi1sk)).toEqual([
        "ENTRY#1-0001-0004",
        "ENTRY#1-0002-0003",
        "ENTRY#2-0001-0001",
      ])
      // ...and the partition really holds a snapshot and a sentinel.
      expect(Option.isSome(yield* EsIdxLedger.readSnapshot(key))).toBe(true)
    }).pipe(provideEsIdx),
  )

  it.effect("#140 beginsWith, between, reverse, limit and consistentRead on the LSI", () =>
    Effect.gen(function* () {
      const key = { ledgerId: "range-1" }
      const events = [1, 2, 3].flatMap((section) =>
        [1, 2, 3].map((item) => new EsIdxRecorded({ section, item })),
      )
      yield* EsIdxLedger.append(key, events, 0)
      const versionsOf = (read: ReadonlyArray<EventStore.StreamEvent<EsIdxEvent>>) =>
        read.map((event) => event.version)

      const section2 = yield* EsIdxLedger.readIndex("byEntry", key, {
        beginsWith: "ENTRY#2-",
        consistentRead: true,
      })
      expect(versionsOf(section2)).toEqual([4, 5, 6])

      const between = yield* EsIdxLedger.readIndex("byEntry", key, {
        between: ["ENTRY#1-0002", "ENTRY#2-0002~"],
      })
      expect(versionsOf(between)).toEqual([2, 3, 4, 5])

      const newestTwo = yield* EsIdxLedger.readIndex("byEntry", key, { reverse: true, limit: 2 })
      expect(versionsOf(newestTwo)).toEqual([9, 8])

      // The same through the composable Query, with a cursor across pages.
      const query = EsIdxLedger.query
        .index("byEntry", key)
        .pipe(Query.where({ beginsWith: "ENTRY#3-" }), Query.consistentRead(), Query.limit(2))
      const first = yield* Query.execute(query)
      expect(first.items.map((event) => event.version)).toEqual([7, 8])
      expect(first.cursor).not.toBeNull()
      const second = yield* Query.execute(query.pipe(Query.startFrom(first.cursor!)))
      expect(second.items.map((event) => event.version)).toEqual([9])

      const pages = yield* Query.paginate(
        EsIdxLedger.query.index("byEntry", key).pipe(Query.reverse),
      ).pipe(Effect.flatMap((stream) => Stream.runCollect(stream)))
      expect(pages.flat().map((event) => event.version)).toEqual([9, 8, 7, 6, 5, 4, 3, 2, 1])
    }).pipe(provideEsIdx),
  )

  it.effect("#140 a GSI sub-stream is scoped to its stream; consistentRead is refused", () =>
    Effect.gen(function* () {
      const a = { ledgerId: "gsi-a" }
      const b = { ledgerId: "gsi-b" }
      yield* EsIdxLedger.append(
        a,
        [
          new EsIdxNoted({ day: "2026-10-03" }),
          new EsIdxRecorded({ section: 1, item: 1 }),
          new EsIdxNoted({ day: "2026-10-01" }),
        ],
        0,
      )
      yield* EsIdxLedger.append(b, [new EsIdxNoted({ day: "2026-10-02" })], 0)

      const notes = yield* EsIdxLedger.readIndex("byDay", a)
      expect(notes.map((event) => event.version)).toEqual([3, 1])
      expect(notes.every((event) => event.streamId === "gsi-a")).toBe(true)
      const early = yield* EsIdxLedger.readIndex("byDay", a, { beginsWith: "DAY#2026-10-01" })
      expect(early.map((event) => event.version)).toEqual([3])

      // The GSI's pk attribute carries the stream's partition key.
      const raw = yield* esIdxRawIndex("gsi1", "gsi1pk", "$es-idx#v1#ledger#gsi-a")
      expect(raw.map((item) => item.gsi1sk)).toEqual(["DAY#2026-10-01", "DAY#2026-10-03"])

      const refused = yield* EsIdxLedger.readIndex("byDay", a, { consistentRead: true }).pipe(
        Effect.flip,
      )
      expect(refused._tag).toBe("ValidationError")
      const refusedQuery = yield* EsIdxLedger.query
        .index("byDay", a)
        .pipe(Query.consistentRead(), Query.collect, Effect.flip)
      expect(refusedQuery._tag).toBe("ValidationError")
    }).pipe(provideEsIdx),
  )

  it.effect("#140 every step of a stepped command is indexed", () =>
    Effect.gen(function* () {
      const key = { ledgerId: "stepped-1" }
      const bound = yield* EventStore.bind(EsIdxLedger)
      const handle = EventStore.commandHandler(esIdxDecider, bound)
      let version = 0
      for (const size of [90, 90, 50]) {
        version = (yield* handle(key, size, { expectedVersion: version })).version
      }
      expect(version).toBe(230)

      const indexed = yield* bound.readIndex("byEntry", key, { consistentRead: true })
      expect(indexed.map((event) => event.version)).toEqual(
        Array.from({ length: 230 }, (_, i) => i + 1),
      )
    }).pipe(provideEsIdx),
  )

  it.effect("#140 streams sharing an LSI never see each other's events", () =>
    Effect.gen(function* () {
      const key = { ledgerId: "shared-1" }
      yield* EsIdxLedger.append(key, [new EsIdxRecorded({ section: 1, item: 1 })], 0)
      yield* EsIdxJournal.append(key, [new EsIdxRecorded({ section: 1, item: 2 })], 0)
      const ledger = yield* EsIdxLedger.readIndex("byEntry", key)
      const journal = yield* EsIdxJournal.readIndex("byEntry", key)
      expect(ledger.map((event) => event.data)).toEqual([
        new EsIdxRecorded({ section: 1, item: 1 }),
      ])
      expect(journal.map((event) => event.data)).toEqual([
        new EsIdxRecorded({ section: 1, item: 2 }),
      ])
    }).pipe(provideEsIdx),
  )

  it.effect("#140 an empty derived key fails the append and writes nothing", () =>
    Effect.gen(function* () {
      const EmptyKey = EventStore.makeStream({
        table: EsIdxTable,
        streamName: "EmptyKey",
        events: [EsIdxRecorded],
        streamId: { composite: ["ledgerId"] },
        indexes: {
          byEntry: { index: "lsi1", sk: "lsi1sk", key: (event) => (event.item > 1 ? "" : "k") },
        },
      })
      const key = { ledgerId: "empty-1" }
      const error = yield* EmptyKey.append(
        key,
        [new EsIdxRecorded({ section: 1, item: 1 }), new EsIdxRecorded({ section: 1, item: 2 })],
        0,
      ).pipe(Effect.flip)
      expect(error._tag).toBe("ValidationError")
      expect(yield* EmptyKey.currentVersion(key, { consistentRead: true })).toBe(0)
    }).pipe(provideEsIdx),
  )

  it.effect(
    "#140 readIndex refuses key bounds DynamoDB rejects, ordering them as DynamoDB does",
    () =>
      Effect.gen(function* () {
        const key = { ledgerId: "bounds-1" }
        yield* EsIdxLedger.append(key, [new EsIdxRecorded({ section: 1, item: 1 })], 0)

        // Each of these is a DynamoValidationError if sent; readIndex refuses
        // them with the library's ValidationError instead.
        for (const options of [
          { beginsWith: "" },
          { between: ["", "ENTRY#9"] },
          { between: ["ENTRY#9", "ENTRY#0"] },
        ] as ReadonlyArray<EventStore.ReadIndexOptions>) {
          const error = yield* EsIdxLedger.readIndex("byEntry", key, options).pipe(Effect.flip)
          expect(error._tag).toBe("ValidationError")
        }

        // U+FFFF sorts after U+1F600 by UTF-16 code units but before it by
        // UTF-8 bytes — DynamoDB's order — so DynamoDB accepts this range.
        const accepted = yield* EsIdxLedger.readIndex("byEntry", key, {
          between: ["￿", "\u{1F600}"],
        })
        expect(accepted).toEqual([])
        const all = yield* EsIdxLedger.readIndex("byEntry", key, {
          between: ["ENTRY#1-0001-0001", "ENTRY#1-0001-0001"],
        })
        expect(all.map((event) => event.version)).toEqual([1])
      }).pipe(provideEsIdx),
  )

  it.effect("#140 events appended before an index is declared are not in it", () =>
    Effect.gen(function* () {
      // The documented contract: index attributes are written only at append
      // time and the library does not backfill.
      const Before = EventStore.makeStream({
        table: EsIdxTable,
        streamName: "LateIndex",
        events: [EsIdxRecorded],
        streamId: { composite: ["ledgerId"] },
      })
      const After = EventStore.makeStream({
        table: EsIdxTable,
        streamName: "LateIndex",
        events: [EsIdxRecorded],
        streamId: { composite: ["ledgerId"] },
        indexes: { byEntry: { index: "lsi1", sk: "lsi1sk", key: esIdxEntryKey } },
      })
      const key = { ledgerId: "late-1" }
      yield* Before.append(
        key,
        [new EsIdxRecorded({ section: 1, item: 1 }), new EsIdxRecorded({ section: 1, item: 2 })],
        0,
      )
      yield* After.append(key, [new EsIdxRecorded({ section: 1, item: 3 })], 2)
      expect(yield* After.read(key, { consistentRead: true })).toHaveLength(3)
      const indexed = yield* After.readIndex("byEntry", key, { consistentRead: true })
      expect(indexed.map((event) => event.version)).toEqual([3])
    }).pipe(provideEsIdx),
  )

  it("#140 definition-time collisions are refused", () => {
    const define = (indexes: unknown) => () =>
      EventStore.makeStream({
        table: EsIdxTable,
        streamName: "Colliding",
        events: [EsIdxRecorded],
        streamId: { composite: ["ledgerId"] },
        indexes: indexes as never,
      })
    const key = () => undefined
    expect(define({ a: { index: "lsi1", sk: "version", key } })).toThrow(/EDD-9064/)
    expect(define({ a: { type: "gsi", index: "gsi1", pk: "pk", sk: "s", key } })).toThrow(
      /EDD-9064/,
    )
    expect(
      define({ a: { index: "lsi1", sk: "s1", key }, b: { index: "lsi1", sk: "s2", key } }),
    ).toThrow(/EDD-9065/)
    expect(
      define({ a: { index: "lsi1", sk: "s", key }, b: { index: "lsi2", sk: "s", key } }),
    ).toThrow(/EDD-9065/)
    expect(define({ a: { type: "gsi", index: "gsi1", sk: "s", key } })).toThrow(/EDD-9063/)
    expect(define({ a: { index: "lsi1", pk: "p", sk: "s", key } })).toThrow(/EDD-9063/)

    // Two streams defining one physical index differently cannot share a table.
    const Other = EventStore.makeStream({
      table: EsIdxTable,
      streamName: "Other",
      events: [EsIdxRecorded],
      streamId: { composite: ["ledgerId"] },
      indexes: { byEntry: { index: "lsi1", sk: "otherSk", key: () => undefined } },
    })
    expect(() => EventStore.indexDefinitions(EsIdxLedger, Other)).toThrow(/EDD-9067/)
  })
})

// ===========================================================================
// Aggregate assembly off the base table (#93)
// ===========================================================================
//
// The collection query is a bare `pk = :pk` over the whole partition, so when the
// aggregate is keyed on the table's primary partition key it needs no secondary
// index. These tests prove it end-to-end: the table is provisioned with neither an
// LSI nor a GSI, no collection SK mirror attribute is written, assembly still
// round-trips, and the base table serves the strongly consistent read that a GSI
// could not.

const BtSchema = DynamoSchema.make({ name: "bt-agg", version: 1 })
const btTableName = `bt-agg-test-${Date.now()}`

class BtLine extends Schema.Class<BtLine>("BtLine")({
  sku: Schema.String.pipe(DynamoModel.identifier),
  qty: Schema.Number,
}) {}

class BtOrderLine extends Schema.Class<BtOrderLine>("BtOrderLine")({
  id: Schema.String,
  sku: Schema.String,
  qty: Schema.Number,
}) {}

class BtOrder extends Schema.Class<BtOrder>("BtOrder")({
  id: Schema.String,
  customer: Schema.String,
  lines: Schema.Array(BtOrderLine),
}) {}

const BtLines = Entity.make({
  model: BtLine,
  entityType: "BtLine",
  primaryKey: {
    pk: { field: "pk", composite: ["sku"] },
    sk: { field: "sk", composite: [] },
  },
})

const BtTable = Table.make({ schema: BtSchema, entities: { BtLines } })

/** No `collection.index` — assembly runs against the base table. */
const BtOrderAggregate = Aggregate.make(BtOrder, {
  table: BtTable,
  schema: BtSchema,
  pk: { field: "pk", composite: ["id"] },
  collection: { name: "btorder" },
  root: { entityType: "BtOrderRoot" },
  edges: {
    lines: Aggregate.many("lines", { entityType: "BtOrderLine" }),
  },
})

/** Same shape, strongly consistent — only legal because it reads the base table. */
const BtConsistentAggregate = Aggregate.make(BtOrder, {
  table: BtTable,
  schema: BtSchema,
  pk: { field: "pk", composite: ["id"] },
  collection: { name: "btorder" },
  consistentRead: true,
  root: { entityType: "BtOrderRoot" },
  edges: {
    lines: Aggregate.many("lines", { entityType: "BtOrderLine" }),
  },
})

const BtTestLayer = Layer.mergeAll(ClientLayer, BtTable.layer({ name: btTableName }))
const provideBt = Effect.provide(BtTestLayer)

describeConnected("Aggregate — base-table assembly (#93)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { BtLines },
          aggregates: { BtOrderAggregate },
          tables: { BtTable },
        })
        yield* db.tables.BtTable.create()
      }).pipe(provideBt, Effect.scoped),
    )
  }, 15000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: btTableName })
      }).pipe(
        provideBt,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("provisions the table with no secondary index at all", () =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const described = yield* client.describeTable({ TableName: btTableName })

      expect(described.Table?.LocalSecondaryIndexes ?? []).toHaveLength(0)
      expect(described.Table?.GlobalSecondaryIndexes ?? []).toHaveLength(0)
      expect(
        (described.Table?.AttributeDefinitions ?? []).map((a) => a.AttributeName).sort(),
      ).toEqual(["pk", "sk"])
    }).pipe(provideBt),
  )

  it.effect("round-trips create → get without an index", () =>
    Effect.gen(function* () {
      yield* BtOrderAggregate.create({
        id: "bt-1",
        customer: "acme",
        lines: [
          { id: "l-1", sku: "widget", qty: 2 },
          { id: "l-2", sku: "gadget", qty: 5 },
        ],
      })

      const loaded = yield* BtOrderAggregate.get({ id: "bt-1" })

      expect(loaded.customer).toBe("acme")
      expect(loaded.lines).toHaveLength(2)
      expect([...loaded.lines].map((l) => l.sku).sort()).toEqual(["gadget", "widget"])
    }).pipe(provideBt),
  )

  it.effect("writes no collection SK mirror attribute on any item", () =>
    Effect.gen(function* () {
      const raw = yield* (yield* DynamoClient).query({
        TableName: btTableName,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: "$bt-agg#v1#btorder#bt-1" } },
      })

      expect((raw.Items ?? []).length).toBeGreaterThan(0)
      for (const item of raw.Items ?? []) {
        expect(item.lsi1sk).toBeUndefined()
      }
    }).pipe(provideBt),
  )

  it.effect("round-trips an update without an index", () =>
    Effect.gen(function* () {
      yield* BtOrderAggregate.update({ id: "bt-1" }, ({ state }) => ({
        ...state,
        customer: "globex",
      }))

      const loaded = yield* BtOrderAggregate.get({ id: "bt-1" })
      expect(loaded.customer).toBe("globex")
      expect(loaded.lines).toHaveLength(2)
    }).pipe(provideBt),
  )

  it.effect("serves a strongly consistent read off the base table", () =>
    Effect.gen(function* () {
      yield* BtConsistentAggregate.create({
        id: "bt-2",
        customer: "initech",
        lines: [{ id: "l-1", sku: "stapler", qty: 1 }],
      })

      // Read-after-write with ConsistentRead — the whole point of dropping the GSI.
      const loaded = yield* BtConsistentAggregate.get({ id: "bt-2" })
      expect(loaded.customer).toBe("initech")
      expect(loaded.lines).toHaveLength(1)
    }).pipe(provideBt),
  )
})

// ---------------------------------------------------------------------------
// #101 — sort key conditions on named-index and primary-key accessors
//
// A stored SK is `$schema#v1#entity#<name>_<cased value>`. Before the fix
// `.where()` compared the raw operand against that composed key, so `gte`
// matched the whole partition while `begins_with` / `between` matched nothing.
// Every operator is verified against real DynamoDB on BOTH a named GSI and the
// primary-key accessor, plus the multi-composite SK shape.
// ---------------------------------------------------------------------------

const skCondSchema = DynamoSchema.make({ name: "skcond", version: 1 })
const skCondTableName = `skcond-test-${Date.now()}`

class BallRecord extends Schema.Class<BallRecord>("BallRecord")({
  matchId: Schema.String,
  ballKey: Schema.String,
}) {}

const SkCondBalls = Entity.make({
  model: BallRecord,
  entityType: "Ball",
  primaryKey: {
    pk: { field: "pk", composite: ["matchId"] },
    sk: { field: "sk", composite: ["ballKey"] },
  },
  indexes: {
    byMatch: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["matchId"] },
      sk: { field: "gsi1sk", composite: ["ballKey"] },
    },
  },
})

// Two SK composites — exercises conditions on a non-terminal composite.
class ReadingRecord extends Schema.Class<ReadingRecord>("ReadingRecord")({
  deviceId: Schema.String,
  status: Schema.String,
  seq: Schema.String,
}) {}

const SkCondReadings = Entity.make({
  model: ReadingRecord,
  entityType: "Reading",
  primaryKey: {
    pk: { field: "pk", composite: ["deviceId"] },
    sk: { field: "sk", composite: ["status", "seq"] },
  },
  indexes: {
    byDevice: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["deviceId"] },
      sk: { field: "gsi1sk", composite: ["status", "seq"] },
    },
  },
})

const SkCondTable = Table.make({
  schema: skCondSchema,
  entities: { SkCondBalls, SkCondReadings },
})
const SkCondLayer = Layer.mergeAll(ClientLayer, SkCondTable.layer({ name: skCondTableName }))
const provideSkCond = Effect.provide(SkCondLayer)

const BALL_KEYS = ["1-008-6-6", "1-009-1-1", "1-009-2-2", "1-010-1-1", "1-011-1-1"] as const

describeConnected("sort key conditions via .where() (closes #101)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: skCondTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(SkCondTable),
        })
        const db = yield* DynamoClient.make({
          entities: { SkCondBalls, SkCondReadings },
          tables: { SkCondTable },
        })
        for (const ballKey of BALL_KEYS) {
          yield* db.entities.SkCondBalls.put({ matchId: "m-1", ballKey })
        }
        for (const [status, seq] of [
          ["active", "0001"],
          ["active", "0002"],
          ["done", "0001"],
          ["done", "0002"],
          ["error", "0001"],
        ] as const) {
          yield* db.entities.SkCondReadings.put({ deviceId: "d-1", status, seq })
        }
      }).pipe(provideSkCond, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: skCondTableName })
      }).pipe(
        provideSkCond,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // ----- named GSI accessor -----

  it.effect("named GSI: no condition returns the whole partition in sort order", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const all = yield* db.entities.SkCondBalls.byMatch({ matchId: "m-1" }).collect()
      expect(all.map((b) => b.ballKey)).toEqual([...BALL_KEYS])
    }).pipe(provideSkCond),
  )

  it.effect("named GSI: gte narrows to 4 (issue #101 expected)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const rows = yield* db.entities.SkCondBalls.byMatch({ matchId: "m-1" })
        .where((t, { gte }) => gte(t.ballKey, "1-009"))
        .collect()
      expect(rows.map((b) => b.ballKey)).toEqual([
        "1-009-1-1",
        "1-009-2-2",
        "1-010-1-1",
        "1-011-1-1",
      ])
    }).pipe(provideSkCond),
  )

  it.effect("named GSI: beginsWith narrows to 2 (issue #101 expected)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const rows = yield* db.entities.SkCondBalls.byMatch({ matchId: "m-1" })
        .where((t, { beginsWith }) => beginsWith(t.ballKey, "1-009"))
        .collect()
      expect(rows.map((b) => b.ballKey)).toEqual(["1-009-1-1", "1-009-2-2"])
    }).pipe(provideSkCond),
  )

  it.effect("named GSI: between narrows to 3 (issue #101 expected)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const rows = yield* db.entities.SkCondBalls.byMatch({ matchId: "m-1" })
        .where((t, { between }) => between(t.ballKey, "1-009", "1-011"))
        .collect()
      expect(rows.map((b) => b.ballKey)).toEqual(["1-009-1-1", "1-009-2-2", "1-010-1-1"])
    }).pipe(provideSkCond),
  )

  it.effect("named GSI: eq / lt / lte / gt narrow correctly", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const q = db.entities.SkCondBalls.byMatch({ matchId: "m-1" })

      const eq = yield* q.where((t, ops) => ops.eq(t.ballKey, "1-009-1-1")).collect()
      expect(eq.map((b) => b.ballKey)).toEqual(["1-009-1-1"])

      const lt = yield* q.where((t, ops) => ops.lt(t.ballKey, "1-010")).collect()
      expect(lt.map((b) => b.ballKey)).toEqual(["1-008-6-6", "1-009-1-1", "1-009-2-2"])

      const lte = yield* q.where((t, ops) => ops.lte(t.ballKey, "1-009-2-2")).collect()
      expect(lte.map((b) => b.ballKey)).toEqual(["1-008-6-6", "1-009-1-1", "1-009-2-2"])

      const gt = yield* q.where((t, ops) => ops.gt(t.ballKey, "1-009-2-2")).collect()
      expect(gt.map((b) => b.ballKey)).toEqual(["1-010-1-1", "1-011-1-1"])
    }).pipe(provideSkCond),
  )

  // ----- primary-key accessor -----

  it.effect("primary: gte / beginsWith / between narrow correctly", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const q = db.entities.SkCondBalls.primary({ matchId: "m-1" })

      const all = yield* q.collect()
      expect(all.map((b) => b.ballKey)).toEqual([...BALL_KEYS])

      const gte = yield* q.where((t, ops) => ops.gte(t.ballKey, "1-009")).collect()
      expect(gte.map((b) => b.ballKey)).toEqual([
        "1-009-1-1",
        "1-009-2-2",
        "1-010-1-1",
        "1-011-1-1",
      ])

      const bw = yield* q.where((t, ops) => ops.beginsWith(t.ballKey, "1-009")).collect()
      expect(bw.map((b) => b.ballKey)).toEqual(["1-009-1-1", "1-009-2-2"])

      const btw = yield* q.where((t, ops) => ops.between(t.ballKey, "1-009", "1-011")).collect()
      expect(btw.map((b) => b.ballKey)).toEqual(["1-009-1-1", "1-009-2-2", "1-010-1-1"])
    }).pipe(provideSkCond),
  )

  it.effect("primary: eq / lt / lte / gt narrow correctly", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const q = db.entities.SkCondBalls.primary({ matchId: "m-1" })

      const eq = yield* q.where((t, ops) => ops.eq(t.ballKey, "1-010-1-1")).collect()
      expect(eq.map((b) => b.ballKey)).toEqual(["1-010-1-1"])

      const lt = yield* q.where((t, ops) => ops.lt(t.ballKey, "1-010")).collect()
      expect(lt.map((b) => b.ballKey)).toEqual(["1-008-6-6", "1-009-1-1", "1-009-2-2"])

      const lte = yield* q.where((t, ops) => ops.lte(t.ballKey, "1-009-2-2")).collect()
      expect(lte.map((b) => b.ballKey)).toEqual(["1-008-6-6", "1-009-1-1", "1-009-2-2"])

      const gt = yield* q.where((t, ops) => ops.gt(t.ballKey, "1-009-2-2")).collect()
      expect(gt.map((b) => b.ballKey)).toEqual(["1-010-1-1", "1-011-1-1"])
    }).pipe(provideSkCond),
  )

  // ----- multi-composite sort key -----

  it.effect("multi-composite: condition on the leading composite spans its subtree", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const q = db.entities.SkCondReadings.byDevice({ deviceId: "d-1" })
      const key = (r: ReadingRecord) => `${r.status}/${r.seq}`

      // eq on a non-terminal composite matches every row with that value.
      const eq = yield* q.where((t, ops) => ops.eq(t.status, "done")).collect()
      expect(eq.map(key)).toEqual(["done/0001", "done/0002"])

      // gte includes the whole `done` subtree and everything after it.
      const gte = yield* q.where((t, ops) => ops.gte(t.status, "done")).collect()
      expect(gte.map(key)).toEqual(["done/0001", "done/0002", "error/0001"])

      // lte is inclusive of the whole `done` subtree.
      const lte = yield* q.where((t, ops) => ops.lte(t.status, "done")).collect()
      expect(lte.map(key)).toEqual(["active/0001", "active/0002", "done/0001", "done/0002"])

      // gt excludes the whole `done` subtree.
      const gt = yield* q.where((t, ops) => ops.gt(t.status, "done")).collect()
      expect(gt.map(key)).toEqual(["error/0001"])

      // lt excludes the whole `done` subtree.
      const lt = yield* q.where((t, ops) => ops.lt(t.status, "done")).collect()
      expect(lt.map(key)).toEqual(["active/0001", "active/0002"])

      // between is inclusive on both ends.
      const btw = yield* q.where((t, ops) => ops.between(t.status, "active", "done")).collect()
      expect(btw.map(key)).toEqual(["active/0001", "active/0002", "done/0001", "done/0002"])

      // beginsWith on a non-terminal composite prefixes the value.
      const bw = yield* q.where((t, ops) => ops.beginsWith(t.status, "a")).collect()
      expect(bw.map(key)).toEqual(["active/0001", "active/0002"])
    }).pipe(provideSkCond),
  )

  it.effect("multi-composite: condition on the trailing composite after pinning the first", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      const key = (r: ReadingRecord) => `${r.status}/${r.seq}`
      const q = db.entities.SkCondReadings.byDevice({ deviceId: "d-1", status: "done" })

      // Every one-sided operator must stay inside the pinned `status_done`
      // prefix — `Query.where` replaces the accessor's own begins_with, so an
      // unclamped bound would leak into `active` / `error` readings.
      const gte = yield* q.where((t, ops) => ops.gte(t.seq, "0002")).collect()
      expect(gte.map(key)).toEqual(["done/0002"])

      const gt = yield* q.where((t, ops) => ops.gt(t.seq, "0001")).collect()
      expect(gt.map(key)).toEqual(["done/0002"])

      const lte = yield* q.where((t, ops) => ops.lte(t.seq, "0002")).collect()
      expect(lte.map(key)).toEqual(["done/0001", "done/0002"])

      const eq = yield* q.where((t, ops) => ops.eq(t.seq, "0001")).collect()
      expect(eq.map(key)).toEqual(["done/0001"])

      const btw = yield* q.where((t, ops) => ops.between(t.seq, "0001", "0002")).collect()
      expect(btw.map(key)).toEqual(["done/0001", "done/0002"])

      const bw = yield* q.where((t, ops) => ops.beginsWith(t.seq, "000")).collect()
      expect(bw.map(key)).toEqual(["done/0001", "done/0002"])
    }).pipe(provideSkCond),
  )

  it.effect("multi-composite: skipping a leading composite is rejected (EDD-9004)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { SkCondBalls, SkCondReadings },
        tables: { SkCondTable },
      })
      expect(() =>
        db.entities.SkCondReadings.byDevice({ deviceId: "d-1" }).where((t, ops) =>
          ops.gte(t.seq, "0002"),
        ),
      ).toThrow(/EDD-9004/)
    }).pipe(provideSkCond),
  )

  it.effect(
    "multi-composite: strict lt on a pinned terminal composite is rejected (EDD-9046)",
    () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { SkCondBalls, SkCondReadings },
          tables: { SkCondTable },
        })
        expect(() =>
          db.entities.SkCondReadings.byDevice({ deviceId: "d-1", status: "done" }).where((t, ops) =>
            ops.lt(t.seq, "0002"),
          ),
        ).toThrow(/EDD-9046/)
      }).pipe(provideSkCond),
  )
})

// ---------------------------------------------------------------------------
// #115 — partial sort-key `begins_with` must terminate on a segment boundary
// #114 — `.where()` operands are serialised like the stored key
//
// #115: `byTenant({ status: "done" })` composed `begins_with(sk,
// "…#status_done")`, which also matched `status_done_archived` and
// `status_doneish`. The delimiter is now appended iff composites remain.
// #114: a numeric composite is zero-padded on write, so a stringly-typed
// `"42"` operand sorted after every stored value. Operands now carry the
// composite's own type and go through the same `serializeValue`.
// ---------------------------------------------------------------------------

const skpSchema = DynamoSchema.make({ name: "skp", version: 1 })
const skpTableName = `skp-test-${Date.now()}`

class PrefixTask extends Schema.Class<PrefixTask>("PrefixTask")({
  tenantId: Schema.String,
  status: Schema.String,
  taskId: Schema.String,
}) {}

const PrefixTasks = Entity.make({
  model: PrefixTask,
  entityType: "PrefixTask",
  primaryKey: {
    pk: { field: "pk", composite: ["tenantId"] },
    sk: { field: "sk", composite: ["status", "taskId"] },
  },
  indexes: {
    byTenant: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["tenantId"] },
      sk: { field: "gsi1sk", composite: ["status", "taskId"] },
    },
  },
})

// Single-composite sort key — supplying it is a COMPLETE key.
class PrefixNote extends Schema.Class<PrefixNote>("PrefixNote")({
  boardId: Schema.String,
  label: Schema.String,
}) {}

const PrefixNotes = Entity.make({
  model: PrefixNote,
  entityType: "PrefixNote",
  primaryKey: {
    pk: { field: "pk", composite: ["boardId"] },
    sk: { field: "sk", composite: ["label"] },
  },
  indexes: {
    byBoard: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["boardId"] },
      sk: { field: "gsi1sk", composite: ["label"] },
    },
  },
})

// Non-string sort key composites.
class TypedSample extends Schema.Class<TypedSample>("TypedSample")({
  deviceId: Schema.String,
  seq: Schema.Number,
  ok: Schema.Boolean,
  at: Schema.Date,
  zoned: Schema.DateTimeUtc,
}) {}

const TypedSamples = Entity.make({
  model: TypedSample,
  entityType: "TypedSample",
  primaryKey: {
    pk: { field: "pk", composite: ["deviceId"] },
    sk: { field: "sk", composite: ["seq"] },
  },
  indexes: {
    byFlag: {
      name: "gsi2",
      pk: { field: "gsi2pk", composite: ["deviceId"] },
      sk: { field: "gsi2sk", composite: ["ok", "seq"] },
    },
    byAt: {
      name: "gsi3",
      pk: { field: "gsi3pk", composite: ["deviceId"] },
      sk: { field: "gsi3sk", composite: ["at"] },
    },
    byZoned: {
      name: "gsi4",
      pk: { field: "gsi4pk", composite: ["deviceId"] },
      sk: { field: "gsi4sk", composite: ["zoned"] },
    },
  },
})

const SkpTable = Table.make({
  schema: skpSchema,
  entities: { PrefixTasks, PrefixNotes, TypedSamples },
})
const SkpLayer = Layer.mergeAll(ClientLayer, SkpTable.layer({ name: skpTableName }))
const provideSkp = Effect.provide(SkpLayer)

const skpEntities = { PrefixTasks, PrefixNotes, TypedSamples }
const skpTables = { SkpTable }

const SAMPLE_SEQS = [5, 42, 100] as const
const sampleAt = (seq: number) => new Date(Date.UTC(2026, 0, 1 + seq, 0, 0, 0))

describeConnected("sort key prefix + typed operands (closes #114, closes #115)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: skpTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(SkpTable),
        })
        const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })

        for (const [status, taskId] of [
          ["done", "t1"],
          ["done", "t2"],
          ["done_archived", "t3"],
          ["doneish", "t4"],
          ["todo", "t5"],
        ] as const) {
          yield* db.entities.PrefixTasks.put({ tenantId: "acme", status, taskId })
        }
        for (const label of ["ship", "shipped", "ship_it"]) {
          yield* db.entities.PrefixNotes.put({ boardId: "b1", label })
        }
        for (const seq of SAMPLE_SEQS) {
          yield* db.entities.TypedSamples.put({
            deviceId: "d1",
            seq,
            ok: seq !== 42,
            at: sampleAt(seq),
            zoned: DateTime.makeUnsafe(sampleAt(seq).toISOString()),
          })
        }
      }).pipe(provideSkp, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: skpTableName })
      }).pipe(
        provideSkp,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  // ----- #115 -----

  it.effect("#115 partial prefix excludes sibling values on a named GSI", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      const rows = yield* db.entities.PrefixTasks.byTenant({
        tenantId: "acme",
        status: "done",
      }).collect()
      // Pre-fix this returned 4 — `done_archived` and `doneish` also begin with
      // `status_done`.
      expect(rows.map((r) => `${r.status}/${r.taskId}`)).toEqual(["done/t1", "done/t2"])
    }).pipe(provideSkp),
  )

  it.effect("#115 partial prefix excludes sibling values on the primary accessor", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      const rows = yield* db.entities.PrefixTasks.primary({
        tenantId: "acme",
        status: "done",
      }).collect()
      expect(rows.map((r) => `${r.status}/${r.taskId}`)).toEqual(["done/t1", "done/t2"])
    }).pipe(provideSkp),
  )

  it.effect("#115 a complete SK composite set still matches its row", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      // The regression guard for the rule: a delimiter here would match nothing.
      const gsi = yield* db.entities.PrefixTasks.byTenant({
        tenantId: "acme",
        status: "done",
        taskId: "t1",
      }).collect()
      expect(gsi.map((r) => r.taskId)).toEqual(["t1"])

      const pk = yield* db.entities.PrefixTasks.primary({
        tenantId: "acme",
        status: "done",
        taskId: "t2",
      }).collect()
      expect(pk.map((r) => r.taskId)).toEqual(["t2"])
    }).pipe(provideSkp),
  )

  it.effect("#115 single-composite sort key is a complete key — still a prefix match", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      // Documented behaviour, unchanged by #115: with every composite supplied
      // the accessor still issues `begins_with` on the full composed key, so
      // longer sibling values match. Use `.get()` for a single exact item.
      const rows = yield* db.entities.PrefixNotes.byBoard({
        boardId: "b1",
        label: "ship",
      }).collect()
      expect(rows.map((r) => r.label).sort()).toEqual(["ship", "ship_it", "shipped"])

      // The documented escape hatch: leave the composite off the accessor and
      // let `.where()` compose an exact `sk = …#label_ship`.
      const exact = yield* db.entities.PrefixNotes.byBoard({ boardId: "b1" })
        .where((t, ops) => ops.eq(t.label, "ship"))
        .collect()
      expect(exact.map((r) => r.label)).toEqual(["ship"])
    }).pipe(provideSkp),
  )

  it.effect("#115 pinned-prefix .where() bounds do not leak into sibling values", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      const q = db.entities.PrefixTasks.byTenant({ tenantId: "acme", status: "done" })

      const gte = yield* q.where((t, ops) => ops.gte(t.taskId, "t1")).collect()
      expect(gte.map((r) => `${r.status}/${r.taskId}`)).toEqual(["done/t1", "done/t2"])

      const gt = yield* q.where((t, ops) => ops.gt(t.taskId, "t1")).collect()
      expect(gt.map((r) => `${r.status}/${r.taskId}`)).toEqual(["done/t2"])

      const lte = yield* q.where((t, ops) => ops.lte(t.taskId, "t2")).collect()
      expect(lte.map((r) => `${r.status}/${r.taskId}`)).toEqual(["done/t1", "done/t2"])
    }).pipe(provideSkp),
  )

  // ----- #114 -----

  it.effect("#114 numeric composite compares against the zero-padded stored key", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      const q = () => db.entities.TypedSamples.primary({ deviceId: "d1" })

      const all = yield* q().collect()
      expect(all.map((r) => r.seq)).toEqual([5, 42, 100])

      const gte = yield* q()
        .where((t, ops) => ops.gte(t.seq, 42))
        .collect()
      expect(gte.map((r) => r.seq)).toEqual([42, 100])

      const lt = yield* q()
        .where((t, ops) => ops.lt(t.seq, 42))
        .collect()
      expect(lt.map((r) => r.seq)).toEqual([5])

      const eq = yield* q()
        .where((t, ops) => ops.eq(t.seq, 42))
        .collect()
      expect(eq.map((r) => r.seq)).toEqual([42])

      const between = yield* q()
        .where((t, ops) => ops.between(t.seq, 5, 42))
        .collect()
      expect(between.map((r) => r.seq)).toEqual([5, 42])
    }).pipe(provideSkp),
  )

  // NOTE: `bigint` composites are covered in the unit suite only
  // (`DynamoClient.test.ts` asserts the 38-digit padded operand). A bigint
  // sort key composite cannot round-trip through DynamoDB today: `Schema.BigInt`
  // fails to decode (the SDK unmarshalls `N` to a JS number) and
  // `Schema.BigIntFromString` composes the key from its ENCODED string, which is
  // not zero-padded. That is a pre-existing modelling gap, unrelated to #114.

  it.effect("#114 boolean composite", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      const truthy = yield* db.entities.TypedSamples.byFlag({ deviceId: "d1" })
        .where((t, ops) => ops.eq(t.ok, true))
        .collect()
      expect(truthy.map((r) => r.seq)).toEqual([5, 100])

      const falsy = yield* db.entities.TypedSamples.byFlag({ deviceId: "d1" })
        .where((t, ops) => ops.eq(t.ok, false))
        .collect()
      expect(falsy.map((r) => r.seq)).toEqual([42])
    }).pipe(provideSkp),
  )

  it.effect("#114 Date composite", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      const rows = yield* db.entities.TypedSamples.byAt({ deviceId: "d1" })
        .where((t, ops) => ops.gte(t.at, sampleAt(42)))
        .collect()
      expect(rows.map((r) => r.seq)).toEqual([42, 100])

      const between = yield* db.entities.TypedSamples.byAt({ deviceId: "d1" })
        .where((t, ops) => ops.between(t.at, sampleAt(5), sampleAt(42)))
        .collect()
      expect(between.map((r) => r.seq)).toEqual([5, 42])
    }).pipe(provideSkp),
  )

  it.effect("#114 DateTime composite", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      const rows = yield* db.entities.TypedSamples.byZoned({ deviceId: "d1" })
        .where((t, ops) => ops.gt(t.zoned, DateTime.makeUnsafe(sampleAt(5).toISOString())))
        .collect()
      expect(rows.map((r) => r.seq)).toEqual([42, 100])
    }).pipe(provideSkp),
  )

  it.effect("#114 string composites are unchanged — serializeValue is identity", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: skpEntities, tables: skpTables })
      const rows = yield* db.entities.PrefixTasks.byTenant({ tenantId: "acme" })
        .where((t, ops) => ops.beginsWith(t.status, "done"))
        .collect()
      expect(rows.map((r) => `${r.status}/${r.taskId}`)).toEqual([
        "done/t1",
        "done/t2",
        "done_archived/t3",
        "doneish/t4",
      ])
    }).pipe(provideSkp),
  )
})

// ---------------------------------------------------------------------------
// Transformed composites — the encoded/decoded gap
//
// Key composition runs on the ENCODED record (`Entity.put` encodes, THEN calls
// `composeAllKeys`), while accessors, `.where()` and key inputs carry DECODED
// model values. For a composite with a `decodeTo` transformation the two forms
// differ, so composing a decoded value produced a different string from the one
// that was stored — the query matched nothing, or everything.
//
// `Schema.BigIntFromString` is the fixture: Type is `bigint`, Encoded is a
// string, so the stored sort key holds `txn_420` and NOT the 38-digit padding
// `serializeValue(420n)` would produce. Values are chosen equal-width so the
// assertions do not depend on lexicographic-vs-numeric ordering.
// ---------------------------------------------------------------------------

const encSchema = DynamoSchema.make({ name: "enc", version: 1 })
const encTableName = `enc-test-${Date.now()}`

class Ledger extends Schema.Class<Ledger>("Ledger")({
  bookId: Schema.String,
  txn: Schema.BigIntFromString,
  label: Schema.String,
}) {}

const Ledgers = Entity.make({
  model: Ledger,
  entityType: "Ledger",
  primaryKey: {
    pk: { field: "pk", composite: ["bookId"] },
    sk: { field: "sk", composite: ["txn"] },
  },
  indexes: {
    byBook: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["bookId"] },
      sk: { field: "gsi1sk", composite: ["txn", "label"] },
    },
  },
})

// Same transformed composite, with the lifecycle features whose internal
// paths compose keys from records read back from DynamoDB (already ENCODED).
class RetainLedger extends Schema.Class<RetainLedger>("RetainLedger")({
  acctId: Schema.String,
  txn: Schema.BigIntFromString,
  note: Schema.String,
}) {}

const RetainLedgers = Entity.make({
  model: RetainLedger,
  entityType: "RetainLedger",
  primaryKey: {
    pk: { field: "pk", composite: ["acctId"] },
    sk: { field: "sk", composite: ["txn"] },
  },
  versioned: { retain: true },
  softDelete: true,
  timestamps: true,
})

const EncTable = Table.make({ schema: encSchema, entities: { Ledgers, RetainLedgers } })
const EncLayer = Layer.mergeAll(ClientLayer, EncTable.layer({ name: encTableName }))
const provideEnc = Effect.provide(EncLayer)
const encEntities = { Ledgers }
const encTables = { EncTable }

describeConnected("transformed sort key composites — encoded/decoded gap", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: encTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(EncTable),
        })
        const db = yield* DynamoClient.make({ entities: encEntities, tables: encTables })
        for (const txn of [100n, 420n, 999n]) {
          yield* db.entities.Ledgers.put({ bookId: "b1", txn, label: "x" })
        }
      }).pipe(provideEnc, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: encTableName })
      }).pipe(
        provideEnc,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("the stored key holds the padded key form of the composite", () =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const raw = yield* client.scan({ TableName: encTableName })
      const sks = (raw.Items ?? []).map((i) => (i.sk as { S?: string } | undefined)?.S ?? "").sort()
      // PADDED: `txn` is numeric-Type / string-Encoded, so the key form rule
      // composes from the bigint and `serializeValue` pads it to 38 digits.
      expect(sks).toEqual([
        `$enc#v1#ledger#txn_${"100".padStart(38, "0")}`,
        `$enc#v1#ledger#txn_${"420".padStart(38, "0")}`,
        `$enc#v1#ledger#txn_${"999".padStart(38, "0")}`,
      ])
    }).pipe(provideEnc),
  )

  it.effect(".where() operand matches the stored key on the primary accessor", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: encEntities, tables: encTables })
      const q = () => db.entities.Ledgers.primary({ bookId: "b1" })

      const all = yield* q().collect()
      expect(all.map((r) => r.txn)).toEqual([100n, 420n, 999n])

      // Pre-fix: 0 rows — the padded operand named a key that does not exist.
      const eq = yield* q()
        .where((t, ops) => ops.eq(t.txn, 420n))
        .collect()
      expect(eq.map((r) => r.txn)).toEqual([420n])

      // Pre-fix: 3 rows — the padded operand sorted below every stored key.
      const gte = yield* q()
        .where((t, ops) => ops.gte(t.txn, 420n))
        .collect()
      expect(gte.map((r) => r.txn)).toEqual([420n, 999n])

      const between = yield* q()
        .where((t, ops) => ops.between(t.txn, 100n, 420n))
        .collect()
      expect(between.map((r) => r.txn)).toEqual([100n, 420n])
    }).pipe(provideEnc),
  )

  it.effect(".where() operand matches the stored key on a named GSI", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: encEntities, tables: encTables })
      const gte = yield* db.entities.Ledgers.byBook({ bookId: "b1" })
        .where((t, ops) => ops.gte(t.txn, 420n))
        .collect()
      expect(gte.map((r) => r.txn)).toEqual([420n, 999n])

      // Pinned transformed composite + a condition on the composite after it.
      const pinned = yield* db.entities.Ledgers.byBook({ bookId: "b1", txn: 420n })
        .where((t, ops) => ops.eq(t.label, "x"))
        .collect()
      expect(pinned.map((r) => r.txn)).toEqual([420n])
    }).pipe(provideEnc),
  )

  it.effect("accessor composites are encoded too", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: encEntities, tables: encTables })
      // Pre-fix: 0 rows on both.
      const gsi = yield* db.entities.Ledgers.byBook({ bookId: "b1", txn: 420n }).collect()
      expect(gsi.map((r) => r.txn)).toEqual([420n])

      const pk = yield* db.entities.Ledgers.primary({ bookId: "b1", txn: 420n }).collect()
      expect(pk.map((r) => r.txn)).toEqual([420n])
    }).pipe(provideEnc),
  )

  // ----- key input is the Type side, one convention across the API -----

  it.effect("get / update / delete take the Type side, like the query path", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: encEntities, tables: encTables })

      // `420n` — the value the domain model holds and the value
      // `.where(eq(t.txn, 420n))` takes. Pre-fix this raised ValidationError.
      const got = yield* db.entities.Ledgers.get({ bookId: "b1", txn: 420n })
      expect(got.txn).toBe(420n)
      expect(got.label).toBe("x")

      const updated = yield* db.entities.Ledgers.update({ bookId: "b1", txn: 420n }).set({
        label: "y",
      })
      expect(updated.label).toBe("y")
      expect(updated.txn).toBe(420n)

      yield* db.entities.Ledgers.delete({ bookId: "b1", txn: 999n })
      const left = yield* db.entities.Ledgers.primary({ bookId: "b1" }).collect()
      expect(left.map((r) => r.txn)).toEqual([100n, 420n])

      // Restore the fixture for the rest of this block.
      yield* db.entities.Ledgers.put({ bookId: "b1", txn: 999n, label: "x" })
      yield* db.entities.Ledgers.update({ bookId: "b1", txn: 420n }).set({ label: "x" })
    }).pipe(provideEnc),
  )

  it.effect("the Encoded side is NOT a public key input — it fails, naming the attribute", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: encEntities, tables: encTables })
      // `"420"` is the wire form. It was never a working input on a transformed
      // composite — pre-fix it silently returned ItemNotFound for a row that
      // exists. It is now rejected like any other wrong-typed key.
      const err = yield* db.entities.Ledgers.get({ bookId: "b1", txn: "420" } as never).pipe(
        Effect.flip,
      )
      expect(err._tag).toBe("ValidationError")
      expect(String((err as { cause?: unknown }).cause)).toMatch(/txn/)
    }).pipe(provideEnc),
  )

  it.effect("a nonsense key value still fails, naming the attribute", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: encEntities, tables: encTables })
      const err = yield* db.entities.Ledgers.get({ bookId: "b1", txn: {} } as never).pipe(
        Effect.flip,
      )
      expect(err._tag).toBe("ValidationError")
      expect(String((err as { cause?: unknown }).cause)).toMatch(/txn/)
    }).pipe(provideEnc),
  )

  // ----- internal paths still hold ENCODED records -----

  it.effect("retain / soft-delete / restore compose correctly from encoded records", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { RetainLedgers },
        tables: { EncTable },
      })

      yield* db.entities.RetainLedgers.put({ acctId: "a1", txn: 77n, note: "one" })

      // `update` recomposes the primary key from `newItem` — a wire-shaped
      // record merged from the stored item — and writes a retain snapshot in
      // the same transaction. Both go through the INTERNAL composePrimaryKey
      // path, not the public key boundary.
      yield* db.entities.RetainLedgers.update({ acctId: "a1", txn: 77n }).set({ note: "two" })
      const afterUpdate = yield* db.entities.RetainLedgers.get({ acctId: "a1", txn: 77n })
      expect(afterUpdate.note).toBe("two")

      const snapshots = yield* db.entities.RetainLedgers.versions({
        acctId: "a1",
        txn: 77n,
      }).collect()
      expect(snapshots.length).toBeGreaterThan(0)

      // Soft delete writes the tombstone from the encoded stored item; restore
      // recomposes every key from that encoded record.
      yield* db.entities.RetainLedgers.delete({ acctId: "a1", txn: 77n })
      const tombstone = yield* db.entities.RetainLedgers.deleted.get({ acctId: "a1", txn: 77n })
      expect(tombstone.txn).toBe(77n)

      const restored = yield* db.entities.RetainLedgers.restore({ acctId: "a1", txn: 77n })
      expect(restored.txn).toBe(77n)
      expect(restored.note).toBe("two")

      // The restored row is reachable by its composed key again.
      const live = yield* db.entities.RetainLedgers.get({ acctId: "a1", txn: 77n })
      expect(live.note).toBe("two")
    }).pipe(provideEnc),
  )

  it.effect("an operand that cannot be encoded is refused (EDD-9050)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: encEntities, tables: encTables })
      expect(() =>
        (
          db.entities.Ledgers.primary({ bookId: "b1" }) as never as {
            where: (fn: (t: never, ops: never) => unknown) => unknown
          }
        ).where((t: never, ops: never) =>
          (ops as { eq: (f: unknown, v: unknown) => unknown }).eq(
            (t as unknown as { txn: unknown }).txn,
            "not-a-number",
          ),
        ),
      ).toThrow(/EDD-9050/)
    }).pipe(provideEnc),
  )
})

// ---------------------------------------------------------------------------
// Composite key form — mixed-width ordering across every composite shape.
//
// The rule: compose from the Encoded form, EXCEPT when the domain type is
// numeric (number / bigint) and the encoded form is a string — then compose
// from the numeric Type form so `serializeValue` pads it.
//
// Values are 5 / 42 / 100 (and equivalently spaced dates) ON PURPOSE. An
// earlier fixture used equal-width values (100/420/999), under which
// lexicographic and numeric order coincide — which is exactly why the suite
// stayed green while `BigIntFromString` keys were being stored unpadded and
// `gte(42n)` was returning 42 and 5 instead of 42 and 100.
// ---------------------------------------------------------------------------

const kfSchema = DynamoSchema.make({ name: "kf", version: 1 })
const kfTableName = `kf-test-${Date.now()}`

const KF_VALUES = [5, 42, 100] as const
const kfDate = (n: number) => new Date(Date.UTC(2026, 0, 1, 0, 0, n))

class Metric extends Schema.Class<Metric>("Metric")({
  boxId: Schema.String,
  txn: Schema.BigIntFromString, // Type bigint, Encoded string → TYPE side
  num: Schema.Number, // Type number, Encoded number → encoded
  epoch: DynamoModel.DateEpochMs, // Type DateTime, Encoded number → encoded
  iso: Schema.Date, // Type Date, Encoded ISO string → encoded
}) {}

const Metrics = Entity.make({
  model: Metric,
  entityType: "Metric",
  primaryKey: {
    pk: { field: "pk", composite: ["boxId"] },
    sk: { field: "sk", composite: ["txn"] },
  },
  indexes: {
    byNum: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["boxId"] },
      sk: { field: "gsi1sk", composite: ["num"] },
    },
    byEpoch: {
      name: "gsi2",
      pk: { field: "gsi2pk", composite: ["boxId"] },
      sk: { field: "gsi2sk", composite: ["epoch"] },
    },
    byIso: {
      name: "gsi3",
      pk: { field: "gsi3pk", composite: ["boxId"] },
      sk: { field: "gsi3sk", composite: ["iso"] },
    },
  },
})

// #111 additions — the multi-item write paths, the aggregate list path, a
// renamed field, and a uniqueness sentinel guard.
class KfReading extends Schema.Class<KfReading>("KfReading")({
  device: Schema.String,
  takenAt: DynamoModel.DateEpochMs,
  value: Schema.Number,
}) {}

const KfReadings = Entity.make({
  model: KfReading,
  entityType: "KfReading",
  primaryKey: {
    pk: { field: "pk", composite: ["device"] },
    sk: { field: "sk", composite: ["takenAt"] },
  },
})

class KfRenamed extends Schema.Class<KfRenamed>("KfRenamed")({
  rid: Schema.String,
  label: Schema.String,
}) {}

const KfRenameds = Entity.make({
  model: DynamoModel.configure(KfRenamed, { label: { field: "lbl" } }),
  entityType: "KfRenamed",
  primaryKey: { pk: { field: "pk", composite: ["rid"] }, sk: { field: "sk", composite: [] } },
})

class KfCustom extends Schema.Class<KfCustom>("KfCustom")({
  cid: Schema.String,
  email: Schema.String,
}) {}

const KfCustomPk = Entity.make({
  model: KfCustom,
  entityType: "KfCustomPk",
  primaryKey: { pk: { field: "pk", composite: ["cid"] }, sk: { field: "sk", composite: [] } },
  unique: { email: ["email"] },
})

class KfLedger extends Schema.Class<KfLedger>("KfLedger")({
  book: Schema.String,
  // The composite this fixture was written for: numeric Type, string Encoded.
  // `serializeValue` pads it to 38 on the write side, so `list`'s old
  // `String(v)` SK prefix looked for `5` where `000...0005` is stored (#111).
  // It briefly had to be weakened to `Schema.Number` because the aggregate
  // write path stored Type-side values and assembly could not decode them back;
  // that is fixed here, so the intended shape is restored.
  seq: Schema.BigIntFromString,
  title: Schema.String,
}) {}

const KfTable = Table.make({
  schema: kfSchema,
  entities: { Metrics, KfReadings, KfRenameds, KfCustomPk },
})

const KfLedgers = Aggregate.make(KfLedger, {
  table: KfTable,
  schema: kfSchema,
  pk: { field: "pk", composite: ["book", "seq"] },
  // No `collection.index` — assembly runs against the base table (#93), so the
  // fixture needs only the list GSI, which `Metrics` already provisions.
  collection: { name: "kfledger" },
  list: {
    index: "gsi1",
    name: "kfledgerlist",
    pk: { field: "gsi1pk", composite: ["book"] },
    sk: { field: "gsi1sk", composite: ["seq"] },
  },
  root: { entityType: "KfLedgerItem" },
  edges: {},
})

const KfLayer = Layer.mergeAll(ClientLayer, KfTable.layer({ name: kfTableName }))
const provideKf = Effect.provide(KfLayer)
const kfEntities = { Metrics, KfReadings, KfRenameds, KfCustomPk }
const kfTables = { KfTable }

describeConnected("composite key form — mixed-width ordering", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: kfTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(KfTable),
        })
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })
        for (const n of KF_VALUES) {
          yield* db.entities.Metrics.put({
            boxId: "b1",
            txn: BigInt(n),
            num: n,
            epoch: DateTime.makeUnsafe(kfDate(n).toISOString()),
            iso: kfDate(n),
          })
        }
      }).pipe(provideKf, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: kfTableName })
      }).pipe(
        provideKf,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("a numeric-Type/string-Encoded composite is stored PADDED", () =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const raw = yield* client.scan({ TableName: kfTableName })
      const sks = (raw.Items ?? []).map((i) => (i.sk as { S?: string } | undefined)?.S ?? "").sort()
      // Pre-fix: #txn_100, #txn_42, #txn_5 — unpadded, so 100 < 42 < 5.
      expect(sks).toEqual([
        `$kf#v1#metric#txn_${"5".padStart(38, "0")}`,
        `$kf#v1#metric#txn_${"42".padStart(38, "0")}`,
        `$kf#v1#metric#txn_${"100".padStart(38, "0")}`,
      ])
    }).pipe(provideKf),
  )

  it.effect("ascending values come back ascending on every composite shape", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })

      // Pre-fix this one returned 100, 42, 5.
      const byTxn = yield* db.entities.Metrics.primary({ boxId: "b1" }).collect()
      expect(byTxn.map((r) => r.txn)).toEqual([5n, 42n, 100n])

      const byNum = yield* db.entities.Metrics.byNum({ boxId: "b1" }).collect()
      expect(byNum.map((r) => r.num)).toEqual([5, 42, 100])

      const byEpoch = yield* db.entities.Metrics.byEpoch({ boxId: "b1" }).collect()
      expect(byEpoch.map((r) => r.num)).toEqual([5, 42, 100])

      const byIso = yield* db.entities.Metrics.byIso({ boxId: "b1" }).collect()
      expect(byIso.map((r) => r.num)).toEqual([5, 42, 100])
    }).pipe(provideKf),
  )

  it.effect("gte / lte / between return the correct rows on each shape", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })

      // THE reported case: pre-fix this returned 42 and 5.
      const txnGte = yield* db.entities.Metrics.primary({ boxId: "b1" })
        .where((t, ops) => ops.gte(t.txn, 42n))
        .collect()
      expect(txnGte.map((r) => r.txn)).toEqual([42n, 100n])

      const txnLte = yield* db.entities.Metrics.primary({ boxId: "b1" })
        .where((t, ops) => ops.lte(t.txn, 42n))
        .collect()
      expect(txnLte.map((r) => r.txn)).toEqual([5n, 42n])

      const txnBetween = yield* db.entities.Metrics.primary({ boxId: "b1" })
        .where((t, ops) => ops.between(t.txn, 42n, 100n))
        .collect()
      expect(txnBetween.map((r) => r.txn)).toEqual([42n, 100n])

      const numGte = yield* db.entities.Metrics.byNum({ boxId: "b1" })
        .where((t, ops) => ops.gte(t.num, 42))
        .collect()
      expect(numGte.map((r) => r.num)).toEqual([42, 100])

      const epochGte = yield* db.entities.Metrics.byEpoch({ boxId: "b1" })
        .where((t, ops) => ops.gte(t.epoch, DateTime.makeUnsafe(kfDate(42).toISOString())))
        .collect()
      expect(epochGte.map((r) => r.num)).toEqual([42, 100])

      const isoBetween = yield* db.entities.Metrics.byIso({ boxId: "b1" })
        .where((t, ops) => ops.between(t.iso, kfDate(5), kfDate(42)))
        .collect()
      expect(isoBetween.map((r) => r.num)).toEqual([5, 42])
    }).pipe(provideKf),
  )

  it.effect("get round-trips every value after the format change", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })
      for (const n of KF_VALUES) {
        const got = yield* db.entities.Metrics.get({ boxId: "b1", txn: BigInt(n) })
        expect(got.txn).toBe(BigInt(n))
        expect(got.num).toBe(n)
      }
    }).pipe(provideKf),
  )

  // -------------------------------------------------------------------------
  // #111 — the multi-item write paths must spell keys exactly as `Entity.put`.
  // Each of these composed a DIFFERENT key before the fix, so a row written
  // through one API was invisible to every accessor of the other.
  // -------------------------------------------------------------------------

  describe("multi-item write paths spell keys the same way (#111)", () => {
    it.effect("Batch.get / transactGet / Batch.write(delete) round-trip a DateEpochMs key", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })
        const at = DateTime.makeUnsafe(1767225600000)

        yield* db.entities.KfReadings.put({ device: "d1", takenAt: at, value: 1 })

        // Pre-fix both returned [null] — the raw DateTime never reached epoch form.
        const [viaBatch] = yield* Batch.get([KfReadings.get({ device: "d1", takenAt: at })])
        expect(viaBatch?.value).toBe(1)
        const [viaTransact] = yield* Transaction.transactGet([
          KfReadings.get({ device: "d1", takenAt: at }),
        ])
        expect(viaTransact?.value).toBe(1)

        // Pre-fix this reported success while the row remained.
        yield* Batch.write([KfReadings.delete({ device: "d1", takenAt: at })])
        const gone = yield* KfReadings.get({ device: "d1", takenAt: at })
          .asEffect()
          .pipe(
            Effect.map(() => "exists"),
            Effect.catchTag("ItemNotFound", () => Effect.succeed("not found")),
          )
        expect(gone).toBe("not found")
      }).pipe(provideKf),
    )

    it.effect("Transaction.check evaluates against the row it names", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })
        const at = DateTime.makeUnsafe(1767225600001)
        yield* db.entities.KfReadings.put({ device: "d2", takenAt: at, value: 7 })

        // Pre-fix this COMMITTED: the guard was evaluated against a key that did
        // not exist, so `attribute_not_exists` was vacuously true.
        const err = yield* Transaction.transactWrite([
          Transaction.check(
            KfReadings.get({ device: "d2", takenAt: at }),
            Expression.condition({ attributeNotExists: "pk" }),
          ),
        ]).pipe(Effect.flip)
        expect(err._tag).toBe("TransactionCancelled")

        // ...and the positive form commits.
        yield* Transaction.transactWrite([
          Transaction.check(
            KfReadings.get({ device: "d2", takenAt: at }),
            Expression.condition({ attributeExists: "pk" }),
          ),
        ])
      }).pipe(provideKf),
    )

    it.effect("a transactWrite([delete, put]) move does not duplicate the row", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })
        const from = DateTime.makeUnsafe(1767225600002)
        const to = DateTime.makeUnsafe(1767225600003)
        yield* db.entities.KfReadings.put({ device: "d3", takenAt: from, value: 1 })

        yield* Transaction.transactWrite([
          db.entities.KfReadings.delete({ device: "d3", takenAt: from }),
          db.entities.KfReadings.put({ device: "d3", takenAt: to, value: 2 }),
        ])

        // Pre-fix the delete missed its target and the put landed → two rows.
        const rows = yield* db.entities.KfReadings.primary({ device: "d3" }).collect()
        expect(rows).toHaveLength(1)
        expect(rows[0]!.value).toBe(2)
      }).pipe(provideKf),
    )

    it.effect("a renamed field is stored identically by put and transactWrite", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })
        const client = yield* DynamoClient

        yield* db.entities.KfRenameds.put({ rid: "r1", label: "viaEntity" })
        yield* Transaction.transactWrite([
          db.entities.KfRenameds.put({ rid: "r2", label: "viaTransact" }),
        ])

        const attrsOf = (rid: string) =>
          client
            .getItem({
              TableName: kfTableName,
              Key: { pk: { S: `$kf#v1#kfrenamed#rid_${rid}` }, sk: { S: "$kf#v1#kfrenamed" } },
            })
            .pipe(Effect.map((r) => Object.keys(r.Item ?? {}).sort()))

        // Pre-fix: put wrote `lbl`, transactWrite wrote `label`.
        const viaEntity = yield* attrsOf("r1")
        const viaTransact = yield* attrsOf("r2")
        expect(viaTransact).toEqual(viaEntity)
        expect(viaTransact).toContain("lbl")
        expect(viaTransact).not.toContain("label")

        // Both are readable through the decode path.
        expect((yield* db.entities.KfRenameds.get({ rid: "r2" })).label).toBe("viaTransact")
      }).pipe(provideKf),
    )

    it.effect("select and filter on a renamed field use its stored name (#133)", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })
        yield* db.entities.KfRenameds.put({ rid: "rs1", label: "renamed" })
        const q = () => db.entities.KfRenameds.primary({ rid: "rs1" })
        expect(yield* q().select(["label"]).collect()).toEqual([{ label: "renamed" }])
        expect(
          yield* q()
            .select((t) => [t.label])
            .collect(),
        ).toEqual([{ label: "renamed" }])
        expect((yield* q().filter({ label: "renamed" }).collect()).map((r) => r.rid)).toEqual([
          "rs1",
        ])
        expect(
          (yield* q()
            .filter((t, { eq }) => eq(t.label, "renamed"))
            .collect()).map((r) => r.rid),
        ).toEqual(["rs1"])
        expect(yield* q().filter({ label: "other" }).collect()).toEqual([])
      }).pipe(provideKf),
    )

    it.effect("the uniqueness sentinel guard names the configured pk attribute", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })
        yield* db.entities.KfCustomPk.put({ cid: "c1", email: "a@x.io" })

        const err = yield* db.entities.KfCustomPk.put({ cid: "c2", email: "a@x.io" })
          .asEffect()
          .pipe(Effect.flip)
        expect(err._tag).toBe("UniqueConstraintViolation")

        // ...and through the transact path, which emits the same guarded sentinel.
        const txErr = yield* Transaction.transactWrite([
          db.entities.KfCustomPk.put({ cid: "c3", email: "a@x.io" }),
        ]).pipe(Effect.flip)
        expect(txErr._tag).toBe("UniqueConstraintViolation")
      }).pipe(provideKf),
    )

    it.effect("an aggregate's list() finds the rows its create() wrote", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: kfEntities,
          aggregates: { KfLedgers },
          tables: kfTables,
        })

        for (const n of KF_VALUES) {
          yield* db.aggregates.KfLedgers.create({
            book: "b1",
            seq: String(n),
            title: `t-${n}`,
          } as never)
        }

        // Pre-fix `list` composed its GSI key from the RAW filter and built the
        // SK prefix with `String(v)`, while `create` composed through the key
        // form and padded via `serializeValue`. The query matched nothing and
        // every aggregate was dropped from the result with no error at all.
        const listed = yield* db.aggregates.KfLedgers.list({ book: "b1" })
        const seqs = listed.data
          .map((l) => (l as unknown as { seq: bigint }).seq)
          .sort((a, b) => (a < b ? -1 : 1))
        expect(seqs).toEqual(KF_VALUES.map((n) => BigInt(n)).sort((a, b) => (a < b ? -1 : 1)))
        // Typed correctly — a bigint, not the stored string.
        expect(typeof seqs[0]).toBe("bigint")

        // A filter that reaches the SK prefix — the `String(v)` path, which
        // looked for `5` where the padded 38-wide spelling is stored.
        const one = yield* db.aggregates.KfLedgers.list({
          book: "b1",
          seq: BigInt(KF_VALUES[0]!),
        })
        expect(one.data).toHaveLength(1)
        expect((one.data[0] as unknown as { title: string }).title).toBe(`t-${KF_VALUES[0]}`)
      }).pipe(provideKf),
    )
  })

  // -------------------------------------------------------------------------
  // #108 — the bound-client read descriptors must compose the SAME key.
  //
  // `db.entities.X.get(key)` now returns a `BoundGet` the read paths can
  // unwrap. `txn` is `Schema.BigIntFromString` — Type `bigint`, Encoded
  // `string` — precisely the shape whose key form used to diverge between
  // APIs (#111): the stored SK holds the 38-wide padded spelling, so anything
  // that composes from the raw value looks up a key that does not exist and
  // reports "not found" against a row that is right there.
  // -------------------------------------------------------------------------

  describe("bound-client get descriptors reach the read paths (#108)", () => {
    it.effect("Batch.get and transactGet accept db.entities.X.get on a transformed key", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })

        const [a, b, missing] = yield* Batch.get([
          db.entities.Metrics.get({ boxId: "b1", txn: 42n }),
          db.entities.Metrics.get({ boxId: "b1", txn: 100n }),
          db.entities.Metrics.get({ boxId: "b1", txn: 7n }),
        ])
        expect(a?.num).toBe(42)
        expect(b?.num).toBe(100)
        expect(missing).toBeUndefined()

        const [t1, t2] = yield* Transaction.transactGet([
          db.entities.Metrics.get({ boxId: "b1", txn: 5n }),
          db.entities.Metrics.get({ boxId: "b1", txn: 42n }),
        ])
        expect(t1?.num).toBe(5)
        expect(t2?.num).toBe(42)

        // Bound and unbound descriptors for the SAME key must agree exactly —
        // in separate requests, because BatchGetItem rejects duplicate keys.
        const [viaBound] = yield* Batch.get([db.entities.Metrics.get({ boxId: "b1", txn: 5n })])
        const [viaUnbound] = yield* Batch.get([Metrics.get({ boxId: "b1", txn: 5n })])
        expect(viaBound?.num).toBe(5)
        expect(viaUnbound?.num).toBe(5)

        // And a mixed request over DISTINCT keys resolves both halves.
        const [mixedBound, mixedUnbound] = yield* Batch.get([
          db.entities.Metrics.get({ boxId: "b1", txn: 5n }),
          Metrics.get({ boxId: "b1", txn: 100n }),
        ])
        expect(mixedBound?.num).toBe(5)
        expect(mixedUnbound?.num).toBe(100)
      }).pipe(provideKf),
    )

    it.effect("Transaction.check on a bound get guards the row it names", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })

        // `attribute_not_exists` against a row that DOES exist must cancel. A
        // mis-composed key makes this vacuously true and the transaction
        // commits — the silent failure this whole section exists for.
        const err = yield* Transaction.transactWrite([
          Transaction.check(
            db.entities.Metrics.get({ boxId: "b1", txn: 42n }),
            Expression.condition({ attributeNotExists: "pk" }),
          ),
        ]).pipe(Effect.flip)
        expect(err._tag).toBe("TransactionCancelled")

        // The inverse condition on the same key commits.
        yield* Transaction.transactWrite([
          Transaction.check(
            db.entities.Metrics.get({ boxId: "b1", txn: 42n }),
            Expression.condition({ attributeExists: "pk" }),
          ),
        ])
      }).pipe(provideKf),
    )

    it.effect("get() is still an Effect against real DynamoDB", () =>
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: kfEntities, tables: kfTables })

        const num = yield* db.entities.Metrics.get({ boxId: "b1", txn: 42n }).pipe(
          Effect.map((m) => m.num),
        )
        expect(num).toBe(42)

        const outcome = yield* db.entities.Metrics.get({ boxId: "b1", txn: 7n }).pipe(
          Effect.map(() => "found" as const),
          Effect.catchTag("ItemNotFound", () => Effect.succeed("not found" as const)),
        )
        expect(outcome).toBe("not found")
      }).pipe(provideKf),
    )
  })
})

// NOTE: aggregate coverage for the key-form rule is UNIT-level
// (`test/AggregateKeyEncoding.test.ts`): padded bigint, mixed-width ordering,
// and the `DateEpochMs` ISO -> epoch change. A connected aggregate fixture on a
// `Schema.BigIntFromString` root composite cannot be written today —
// `Aggregate.create` fails re-encoding the assembled root ("Expected string at
// [\"txn\"]") on the real write path, before any key is composed. That is a
// pre-existing aggregate encode defect, independent of this rule, and is
// reported separately.

// ---------------------------------------------------------------------------
// S1 — key-form must hold across EVERY composition site, not just `put`.
//
// `composeAllKeys` (put) normalised while `composeGsiKeysForUpdatePolicyAware`
// (update) did not, so an `update()` rewrote a padded `gsi1pk` back to its
// unpadded form and evicted the row from its own GSI. Mixed widths (5/42/100)
// throughout — equal-width values hide exactly this.
//
// Covers the canonical GSI-composite shapes from CLAUDE.md against a
// transformed composite: multi-writer, PK-composites-only, hierarchical, hole
// pattern, all-mutable, and empty-composite half.
// ---------------------------------------------------------------------------

const s1Schema = DynamoSchema.make({ name: "s1", version: 1 })
const s1TableName = `s1-test-${Date.now()}`
const S1_VALUES = [5, 42, 100] as const

class S1Row extends Schema.Class<S1Row>("S1Row")({
  acct: Schema.String,
  txn: Schema.BigIntFromString,
  region: Schema.optional(Schema.String),
  site: Schema.optional(Schema.String),
  status: Schema.optional(Schema.String),
  binding: Schema.optional(Schema.String),
  note: Schema.String,
}) {}

const s1Indexes = {
  // Shape 5 — all composites mutable, and the S1 reproducer.
  byTxn: {
    name: "gsi1",
    pk: { field: "gsi1pk", composite: ["txn"] },
    sk: { field: "gsi1sk", composite: ["acct"] },
  },
  // Shape 2 — PK-composites-only (both halves are primary key composites).
  byAcctTxn: {
    name: "gsi2",
    pk: { field: "gsi2pk", composite: ["acct"] },
    sk: { field: "gsi2sk", composite: ["txn"] },
  },
  // Shape 3 — hierarchical, with the transformed composite as the leaf.
  byHier: {
    name: "gsi3",
    pk: { field: "gsi3pk", composite: ["acct"] },
    sk: { field: "gsi3sk", composite: ["region", "site", "txn"] },
  },
  // Shape 1 + 4 — multi-writer / hole pattern: an optional leading composite
  // with the transformed composite trailing it.
  byStatus: {
    name: "gsi4",
    pk: { field: "gsi4pk", composite: ["acct"] },
    sk: { field: "gsi4sk", composite: ["status", "txn"] },
  },
  // Shape 6 — empty-composite half, transformed composite on the PK half.
  byBinding: {
    name: "gsi5",
    pk: { field: "gsi5pk", composite: ["txn"] },
    sk: { field: "gsi5sk", composite: [] },
  },
} as const

const S1Rows = Entity.make({
  model: S1Row,
  entityType: "S1Row",
  primaryKey: {
    pk: { field: "pk", composite: ["acct"] },
    sk: { field: "sk", composite: ["txn"] },
  },
  indexes: s1Indexes,
})

// Retain + soft-delete variant, for the lifecycle round-trips.
const S1Retained = Entity.make({
  model: S1Row,
  entityType: "S1Retained",
  primaryKey: {
    pk: { field: "pk", composite: ["acct"] },
    sk: { field: "sk", composite: ["txn"] },
  },
  indexes: { byTxn: s1Indexes.byTxn },
  versioned: { retain: true },
  softDelete: true,
})

const S1Table = Table.make({ schema: s1Schema, entities: { S1Rows, S1Retained } })
const S1Layer = Layer.mergeAll(ClientLayer, S1Table.layer({ name: s1TableName }))
const provideS1 = Effect.provide(S1Layer)
const s1Entities = { S1Rows, S1Retained }
const s1Tables = { S1Table }

describeConnected("key form holds across every composition site (S1)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: s1TableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(S1Table),
        })
      }).pipe(provideS1, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: s1TableName })
      }).pipe(
        provideS1,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("S1: update() must not evict the row from its own GSI", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: s1Entities, tables: s1Tables })
      for (const n of S1_VALUES) {
        yield* db.entities.S1Rows.put({ acct: "a1", txn: BigInt(n), note: "a" })

        const before = yield* db.entities.S1Rows.byTxn({ txn: BigInt(n) }).collect()
        expect(before.map((r) => r.txn)).toEqual([BigInt(n)])

        yield* db.entities.S1Rows.update({ acct: "a1", txn: BigInt(n) }).set({ note: "b" })

        // Pre-fix: 0 rows — the update rewrote gsi1pk unpadded.
        const after = yield* db.entities.S1Rows.byTxn({ txn: BigInt(n) }).collect()
        expect(after.map((r) => r.txn)).toEqual([BigInt(n)])
        expect(after[0]!.note).toBe("b")
      }
    }).pipe(provideS1),
  )

  it.effect("every canonical GSI shape survives an update", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: s1Entities, tables: s1Tables })
      yield* db.entities.S1Rows.put({
        acct: "a2",
        txn: 42n,
        region: "apac",
        site: "syd",
        status: "active",
        binding: "b-1",
        note: "a",
      })
      yield* db.entities.S1Rows.update({ acct: "a2", txn: 42n }).set({ note: "b" })

      // Shape 5 / S1, shape 2, shape 3, shapes 1+4, shape 6.
      expect(
        (yield* db.entities.S1Rows.byTxn({ txn: 42n }).collect()).some((r) => r.acct === "a2"),
      ).toBe(true)
      expect((yield* db.entities.S1Rows.byAcctTxn({ acct: "a2", txn: 42n }).collect()).length).toBe(
        1,
      )
      expect(
        (yield* db.entities.S1Rows.byHier({ acct: "a2", region: "apac", site: "syd" }).collect())
          .length,
      ).toBe(1)
      expect(
        (yield* db.entities.S1Rows.byStatus({ acct: "a2", status: "active" }).collect()).length,
      ).toBe(1)
      // gsi5 is keyed on `txn` alone, so other accounts share the partition.
      expect(
        (yield* db.entities.S1Rows.byBinding({ txn: 42n }).collect()).some((r) => r.acct === "a2"),
      ).toBe(true)
    }).pipe(provideS1),
  )

  it.effect("hole pattern: an absent leading composite does not corrupt the trailing one", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: s1Entities, tables: s1Tables })
      // `status` absent → byStatus cannot compose; byTxn must still hold.
      yield* db.entities.S1Rows.put({ acct: "a3", txn: 100n, note: "a" })
      yield* db.entities.S1Rows.update({ acct: "a3", txn: 100n }).set({ note: "b" })
      expect(
        (yield* db.entities.S1Rows.byTxn({ txn: 100n }).collect()).some((r) => r.acct === "a3"),
      ).toBe(true)
    }).pipe(provideS1),
  )

  it.effect("retain-enabled update keeps the GSI key, and versions round-trip", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: s1Entities, tables: s1Tables })
      yield* db.entities.S1Retained.put({ acct: "r1", txn: 42n, note: "a" })
      yield* db.entities.S1Retained.update({ acct: "r1", txn: 42n }).set({ note: "b" })

      const found = yield* db.entities.S1Retained.byTxn({ txn: 42n }).collect()
      expect(found.some((r) => r.acct === "r1")).toBe(true)

      const versions = yield* db.entities.S1Retained.versions({ acct: "r1", txn: 42n }).collect()
      expect(versions.length).toBeGreaterThan(0)

      // getVersion — pre-fix returned ItemNotFound for a row that exists.
      const v1 = yield* db.entities.S1Retained.getVersion({ acct: "r1", txn: 42n }, 1)
      expect(v1.txn).toBe(42n)
    }).pipe(provideS1),
  )

  it.effect("soft-delete get / restore / purge round-trip on a transformed composite", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: s1Entities, tables: s1Tables })
      yield* db.entities.S1Retained.put({ acct: "r2", txn: 100n, note: "a" })
      yield* db.entities.S1Retained.delete({ acct: "r2", txn: 100n })

      // deleted.get — pre-fix ItemNotFound while deleted.list found the row.
      const tomb = yield* db.entities.S1Retained.deleted.get({ acct: "r2", txn: 100n })
      expect(tomb.txn).toBe(100n)
      const listed = yield* db.entities.S1Retained.deleted.list({ acct: "r2", txn: 100n }).collect()
      expect(listed.length).toBeGreaterThan(0)

      const restored = yield* db.entities.S1Retained.restore({ acct: "r2", txn: 100n })
      expect(restored.txn).toBe(100n)
      expect((yield* db.entities.S1Retained.byTxn({ txn: 100n }).collect()).length).toBeGreaterThan(
        0,
      )

      // purge — pre-fix reported success and deleted nothing.
      yield* db.entities.S1Retained.purge({ acct: "r2", txn: 100n })
      const left = yield* db.entities.S1Retained.primary({ acct: "r2", txn: 100n }).collect()
      expect(left).toEqual([])
    }).pipe(provideS1),
  )

  it.effect("transact and batch puts compose the SAME key as entity put (#111)", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: s1Entities, tables: s1Tables })
      const client = yield* DynamoClient

      const sksOf = (acct: string) =>
        client
          .query({
            TableName: s1TableName,
            KeyConditionExpression: "#pk = :pk",
            ExpressionAttributeNames: { "#pk": "pk" },
            ExpressionAttributeValues: { ":pk": { S: `$s1#v1#s1row#acct_${acct}` } },
          })
          .pipe(Effect.map((r) => (r.Items ?? []).map((i) => i.sk?.S)))

      for (const n of S1_VALUES) {
        yield* db.entities.S1Rows.put({ acct: `kf-e-${n}`, txn: BigInt(n), note: "entity" })
        yield* Transaction.transactWrite([
          db.entities.S1Rows.put({ acct: `kf-t-${n}`, txn: BigInt(n), note: "transact" }),
        ])
        yield* Batch.write([
          db.entities.S1Rows.put({ acct: `kf-b-${n}`, txn: BigInt(n), note: "batch" }),
        ])

        const viaEntity = yield* sksOf(`kf-e-${n}`)
        const viaTransact = yield* sksOf(`kf-t-${n}`)
        const viaBatch = yield* sksOf(`kf-b-${n}`)

        // Pre-fix the transact/batch rows carried `txn_5` where the entity row
        // carried the padded spelling, so each write produced an orphan row no
        // accessor could read.
        expect(viaEntity).toHaveLength(1)
        expect(viaTransact).toEqual(viaEntity)
        expect(viaBatch).toEqual(viaEntity)
        expect(viaEntity[0]).toContain(String(n).padStart(38, "0"))
      }

      // ...and the rows are reachable through the typed accessors.
      for (const n of S1_VALUES) {
        expect(
          (yield* db.entities.S1Rows.byTxn({ txn: BigInt(n) }).collect()).map((r) => r.acct).sort(),
        ).toEqual(expect.arrayContaining([`kf-b-${n}`, `kf-e-${n}`, `kf-t-${n}`]))
      }
    }).pipe(provideS1),
  )
})

// ---------------------------------------------------------------------------
// Collections and vector search must compose keys with the SAME key form as
// the entity accessors and the write path.
//
// `db.collections.*` and `Collections.make()` passed the caller's raw record
// straight to `composePk`, and `BoundVectorQuery` did the same for its
// partition — so two accessors over one index, with the same values, disagreed.
// Mixed widths 5/42/100; the composites are a `DateEpochMs` (Type DateTime,
// Encoded number) and a `BigIntFromString` (Type bigint, Encoded string), the
// two shapes a plain `Schema.String` fixture cannot distinguish.
// ---------------------------------------------------------------------------

const cfSchema = DynamoSchema.make({ name: "cf", version: 1 })
const cfTableName = `cf-test-${Date.now()}`
const CF_VALUES = [5, 42, 100] as const
const cfAt = (n: number) =>
  DateTime.makeUnsafe(new Date(Date.UTC(2026, 0, 1, 0, 0, n)).toISOString())

class CfReading extends Schema.Class<CfReading>("CfReading")({
  siteId: Schema.String,
  takenAt: DynamoModel.DateEpochMs,
  txn: Schema.BigIntFromString,
  readingId: Schema.String,
}) {}

const CfReadings = Entity.make({
  model: CfReading,
  entityType: "CfReading",
  primaryKey: {
    pk: { field: "pk", composite: ["readingId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byWindow: {
      name: "gsi1",
      collection: "cfWindow",
      pk: { field: "gsi1pk", composite: ["siteId", "takenAt"] },
      sk: { field: "gsi1sk", composite: ["readingId"] },
    },
    byTxn: {
      name: "gsi2",
      collection: "cfLedger",
      pk: { field: "gsi2pk", composite: ["txn"] },
      sk: { field: "gsi2sk", composite: ["readingId"] },
    },
  },
})

const CfTable = Table.make({ schema: cfSchema, entities: { CfReadings } })
const CfExplicit = Collection.make("cfWindow", { CfReadings })
const CfLayer = Layer.mergeAll(ClientLayer, CfTable.layer({ name: cfTableName }))
const provideCf = Effect.provide(CfLayer)

describeConnected("collections and vector search share the entity key form", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: cfTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(CfTable),
        })
        const db = yield* DynamoClient.make({
          entities: { CfReadings },
          tables: { CfTable },
        })
        for (const n of CF_VALUES) {
          yield* db.entities.CfReadings.put({
            siteId: "s1",
            takenAt: cfAt(n),
            txn: BigInt(n),
            readingId: `r${n}`,
          })
        }
      }).pipe(provideCf, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: cfTableName })
      }).pipe(
        provideCf,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("entity accessor and collection accessor agree — DateEpochMs composite", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CfReadings }, tables: { CfTable } })
      for (const n of CF_VALUES) {
        const viaEntity = yield* db.entities.CfReadings.byWindow({
          siteId: "s1",
          takenAt: cfAt(n),
        }).collect()
        // Pre-fix: 0 rows — the collection composed the partition from the raw
        // `DateTime` while the row was written from the epoch key form.
        const viaCollection = yield* (
          db.collections as unknown as {
            cfWindow: (c: Record<string, unknown>) => {
              collect: () => Effect.Effect<{ CfReadings: ReadonlyArray<CfReading> }, never>
            }
          }
        )
          .cfWindow({ siteId: "s1", takenAt: cfAt(n) })
          .collect()
        expect(viaEntity.map((r) => r.readingId)).toEqual([`r${n}`])
        expect(viaCollection.CfReadings.map((r) => r.readingId)).toEqual([`r${n}`])
      }
    }).pipe(provideCf),
  )

  it.effect("entity accessor and collection accessor agree — BigIntFromString composite", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CfReadings }, tables: { CfTable } })
      for (const n of CF_VALUES) {
        const viaEntity = yield* db.entities.CfReadings.byTxn({ txn: BigInt(n) }).collect()
        const viaCollection = yield* (
          db.collections as unknown as {
            cfLedger: (c: Record<string, unknown>) => {
              collect: () => Effect.Effect<{ CfReadings: ReadonlyArray<CfReading> }, never>
            }
          }
        )
          .cfLedger({ txn: BigInt(n) })
          .collect()
        expect(viaEntity.map((r) => r.readingId)).toEqual([`r${n}`])
        expect(viaCollection.CfReadings.map((r) => r.readingId)).toEqual([`r${n}`])
      }
    }).pipe(provideCf),
  )

  it.effect("explicit Collections.make() composes the same partition", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CfReadings }, tables: { CfTable } })
      for (const n of CF_VALUES) {
        const q = (
          CfExplicit as unknown as {
            query: (c: Record<string, unknown>) => Query.Query<unknown>
          }
        ).query({ siteId: "s1", takenAt: cfAt(n) })
        const rows = yield* db.entities.CfReadings.collect(q)
        expect(rows.length).toBe(1)
      }
    }).pipe(provideCf),
  )
})

// ===========================================================================
// Aggregate attribute encoding — every transformed shape round-trips (#116)
// ===========================================================================
//
// Aggregates build their rows from the schema-DECODED domain object and
// marshalled the Type value straight to DynamoDB, while the read path decodes
// with the same schema. For any transformed field the two disagreed: a
// `BigIntFromString` landed as `{N:"5"}` and assembly's decode rejected it, so
// the aggregate could not round-trip at all. Dates were noticed first (#72) and
// got a date-only pass; this pins the general rule for every shape, on the root,
// on a `many` edge element, and on a ref-hydrated edge.

const aeSchema = DynamoSchema.make({ name: "ae", version: 1 })
const aeTableName = `ae-test-${Date.now()}`

/** The referenced entity — its own schema must encode a hydrated ref. */
class AeMaker extends Schema.Class<AeMaker>("AeMaker")({
  makerId: Schema.String,
  // Transformed field INSIDE the ref target.
  founded: Schema.BigIntFromString,
}) {}

const AeMakers = PureEntity.make({
  model: DynamoModel.configure(AeMaker, { makerId: { identifier: true } }),
  entityType: "AeMaker",
  primaryKey: { pk: { field: "pk", composite: ["makerId"] }, sk: { field: "sk", composite: [] } },
})

/** A `many` edge element carrying the full shape matrix. */
class AePart extends Schema.Class<AePart>("AePart")({
  // `id` (not `partId`): `extractRefIdentifiers` uses it as the element's SK
  // composite, so without it two `many` elements collide on one sort key.
  id: Schema.String,
  bigStr: Schema.BigIntFromString,
  numStr: Schema.NumberFromString,
  epoch: DynamoModel.DateEpochMs,
  plainDate: Schema.Date,
  dtUtc: Schema.DateTimeUtc,
  plainNum: Schema.Number,
  plainStr: Schema.String,
}) {}

const AeParts = PureEntity.make({
  model: DynamoModel.configure(AePart, { id: { identifier: true } }),
  entityType: "AePart",
  primaryKey: { pk: { field: "pk", composite: ["id"] }, sk: { field: "sk", composite: [] } },
})

/** A ref-hydrated `one` edge. */
class AeSupplier extends Schema.Class<AeSupplier>("AeSupplier")({
  supplierId: Schema.String,
  since: Schema.NumberFromString,
  maker: AeMaker.pipe(DynamoModel.ref),
}) {}

const AeSuppliers = PureEntity.make({
  model: DynamoModel.configure(AeSupplier, { supplierId: { identifier: true } }),
  entityType: "AeSupplier",
  primaryKey: {
    pk: { field: "pk", composite: ["supplierId"] },
    sk: { field: "sk", composite: [] },
  },
  refs: { maker: { entity: AeMakers } },
})

/** The aggregate root — same matrix again, at the root level. */
class AeMachine extends Schema.Class<AeMachine>("AeMachine")({
  machineId: Schema.String,
  bigStr: Schema.BigIntFromString,
  numStr: Schema.NumberFromString,
  epoch: DynamoModel.DateEpochMs,
  plainDate: Schema.Date,
  dtUtc: Schema.DateTimeUtc,
  plainNum: Schema.Number,
  plainStr: Schema.String,
  supplier: Schema.optionalKey(AeSupplier),
  parts: Schema.optionalKey(Schema.Array(AePart)),
}) {}

/** An untransformed model — its composed keys must be byte-identical. */
class AePlain extends Schema.Class<AePlain>("AePlain")({
  plainId: Schema.String,
  label: Schema.String,
  count: Schema.Number,
}) {}

const AeTable = Table.make({
  schema: aeSchema,
  entities: { AeMakers, AeParts, AeSuppliers },
})

const AeMachineAggregate = Aggregate.make(AeMachine, {
  table: AeTable,
  schema: aeSchema,
  pk: { field: "pk", composite: ["machineId"] },
  collection: { name: "aemachine" },
  root: { entityType: "AeMachineItem" },
  edges: {
    supplier: Aggregate.one("supplier", { entityType: "AeMachineSupplier", entity: AeSuppliers }),
    parts: Aggregate.many("parts", { entityType: "AeMachinePart", entity: AeParts }),
  },
})

const AePlainAggregate = Aggregate.make(AePlain, {
  table: AeTable,
  schema: aeSchema,
  pk: { field: "pk", composite: ["plainId"] },
  collection: { name: "aeplain" },
  root: { entityType: "AePlainItem" },
  edges: {},
})

const AeLayer = Layer.mergeAll(ClientLayer, AeTable.layer({ name: aeTableName }))
const provideAe = Effect.provide(AeLayer)
const aeAggregates = { AeMachineAggregate, AePlainAggregate }
const aeTables = { AeTable }

const AE_EPOCH_MS = 1767225600000
const AE_ISO = "2026-01-01T00:00:00.000Z"

describeConnected("aggregate attribute encoding round-trips every shape (#116)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: aeTableName,
          BillingMode: "PAY_PER_REQUEST",
          ...Table.definition(AeTable),
        })
      }).pipe(provideAe, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: aeTableName })
      }).pipe(
        provideAe,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  /** The full matrix, as the create input takes it (Encoded side). */
  const shapeInput = (id: string) => ({
    bigStr: "420",
    numStr: "3.5",
    epoch: AE_ISO,
    plainDate: AE_ISO,
    dtUtc: AE_ISO,
    plainNum: 7,
    plainStr: `s-${id}`,
  })

  /** Assert every shape came back with the right value AND the right type. */
  const expectShapes = (o: Record<string, unknown>) => {
    expect(o.bigStr).toBe(420n)
    expect(typeof o.bigStr).toBe("bigint")
    expect(o.numStr).toBe(3.5)
    expect(typeof o.numStr).toBe("number")
    expect(DateTime.toEpochMillis(o.epoch as DateTime.Utc)).toBe(AE_EPOCH_MS)
    expect((o.plainDate as Date).toISOString()).toBe(AE_ISO)
    expect(DateTime.toEpochMillis(o.dtUtc as DateTime.Utc)).toBe(AE_EPOCH_MS)
    expect(o.plainNum).toBe(7)
    expect(typeof o.plainNum).toBe("number")
    expect(typeof o.plainStr).toBe("string")
  }

  it.effect("every shape round-trips on the aggregate ROOT", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })

      yield* db.aggregates.AeMachineAggregate.create({
        machineId: "m-root",
        ...shapeInput("root"),
      } as never)

      // Pre-fix this threw `aggregate.assemble` with an Encoding issue: the
      // Type-side bigint was stored as `{N:"420"}` and `BigIntFromString`
      // rejected a number.
      const got = yield* db.aggregates.AeMachineAggregate.get({ machineId: "m-root" })
      expectShapes(got as unknown as Record<string, unknown>)
    }).pipe(provideAe),
  )

  it.effect("every shape round-trips on a MANY edge element", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })

      yield* db.aggregates.AeMachineAggregate.create({
        machineId: "m-many",
        ...shapeInput("many"),
        parts: [
          { id: "p-1", ...shapeInput("p1") },
          { id: "p-2", ...shapeInput("p2") },
        ],
      } as never)

      const got = (yield* db.aggregates.AeMachineAggregate.get({
        machineId: "m-many",
      })) as unknown as { parts: ReadonlyArray<Record<string, unknown>> }

      expect(got.parts).toHaveLength(2)
      for (const part of got.parts) expectShapes(part)
      expect(got.parts.map((p) => p.id).sort()).toEqual(["p-1", "p-2"])
    }).pipe(provideAe),
  )

  it.effect("a REF-hydrated edge encodes with the referenced entity's own schema", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { AeMakers },
        aggregates: aeAggregates,
        tables: aeTables,
      })

      yield* db.entities.AeMakers.put({ makerId: "mk-1", founded: 1897n })

      // The aggregate stores what the schema holds — the nested ref object, the
      // same shape write-time hydration would have produced on the entity path.
      yield* db.aggregates.AeMachineAggregate.create({
        machineId: "m-ref",
        ...shapeInput("ref"),
        supplier: {
          supplierId: "sup-1",
          since: "1999",
          maker: { makerId: "mk-1", founded: "1897" },
        },
      } as never)

      const got = (yield* db.aggregates.AeMachineAggregate.get({
        machineId: "m-ref",
      })) as unknown as {
        supplier: { supplierId: string; since: number; maker: { makerId: string; founded: bigint } }
      }

      // The edge's own transformed field...
      expect(got.supplier.since).toBe(1999)
      expect(typeof got.supplier.since).toBe("number")
      // ...and the field INSIDE the hydrated ref, which is only correct if the
      // ref was encoded with `AeMaker`'s schema rather than the aggregate's.
      expect(got.supplier.maker.makerId).toBe("mk-1")
      expect(got.supplier.maker.founded).toBe(1897n)
      expect(typeof got.supplier.maker.founded).toBe("bigint")
    }).pipe(provideAe),
  )

  it.effect("context values propagated onto edge rows are encoded once, the same way", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })
      const client = yield* DynamoClient

      yield* db.aggregates.AeMachineAggregate.create({
        machineId: "m-ctx",
        ...shapeInput("ctx"),
        parts: [{ id: "p-9", ...shapeInput("p9") }],
      } as never)

      // Same logical field must be stored the SAME way on every row of the
      // partition — the divergence class this whole line of work closes.
      const rows = yield* client.query({
        TableName: aeTableName,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: "$ae#v1#aemachine#m-ctx" } },
      })
      const bigStrs = (rows.Items ?? [])
        .filter((i) => i.bigStr !== undefined)
        .map((i) => JSON.stringify(i.bigStr))
      expect(bigStrs.length).toBeGreaterThan(1)
      expect(new Set(bigStrs).size).toBe(1)
      // ...and it is the ENCODED string form, which is what decode expects.
      expect(bigStrs[0]).toBe(JSON.stringify({ S: "420" }))
    }).pipe(provideAe),
  )

  it.effect("composed keys are byte-identical for an untransformed model", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })
      const client = yield* DynamoClient

      yield* db.aggregates.AePlainAggregate.create({
        plainId: "pl-1",
        label: "L",
        count: 3,
      } as never)

      // Encoding ATTRIBUTES must not move a single byte of a composed KEY.
      // Keys come from the assembled object through the key form, and this is
      // the value that spelling produces — pinned literally.
      const rows = yield* client.query({
        TableName: aeTableName,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: "$ae#v1#aeplain#pl-1" } },
      })
      expect(rows.Items ?? []).toHaveLength(1)
      const row = (rows.Items ?? [])[0]!
      expect(row.pk?.S).toBe("$ae#v1#aeplain#pl-1")
      expect(row.sk?.S).toBe("$ae#v1#aeplainitem")
      // Untransformed attributes are stored exactly as before.
      expect(row.label?.S).toBe("L")
      expect(row.count?.N).toBe("3")

      const got = yield* db.aggregates.AePlainAggregate.get({ plainId: "pl-1" })
      expect((got as unknown as { count: number }).count).toBe(3)
    }).pipe(provideAe),
  )

  // -------------------------------------------------------------------------
  // `aggregate.update` round-trips the same matrix (#116).
  //
  // Update re-decodes the MUTATED state, whose fields may hold either the domain
  // value the caller set or the wire value that came back from storage. That is
  // not date-specific, so `tolerantTransforms` now substitutes a tolerant
  // schema for EVERY leaf transform, not just dates.
  // -------------------------------------------------------------------------

  it.effect("update touching only an UNTRANSFORMED field round-trips every shape", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })
      const client = yield* DynamoClient

      yield* db.aggregates.AeMachineAggregate.create({
        machineId: "m-upd1",
        ...shapeInput("upd1"),
        parts: [{ id: "p-u1", ...shapeInput("pu1") }],
      } as never)

      // This is the case that used to fail: the mutation never touches a
      // transformed field, but `state` still CARRIES them in domain form, and
      // the re-decode rejected them before any item was built.
      yield* db.aggregates.AeMachineAggregate.update({ machineId: "m-upd1" }, ({ state }) => ({
        ...state,
        plainStr: "renamed",
      }))

      const got = (yield* db.aggregates.AeMachineAggregate.get({
        machineId: "m-upd1",
      })) as unknown as Record<string, unknown> & {
        parts: ReadonlyArray<Record<string, unknown>>
      }
      expect(got.plainStr).toBe("renamed")
      expectShapes({ ...got, plainStr: "x" })
      // The `many` edge survives the update untouched and still decodes.
      expectShapes(got.parts[0]!)

      // Stored bytes are the ENCODED form on every row, root and edge alike.
      const rows = yield* client.query({
        TableName: aeTableName,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: "$ae#v1#aemachine#m-upd1" } },
      })
      for (const row of rows.Items ?? []) {
        if (row.bigStr === undefined) continue
        expect(row.bigStr).toEqual({ S: "420" })
        expect(row.numStr).toEqual({ S: "3.5" })
        expect(row.epoch).toEqual({ N: String(AE_EPOCH_MS) })
        expect(row.plainDate).toEqual({ S: AE_ISO })
        expect(row.dtUtc).toEqual({ S: AE_ISO })
        expect(row.plainNum).toEqual({ N: "7" })
      }
    }).pipe(provideAe),
  )

  it.effect("update touching the TRANSFORMED field itself round-trips", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })
      const client = yield* DynamoClient

      yield* db.aggregates.AeMachineAggregate.create({
        machineId: "m-upd2",
        ...shapeInput("upd2"),
      } as never)

      yield* db.aggregates.AeMachineAggregate.update({ machineId: "m-upd2" }, ({ state }) => ({
        ...state,
        bigStr: 999n,
        numStr: 12.5,
        plainNum: 42,
      }))

      const got = (yield* db.aggregates.AeMachineAggregate.get({
        machineId: "m-upd2",
      })) as unknown as { bigStr: bigint; numStr: number; plainNum: number }
      expect(got.bigStr).toBe(999n)
      expect(typeof got.bigStr).toBe("bigint")
      expect(got.numStr).toBe(12.5)
      expect(typeof got.numStr).toBe("number")
      expect(got.plainNum).toBe(42)

      const rows = yield* client.query({
        TableName: aeTableName,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: "$ae#v1#aemachine#m-upd2" } },
      })
      const root = (rows.Items ?? [])[0]!
      expect(root.bigStr).toEqual({ S: "999" })
      expect(root.numStr).toEqual({ S: "12.5" })
      expect(root.plainNum).toEqual({ N: "42" })
    }).pipe(provideAe),
  )

  it.effect("update round-trips a MANY edge element and a REF-hydrated edge", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })

      yield* db.aggregates.AeMachineAggregate.create({
        machineId: "m-upd3",
        ...shapeInput("upd3"),
        supplier: {
          supplierId: "sup-3",
          since: "1999",
          maker: { makerId: "mk-3", founded: "1897" },
        },
        parts: [{ id: "p-a", ...shapeInput("pa") }],
      } as never)

      // Mutate INSIDE the many edge and inside the ref-hydrated edge.
      yield* db.aggregates.AeMachineAggregate.update({ machineId: "m-upd3" }, ({ state }) => ({
        ...state,
        parts: [new AePart({ ...(state as any).parts[0], bigStr: 777n })],
        supplier: new AeSupplier({
          ...(state as any).supplier,
          since: 2001,
          maker: new AeMaker({ ...(state as any).supplier.maker, founded: 1900n }),
        }),
      }))

      const got = (yield* db.aggregates.AeMachineAggregate.get({
        machineId: "m-upd3",
      })) as unknown as {
        parts: ReadonlyArray<Record<string, unknown>>
        supplier: { since: number; maker: { founded: bigint } }
      }
      expect(got.parts[0]!.bigStr).toBe(777n)
      expect(typeof got.parts[0]!.bigStr).toBe("bigint")
      expect(got.supplier.since).toBe(2001)
      expect(got.supplier.maker.founded).toBe(1900n)
      expect(typeof got.supplier.maker.founded).toBe("bigint")
    }).pipe(provideAe),
  )

  it.effect("keys stay byte-identical after an update", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })
      const client = yield* DynamoClient

      yield* db.aggregates.AePlainAggregate.create({
        plainId: "pl-upd",
        label: "L",
        count: 3,
      } as never)

      const keysOf = () =>
        client
          .query({
            TableName: aeTableName,
            KeyConditionExpression: "#pk = :pk",
            ExpressionAttributeNames: { "#pk": "pk" },
            ExpressionAttributeValues: { ":pk": { S: "$ae#v1#aeplain#pl-upd" } },
          })
          .pipe(Effect.map((r) => (r.Items ?? []).map((i) => `${i.pk?.S}|${i.sk?.S}`).sort()))

      const before = yield* keysOf()
      yield* db.aggregates.AePlainAggregate.update({ plainId: "pl-upd" }, ({ state }) => ({
        ...state,
        label: "L2",
      }))
      expect(yield* keysOf()).toEqual(before)
      expect(before).toEqual(["$ae#v1#aeplain#pl-upd|$ae#v1#aeplainitem"])
    }).pipe(provideAe),
  )

  // A tolerant decode must not become a LAX one. If this ever passes, the
  // substitution has stopped validating and every guarantee above is hollow.
  it.effect("a nonsense value is STILL rejected on the update path", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: {},
        aggregates: aeAggregates,
        tables: aeTables,
      })

      yield* db.aggregates.AeMachineAggregate.create({
        machineId: "m-bad",
        ...shapeInput("bad"),
      } as never)

      // Neither the wire form (a numeric string) nor the domain form (a bigint).
      const err = yield* db.aggregates.AeMachineAggregate.update(
        { machineId: "m-bad" },
        ({ state }) => ({ ...state, bigStr: "not-a-number" }) as never,
      ).pipe(Effect.flip)
      expect(err._tag).toBe("ValidationError")

      // A wrong-typed value for an untransformed field is still rejected too.
      const err2 = yield* db.aggregates.AeMachineAggregate.update(
        { machineId: "m-bad" },
        ({ state }) => ({ ...state, plainNum: "seven" }) as never,
      ).pipe(Effect.flip)
      expect(err2._tag).toBe("ValidationError")

      // ...and the stored row is untouched by either attempt.
      const got = (yield* db.aggregates.AeMachineAggregate.get({
        machineId: "m-bad",
      })) as unknown as Record<string, unknown>
      expectShapes(got)
    }).pipe(provideAe),
  )

  // NOTE — diff narrowing on a transformed model is asserted at the UNIT level
  // (`test/Aggregate.test.ts`, "skips write when nothing changed, on a
  // transformed model"), where the transactWrite call count is observable. A
  // connected test can only compare stored bytes, which look identical whether
  // or not the write was skipped.
})

// ---------------------------------------------------------------------------
// Aggregate.list — server-side filter, reverse, and the sharded branch (#104)
//
// `list` reads root items off a list GSI and then assembles each one with its
// own partition read (the N+1). Filtering in memory therefore paid a full
// assembly for every aggregate it then discarded, `limit` could not mean "this
// many MATCHING aggregates", and the sharded branch dropped `limit`/`cursor`
// on the floor. All three are asserted here against real DynamoDB, because the
// interesting behaviour — a `Limit` that bounds rows EXAMINED while the filter
// runs after — is exactly what a mock cannot reproduce.
// ---------------------------------------------------------------------------

const alSchema = DynamoSchema.make({ name: "agglist", version: 1 })
const alTableName = `agg-list-test-${Date.now()}`

/** A `many` edge element — `id` is what `extractRefIdentifiers` keys elements by. */
class AlLine extends Schema.Class<AlLine>("AlLine")({
  id: Schema.String,
  sku: Schema.String,
  qty: Schema.Number,
}) {}

class AlOrder extends Schema.Class<AlOrder>("AlOrder")({
  orderId: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["pending", "shipped"]),
  total: Schema.Number,
  lines: Schema.Array(AlLine),
}) {}

/** Anchors the table's primary key shape; the list GSI comes from the aggregate. */
class AlAnchor extends Schema.Class<AlAnchor>("AlAnchor")({
  anchorId: Schema.String,
}) {}

const AlAnchors = Entity.make({
  model: AlAnchor,
  entityType: "AlAnchor",
  primaryKey: { pk: { field: "pk", composite: ["anchorId"] }, sk: { field: "sk", composite: [] } },
})

const AlTable = Table.make({ schema: alSchema, entities: { AlAnchors } })

const AlOrderAggregate = Aggregate.make(AlOrder, {
  table: AlTable,
  schema: alSchema,
  pk: { field: "pk", composite: ["orderId"] },
  // No collection index — assembly runs against the base table.
  collection: { name: "alorder" },
  list: {
    index: "gsi1",
    name: "alorderlist",
    pk: { field: "gsi1pk", composite: ["customerId"] },
    sk: { field: "gsi1sk", composite: ["orderId"] },
  },
  root: { entityType: "AlOrderItem" },
  edges: {
    lines: Aggregate.many("lines", { entityType: "AlOrderLine" }),
  },
})

/** Same shape, sharded — the branch that used to discard `limit` and `cursor`. */
const AlShardedAggregate = Aggregate.make(AlOrder, {
  table: AlTable,
  schema: alSchema,
  pk: { field: "pk", composite: ["orderId"] },
  collection: { name: "alsharded" },
  list: {
    index: "gsi1",
    name: "alshardedlist",
    pk: { field: "gsi1pk", composite: ["customerId"] },
    sk: { field: "gsi1sk", composite: ["orderId"] },
    cardinality: 3,
  },
  root: { entityType: "AlShardedItem" },
  edges: {
    lines: Aggregate.many("lines", { entityType: "AlShardedLine" }),
  },
})

/**
 * Every `query` this suite issues, tagged by index. Requests naming the list
 * GSI are root-item reads; the rest are the per-aggregate assembly reads whose
 * count IS the N+1 claim.
 */
const alQueryLog: Array<string | undefined> = []

const alCountingClient = (client: DynamoClientService): DynamoClientService => ({
  ...client,
  query: (input) => {
    alQueryLog.push(input.IndexName)
    return client.query(input)
  },
})

const AlClientLayer = Layer.effect(
  DynamoClient,
  Effect.map(DynamoClient, (client) => alCountingClient(client)),
).pipe(Layer.provide(ClientLayer))

const AlLayer = Layer.mergeAll(AlClientLayer, AlTable.layer({ name: alTableName }))
const provideAl = Effect.provide(AlLayer)
const alTables = { AlTable }
const alAggregates = { AlOrderAggregate, AlShardedAggregate }

/** Root reads (list GSI) and assembly reads (base table) since the last reset. */
const alCounts = () => {
  const root = alQueryLog.filter((index) => index === "gsi1").length
  return { assemblies: alQueryLog.length - root, root }
}

/** 12 orders, every third one shipped: positions 3, 6, 9 and 12. */
const AL_ORDERS = Array.from({ length: 12 }, (_, i) => {
  const n = i + 1
  return {
    customerId: "c1",
    lines: [{ id: `l-${n}-a`, qty: n, sku: "sku-a" }],
    orderId: `o-${String(n).padStart(2, "0")}`,
    status: n % 3 === 0 ? ("shipped" as const) : ("pending" as const),
    total: n * 10,
  }
})

describeConnected("Aggregate.list — filtered pagination, reverse, sharding (#104)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          aggregates: alAggregates,
          entities: { AlAnchors },
          tables: alTables,
        })
        yield* db.tables.AlTable.create()

        for (const order of AL_ORDERS) {
          yield* db.aggregates.AlOrderAggregate.create(order as never)
          // The sharded twin: same rows, different entity types + list name.
          yield* db.aggregates.AlShardedAggregate.create({
            ...order,
            customerId: "c2",
            orderId: `s-${order.orderId}`,
          } as never)
        }
      }).pipe(provideAl, Effect.scoped),
    )
  }, 60000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: alTableName })
      }).pipe(
        provideAl,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  beforeEach(() => {
    alQueryLog.length = 0
  })

  const ids = (result: { data: ReadonlyArray<unknown> }) =>
    result.data.map((o) => (o as { orderId: string }).orderId)

  it.effect("a filtered list returns a FULL page of matches, not a short one", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      // `pageSize: 1` makes every request examine exactly one row, so the six
      // pending rows between the matches each come back EMPTY. Pre-#104 this
      // shape was inexpressible: `limit` was DynamoDB's `Limit`, which bounds
      // rows examined, so `limit: 3` under a filter returned whatever survived.
      const page = yield* db.aggregates.AlOrderAggregate.list(
        { customerId: "c1" },
        { filter: { status: "shipped" }, limit: 3, pageSize: 1 },
      )

      expect(ids(page)).toEqual(["o-03", "o-06", "o-09"])
      // Nine root requests to fill a three-item page — six returned nothing.
      expect(alCounts().root).toBe(9)
      // Assembly ran for the three matches only: rows 1-2, 4-5, 7-8 were
      // examined and rejected server-side, and never assembled.
      expect(alCounts().assemblies).toBe(3)
      // A fourth match (o-12) is still out there, so the page is not the end.
      expect(page.cursor).not.toBeNull()
    }).pipe(provideAl),
  )

  it.effect("the cursor resumes after the last aggregate RETURNED, and nulls only at the end", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      const first = yield* db.aggregates.AlOrderAggregate.list(
        { customerId: "c1" },
        { filter: { status: "shipped" }, limit: 3, pageSize: 1 },
      )
      expect(ids(first)).toEqual(["o-03", "o-06", "o-09"])

      const second = yield* db.aggregates.AlOrderAggregate.list(
        { customerId: "c1" },
        { cursor: first.cursor!, filter: { status: "shipped" }, limit: 3, pageSize: 1 },
      )

      // Resumes after o-09 — no repeats, nothing skipped.
      expect(ids(second)).toEqual(["o-12"])
      // Short only because the range genuinely ended, and the cursor says so.
      expect(second.cursor).toBeNull()
    }).pipe(provideAl),
  )

  it.effect("an over-reading page rebuilds its cursor so nothing is skipped", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      // No pageSize: one request reads the whole partition and returns all four
      // matches, of which two are kept. `LastEvaluatedKey` is absent (the range
      // ended), so a passed-through cursor would have claimed exhaustion and
      // lost o-09 and o-12 outright.
      const collected: Array<string> = []
      let cursor: string | null = null
      let pages = 0
      do {
        // Annotated: without it `page` infers through the `cursor` it assigns,
        // which makes the initialiser self-referential (TS7022).
        const page: { data: ReadonlyArray<AlOrder>; cursor: string | null } =
          yield* db.aggregates.AlOrderAggregate.list(
            { customerId: "c1" },
            cursor === null
              ? { filter: { status: "shipped" }, limit: 2 }
              : { cursor, filter: { status: "shipped" }, limit: 2 },
          )
        collected.push(...ids(page))
        cursor = page.cursor
        pages++
      } while (cursor !== null && pages < 10)

      expect(collected).toEqual(["o-03", "o-06", "o-09", "o-12"])
      expect(cursor).toBeNull()
    }).pipe(provideAl),
  )

  it.effect("the N+1 assembly is what filtering saves — measured", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      const all = yield* db.aggregates.AlOrderAggregate.list({ customerId: "c1" })
      expect(all.data).toHaveLength(12)
      const unfiltered = alCounts()
      expect(unfiltered.assemblies).toBe(12)

      alQueryLog.length = 0

      const shipped = yield* db.aggregates.AlOrderAggregate.list(
        { customerId: "c1" },
        { filter: { status: "shipped" } },
      )
      expect(ids(shipped)).toEqual(["o-03", "o-06", "o-09", "o-12"])
      const filtered = alCounts()

      // Same rows examined, a third of the partition reads: the eight rejected
      // aggregates are never assembled.
      expect(filtered.root).toBe(unfiltered.root)
      expect(filtered.assemblies).toBe(4)
    }).pipe(provideAl),
  )

  it.effect("the filter callback form reaches the same server-side predicate", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      const page = yield* db.aggregates.AlOrderAggregate.list(
        { customerId: "c1" },
        { filter: (t, { gt }) => gt(t.total, 90) },
      )

      expect(ids(page)).toEqual(["o-10", "o-11", "o-12"])
      expect(alCounts().assemblies).toBe(3)
    }).pipe(provideAl),
  )

  it.effect("reverse walks the list index descending", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      const page = yield* db.aggregates.AlOrderAggregate.list(
        { customerId: "c1" },
        { filter: { status: "shipped" }, limit: 2, reverse: true },
      )

      expect(ids(page)).toEqual(["o-12", "o-09"])

      const forward = yield* db.aggregates.AlOrderAggregate.list(
        { customerId: "c1" },
        { filter: { status: "shipped" }, limit: 2 },
      )
      expect(ids(forward)).toEqual(["o-03", "o-06"])
    }).pipe(provideAl),
  )

  it.effect("sharded: limit bounds the merged fan-out", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      const all = yield* db.aggregates.AlShardedAggregate.list({ customerId: "c2" })
      expect(all.data).toHaveLength(12)
      expect(all.cursor).toBeNull()

      alQueryLog.length = 0

      // Previously the option was accepted and discarded — this returned all 12.
      const bounded = yield* db.aggregates.AlShardedAggregate.list(
        { customerId: "c2" },
        { limit: 4 },
      )
      expect(bounded.data).toHaveLength(4)
      // Three shard reads, and only the four surviving rows are assembled.
      expect(alCounts().root).toBe(3)
      expect(alCounts().assemblies).toBe(4)
      // No resumable position across shards — never a cursor that lies.
      expect(bounded.cursor).toBeNull()
    }).pipe(provideAl),
  )

  it.effect("sharded: a filter reaches every shard", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      const page = yield* db.aggregates.AlShardedAggregate.list(
        { customerId: "c2" },
        { filter: { status: "shipped" } },
      )

      expect(ids(page).sort()).toEqual(["s-o-03", "s-o-06", "s-o-09", "s-o-12"])
      expect(alCounts().assemblies).toBe(4)
    }).pipe(provideAl),
  )

  it.effect("sharded: a cursor is REJECTED (EDD-9051), never silently ignored", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        aggregates: alAggregates,
        entities: { AlAnchors },
        tables: alTables,
      })

      // A cursor minted by the unsharded aggregate — structurally valid, and
      // meaningless here. The old code accepted it and restarted from the top.
      const source = yield* db.aggregates.AlOrderAggregate.list({ customerId: "c1" }, { limit: 2 })
      expect(source.cursor).not.toBeNull()

      alQueryLog.length = 0

      const error = yield* db.aggregates.AlShardedAggregate.list(
        { customerId: "c2" },
        { cursor: source.cursor!, limit: 2 },
      ).pipe(Effect.flip)

      expect(error._tag).toBe("ValidationError")
      expect((error as { cause: string }).cause).toContain("EDD-9051")
      // Rejected before any request was made.
      expect(alQueryLog).toHaveLength(0)
    }).pipe(provideAl),
  )
})

// ---------------------------------------------------------------------------
// Client-side predicates — `.filterBy()` / `ListOptions.filterBy` (#122)
// ---------------------------------------------------------------------------
//
// The motivating case, end to end. DynamoDB has no `lower()`, so a
// `FilterExpression` compares the stored attribute byte-for-byte and a
// case-insensitive match is inexpressible server-side. Filtering after the
// fact breaks pagination two ways — a short page, and a cursor that resumes
// after the last item RETURNED rather than the last one KEPT.

const cpSchema = DynamoSchema.make({ name: "clientpred", version: 1 })
const cpTableName = `clientpred-test-${Date.now()}`

class CpVenue extends Schema.Class<CpVenue>("CpVenue")({
  city: Schema.String,
  venueId: Schema.String,
  name: Schema.String,
}) {}

const CpVenues = Entity.make({
  model: CpVenue,
  entityType: "CpVenue",
  primaryKey: {
    pk: { field: "pk", composite: ["city"] },
    sk: { field: "sk", composite: ["venueId"] },
  },
})

const CpTable = Table.make({ schema: cpSchema, entities: { CpVenues } })
const provideCp = Effect.provide(Layer.mergeAll(ClientLayer, CpTable.layer({ name: cpTableName })))

/**
 * Nine venues, alternating so that a match is never adjacent to a match — a
 * page can only be filled by reading past the rejects.
 */
const CP_VENUES = [
  { venueId: "v-1", name: "Melbourne Cricket Ground" },
  { venueId: "v-2", name: "Geelong Oval" },
  { venueId: "v-3", name: "melbourne Park" },
  { venueId: "v-4", name: "Ballarat Reserve" },
  { venueId: "v-5", name: "MELBOURNE Zoo" },
  { venueId: "v-6", name: "Bendigo Ground" },
  { venueId: "v-7", name: "MelBourne Docklands" },
  { venueId: "v-8", name: "Shepparton Park" },
  { venueId: "v-9", name: "melbournE Olympic Park" },
]

describeConnected("client-side predicates (closes #122)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({ entities: { CpVenues }, tables: { CpTable } })
        yield* db.tables.CpTable.create()
        for (const v of CP_VENUES) {
          yield* db.entities.CpVenues.put({ city: "vic", ...v })
        }
      }).pipe(provideCp, Effect.scoped),
    )
  }, 60000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: cpTableName })
      }).pipe(
        provideCp,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  const isMelbourne = (v: CpVenue) => v.name.toLowerCase().startsWith("melbourne")

  it.effect("a server-side filter cannot do this — that is the whole reason it exists", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CpVenues }, tables: { CpTable } })

      // `begins_with` on the stored attribute is byte-for-byte. Of the five
      // venues a reader would call a match, exactly one is spelled so that it
      // survives: "melbourne Park". Even "melbournE Olympic Park" is excluded,
      // on the strength of a single capital letter nine characters in.
      const serverSide = yield* db.entities.CpVenues.primary({ city: "vic" })
        .filter((t, { beginsWith }) => beginsWith(t.name, "melbourne"))
        .collect()
      expect(serverSide.map((v) => v.venueId)).toEqual(["v-3"])

      // The predicate sees the decoded item and folds both sides.
      const clientSide = yield* db.entities.CpVenues.primary({ city: "vic" })
        .filterBy(isMelbourne)
        .collect()
      expect(clientSide.map((v) => v.venueId)).toEqual(["v-1", "v-3", "v-5", "v-7", "v-9"])
    }).pipe(provideCp),
  )

  it.effect("limit fills a FULL page of accepted items, reading past the rejects", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CpVenues }, tables: { CpTable } })

      // `pageSize: 1` makes every request examine exactly one row, so the four
      // non-matching rows each come back and are rejected in the loop.
      const page = yield* db.entities.CpVenues.primary({ city: "vic" })
        .filterBy(isMelbourne)
        .pageSize(1)
        .limit(3)
        .fetch()

      expect(page.items.map((v) => v.venueId)).toEqual(["v-1", "v-3", "v-5"])
      expect(page.cursor).not.toBeNull()
    }).pipe(provideCp),
  )

  it.effect("the cursor resumes after the last item KEPT — nothing repeated, nothing skipped", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CpVenues }, tables: { CpTable } })

      const collected: Array<string> = []
      let cursor: string | null = null
      let pages = 0
      do {
        const query = db.entities.CpVenues.primary({ city: "vic" }).filterBy(isMelbourne).limit(2)
        const page: { items: Array<CpVenue>; cursor: string | null } = yield* (
          cursor === null ? query : query.startFrom(cursor)
        ).fetch()
        collected.push(...page.items.map((v) => v.venueId))
        cursor = page.cursor
        pages++
      } while (cursor !== null && pages < 10)

      // Every match, exactly once, in order — the property that breaks when a
      // caller filters `page.items` itself.
      expect(collected).toEqual(["v-1", "v-3", "v-5", "v-7", "v-9"])
      expect(cursor).toBeNull()
    }).pipe(provideCp),
  )

  it.effect("count reflects the predicate rather than the unfiltered row count", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CpVenues }, tables: { CpTable } })

      const all = yield* db.entities.CpVenues.primary({ city: "vic" }).count()
      expect(all).toBe(9)

      // `Select: "COUNT"` returns no items to test, so this reads the rows and
      // counts the accepted ones — the alternative is reporting 9.
      const matching = yield* db.entities.CpVenues.primary({ city: "vic" })
        .filterBy(isMelbourne)
        .count()
      expect(matching).toBe(5)
    }).pipe(provideCp),
  )

  it.effect("paginate streams only accepted items", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CpVenues }, tables: { CpTable } })

      const stream = db.entities.CpVenues.primary({ city: "vic" })
        .filterBy(isMelbourne)
        .pageSize(2)
        .paginate()
      const items = yield* Stream.runCollect(stream)

      expect(items.map((v) => v.venueId)).toEqual(["v-1", "v-3", "v-5", "v-7", "v-9"])
    }).pipe(provideCp),
  )

  it.effect("a server-side filter and a client-side predicate compose", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({ entities: { CpVenues }, tables: { CpTable } })

      // The FilterExpression rejects rows before they cross the wire; the
      // predicate then folds case on what is left.
      const items = yield* db.entities.CpVenues.primary({ city: "vic" })
        .filter((t, { contains }) => contains(t.name, "Park"))
        .filterBy(isMelbourne)
        .collect()

      expect(items.map((v) => v.venueId)).toEqual(["v-3", "v-9"])
    }).pipe(provideCp),
  )
})

// ---------------------------------------------------------------------------
// Renamed attributes across the lifecycle (#127)
// ---------------------------------------------------------------------------
//
// A stored row is keyed by ATTRIBUTE name; key composition and unique-sentinel
// composition read DOMAIN names. Every operation that reads a row back and then
// composes from it has to bridge that gap, and the soft-delete namespace did
// not: `deleted.get` / `deleted.list` decoded an attribute-keyed row against a
// domain-keyed schema, `restore` composed keys from it, and four sentinel sites
// read a renamed constraint field off it — getting `undefined`, which the sparse
// rule reads as "constraint unset". No existing fixture paired a rename with
// either feature, which is exactly why this shipped.

const rnSchema = DynamoSchema.make({ name: "renamed", version: 1 })
const rnTableName = `renamed-test-${Date.now()}`

class RnWidget extends Schema.Class<RnWidget>("RnWidget")({
  id: Schema.String,
  name: Schema.String,
  channel: Schema.String,
}) {}

/** The identifier itself is stored as `widgetId` — issue #127 as reported. */
const RnWidgets = Entity.make({
  model: DynamoModel.configure(RnWidget, { id: { field: "widgetId", identifier: true } }),
  entityType: "RnWidget",
  softDelete: true,
  versioned: true,
  primaryKey: {
    pk: { field: "pk", composite: ["id"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byChannel: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["channel"] },
      sk: { field: "gsi1sk", composite: ["id"] },
    },
  },
})

/** The UNIQUE constraint field is stored as `widgetName`, with soft delete. */
const RnUniques = Entity.make({
  model: DynamoModel.configure(RnWidget, { name: { field: "widgetName" } }),
  entityType: "RnUnique",
  softDelete: true,
  unique: { name: ["name"] },
  primaryKey: {
    pk: { field: "pk", composite: ["id"] },
    sk: { field: "sk", composite: [] },
  },
})

/** Same, hard delete — the sentinel release runs on a different code path. */
const RnHards = Entity.make({
  model: DynamoModel.configure(RnWidget, { name: { field: "widgetName" } }),
  entityType: "RnHard",
  unique: { name: ["name"] },
  primaryKey: {
    pk: { field: "pk", composite: ["id"] },
    sk: { field: "sk", composite: [] },
  },
})

const RnTable = Table.make({
  schema: rnSchema,
  entities: { RnWidgets, RnUniques, RnHards },
})
const provideRn = Effect.provide(Layer.mergeAll(ClientLayer, RnTable.layer({ name: rnTableName })))

describeConnected("renamed attributes across the lifecycle (closes #127)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { RnWidgets, RnUniques, RnHards },
          tables: { RnTable },
        })
        yield* db.tables.RnTable.create()
      }).pipe(provideRn, Effect.scoped),
    )
  }, 60000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: rnTableName })
      }).pipe(
        provideRn,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("a renamed identifier round-trips soft delete → deleted.get/list → restore", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { RnWidgets, RnUniques, RnHards },
        tables: { RnTable },
      })

      yield* db.entities.RnWidgets.create({ id: "w1", name: "Widget One", channel: "news" })
      yield* db.entities.RnWidgets.delete({ id: "w1" })

      // All three used to fail: two as ValidationError("Missing key"), restore
      // as a DEFECT out of `KeyComposer.extractComposites`.
      const tombstone = yield* db.entities.RnWidgets.deleted.get({ id: "w1" })
      expect(tombstone.id).toBe("w1")
      expect(tombstone.name).toBe("Widget One")

      const tombstones = yield* db.entities.RnWidgets.deleted.list({ id: "w1" }).collect()
      expect(tombstones.map((t) => t.id)).toEqual(["w1"])

      const restored = yield* db.entities.RnWidgets.restore({ id: "w1" })
      expect(restored.id).toBe("w1")

      // Restored by its primary key AND back in the GSI — the keys were
      // recomposed from the renamed row, not just copied off the tombstone.
      const fetched = yield* db.entities.RnWidgets.get({ id: "w1" })
      expect(fetched.name).toBe("Widget One")
      const byChannel = yield* db.entities.RnWidgets.byChannel({ channel: "news" }).collect()
      expect(byChannel.map((w) => w.id)).toEqual(["w1"])

      // The tombstone is gone, and the row is stored under `widgetId`.
      const gone = yield* db.entities.RnWidgets.deleted.get({ id: "w1" }).pipe(Effect.flip)
      expect(gone._tag).toBe("ItemNotFound")
      const client = yield* DynamoClient
      const raw = yield* client.getItem({
        TableName: rnTableName,
        Key: {
          pk: { S: "$renamed#v1#rnwidget#id_w1" },
          sk: { S: "$renamed#v1#rnwidget" },
        },
      })
      const stored = fromAttributeMap(raw.Item!)
      expect(stored.widgetId).toBe("w1")
      expect(stored.id).toBeUndefined()
    }).pipe(provideRn),
  )

  it.effect("soft delete releases the sentinel of a renamed unique field", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { RnWidgets, RnUniques, RnHards },
        tables: { RnTable },
      })

      yield* db.entities.RnUniques.create({ id: "u1", name: "Alpha", channel: "news" })
      yield* db.entities.RnUniques.delete({ id: "u1" })

      // The sentinel was orphaned, so the value could never be used again.
      const reused = yield* db.entities.RnUniques.create({
        id: "u2",
        name: "Alpha",
        channel: "news",
      })
      expect(reused.id).toBe("u2")
    }).pipe(provideRn),
  )

  it.effect("hard delete releases the sentinel of a renamed unique field", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { RnWidgets, RnUniques, RnHards },
        tables: { RnTable },
      })

      yield* db.entities.RnHards.create({ id: "h1", name: "Beta", channel: "news" })
      yield* db.entities.RnHards.delete({ id: "h1" })

      const reused = yield* db.entities.RnHards.create({
        id: "h2",
        name: "Beta",
        channel: "news",
      })
      expect(reused.id).toBe("h2")
    }).pipe(provideRn),
  )

  it.effect("restore re-establishes the sentinel of a renamed unique field", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { RnWidgets, RnUniques, RnHards },
        tables: { RnTable },
      })

      yield* db.entities.RnUniques.create({ id: "r1", name: "Gamma", channel: "news" })
      yield* db.entities.RnUniques.delete({ id: "r1" })
      yield* db.entities.RnUniques.restore({ id: "r1" })

      // Restoring without a sentinel silently reopened the value to everybody.
      const err = yield* db.entities.RnUniques.create({
        id: "r2",
        name: "Gamma",
        channel: "news",
      })
        .asEffect()
        .pipe(Effect.flip)
      expect(err._tag).toBe("UniqueConstraintViolation")
    }).pipe(provideRn),
  )

  it.effect("restore refuses when the renamed unique value was taken meanwhile", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { RnWidgets, RnUniques, RnHards },
        tables: { RnTable },
      })

      // The sharp version of the pair above: the release and the
      // re-establishment are both observable, and neither can be faked by an
      // orphan left behind by the delete.
      yield* db.entities.RnUniques.create({ id: "z1", name: "Zeta", channel: "news" })
      yield* db.entities.RnUniques.delete({ id: "z1" })
      yield* db.entities.RnUniques.create({ id: "z2", name: "Zeta", channel: "news" })

      const err = yield* db.entities.RnUniques.restore({ id: "z1" }).pipe(Effect.flip)
      expect(err._tag).toBe("UniqueConstraintViolation")
    }).pipe(provideRn),
  )

  it.effect("purge cleans the sentinel of a renamed unique field", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { RnWidgets, RnUniques, RnHards },
        tables: { RnTable },
      })

      yield* db.entities.RnHards.create({ id: "p1", name: "Delta", channel: "news" })
      yield* db.entities.RnHards.purge({ id: "p1" })

      const reused = yield* db.entities.RnHards.create({
        id: "p2",
        name: "Delta",
        channel: "news",
      })
      expect(reused.id).toBe("p2")
    }).pipe(provideRn),
  )

  it.effect("a transact put still emits the sentinel of a renamed unique field", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { RnWidgets, RnUniques, RnHards },
        tables: { RnTable },
      })

      yield* Transaction.transactWrite([
        db.entities.RnHards.put({ id: "t1", name: "Epsilon", channel: "news" }),
      ])

      // Without the sentinel the constraint simply stopped being enforced for
      // anything written through `transactWrite`.
      const err = yield* db.entities.RnHards.create({
        id: "t2",
        name: "Epsilon",
        channel: "news",
      })
        .asEffect()
        .pipe(Effect.flip)
      expect(err._tag).toBe("UniqueConstraintViolation")
    }).pipe(provideRn),
  )
})

// ---------------------------------------------------------------------------
// preserveUnique restore + restore-time snapshots
// ---------------------------------------------------------------------------
//
// Two bugs on the restore path, both independent of any field rename, both
// found while fixing #127:
//
//   1. `softDelete: { preserveUnique: true }` KEEPS the reservation across the
//      delete, so the restore-time sentinel Put's `attribute_not_exists` guard
//      could never hold — every restore of a constrained entity was cancelled
//      and reported as a violation against its own reservation.
//   2. The restore-time retain snapshot was built from the tombstone, so it
//      inherited `deletedAt` and the soft-delete TTL — and it lands on the same
//      `#v#<version>` SK the delete-time snapshot used, replacing a clean
//      snapshot with one that looks deleted and expires.

const puSchema = DynamoSchema.make({ name: "purestore", version: 1 })
const puTableName = `purestore-test-${Date.now()}`

class PuItem extends Schema.Class<PuItem>("PuItem")({
  id: Schema.String,
  name: Schema.String,
}) {}

const PuUniques = Entity.make({
  model: PuItem,
  entityType: "PuUnique",
  softDelete: { preserveUnique: true },
  unique: { name: ["name"] },
  primaryKey: {
    pk: { field: "pk", composite: ["id"] },
    sk: { field: "sk", composite: [] },
  },
})

const PuSnaps = Entity.make({
  model: PuItem,
  entityType: "PuSnap",
  versioned: { retain: true },
  softDelete: { ttl: Duration.hours(1) },
  primaryKey: {
    pk: { field: "pk", composite: ["id"] },
    sk: { field: "sk", composite: [] },
  },
})

const PuTable = Table.make({ schema: puSchema, entities: { PuUniques, PuSnaps } })
const providePu = Effect.provide(Layer.mergeAll(ClientLayer, PuTable.layer({ name: puTableName })))

describeConnected("preserveUnique restore + restore-time snapshots", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { PuUniques, PuSnaps },
          tables: { PuTable },
        })
        yield* db.tables.PuTable.create()
      }).pipe(providePu, Effect.scoped),
    )
  }, 60000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: puTableName })
      }).pipe(
        providePu,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("preserveUnique: a soft-deleted row can be restored and keeps its reservation", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { PuUniques, PuSnaps },
        tables: { PuTable },
      })

      yield* db.entities.PuUniques.create({ id: "a1", name: "Alpha" })
      yield* db.entities.PuUniques.delete({ id: "a1" })

      // The reservation survives the delete — that IS `preserveUnique`.
      const taken = yield* db.entities.PuUniques.create({ id: "a2", name: "Alpha" })
        .asEffect()
        .pipe(Effect.flip)
      expect(taken._tag).toBe("UniqueConstraintViolation")

      // And restore re-claims it rather than colliding with it. This used to
      // fail as a UniqueConstraintViolation against the row's own sentinel,
      // which made `preserveUnique` and `restore` mutually exclusive.
      const restored = yield* db.entities.PuUniques.restore({ id: "a1" })
      expect(restored.name).toBe("Alpha")
      const fetched = yield* db.entities.PuUniques.get({ id: "a1" })
      expect(fetched.name).toBe("Alpha")

      // Still enforced afterwards.
      const stillTaken = yield* db.entities.PuUniques.create({ id: "a3", name: "Alpha" })
        .asEffect()
        .pipe(Effect.flip)
      expect(stillTaken._tag).toBe("UniqueConstraintViolation")
    }).pipe(providePu),
  )

  it.effect("preserveUnique: restore is refused when the reservation belongs to someone else", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { PuUniques, PuSnaps },
        tables: { PuTable },
      })
      const client = yield* DynamoClient

      yield* db.entities.PuUniques.create({ id: "b1", name: "Beta" })
      yield* db.entities.PuUniques.delete({ id: "b1" })

      // Hand the reservation to another item, exactly as a competing write
      // would leave it. The relaxed guard must key off the back-pointer, not
      // merely off the sentinel's existence.
      const sentinelKey = DynamoSchema.composeUniqueKey(puSchema, "PuUnique", "name", ["Beta"])
      yield* client.putItem({
        TableName: puTableName,
        Item: {
          pk: { S: sentinelKey.pk },
          sk: { S: sentinelKey.sk },
          __edd_e__: { S: "PuUnique._unique.name" },
          _entity_pk: { S: "$purestore#v1#puunique#id_someone-else" },
          _entity_sk: { S: "$purestore#v1#puunique" },
        },
      })

      const err = yield* db.entities.PuUniques.restore({ id: "b1" }).pipe(Effect.flip)
      expect(err._tag).toBe("UniqueConstraintViolation")
    }).pipe(providePu),
  )

  it.effect("the restore-time snapshot carries neither deletedAt nor the soft-delete TTL", () =>
    Effect.gen(function* () {
      const db = yield* DynamoClient.make({
        entities: { PuUniques, PuSnaps },
        tables: { PuTable },
      })
      const client = yield* DynamoClient

      yield* db.entities.PuSnaps.create({ id: "s1", name: "Snap" })
      yield* db.entities.PuSnaps.delete({ id: "s1" })
      yield* db.entities.PuSnaps.restore({ id: "s1" })

      const scanned = yield* client.scan({ TableName: puTableName })
      const snapshots = (scanned.Items ?? [])
        .map((item) => fromAttributeMap(item))
        .filter((row) => typeof row.sk === "string" && row.sk.includes("#v#"))

      expect(snapshots).toHaveLength(1)
      const snapshot = snapshots[0]!
      expect(snapshot.sk).toBe("$purestore#v1#pusnap#v#0000001")
      expect(snapshot.name).toBe("Snap")
      // Both used to ride along from the tombstone, and this row REPLACED the
      // clean snapshot the delete wrote at the same SK.
      expect(snapshot.deletedAt).toBeUndefined()
      expect(snapshot._ttl).toBeUndefined()
    }).pipe(providePu),
  )
})

// ---------------------------------------------------------------------------
// EventStore stream casing under `casing: "preserve"`
// ---------------------------------------------------------------------------
//
// Without `casing` a stream lower-cases its name in keys whatever the schema's
// casing; with `casing: "preserve"` it keeps the name as written, like an entity
// type on this schema. The two write different keys, so two streams that differ
// only in this option are separate stores — the hazard the docs warn about when
// setting `casing` on an existing stream.

const lcSchema = DynamoSchema.make({ name: "LcStore", version: 1, casing: "preserve" })

class LcCursor extends Schema.Class<LcCursor>("LcCursor")({
  deviceId: Schema.String,
  mark: Schema.Number,
}) {}

const LcCursors = Entity.make({
  model: LcCursor,
  entityType: "Cursor",
  primaryKey: {
    pk: { field: "pk", composite: ["deviceId"] },
    sk: { field: "sk", composite: [] },
  },
})

class LcPlaced extends Schema.TaggedClass<LcPlaced>()("LcPlaced", { qty: Schema.Number }) {}

const LcState = Schema.Struct({ total: Schema.Number })

const LcTable = Table.make({ schema: lcSchema, entities: { LcCursors } })
const lcTableName = `lc-casing-${Date.now()}`

const lcStream = (casing: DynamoSchema.Casing | undefined) =>
  EventStore.makeStream({
    table: LcTable,
    streamName: "OrderBook",
    events: [LcPlaced],
    streamId: { composite: ["orderId"] },
    snapshot: { schema: LcState },
    ...(casing !== undefined ? { casing } : {}),
  })

const LcDefaultStream = lcStream(undefined)
const LcPreserveStream = lcStream("preserve")

const provideLc = Effect.provide(Layer.mergeAll(ClientLayer, LcTable.layer({ name: lcTableName })))

const lcPartition = (pk: string) =>
  Effect.gen(function* () {
    const client = yield* DynamoClient
    const raw = yield* client.query({
      TableName: lcTableName,
      KeyConditionExpression: "#pk = :pk",
      ExpressionAttributeNames: { "#pk": "pk" },
      ExpressionAttributeValues: { ":pk": { S: pk } },
    })
    return (raw.Items ?? []).map((i) => fromAttributeMap(i))
  })

describeConnected("EventStore stream casing (schema casing: preserve)", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.createTable({
          TableName: lcTableName,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
        })
      }).pipe(provideLc, Effect.scoped),
    )
  }, 20000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: lcTableName }).pipe(Effect.catch(() => Effect.void))
      }).pipe(provideLc, Effect.scoped),
    )
  }, 15000)

  it.effect("omitted keeps the lower-cased stream name; entity keys follow the schema", () =>
    Effect.gen(function* () {
      yield* LcDefaultStream.append({ orderId: "Ord-1" }, [new LcPlaced({ qty: 2 })], 0, {
        additionalItems: [LcCursors.put({ deviceId: "Dev-A", mark: 1 })],
        idempotency: { commandId: "Cmd-1" },
      })
      yield* LcDefaultStream.writeSnapshot({ orderId: "Ord-1" }, { total: 2 }, 1)

      const items = yield* lcPartition("$LcStore#v1#orderbook#Ord-1")
      expect(items.map((i) => [i.sk, i.__edd_e__])).toEqual([
        ["$LcStore#v1#orderbook.command#Cmd-1", "orderbook.command"],
        ["$LcStore#v1#orderbook.event_1#0000000001", "orderbook.event"],
        ["$LcStore#v1#orderbook.snapshot", "orderbook.snapshot"],
      ])

      const cursor = yield* lcPartition("$LcStore#v1#Cursor#deviceId_Dev-A")
      expect(cursor).toHaveLength(1)
    }).pipe(provideLc),
  )

  it.effect(`casing: "preserve" writes and reads the stream name as written, end to end`, () =>
    Effect.gen(function* () {
      const key = { orderId: "Ord-2" }
      yield* LcPreserveStream.append(key, [new LcPlaced({ qty: 3 })], 0, {
        idempotency: { commandId: "Cmd-2" },
      })
      yield* LcPreserveStream.writeSnapshot(key, { total: 3 }, 1)
      yield* LcPreserveStream.append(key, [new LcPlaced({ qty: 4 })], 1)

      const items = yield* lcPartition("$LcStore#v1#OrderBook#Ord-2")
      expect(items.map((i) => [i.sk, i.__edd_e__])).toEqual([
        ["$LcStore#v1#OrderBook.command#Cmd-2", "orderbook.command"],
        ["$LcStore#v1#OrderBook.event_1#0000000001", "orderbook.event"],
        ["$LcStore#v1#OrderBook.event_1#0000000002", "orderbook.event"],
        ["$LcStore#v1#OrderBook.snapshot", "orderbook.snapshot"],
      ])

      // Every read path finds what the write paths wrote.
      expect((yield* LcPreserveStream.read(key)).map((e) => e.version)).toEqual([1, 2])
      expect((yield* LcPreserveStream.readFrom(key, 1)).map((e) => e.version)).toEqual([2])
      expect(yield* LcPreserveStream.currentVersion(key)).toBe(2)
      const snapshot = yield* LcPreserveStream.readSnapshot(key)
      expect(Option.map(snapshot, (s) => s.asOfVersion)).toEqual(Option.some(1))

      // The idempotency sentinel is found too: a replay is rejected.
      const replay = yield* LcPreserveStream.append(key, [new LcPlaced({ qty: 3 })], 2, {
        idempotency: { commandId: "Cmd-2" },
      }).pipe(Effect.flip)
      expect(replay._tag).toBe("DuplicateCommand")
    }).pipe(provideLc),
  )

  it.effect("the two settings are separate stores under preserve", () =>
    Effect.gen(function* () {
      const key = { orderId: "Ord-3" }
      yield* LcDefaultStream.append(key, [new LcPlaced({ qty: 1 })], 0)

      // Setting `casing` on an existing stream no longer sees its history.
      expect(yield* LcPreserveStream.currentVersion(key)).toBe(0)
      expect(yield* LcPreserveStream.read(key)).toEqual([])
      expect(yield* LcDefaultStream.currentVersion(key)).toBe(1)
    }).pipe(provideLc),
  )
})

// ---------------------------------------------------------------------------
// Index-level `casing` (was silently dropped for GSIs before 1.22)
// ---------------------------------------------------------------------------
//
// The schema folds case; one GSI keeps it. Ids that differ only by case must
// stay distinct through that index — on put, query, update and the collection —
// while the primary key still folds them together.

const icSchema = DynamoSchema.make({ name: "icstore", version: 1 })

class IcDevice extends Schema.Class<IcDevice>("IcDevice")({
  deviceId: Schema.String,
  ownerId: Schema.String,
  site: Schema.String,
}) {}

const IcDevices = Entity.make({
  model: IcDevice,
  entityType: "Device",
  primaryKey: {
    pk: { field: "pk", composite: ["deviceId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byOwner: {
      name: "gsi1",
      casing: "preserve",
      pk: { field: "gsi1pk", composite: ["ownerId"] },
      sk: { field: "gsi1sk", composite: ["deviceId"] },
    },
    bySite: {
      name: "gsi2",
      collection: "Fleet",
      casing: "preserve",
      pk: { field: "gsi2pk", composite: ["site"] },
      sk: { field: "gsi2sk", composite: ["deviceId"] },
    },
  },
})

class IcGateway extends Schema.Class<IcGateway>("IcGateway")({
  gatewayId: Schema.String,
  site: Schema.String,
}) {}

const IcGateways = Entity.make({
  model: IcGateway,
  entityType: "Gateway",
  primaryKey: {
    pk: { field: "pk", composite: ["gatewayId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    bySite: {
      name: "gsi2",
      collection: "Fleet",
      casing: "preserve",
      pk: { field: "gsi2pk", composite: ["site"] },
      sk: { field: "gsi2sk", composite: ["gatewayId"] },
    },
  },
})

const icTableName = `index-casing-${Date.now()}`
const IcTable = Table.make({ schema: icSchema, entities: { IcDevices, IcGateways } })
const provideIc = Effect.provide(Layer.mergeAll(ClientLayer, IcTable.layer({ name: icTableName })))
const icClient = DynamoClient.make({
  entities: { IcDevices, IcGateways },
  tables: { IcTable },
})

describeConnected("index-level casing", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* icClient
        yield* db.tables.IcTable.create()
      }).pipe(provideIc, Effect.scoped),
    )
  }, 60000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: icTableName })
      }).pipe(
        provideIc,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 15000)

  it.effect("keeps ids that differ only by case apart through the preserving index", () =>
    Effect.gen(function* () {
      const db = yield* icClient
      yield* db.entities.IcDevices.put({ deviceId: "d-1", ownerId: "Own-A", site: "Site-X" })
      yield* db.entities.IcDevices.put({ deviceId: "d-2", ownerId: "own-a", site: "site-x" })
      yield* db.entities.IcGateways.put({ gatewayId: "g-1", site: "Site-X" })

      const upper = yield* db.entities.IcDevices.byOwner({ ownerId: "Own-A" }).collect()
      expect(upper.map((d) => d.deviceId)).toEqual(["d-1"])
      const lower = yield* db.entities.IcDevices.byOwner({ ownerId: "own-a" }).collect()
      expect(lower.map((d) => d.deviceId)).toEqual(["d-2"])

      const narrowed = yield* db.entities.IcDevices.byOwner({
        ownerId: "Own-A",
        deviceId: "d-1",
      }).collect()
      expect(narrowed).toHaveLength(1)

      const fleet = yield* db.collections.Fleet!({ site: "Site-X" }).collect()
      expect(fleet.IcDevices!.map((d: IcDevice) => d.deviceId)).toEqual(["d-1"])
      expect(fleet.IcGateways!.map((g: IcGateway) => g.gatewayId)).toEqual(["g-1"])

      // The primary key still folds case under the schema's casing.
      const folded = yield* db.entities.IcDevices.get({ deviceId: "D-1" })
      expect(folded.ownerId).toBe("Own-A")
    }).pipe(provideIc),
  )

  it.effect("update recomposes the preserving index's keys", () =>
    Effect.gen(function* () {
      const db = yield* icClient
      yield* db.entities.IcDevices.put({ deviceId: "d-3", ownerId: "Own-B", site: "Site-Y" })
      yield* db.entities.IcDevices.update({ deviceId: "d-3" }).set({ ownerId: "Own-C" })

      expect(yield* db.entities.IcDevices.byOwner({ ownerId: "Own-B" }).collect()).toEqual([])
      const moved = yield* db.entities.IcDevices.byOwner({ ownerId: "Own-C" }).collect()
      expect(moved.map((d) => d.deviceId)).toEqual(["d-3"])
      expect(yield* db.entities.IcDevices.byOwner({ ownerId: "own-c" }).collect()).toEqual([])
    }).pipe(provideIc),
  )
})

// ===========================================================================
// #133 — aggregate DateTimes nested in containers are stored in wire form
// ===========================================================================
//
// The downstream `Match` shape from #133, trimmed to its date-bearing paths:
// a root array of a date transform, a root array of a class with dates, a ref
// in a root `many` element (declared `sk.composite`), and a ref in a `many`
// element of a sub-aggregate bound twice. Entity models are configured wrappers
// with an identifier rename, and `dateOfBirth` carries a decoding default.
//
// Before #133 every one of those nested dates was stored as a marshalled
// `{ epochMilliseconds, "~effect/DateTime", _tag }` map, which effect 4.0.0
// either rejects (rc-era maps) or reads back as a plain object (4.0.0-era
// maps). The ref element field is exercised both as the plain entity class
// (matched by field name) and `DynamoModel.ref`-annotated — the latter could not
// read back even a fresh write.

const I133PersonFields = {
  id: Schema.String,
  name: Schema.String,
  dateOfBirth: Schema.DateTimeUtcFromString.pipe(
    Schema.withDecodingDefault(Effect.succeed("1800-01-01")),
  ),
}
class I133Team extends Schema.Class<I133Team>("I133Team")({
  id: Schema.String,
  name: Schema.String,
}) {}
class I133Coach extends Schema.Class<I133Coach>("I133Coach")({ ...I133PersonFields }) {}
class I133Player extends Schema.Class<I133Player>("I133Player")({ ...I133PersonFields }) {}
class I133Umpire extends Schema.Class<I133Umpire>("I133Umpire")({ ...I133PersonFields }) {}

const i133PkSk = {
  pk: { field: "pk", composite: ["id"] },
  sk: { field: "sk", composite: [] },
} as const
const I133Teams = Entity.make({
  model: DynamoModel.configure(I133Team, { id: { field: "teamId", identifier: true } }),
  entityType: "Team",
  primaryKey: i133PkSk,
})
const I133Coaches = Entity.make({
  model: DynamoModel.configure(I133Coach, { id: { field: "coachId", identifier: true } }),
  entityType: "Coach",
  primaryKey: i133PkSk,
})
const I133Players = Entity.make({
  model: DynamoModel.configure(I133Player, { id: { field: "playerId", identifier: true } }),
  entityType: "Player",
  primaryKey: i133PkSk,
})
const I133Umpires = Entity.make({
  model: DynamoModel.configure(I133Umpire, { id: { field: "umpireId", identifier: true } }),
  entityType: "Umpire",
  primaryKey: i133PkSk,
})

class I133Session extends Schema.Class<I133Session>("I133Session")({
  number: Schema.Number,
  startTime: Schema.DateTimeUtcFromString,
  finishTime: Schema.optionalKey(Schema.DateTimeUtcFromString),
}) {}

const I133Schema = DynamoSchema.make({ name: "issue133", version: 1 })
const I133Table = Table.make({
  schema: I133Schema,
  entities: { I133Teams, I133Coaches, I133Players, I133Umpires },
})

const makeI133Match = (kind: "plain" | "ref") => {
  const playerField = kind === "plain" ? I133Player : I133Player.pipe(DynamoModel.ref)
  const umpireField = kind === "plain" ? I133Umpire : I133Umpire.pipe(DynamoModel.ref)
  class PlayerSheet extends Schema.Class<PlayerSheet>(`I133PlayerSheet-${kind}`)({
    player: playerField as typeof I133Player,
    isCaptain: Schema.optionalKey(Schema.Boolean),
  }) {}
  class TeamSheet extends Schema.Class<TeamSheet>(`I133TeamSheet-${kind}`)({
    team: I133Team,
    coach: I133Coach,
    homeTeam: Schema.Boolean,
    players: Schema.Array(PlayerSheet),
  }) {}
  class UmpireSheet extends Schema.Class<UmpireSheet>(`I133UmpireSheet-${kind}`)({
    umpire: umpireField as typeof I133Umpire,
    role: Schema.Literals(["onfield", "third"]),
  }) {}
  class Match extends Schema.Class<Match>(`I133Match-${kind}`)({
    id: Schema.String,
    name: Schema.String,
    startDate: Schema.DateTimeUtcFromString,
    matchDays: Schema.optionalKey(Schema.Array(Schema.DateTimeUtcFromString)),
    sessions: Schema.optionalKey(Schema.Array(I133Session)),
    team1: TeamSheet,
    team2: TeamSheet,
    umpires: Schema.optionalKey(Schema.Array(UmpireSheet)),
  }) {}
  const TeamSheetAggregate = Aggregate.make(TeamSheet, {
    root: { entityType: "MatchTeam" },
    edges: {
      team: Aggregate.ref(I133Teams),
      coach: Aggregate.one("coach", { entityType: "MatchCoach", entity: I133Coaches }),
      players: Aggregate.many("players", { entityType: "MatchPlayer", entity: I133Players }),
    },
  })
  return Aggregate.make(Match, {
    table: I133Table,
    schema: I133Schema,
    pk: { field: "pk", composite: ["id"] },
    collection: { name: `match${kind}` }, // no index: whole-partition read on the base table
    root: { entityType: "MatchItem" },
    edges: {
      team1: TeamSheetAggregate.with({ discriminator: { teamNumber: 1 } }),
      team2: TeamSheetAggregate.with({ discriminator: { teamNumber: 2 } }),
      umpires: Aggregate.many("umpires", {
        entityType: "MatchUmpire",
        entity: I133Umpires,
        sk: { composite: ["role", "umpire.id"] },
      }),
    },
  })
}

const i133TableName = `issue133-${Date.now()}`
const I133TestLayer = Layer.mergeAll(ClientLayer, I133Table.layer({ name: i133TableName }))
const provideI133 = Effect.provide(I133TestLayer)

const I133_DOB = "2000-01-01T00:00:00.000Z"
const I133_DOB_MS = 946684800000
const I133_DAY2 = "2000-01-02T00:00:00.000Z"

const i133LegacyMap = (typeIdKey: string) => (ms: number) => ({
  M: {
    epochMilliseconds: { N: String(ms) },
    [typeIdKey]: { S: typeIdKey },
    _tag: { S: "Utc" },
  },
})
const i133LegacyForms = [
  ["rc-era map", i133LegacyMap("~effect/time/DateTime")],
  ["4.0.0-era map", i133LegacyMap("~effect/DateTime")],
] as const

/** A real `DateTime.Utc` for `ms`, not a plain object that duck-types as one. */
const i133IsRealUtc = (value: unknown, ms: number) =>
  DateTime.isDateTime(value) &&
  Object.getPrototypeOf(value) !== Object.prototype &&
  Equal.equals(value, DateTime.makeUnsafe(ms))

class I133Slot extends Schema.Class<I133Slot>("I133Slot")({ at: Schema.DateTimeUtcFromString }) {}
class I133Holder extends Schema.Class<I133Holder>("I133Holder")({
  id: Schema.String,
  nullSelf: Schema.NullOr(Schema.DateTimeUtc),
  arrNullSelf: Schema.Array(Schema.NullOr(Schema.DateTimeUtc)),
  rec: Schema.Record(Schema.String, Schema.DateTimeUtcFromString),
  tup: Schema.Tuple([Schema.String, Schema.DateTimeUtcFromString]),
  either: Schema.Union([I133Slot, Schema.String]),
  mixed: Schema.Union([Schema.DateTimeUtcFromString, Schema.Number]),
  // A many edge whose element IS a DynamoModel.ref-annotated entity class.
  players: Schema.Array(I133Player.pipe(DynamoModel.ref)),
}) {}
const I133HolderAggregate = Aggregate.make(I133Holder, {
  table: I133Table,
  schema: I133Schema,
  pk: { field: "pk", composite: ["id"] },
  collection: { name: "holder" },
  root: { entityType: "HolderItem" },
  edges: {
    players: Aggregate.many("players", { entityType: "HolderPlayer", entity: I133Players }),
  },
})

describeConnected("#133 — aggregate DateTimes nested in containers", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { I133Teams, I133Coaches, I133Players, I133Umpires },
          tables: { I133Table },
        })
        yield* db.tables.I133Table.create()
        const dob = DateTime.makeUnsafe(I133_DOB)
        yield* db.entities.I133Teams.put({ id: "team-1", name: "Team One" })
        yield* db.entities.I133Teams.put({ id: "team-2", name: "Team Two" })
        yield* db.entities.I133Coaches.put({ id: "coach-1", name: "Coach One", dateOfBirth: dob })
        yield* db.entities.I133Coaches.put({ id: "coach-2", name: "Coach Two", dateOfBirth: dob })
        yield* db.entities.I133Players.put({ id: "player-1", name: "Player One", dateOfBirth: dob })
        yield* db.entities.I133Players.put({ id: "player-2", name: "Player Two", dateOfBirth: dob })
        yield* db.entities.I133Umpires.put({ id: "umpire-1", name: "Umpire One", dateOfBirth: dob })
      }).pipe(provideI133, Effect.scoped),
    )
  }, 30000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: i133TableName })
      }).pipe(
        provideI133,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 30000)

  it.effect("Union / Record / Tuple containers and an annotated-ref many edge round-trip", () =>
    Effect.gen(function* () {
      const dt = DateTime.makeUnsafe(I133_DOB_MS)
      yield* I133HolderAggregate.create({
        id: "h1",
        nullSelf: dt,
        arrNullSelf: [dt, null],
        rec: { a: I133_DOB },
        tup: ["x", I133_DOB],
        either: { at: I133_DOB },
        mixed: 5,
        players: ["player-1"],
      } as any)

      const client = yield* DynamoClient
      const { Items = [] } = yield* client.query({
        TableName: i133TableName,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: "$issue133#v1#holder#h1" } },
        ConsistentRead: true,
      })
      const root = Items.find((i) => i.__edd_e__?.S === "HolderItem")!
      const player = Items.find((i) => i.__edd_e__?.S === "HolderPlayer")!
      expect({
        nullSelf: root.nullSelf,
        arrNullSelf: root.arrNullSelf,
        rec: root.rec,
        tup: root.tup,
        either: root.either,
        mixed: root.mixed,
        dob: player.dateOfBirth,
      }).toEqual({
        nullSelf: { S: I133_DOB },
        arrNullSelf: { L: [{ S: I133_DOB }, { NULL: true }] },
        rec: { M: { a: { S: I133_DOB } } },
        tup: { L: [{ S: "x" }, { S: I133_DOB }] },
        either: { M: { at: { S: I133_DOB } } },
        mixed: { N: "5" },
        dob: { S: I133_DOB },
      })

      // A legacy map in the annotated-ref element reads back as a real DateTime.
      yield* client.updateItem({
        TableName: i133TableName,
        Key: { pk: player.pk!, sk: player.sk! },
        UpdateExpression: "SET #d = :v",
        ExpressionAttributeNames: { "#d": "dateOfBirth" },
        ExpressionAttributeValues: { ":v": i133LegacyMap("~effect/DateTime")(I133_DOB_MS) as any },
      })

      const got = (yield* I133HolderAggregate.get({ id: "h1" } as any)) as any
      expect(i133IsRealUtc(got.nullSelf, I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.arrNullSelf[0], I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.rec.a, I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.tup[1], I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.either.at, I133_DOB_MS)).toBe(true)
      expect(got.mixed).toBe(5)
      expect(i133IsRealUtc(got.players[0].dateOfBirth, I133_DOB_MS)).toBe(true)

      // A no-op update and a mutating one both succeed.
      yield* I133HolderAggregate.update({ id: "h1" } as any, (c: any) => c.state)
      const updated = (yield* I133HolderAggregate.update({ id: "h1" } as any, (c: any) => ({
        ...c.state,
        nullSelf: null,
        mixed: DateTime.makeUnsafe(I133_DOB_MS),
      }))) as any
      expect(updated.nullSelf).toBe(null)
      const after = (yield* I133HolderAggregate.get({ id: "h1" } as any)) as any
      expect(i133IsRealUtc(after.mixed, I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(after.players[0].dateOfBirth, I133_DOB_MS)).toBe(true)
    }).pipe(provideI133),
  )

  for (const kind of ["plain", "ref"] as const) {
    const MatchAggregate = makeI133Match(kind)
    const createMatch = (id: string) =>
      MatchAggregate.create({
        id,
        name: "Match",
        startDate: I133_DOB,
        matchDays: [I133_DOB, I133_DAY2],
        sessions: [{ number: 1, startTime: I133_DOB }],
        team1: {
          teamId: "team-1",
          coachId: "coach-1",
          homeTeam: true,
          players: [{ playerId: "player-1" }],
        },
        team2: {
          teamId: "team-2",
          coachId: "coach-2",
          homeTeam: false,
          players: [{ playerId: "player-2" }],
        },
        umpires: [{ umpireId: "umpire-1", role: "onfield" }],
      })
    const getMatch = (id: string) =>
      MatchAggregate.get({ id } as any) as Effect.Effect<any, unknown, any>

    /** Raw items of one match partition. */
    const rawItems = (id: string) =>
      Effect.gen(function* () {
        const client = yield* DynamoClient
        const { Items = [] } = yield* client.query({
          TableName: i133TableName,
          KeyConditionExpression: "#pk = :pk",
          ExpressionAttributeNames: { "#pk": "pk" },
          ExpressionAttributeValues: { ":pk": { S: `$issue133#v1#match${kind}#${id}` } },
          ConsistentRead: true,
        })
        return Items as ReadonlyArray<Record<string, any>>
      })
    const first = (items: ReadonlyArray<Record<string, any>>, entityType: string) =>
      items.find((item) => item.__edd_e__?.S === entityType)!

    /** Overwrite one nested attribute of a stored item with a legacy map. */
    const overwrite = (
      item: Record<string, any>,
      path: string,
      names: Record<string, string>,
      value: unknown,
    ) =>
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.updateItem({
          TableName: i133TableName,
          Key: { pk: item.pk, sk: item.sk },
          UpdateExpression: `SET ${path} = :v`,
          ExpressionAttributeNames: names,
          ExpressionAttributeValues: { ":v": value as any },
        })
      })

    const label = kind === "plain" ? "plain class element" : "DynamoModel.ref element"

    it.effect(`${label}: stores every date leaf in wire form (S)`, () =>
      Effect.gen(function* () {
        yield* createMatch("w1")
        const items = yield* rawItems("w1")
        const root = first(items, "MatchItem")
        const stored = {
          "MatchItem.startDate": root.startDate,
          "MatchItem.matchDays": root.matchDays,
          "MatchItem.sessions[0].startTime": root.sessions?.L?.[0]?.M?.startTime,
          "MatchPlayer.player.dateOfBirth": first(items, "MatchPlayer").player?.M?.dateOfBirth,
          "MatchUmpire.umpire.dateOfBirth": first(items, "MatchUmpire").umpire?.M?.dateOfBirth,
          "MatchCoach.dateOfBirth": first(items, "MatchCoach").dateOfBirth,
        }
        expect(stored).toEqual({
          "MatchItem.startDate": { S: I133_DOB },
          "MatchItem.matchDays": { L: [{ S: I133_DOB }, { S: I133_DAY2 }] },
          "MatchItem.sessions[0].startTime": { S: I133_DOB },
          "MatchPlayer.player.dateOfBirth": { S: I133_DOB },
          "MatchUmpire.umpire.dateOfBirth": { S: I133_DOB },
          "MatchCoach.dateOfBirth": { S: I133_DOB },
        })
        // The declared sk composite still keys the umpire row on its id.
        expect(first(items, "MatchUmpire").sk.S).toBe("$issue133#v1#matchumpire#onfield#umpire-1")
      }).pipe(provideI133),
    )

    it.effect(`${label}: reads a fresh write back as real DateTime.Utc instances`, () =>
      Effect.gen(function* () {
        yield* createMatch("w2")
        const got = yield* getMatch("w2")
        expect({
          startDate: i133IsRealUtc(got.startDate, I133_DOB_MS),
          "matchDays[0]": i133IsRealUtc(got.matchDays?.[0], I133_DOB_MS),
          "sessions[0].startTime": i133IsRealUtc(got.sessions?.[0]?.startTime, I133_DOB_MS),
          "team1.players[0].player.dateOfBirth": i133IsRealUtc(
            got.team1.players[0]?.player.dateOfBirth,
            I133_DOB_MS,
          ),
          "team2.players[0].player.dateOfBirth": i133IsRealUtc(
            got.team2.players[0]?.player.dateOfBirth,
            I133_DOB_MS,
          ),
          "umpires[0].umpire.dateOfBirth": i133IsRealUtc(
            got.umpires?.[0]?.umpire.dateOfBirth,
            I133_DOB_MS,
          ),
          "team1.coach.dateOfBirth": i133IsRealUtc(got.team1.coach.dateOfBirth, I133_DOB_MS),
        }).toEqual({
          startDate: true,
          "matchDays[0]": true,
          "sessions[0].startTime": true,
          "team1.players[0].player.dateOfBirth": true,
          "team2.players[0].player.dateOfBirth": true,
          "umpires[0].umpire.dateOfBirth": true,
          "team1.coach.dateOfBirth": true,
        })
      }).pipe(provideI133),
    )

    for (const [formName, form] of i133LegacyForms) {
      const id = (n: number) => `${formName.startsWith("rc") ? "rc" : "v4"}${n}`

      it.effect(`${label}: reads a stored ${formName} in MatchItem.matchDays[]`, () =>
        Effect.gen(function* () {
          yield* createMatch(id(3))
          const root = first(yield* rawItems(id(3)), "MatchItem")
          yield* overwrite(root, "#a[0]", { "#a": "matchDays" }, form(I133_DOB_MS))
          const got = yield* getMatch(id(3))
          expect(i133IsRealUtc(got.matchDays?.[0], I133_DOB_MS)).toBe(true)
        }).pipe(provideI133),
      )

      it.effect(`${label}: reads a stored ${formName} in MatchItem.sessions[].startTime`, () =>
        Effect.gen(function* () {
          yield* createMatch(id(4))
          const root = first(yield* rawItems(id(4)), "MatchItem")
          yield* overwrite(
            root,
            "#a[0].#b",
            { "#a": "sessions", "#b": "startTime" },
            form(I133_DOB_MS),
          )
          const got = yield* getMatch(id(4))
          expect(i133IsRealUtc(got.sessions?.[0]?.startTime, I133_DOB_MS)).toBe(true)
        }).pipe(provideI133),
      )

      it.effect(`${label}: reads a stored ${formName} in MatchPlayer.player.dateOfBirth`, () =>
        Effect.gen(function* () {
          yield* createMatch(id(5))
          const player = first(yield* rawItems(id(5)), "MatchPlayer")
          yield* overwrite(
            player,
            "#a.#b",
            { "#a": "player", "#b": "dateOfBirth" },
            form(I133_DOB_MS),
          )
          const got = yield* getMatch(id(5))
          for (const sheet of [...got.team1.players, ...got.team2.players]) {
            expect(i133IsRealUtc(sheet.player.dateOfBirth, I133_DOB_MS)).toBe(true)
          }
        }).pipe(provideI133),
      )

      it.effect(`${label}: reads a stored ${formName} in MatchUmpire.umpire.dateOfBirth`, () =>
        Effect.gen(function* () {
          yield* createMatch(id(6))
          const umpire = first(yield* rawItems(id(6)), "MatchUmpire")
          yield* overwrite(
            umpire,
            "#a.#b",
            { "#a": "umpire", "#b": "dateOfBirth" },
            form(I133_DOB_MS),
          )
          const got = yield* getMatch(id(6))
          expect(i133IsRealUtc(got.umpires?.[0]?.umpire.dateOfBirth, I133_DOB_MS)).toBe(true)
        }).pipe(provideI133),
      )
    }

    it.effect(`${label}: update over a legacy root row rewrites it in wire form`, () =>
      Effect.gen(function* () {
        yield* createMatch("u1")
        const root = first(yield* rawItems("u1"), "MatchItem")
        yield* overwrite(
          root,
          "#a[0]",
          { "#a": "matchDays" },
          i133LegacyMap("~effect/time/DateTime")(I133_DOB_MS),
        )
        const updated = yield* MatchAggregate.update({ id: "u1" } as any, (c: any) => ({
          ...c.state,
          name: "Renamed",
        }))
        expect((updated as any).name).toBe("Renamed")
        const after = first(yield* rawItems("u1"), "MatchItem")
        expect(after.matchDays).toEqual({ L: [{ S: I133_DOB }, { S: I133_DAY2 }] })
        const got = yield* getMatch("u1")
        expect(i133IsRealUtc(got.matchDays?.[0], I133_DOB_MS)).toBe(true)
        expect(i133IsRealUtc(got.team1.players[0]?.player.dateOfBirth, I133_DOB_MS)).toBe(true)
      }).pipe(provideI133),
    )
  }
})

// ===========================================================================
// #133 — entity self dates nested in Union / Record / Tuple containers
// ===========================================================================
//
// An entity substitutes self dates (`Schema.DateTimeUtc`, `storedAs(...)`) with
// a transform to their wire primitive, but the substitution stopped at a Union,
// a Record or a Tuple, so those fields stored the marshalled `DateTime` itself.
// Path-based update values were never encoded at all.

class E133Stamp extends Schema.Class<E133Stamp>("E133Stamp")({ at: Schema.DateTimeUtc }) {}
class E133Event extends Schema.Class<E133Event>("E133Event")({
  eventId: Schema.String.pipe(DynamoModel.identifier),
  venue: Schema.String,
  nullAt: Schema.NullOr(Schema.DateTimeUtc),
  nullMs: Schema.NullOr(Schema.DateTimeUtc.pipe(DynamoModel.storedAs(DynamoModel.DateEpochMs))),
  stamp: Schema.NullOr(E133Stamp),
  rec: Schema.Record(Schema.String, Schema.DateTimeUtc),
  tup: Schema.Tuple([Schema.String, Schema.DateTimeUtc]),
  arrNull: Schema.Array(Schema.NullOr(Schema.DateTimeUtc)),
  days: Schema.Array(Schema.DateTimeUtc),
}) {}
class E133Note extends Schema.Class<E133Note>("E133Note")({
  noteId: Schema.String,
  venue: Schema.String,
  event: E133Event.pipe(DynamoModel.ref),
}) {}

const E133Schema = DynamoSchema.make({ name: "edd133e", version: 1 })
const E133Events = Entity.make({
  model: E133Event,
  entityType: "E133Event",
  primaryKey: {
    pk: { field: "pk", composite: ["eventId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byVenue: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["venue"] },
      sk: { field: "gsi1sk", composite: ["eventId"] },
      collection: "e133venue",
    },
  },
})
const E133Notes = Entity.make({
  model: E133Note,
  entityType: "E133Note",
  primaryKey: {
    pk: { field: "pk", composite: ["noteId"] },
    sk: { field: "sk", composite: [] },
  },
  indexes: {
    byVenue: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["venue"] },
      sk: { field: "gsi1sk", composite: ["noteId"] },
      collection: "e133venue",
    },
  },
  refs: { event: { entity: E133Events } },
})
const E133Table = Table.make({ schema: E133Schema, entities: { E133Events, E133Notes } })
const e133TableName = `edd133e-${Date.now()}`
const provideE133 = Effect.provide(
  Layer.mergeAll(ClientLayer, E133Table.layer({ name: e133TableName })),
)
const e133Client = DynamoClient.make({
  entities: { E133Events, E133Notes },
  tables: { E133Table },
})

const E133_DOB = "2000-01-01T00:00:00.000Z"
const E133_DOB_MS = 946684800000
const E133_LATER = "2000-01-01T00:00:01.000Z"
const E133_LATER_MS = E133_DOB_MS + 1000
const e133Dt = DateTime.makeUnsafe(E133_DOB_MS)
const e133Later = DateTime.makeUnsafe(E133_LATER_MS)

const e133Raw = (pk: string) =>
  Effect.gen(function* () {
    const client = yield* DynamoClient
    const { Item } = yield* client.getItem({
      TableName: e133TableName,
      Key: { pk: { S: pk }, sk: { S: "$edd133e#v1#e133event" } },
      ConsistentRead: true,
    })
    return Item as Record<string, any>
  })

const e133Event = (eventId: string) => ({
  eventId,
  venue: "mcg",
  nullAt: e133Dt,
  nullMs: e133Dt,
  stamp: new E133Stamp({ at: e133Dt }),
  rec: { a: e133Dt },
  tup: ["x", e133Dt] as const,
  arrNull: [e133Dt, null],
  days: [e133Dt],
})

describeConnected("#133 — entity self dates nested in containers", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* e133Client
        yield* db.tables.E133Table.create()
      }).pipe(provideE133, Effect.scoped),
    )
  }, 30000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: e133TableName })
      }).pipe(
        provideE133,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 30000)

  it.effect("stores wire form, reads real instances, and keeps keys", () =>
    Effect.gen(function* () {
      const db = yield* e133Client
      yield* db.entities.E133Events.put(e133Event("ev1") as any)
      const item = yield* e133Raw("$edd133e#v1#e133event#eventid_ev1")
      expect({
        nullAt: item.nullAt,
        nullMs: item.nullMs,
        stamp: item.stamp,
        rec: item.rec,
        tup: item.tup,
        arrNull: item.arrNull,
        gsi1pk: item.gsi1pk,
        gsi1sk: item.gsi1sk,
      }).toEqual({
        nullAt: { S: E133_DOB },
        nullMs: { N: String(E133_DOB_MS) },
        stamp: { M: { at: { S: E133_DOB } } },
        rec: { M: { a: { S: E133_DOB } } },
        tup: { L: [{ S: "x" }, { S: E133_DOB }] },
        arrNull: { L: [{ S: E133_DOB }, { NULL: true }] },
        gsi1pk: { S: "$edd133e#v1#e133venue#venue_mcg" },
        gsi1sk: { S: "$edd133e#v1#e133event_1#eventid_ev1" },
      })

      const got = (yield* db.entities.E133Events.get({ eventId: "ev1" })) as any
      expect(i133IsRealUtc(got.nullAt, E133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.nullMs, E133_DOB_MS)).toBe(true)
      expect(got.stamp).toBeInstanceOf(E133Stamp)
      expect(i133IsRealUtc(got.stamp.at, E133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.rec.a, E133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.tup[1], E133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.arrNull[0], E133_DOB_MS)).toBe(true)
    }).pipe(provideE133),
  )

  it.effect(".set() and path operations store wire form", () =>
    Effect.gen(function* () {
      const db = yield* e133Client
      yield* db.entities.E133Events.put(e133Event("ev2") as any)
      yield* db.entities.E133Events.update({ eventId: "ev2" }).set({
        nullAt: e133Later,
        rec: { b: e133Later },
      } as any)
      yield* db.entities.E133Events.update({ eventId: "ev2" })
        .pathSet({ segments: ["stamp", "at"], value: e133Later, isPath: false })
        .pathSet({ segments: ["days", 0], value: e133Later, isPath: false })
        .pathAppend({ segments: ["arrNull"], value: [e133Later] })
      const item = yield* e133Raw("$edd133e#v1#e133event#eventid_ev2")
      expect({
        nullAt: item.nullAt,
        rec: item.rec,
        stamp: item.stamp,
        days: item.days,
        arrNull: item.arrNull,
      }).toEqual({
        nullAt: { S: E133_LATER },
        rec: { M: { b: { S: E133_LATER } } },
        stamp: { M: { at: { S: E133_LATER } } },
        days: { L: [{ S: E133_LATER }] },
        arrNull: { L: [{ S: E133_DOB }, { NULL: true }, { S: E133_LATER }] },
      })
      const got = (yield* db.entities.E133Events.get({ eventId: "ev2" })) as any
      expect(i133IsRealUtc(got.stamp.at, E133_LATER_MS)).toBe(true)
      expect(i133IsRealUtc(got.arrNull[2], E133_LATER_MS)).toBe(true)
    }).pipe(provideE133),
  )

  it.effect("reads a legacy marshalled map back as a real DateTime", () =>
    Effect.gen(function* () {
      const db = yield* e133Client
      yield* db.entities.E133Events.put(e133Event("ev3") as any)
      const client = yield* DynamoClient
      yield* client.updateItem({
        TableName: e133TableName,
        Key: { pk: { S: "$edd133e#v1#e133event#eventid_ev3" }, sk: { S: "$edd133e#v1#e133event" } },
        UpdateExpression: "SET #n = :v, #r.#a = :v",
        ExpressionAttributeNames: { "#n": "nullAt", "#r": "rec", "#a": "a" },
        ExpressionAttributeValues: {
          ":v": i133LegacyMap("~effect/time/DateTime")(E133_DOB_MS) as any,
        },
      })
      const got = (yield* db.entities.E133Events.get({ eventId: "ev3" })) as any
      expect(i133IsRealUtc(got.nullAt, E133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.rec.a, E133_DOB_MS)).toBe(true)
    }).pipe(provideE133),
  )

  it.effect("Batch, Transaction, collection and ref hydration decode real instances", () =>
    Effect.gen(function* () {
      const db = yield* e133Client
      yield* Transaction.transactWrite([
        E133Events.put(e133Event("ev4") as any),
        E133Events.put(e133Event("ev5") as any),
      ])
      const [a, b] = yield* Batch.get([
        E133Events.get({ eventId: "ev4" }),
        E133Events.get({ eventId: "ev5" }),
      ])
      expect(i133IsRealUtc((a as any)?.nullAt, E133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc((b as any)?.tup[1], E133_DOB_MS)).toBe(true)
      const [c] = yield* Transaction.transactGet([E133Events.get({ eventId: "ev4" })])
      expect(i133IsRealUtc((c as any)?.rec.a, E133_DOB_MS)).toBe(true)

      // A plain entity's ref is hydrated from the target and denormalised.
      yield* db.entities.E133Notes.put({ noteId: "n1", venue: "mcg", eventId: "ev4" } as any)
      const client = yield* DynamoClient
      const { Item } = yield* client.getItem({
        TableName: e133TableName,
        Key: { pk: { S: "$edd133e#v1#e133note#noteid_n1" }, sk: { S: "$edd133e#v1#e133note" } },
        ConsistentRead: true,
      })
      expect((Item as any).event.M.nullAt).toEqual({ S: E133_DOB })
      expect((Item as any).event.M.rec).toEqual({ M: { a: { S: E133_DOB } } })
      const note = (yield* db.entities.E133Notes.get({ noteId: "n1" })) as any
      expect(i133IsRealUtc(note.event.nullAt, E133_DOB_MS)).toBe(true)

      const venue = yield* db.collections.e133venue!({ venue: "mcg" }).collect()
      const fromCollection = (venue.E133Events as Array<any>).find((e) => e.eventId === "ev4")
      expect(i133IsRealUtc(fromCollection?.nullAt, E133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc((venue.E133Notes as Array<any>)[0]?.event.tup[1], E133_DOB_MS)).toBe(
        true,
      )
    }).pipe(provideE133),
  )
})

// ===========================================================================
// #133 — a sub-aggregate nested inside a sub-aggregate
// ===========================================================================
//
// The nested sub-aggregate's rows were written with only their own
// discriminator, so assembly (which matches on the parent's too) found none —
// `Missing key at ["club"]["squad"]` — and two bindings of the parent keyed
// their inner rows identically.

class N133Player extends Schema.Class<N133Player>("N133Player")({ ...I133PersonFields }) {}
class N133Coach extends Schema.Class<N133Coach>("N133Coach")({ ...I133PersonFields }) {}
const N133Players = Entity.make({
  model: DynamoModel.configure(N133Player, { id: { field: "playerId", identifier: true } }),
  entityType: "Player",
  primaryKey: i133PkSk,
})
const N133Coaches = Entity.make({
  model: DynamoModel.configure(N133Coach, { id: { field: "coachId", identifier: true } }),
  entityType: "Coach",
  primaryKey: i133PkSk,
})
class N133SquadPlayer extends Schema.Class<N133SquadPlayer>("N133SquadPlayer")({
  player: N133Player,
  joined: Schema.DateTimeUtcFromString,
}) {}
class N133Squad extends Schema.Class<N133Squad>("N133Squad")({
  name: Schema.String,
  players: Schema.Array(N133SquadPlayer),
}) {}
class N133Club extends Schema.Class<N133Club>("N133Club")({
  name: Schema.String,
  coach: N133Coach.pipe(DynamoModel.ref),
  squad: N133Squad,
}) {}
class N133League extends Schema.Class<N133League>("N133League")({
  id: Schema.String,
  season: Schema.String,
  club1: N133Club,
  club2: N133Club,
}) {}

const N133Schema = DynamoSchema.make({ name: "edd133n", version: 1 })
const N133Table = Table.make({ schema: N133Schema, entities: { N133Players, N133Coaches } })
const N133SquadAggregate = Aggregate.make(N133Squad, {
  root: { entityType: "NSquad" },
  edges: {
    players: Aggregate.many("players", { entityType: "NSquadPlayer", entity: N133Players }),
  },
})
const N133ClubAggregate = Aggregate.make(N133Club, {
  root: { entityType: "NClub" },
  edges: {
    coach: Aggregate.one("coach", { entityType: "NCoach", entity: N133Coaches }),
    squad: N133SquadAggregate.with({ discriminator: { squadNo: 1 } }),
  },
})
const N133LeagueAggregate = Aggregate.make(N133League, {
  table: N133Table,
  schema: N133Schema,
  pk: { field: "pk", composite: ["id"] },
  collection: { name: "league" },
  list: {
    index: "gsi1",
    name: "leagues",
    pk: { field: "gsi1pk", composite: ["season"] },
    sk: { field: "gsi1sk", composite: ["id"] },
  },
  root: { entityType: "NLeague" },
  edges: {
    club1: N133ClubAggregate.with({ discriminator: { clubNo: 1 } }),
    club2: N133ClubAggregate.with({ discriminator: { clubNo: 2 } }),
  },
})
const n133TableName = `edd133n-${Date.now()}`
const provideN133 = Effect.provide(
  Layer.mergeAll(ClientLayer, N133Table.layer({ name: n133TableName })),
)
const n133Client = DynamoClient.make({
  entities: { N133Players, N133Coaches },
  aggregates: { N133LeagueAggregate },
  tables: { N133Table },
})
const n133League = (id: string) => ({
  id,
  season: "2026",
  club1: {
    name: "One",
    coachId: "c1",
    squad: { name: "A", players: [{ playerId: "p1", joined: I133_DOB }] },
  },
  club2: {
    name: "Two",
    coachId: "c2",
    squad: { name: "B", players: [{ playerId: "p2", joined: I133_DAY2 }] },
  },
})

describeConnected("#133 — nested sub-aggregates", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* n133Client
        yield* db.tables.N133Table.create()
        const dob = DateTime.makeUnsafe(I133_DOB)
        yield* db.entities.N133Players.put({ id: "p1", name: "P1", dateOfBirth: dob })
        yield* db.entities.N133Players.put({ id: "p2", name: "P2", dateOfBirth: dob })
        yield* db.entities.N133Coaches.put({ id: "c1", name: "C1", dateOfBirth: dob })
        yield* db.entities.N133Coaches.put({ id: "c2", name: "C2", dateOfBirth: dob })
      }).pipe(provideN133, Effect.scoped),
    )
  }, 30000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: n133TableName })
      }).pipe(
        provideN133,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 30000)

  it.effect("create, get, list, update and delete across both levels", () =>
    Effect.gen(function* () {
      const db = yield* n133Client
      const leagues = db.aggregates.N133LeagueAggregate
      yield* leagues.create(n133League("l1") as any)
      yield* leagues.create(n133League("l2") as any)

      const client = yield* DynamoClient
      const raw = (id: string) =>
        client
          .query({
            TableName: n133TableName,
            KeyConditionExpression: "#pk = :pk",
            ExpressionAttributeNames: { "#pk": "pk" },
            ExpressionAttributeValues: { ":pk": { S: `$edd133n#v1#league#${id}` } },
            ConsistentRead: true,
          })
          .pipe(Effect.map(({ Items = [] }) => Items as ReadonlyArray<Record<string, any>>))
      const sks = (yield* raw("l1")).map((i) => i.sk.S as string).sort()
      expect(sks).toEqual([
        "$edd133n#v1#nclub#clubno#0000000000000001",
        "$edd133n#v1#nclub#clubno#0000000000000002",
        "$edd133n#v1#ncoach#clubno#0000000000000001",
        "$edd133n#v1#ncoach#clubno#0000000000000002",
        "$edd133n#v1#nleague",
        "$edd133n#v1#nsquad#clubno#0000000000000001#squadno#0000000000000001",
        "$edd133n#v1#nsquad#clubno#0000000000000002#squadno#0000000000000001",
        "$edd133n#v1#nsquadplayer#clubno#0000000000000001#squadno#0000000000000001#p1",
        "$edd133n#v1#nsquadplayer#clubno#0000000000000002#squadno#0000000000000001#p2",
      ])

      const got = (yield* leagues.get({ id: "l1" } as any)) as N133League
      expect(got.club2.squad.name).toBe("B")
      expect(i133IsRealUtc(got.club2.squad.players[0]!.joined, I133_DOB_MS + 86_400_000)).toBe(true)
      expect(i133IsRealUtc(got.club1.coach.dateOfBirth, I133_DOB_MS)).toBe(true)

      const listed = yield* leagues.list({ season: "2026" })
      expect(listed.data.map((l: any) => l.id).sort()).toEqual(["l1", "l2"])
      expect(listed.data.every((l: any) => l.club1.squad.name === "A")).toBe(true)

      yield* leagues.update({ id: "l1" } as any, (c: any) => ({
        ...c.state,
        club2: { ...c.state.club2, squad: { ...c.state.club2.squad, name: "B2" } },
      }))
      const updated = (yield* leagues.get({ id: "l1" } as any)) as N133League
      expect(updated.club2.squad.name).toBe("B2")
      expect(updated.club1.squad.name).toBe("A")

      yield* leagues.delete({ id: "l1" } as any)
      expect(yield* raw("l1")).toEqual([])
      expect((yield* raw("l2")).length).toBe(9)
    }).pipe(provideN133),
  )
  it.effect("create of an existing aggregate fails and writes nothing (#134)", () =>
    Effect.gen(function* () {
      const db = yield* n133Client
      const leagues = db.aggregates.N133LeagueAggregate
      const client = yield* DynamoClient
      const raw = (id: string) =>
        client
          .query({
            TableName: n133TableName,
            KeyConditionExpression: "#pk = :pk",
            ExpressionAttributeNames: { "#pk": "pk" },
            ExpressionAttributeValues: { ":pk": { S: `$edd133n#v1#league#${id}` } },
            ConsistentRead: true,
          })
          .pipe(Effect.map(({ Items = [] }) => Items as ReadonlyArray<Record<string, any>>))
      yield* leagues.create(n133League("dup") as any)
      const before = yield* raw("dup")
      expect(before.length).toBe(9)
      // Different root fields, edges and sub-aggregate rows — new sort keys
      // (p2 in club1, p1 in club2) that must not appear.
      const again = {
        ...n133League("dup"),
        season: "2027",
        club1: {
          name: "Uno",
          coachId: "c2",
          squad: { name: "X", players: [{ playerId: "p2", joined: I133_DAY2 }] },
        },
        club2: {
          name: "Dos",
          coachId: "c1",
          squad: { name: "Y", players: [{ playerId: "p1", joined: I133_DOB }] },
        },
      }
      const error = yield* leagues.create(again as any).pipe(Effect.flip)
      expect(error._tag).toBe("ConditionalCheckFailed")
      const after = yield* raw("dup")
      const bySk = (items: ReadonlyArray<Record<string, any>>) =>
        [...items].sort((a, b) => (a.sk.S < b.sk.S ? -1 : 1))
      expect(bySk(after)).toEqual(bySk(before))
      const listed = yield* leagues.list({ season: "2027" })
      expect(listed.data).toEqual([])
      // A list filter follows the empty-part rules: refused, never widened.
      for (const filter of [
        (_: any, o: any) => o.or(),
        (t: any, o: any) => o.or(o.eq(t.id, "dup"), o.and()),
      ]) {
        const refused = yield* leagues.list({ season: "2026" }, { filter } as any).pipe(Effect.flip)
        expect(refused._tag).toBe("ValidationError")
      }
      // Orphan edge rows (the root gone, its edges left) don't stop create;
      // `delete` clears the whole partition, orphans included, as documented.
      yield* client.deleteItem({
        TableName: n133TableName,
        Key: { pk: { S: "$edd133n#v1#league#dup" }, sk: { S: "$edd133n#v1#nleague" } },
      })
      expect((yield* raw("dup")).length).toBe(8)
      yield* leagues.delete({ id: "dup" } as any)
      expect(yield* raw("dup")).toEqual([])
      yield* leagues.create(again as any)
      const fresh = (yield* leagues.get({ id: "dup" } as any)) as N133League
      expect(fresh.club1.name).toBe("Uno")
      expect((yield* raw("dup")).length).toBe(9)
    }).pipe(provideN133),
  )
})

// ===========================================================================
// #133 — unions with a colliding member, wire-form path values, configured
// union storage, legacy raw values and plain bigints
// ===========================================================================

class X133Row extends Schema.Class<X133Row>("X133Row")({
  id: Schema.String,
  dateOrText: Schema.Union([Schema.DateTimeUtc, Schema.String]),
  textOrDate: Schema.Union([Schema.String, Schema.DateTimeUtc]),
  epochOrNull: Schema.NullOr(
    Schema.DateTimeUtc.pipe(DynamoModel.storedAs(DynamoModel.DateEpochMs)),
  ),
  configured: Schema.NullOr(Schema.DateTimeUtc),
  b64: Schema.StringFromBase64,
  nfs: Schema.NumberFromString,
  big: Schema.BigInt,
}) {}
const X133Schema = DynamoSchema.make({ name: "edd133x", version: 1 })
const X133Rows = Entity.make({
  model: DynamoModel.configure(X133Row, { configured: { storedAs: DynamoModel.DateEpochMs } }),
  entityType: "X133Row",
  primaryKey: { pk: { field: "pk", composite: ["id"] }, sk: { field: "sk", composite: [] } },
})
class X133Agg extends Schema.Class<X133Agg>("X133Agg")({
  id: Schema.String,
  dateOrText: Schema.Union([Schema.DateTimeUtc, Schema.String]),
  big: Schema.BigInt,
}) {}
const X133Table = Table.make({ schema: X133Schema, entities: { X133Rows } })
const X133Aggregate = Aggregate.make(X133Agg, {
  table: X133Table,
  schema: X133Schema,
  pk: { field: "pk", composite: ["id"] },
  collection: { name: "xagg" },
  root: { entityType: "X133AggItem" },
  edges: {},
})
const x133TableName = `edd133x-${Date.now()}`
const provideX133 = Effect.provide(
  Layer.mergeAll(ClientLayer, X133Table.layer({ name: x133TableName })),
)
const x133Client = DynamoClient.make({
  entities: { X133Rows },
  aggregates: { X133Aggregate },
  tables: { X133Table },
})
const x133Key = (id: string) => ({
  pk: { S: `$edd133x#v1#x133row#id_${id}` },
  sk: { S: "$edd133x#v1#x133row" },
})

describeConnected("#133 — colliding unions, wire path values, legacy raw values", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* x133Client
        yield* db.tables.X133Table.create()
      }).pipe(provideX133, Effect.scoped),
    )
  }, 30000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: x133TableName })
      }).pipe(
        provideX133,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 30000)

  it.effect("entity: each union member reads back as itself; storage as configured", () =>
    Effect.gen(function* () {
      const db = yield* x133Client
      const client = yield* DynamoClient
      const dt = DateTime.makeUnsafe(I133_DOB_MS)
      const base = { b64: "hi", nfs: 1, big: 12345678901234567890n }
      yield* db.entities.X133Rows.put({
        id: "a",
        dateOrText: "2020",
        textOrDate: "5",
        epochOrNull: null,
        configured: dt,
        ...base,
      } as any)
      yield* db.entities.X133Rows.put({
        id: "b",
        dateOrText: dt,
        textOrDate: dt,
        epochOrNull: dt,
        configured: null,
        ...base,
      } as any)
      const raw = (id: string) =>
        client
          .getItem({ TableName: x133TableName, Key: x133Key(id), ConsistentRead: true })
          .pipe(Effect.map(({ Item }) => Item as Record<string, any>))
      const a = yield* raw("a")
      const b = yield* raw("b")
      expect([a.dateOrText, a.textOrDate, a.epochOrNull, a.configured]).toEqual([
        { S: "2020" },
        { S: "5" },
        { NULL: true },
        { N: String(I133_DOB_MS) },
      ])
      expect([b.dateOrText, b.textOrDate, b.epochOrNull, b.big]).toEqual([
        { S: I133_DOB },
        { S: I133_DOB },
        { N: String(I133_DOB_MS) },
        { N: "12345678901234567890" },
      ])

      const gotA = (yield* db.entities.X133Rows.get({ id: "a" })) as any
      expect([gotA.dateOrText, gotA.textOrDate, gotA.epochOrNull]).toEqual(["2020", "5", null])
      expect(i133IsRealUtc(gotA.configured, I133_DOB_MS)).toBe(true)
      expect(gotA.big).toBe(12345678901234567890n)
      const gotB = (yield* db.entities.X133Rows.get({ id: "b" })) as any
      expect(i133IsRealUtc(gotB.dateOrText, I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(gotB.textOrDate, I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(gotB.epochOrNull, I133_DOB_MS)).toBe(true)

      // Path values: ambiguous wire values pass through, domain values encode.
      yield* db.entities.X133Rows.update({ id: "a" })
        .pathSet({ segments: ["b64"], value: "aGk=", isPath: false })
        .pathSet({ segments: ["nfs"], value: 7, isPath: false })
        .pathSet({ segments: ["configured"], value: dt, isPath: false })
      const a2 = yield* raw("a")
      expect([a2.b64, a2.nfs, a2.configured]).toEqual([
        { S: "aGk=" },
        { S: "7" },
        { N: String(I133_DOB_MS) },
      ])
    }).pipe(provideX133),
  )

  it.effect("entity: rows left by older releases read back", () =>
    Effect.gen(function* () {
      const db = yield* x133Client
      const client = yield* DynamoClient
      yield* client.putItem({
        TableName: x133TableName,
        Item: {
          ...x133Key("legacy"),
          __edd_e__: { S: "X133Row" },
          id: { S: "legacy" },
          dateOrText: { S: "2020" },
          textOrDate: i133LegacyMap("~effect/time/DateTime")(I133_DOB_MS) as any,
          epochOrNull: { N: String(I133_DOB_MS) },
          configured: i133LegacyMap("~effect/DateTime")(I133_DOB_MS) as any,
          b64: { S: "aGk=" },
          nfs: { N: "3" },
          big: { N: "9" },
        },
      })
      const got = (yield* db.entities.X133Rows.get({ id: "legacy" })) as any
      expect(got.dateOrText).toBe("2020")
      expect(i133IsRealUtc(got.textOrDate, I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.epochOrNull, I133_DOB_MS)).toBe(true)
      expect(i133IsRealUtc(got.configured, I133_DOB_MS)).toBe(true)
      expect(got.nfs).toBe(3)
      expect(got.big).toBe(9n)
    }).pipe(provideX133),
  )

  it.effect("aggregate: a root union and a plain bigint round-trip", () =>
    Effect.gen(function* () {
      const db = yield* x133Client
      const aggs = db.aggregates.X133Aggregate
      yield* aggs.create({ id: "g1", dateOrText: "2020", big: 5n } as any)
      yield* aggs.create({
        id: "g2",
        dateOrText: DateTime.makeUnsafe(I133_DOB_MS),
        big: 12345678901234567890n,
      } as any)
      const g1 = (yield* aggs.get({ id: "g1" } as any)) as any
      const g2 = (yield* aggs.get({ id: "g2" } as any)) as any
      expect(g1.dateOrText).toBe("2020")
      expect(g1.big).toBe(5n)
      expect(i133IsRealUtc(g2.dateOrText, I133_DOB_MS)).toBe(true)
      expect(g2.big).toBe(12345678901234567890n)
      yield* aggs.update({ id: "g1" } as any, (c: any) => c.state)
    }).pipe(provideX133),
  )
})

// ===========================================================================
// #133 — nested unions, zoned offsets and class values set by path
// ===========================================================================

class Y133Cred extends Schema.Class<Y133Cred>("Y133Cred")({
  user: Schema.String,
  token: Schema.Redacted(Schema.String),
  issued: Schema.Date,
  at: Schema.DateTimeUtc,
}) {}
class Y133Row extends Schema.Class<Y133Row>("Y133Row")({
  id: Schema.String,
  nested: Schema.Union([Schema.NullOr(Schema.DateTimeUtc), Schema.String]),
  zoned: Schema.DateTimeZoned,
  zonedOrText: Schema.Union([Schema.DateTimeZoned, Schema.String]),
  cred: Y133Cred,
  creds: Schema.Array(Y133Cred),
  dmZoned: DynamoModel.DateTimeZoned,
  mixed: Schema.Struct({
    b64: Schema.StringFromBase64,
    at: Schema.DateTimeUtc,
    n: Schema.NumberFromString,
  }),
}) {}
const Y133Schema = DynamoSchema.make({ name: "edd133y", version: 1 })
const Y133Rows = Entity.make({
  model: Y133Row,
  entityType: "Y133Row",
  primaryKey: { pk: { field: "pk", composite: ["id"] }, sk: { field: "sk", composite: [] } },
})
const Y133Table = Table.make({ schema: Y133Schema, entities: { Y133Rows } })
const y133TableName = `edd133y-${Date.now()}`
const provideY133 = Effect.provide(
  Layer.mergeAll(ClientLayer, Y133Table.layer({ name: y133TableName })),
)
const y133Client = DynamoClient.make({ entities: { Y133Rows }, tables: { Y133Table } })

describeConnected("#133 — nested unions, zoned offsets, class path values", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* y133Client
        yield* db.tables.Y133Table.create()
      }).pipe(provideY133, Effect.scoped),
    )
  }, 30000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: y133TableName })
      }).pipe(
        provideY133,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 30000)

  it.effect("stores and reads each member, zone and class value faithfully", () =>
    Effect.gen(function* () {
      const db = yield* y133Client
      const client = yield* DynamoClient
      const offset = DateTime.makeZonedUnsafe(I133_DOB_MS, {
        timeZone: DateTime.zoneMakeOffset(5 * 3_600_000),
      })
      const named = DateTime.makeZonedUnsafe(I133_DOB_MS, { timeZone: "Europe/London" })
      const cred = new Y133Cred({
        user: "u",
        token: Redacted.make("secret"),
        issued: new Date(I133_DOB_MS),
        at: DateTime.makeUnsafe(I133_DOB_MS),
      })
      const storedCred = {
        M: {
          user: { S: "u" },
          token: { S: "secret" },
          issued: { S: I133_DOB },
          at: { S: I133_DOB },
        },
      }
      yield* db.entities.Y133Rows.put({
        id: "y",
        nested: "2020",
        zoned: offset,
        zonedOrText: named,
        cred,
        creds: [],
        dmZoned: offset,
        mixed: { b64: "hi", at: DateTime.makeUnsafe(I133_DOB_MS), n: 1 },
      } as any)
      yield* db.entities.Y133Rows.update({ id: "y" })
        .pathSet({ segments: ["cred"], value: cred, isPath: false })
        .pathAppend({ segments: ["creds"], value: [cred] })
        // `n` already wire, `at` domain, `b64` a plain string: encoded leaf by leaf.
        .pathSet({
          segments: ["mixed"],
          value: { b64: "hi", at: DateTime.makeUnsafe(I133_DOB_MS), n: "5" },
          isPath: false,
        })
      const { Item } = yield* client.getItem({
        TableName: y133TableName,
        Key: { pk: { S: "$edd133y#v1#y133row#id_y" }, sk: { S: "$edd133y#v1#y133row" } },
        ConsistentRead: true,
      })
      const item = Item as Record<string, any>
      expect([item.nested, item.zoned, item.zonedOrText, item.cred, item.creds]).toEqual([
        { S: "2020" },
        { S: "2000-01-01T05:00:00.000+05:00" },
        { S: DateTime.formatIsoZoned(named) },
        storedCred,
        { L: [storedCred] },
      ])
      const got = (yield* db.entities.Y133Rows.get({ id: "y" })) as any
      expect(got.nested).toBe("2020")
      expect(DateTime.formatIsoZoned(got.zoned)).toBe("2000-01-01T05:00:00.000+05:00")
      expect(DateTime.formatIsoZoned(got.zonedOrText)).toBe(DateTime.formatIsoZoned(named))
      expect(Redacted.value(got.cred.token)).toBe("secret")
      expect(got.creds[0].issued.getTime()).toBe(I133_DOB_MS)
      expect(item.dmZoned).toEqual({ S: "2000-01-01T05:00:00.000+05:00" })
      expect(DateTime.formatIsoZoned(got.dmZoned)).toBe("2000-01-01T05:00:00.000+05:00")
      expect(item.mixed).toEqual({
        M: { b64: { S: "aGk=" }, at: { S: I133_DOB }, n: { S: "5" } },
      })
      expect(got.mixed.b64).toBe("hi")
      expect(i133IsRealUtc(got.mixed.at, I133_DOB_MS)).toBe(true)
      expect(got.mixed.n).toBe(5)
    }).pipe(provideY133),
  )
})

// ===========================================================================
// #133 — container checks enforced on writes, path values under a ref field
// ===========================================================================

class Z133Author extends Schema.Class<Z133Author>("Z133Author")({
  authorId: Schema.String.pipe(DynamoModel.identifier),
  name: Schema.String,
  born: Schema.DateTimeUtc,
  rank: Schema.NumberFromString,
}) {}
class Z133Note extends Schema.Class<Z133Note>("Z133Note")({
  id: Schema.String,
  days: Schema.Array(Schema.DateTimeUtc).check(Schema.isMaxLength(2)),
  author: Z133Author.pipe(DynamoModel.ref),
}) {}
class Z133Agg extends Schema.Class<Z133Agg>("Z133Agg")({
  id: Schema.String,
  days: Schema.Array(Schema.DateTimeUtc).check(Schema.isMaxLength(2)),
}) {}
const Z133Schema = DynamoSchema.make({ name: "edd133z", version: 1 })
const Z133Authors = Entity.make({
  model: Z133Author,
  entityType: "Z133Author",
  primaryKey: { pk: { field: "pk", composite: ["authorId"] }, sk: { field: "sk", composite: [] } },
})
const Z133Notes = Entity.make({
  model: Z133Note,
  entityType: "Z133Note",
  primaryKey: { pk: { field: "pk", composite: ["id"] }, sk: { field: "sk", composite: [] } },
  refs: { author: { entity: Z133Authors } },
})
const Z133Table = Table.make({ schema: Z133Schema, entities: { Z133Authors, Z133Notes } })
const Z133Aggregate = Aggregate.make(Z133Agg, {
  table: Z133Table,
  schema: Z133Schema,
  pk: { field: "pk", composite: ["id"] },
  collection: { name: "zagg" },
  root: { entityType: "Z133AggItem" },
  edges: {},
})
const z133TableName = `edd133z-${Date.now()}`
const provideZ133 = Effect.provide(
  Layer.mergeAll(ClientLayer, Z133Table.layer({ name: z133TableName })),
)
const z133Client = DynamoClient.make({
  entities: { Z133Authors, Z133Notes },
  aggregates: { Z133Aggregate },
  tables: { Z133Table },
})

describeConnected("#133 — container checks on writes, path values under a ref", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* z133Client
        yield* db.tables.Z133Table.create()
        yield* db.entities.Z133Authors.put({
          authorId: "a1",
          name: "Ann",
          born: DateTime.makeUnsafe(I133_DOB_MS),
          rank: 1,
        })
      }).pipe(provideZ133, Effect.scoped),
    )
  }, 30000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: z133TableName })
      }).pipe(
        provideZ133,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 30000)

  it.effect("writes breaking a check are rejected; a stored violating row still reads", () =>
    Effect.gen(function* () {
      const db = yield* z133Client
      const client = yield* DynamoClient
      const dt = DateTime.makeUnsafe(I133_DOB_MS)
      const three = [dt, dt, dt]
      yield* db.entities.Z133Notes.put({ id: "n1", days: [dt], authorId: "a1" } as any)
      const notes = db.entities.Z133Notes as any
      for (const write of [
        notes.put({ id: "n2", days: three, authorId: "a1" }).asEffect(),
        notes.update({ id: "n1" }).set({ days: three }).asEffect(),
        notes
          .update({ id: "n1" })
          .pathSet({ segments: ["days"], value: three, isPath: false })
          .asEffect(),
        db.aggregates.Z133Aggregate.create({ id: "g1", days: three } as any),
      ] as ReadonlyArray<Effect.Effect<unknown, unknown>>) {
        expect(((yield* Effect.flip(write)) as { _tag?: string })._tag).toBe("ValidationError")
      }

      yield* db.aggregates.Z133Aggregate.create({ id: "g2", days: [dt] } as any)
      yield* client.updateItem({
        TableName: z133TableName,
        Key: { pk: { S: "$edd133z#v1#zagg#g2" }, sk: { S: "$edd133z#v1#z133aggitem" } },
        UpdateExpression: "SET #d = :v",
        ExpressionAttributeNames: { "#d": "days" },
        ExpressionAttributeValues: {
          ":v": { L: [{ S: I133_DOB }, { S: I133_DOB }, { S: I133_DOB }] },
        },
      })
      const got = (yield* db.aggregates.Z133Aggregate.get({ id: "g2" } as any)) as any
      expect(got.days).toHaveLength(3)
      yield* db.aggregates.Z133Aggregate.update({ id: "g2" } as any, (c: any) => ({
        ...c.state,
        days: [dt],
      }))
      expect(
        ((yield* db.aggregates.Z133Aggregate.get({ id: "g2" } as any)) as any).days,
      ).toHaveLength(1)
    }).pipe(provideZ133),
  )

  it.effect("path values under a DynamoModel.ref field are stored in wire form", () =>
    Effect.gen(function* () {
      const db = yield* z133Client
      const client = yield* DynamoClient
      yield* db.entities.Z133Notes.put({ id: "n3", days: [], authorId: "a1" } as any)
      yield* db.entities.Z133Notes.update({ id: "n3" })
        .pathSet({
          segments: ["author", "born"],
          value: DateTime.makeUnsafe(I133_DOB_MS + 1000),
          isPath: false,
        })
        .pathSet({ segments: ["author", "rank"], value: 7, isPath: false })
      const { Item } = yield* client.getItem({
        TableName: z133TableName,
        Key: { pk: { S: "$edd133z#v1#z133note#id_n3" }, sk: { S: "$edd133z#v1#z133note" } },
        ConsistentRead: true,
      })
      const author = (Item as any).author.M
      expect([author.born, author.rank]).toEqual([{ S: "2000-01-01T00:00:01.000Z" }, { S: "7" }])
      const got = (yield* db.entities.Z133Notes.get({ id: "n3" })) as any
      expect(i133IsRealUtc(got.author.born, I133_DOB_MS + 1000)).toBe(true)
      expect(got.author.rank).toBe(7)
    }).pipe(provideZ133),
  )
})

// ===========================================================================
// #133 — path operations on a versioned (retain) entity
// ===========================================================================

class R133Doc extends Schema.Class<R133Doc>("R133Doc")({
  id: Schema.String,
  n: Schema.Number,
  tags: Schema.Array(Schema.String),
  days: Schema.Array(Schema.DateTimeUtc).check(Schema.isMaxLength(3)),
  labels: Schema.ReadonlySet(Schema.String),
  nested: Schema.Struct({ at: Schema.DateTimeUtc, count: Schema.Number }),
  opt: Schema.optionalKey(Schema.String),
  gone: Schema.optionalKey(Schema.String),
}) {}
const R133Schema = DynamoSchema.make({ name: "edd133r", version: 1 })
const R133Docs = Entity.make({
  model: R133Doc,
  entityType: "R133Doc",
  primaryKey: { pk: { field: "pk", composite: ["id"] }, sk: { field: "sk", composite: [] } },
  versioned: { retain: true },
})
const R133Table = Table.make({ schema: R133Schema, entities: { R133Docs } })
const r133TableName = `edd133r-${Date.now()}`
const provideR133 = Effect.provide(
  Layer.mergeAll(ClientLayer, R133Table.layer({ name: r133TableName })),
)
const r133Client = DynamoClient.make({ entities: { R133Docs }, tables: { R133Table } })

describeConnected("#133 — path operations on a retain entity", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* r133Client
        yield* db.tables.R133Table.create()
      }).pipe(provideR133, Effect.scoped),
    )
  }, 30000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: r133TableName })
      }).pipe(
        provideR133,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 30000)

  it.effect("applies, validates and snapshots every path operation", () =>
    Effect.gen(function* () {
      const db = yield* r133Client
      const client = yield* DynamoClient
      const dt = DateTime.makeUnsafe(I133_DOB_MS)
      const later = DateTime.makeUnsafe(I133_DOB_MS + 1000)
      yield* db.entities.R133Docs.put({
        id: "d1",
        n: 1,
        tags: ["a", "b"],
        days: [dt],
        labels: new Set(["x", "y"]),
        nested: { at: dt, count: 5 },
        gone: "bye",
      } as any)
      const updated = (yield* db.entities.R133Docs.update({ id: "d1" })
        .expectedVersion(1)
        .pathSet({ segments: ["nested", "at"], value: later, isPath: false })
        .pathIfNotExists({ segments: ["opt"], value: "set" })
        .pathAdd({ segments: ["n"], value: 2 })
        .pathSubtract({ segments: ["nested", "count"], value: 1, isPath: false })
        .pathAppend({ segments: ["days"], value: [later] })
        .pathPrepend({ segments: ["tags"], value: ["first"] })
        .pathDelete({ segments: ["labels"], value: new Set(["x"]) })
        .pathRemove(["gone"])) as any
      expect(updated.n).toBe(3)
      expect([...updated.labels].sort()).toEqual(["y"])
      expect(updated.tags).toEqual(["first", "a", "b"])
      expect(updated.nested.count).toBe(4)
      expect(i133IsRealUtc(updated.nested.at, I133_DOB_MS + 1000)).toBe(true)
      expect(updated.days).toHaveLength(2)
      expect(updated.opt).toBe("set")
      expect(updated.gone).toBeUndefined()
      expect(updated.version).toBe(2)

      const raw = (sk: string) =>
        client
          .getItem({
            TableName: r133TableName,
            Key: { pk: { S: "$edd133r#v1#r133doc#id_d1" }, sk: { S: sk } },
            ConsistentRead: true,
          })
          .pipe(Effect.map(({ Item }) => Item as Record<string, any>))
      const current = yield* raw("$edd133r#v1#r133doc")
      expect(current.nested).toEqual({
        M: { at: { S: "2000-01-01T00:00:01.000Z" }, count: { N: "4" } },
      })
      const snapshot = yield* raw("$edd133r#v1#r133doc#v#0000001")
      expect([snapshot.n, snapshot.nested, snapshot.gone]).toEqual([
        { N: "1" },
        { M: { at: { S: I133_DOB }, count: { N: "5" } } },
        { S: "bye" },
      ])

      const docs = db.entities.R133Docs as any
      const stale = yield* Effect.flip(
        docs
          .update({ id: "d1" })
          .expectedVersion(1)
          .pathSet({ segments: ["n"], value: 7, isPath: false })
          .asEffect() as Effect.Effect<unknown, { readonly _tag: string }>,
      )
      expect(stale._tag).toBe("OptimisticLockError")
      const tooMany = yield* Effect.flip(
        docs
          .update({ id: "d1" })
          .pathSet({ segments: ["days"], value: [dt, dt, dt, dt], isPath: false })
          .asEffect() as Effect.Effect<unknown, { readonly _tag: string }>,
      )
      expect(tooMany._tag).toBe("ValidationError")
      expect((yield* raw("$edd133r#v1#r133doc")).version).toEqual({ N: "2" })
    }).pipe(provideR133),
  )
})

// ---------------------------------------------------------------------------
// Retain vs non-retain parity: DynamoDB applies the path operations of both
// ---------------------------------------------------------------------------

class P133Doc extends Schema.Class<P133Doc>("P133Doc")({
  id: Schema.String,
  name: Schema.String,
  tag: Schema.String,
  list: Schema.Array(Schema.String),
  n: Schema.Number,
  labels: Schema.ReadonlySet(Schema.String),
  opt: Schema.optional(Schema.Array(Schema.String)),
}) {}
const P133Schema = DynamoSchema.make({ name: "edd133p", version: 1 })
const p133Key = {
  pk: { field: "pk", composite: ["id"] },
  sk: { field: "sk", composite: [] },
} as const
const P133Retained = Entity.make({
  model: P133Doc,
  entityType: "P133Retained",
  primaryKey: p133Key,
  versioned: { retain: true },
})
const P133Plain = Entity.make({
  model: P133Doc,
  entityType: "P133Plain",
  primaryKey: p133Key,
  versioned: true,
})
const P133Table = Table.make({ schema: P133Schema, entities: { P133Retained, P133Plain } })
const p133TableName = `edd133p-${Date.now()}`
const provideP133 = Effect.provide(
  Layer.mergeAll(ClientLayer, P133Table.layer({ name: p133TableName })),
)
const p133Client = DynamoClient.make({
  entities: { P133Retained, P133Plain },
  tables: { P133Table },
})

describeConnected("#133 — retain path operations match DynamoDB's own", () => {
  beforeAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* p133Client
        yield* db.tables.P133Table.create()
      }).pipe(provideP133, Effect.scoped),
    )
  }, 30000)

  afterAll(async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteTable({ TableName: p133TableName })
      }).pipe(
        provideP133,
        Effect.scoped,
        Effect.catchTag("ResourceNotFoundError", () => Effect.void),
      ),
    )
  }, 30000)

  const sequences: ReadonlyArray<readonly [string, (u: any) => any]> = [
    ["remove list[0] and list[2]", (u) => u.pathRemove(["list", 0]).pathRemove(["list", 2])],
    [
      "set tag, copy tag into name",
      (u) =>
        u
          .pathSet({ segments: ["tag"], value: "t2", isPath: false })
          .pathSet({ segments: ["name"], value: undefined, isPath: true, valueSegments: ["tag"] }),
    ],
    [
      "set and remove the same path",
      (u) =>
        u.pathSet({ segments: ["list", 1], value: "z", isPath: false }).pathRemove(["list", 1]),
    ],
    [
      "if_not_exists on an absent attribute",
      (u) => u.pathIfNotExists({ segments: ["opt"], value: ["w"] }),
    ],
    [
      "if_not_exists on a present attribute",
      (u) => u.pathIfNotExists({ segments: ["n"], value: 99 }),
    ],
    ["append to a missing list", (u) => u.pathAppend({ segments: ["opt"], value: ["q"] })],
    ["delete from a non-set", (u) => u.pathDelete({ segments: ["list"], value: new Set(["a"]) })],
    [
      "set past the end, add, prepend",
      (u) =>
        u
          .pathSet({ segments: ["list", 10], value: "z", isPath: false })
          .pathAdd({ segments: ["n"], value: 5 })
          .pathAdd({ segments: ["labels"], value: new Set(["c"]) }),
    ],
  ]

  it.effect("each sequence stores the same item, or fails the same way", () =>
    Effect.gen(function* () {
      const db = yield* p133Client
      const client = yield* DynamoClient
      const read = (entityType: string, id: string, sk: string) =>
        client
          .getItem({
            TableName: p133TableName,
            Key: {
              pk: { S: `$edd133p#v1#${entityType.toLowerCase()}#id_${id}` },
              sk: { S: sk },
            },
            ConsistentRead: true,
          })
          .pipe(Effect.map(({ Item }) => Item as Record<string, any> | undefined))
      const fields = (item: Record<string, any> | undefined) => {
        if (item === undefined) return undefined
        // Each create draws its own random incarnation token (`__edd_i__`).
        const { pk, sk, __edd_e__, __edd_i__, updatedAt, createdAt, ...rest } = item
        return rest
      }
      const base = {
        name: "n",
        tag: "t",
        list: ["a", "b", "c", "d"],
        n: 1,
        labels: new Set(["a", "b"]),
      }
      for (const [index, [label, apply]] of sequences.entries()) {
        const id = `s${index}`
        yield* db.entities.P133Retained.put({ id, ...base } as any)
        yield* db.entities.P133Plain.put({ id, ...base } as any)
        // A record update first, so the path update below snapshots version 2
        // (`put` on a retain entity already writes the version 1 snapshot).
        yield* db.entities.P133Retained.update({ id }).set({ name: "pre" } as any)
        yield* db.entities.P133Plain.update({ id }).set({ name: "pre" } as any)
        const retained = yield* Effect.exit(
          Effect.suspend(
            () =>
              apply(db.entities.P133Retained.update({ id })).asEffect() as Effect.Effect<
                unknown,
                unknown
              >,
          ),
        )
        const plain = yield* Effect.exit(
          Effect.suspend(
            () =>
              apply(db.entities.P133Plain.update({ id })).asEffect() as Effect.Effect<
                unknown,
                unknown
              >,
          ),
        )
        const outcome = (exit: Exit.Exit<unknown, unknown>) =>
          Exit.isSuccess(exit) ? "ok" : String((Cause.squash(exit.cause) as any)?._tag ?? "error")
        expect([label, outcome(retained)]).toEqual([label, outcome(plain)])
        const retainedItem = yield* read("P133Retained", id, "$edd133p#v1#p133retained")
        const plainItem = yield* read("P133Plain", id, "$edd133p#v1#p133plain")
        expect([label, fields(retainedItem)]).toEqual([label, fields(plainItem)])
        // A snapshot of the pre-update item exactly when the update applied.
        const snapshot = yield* read("P133Retained", id, "$edd133p#v1#p133retained#v#0000002")
        if (Exit.isSuccess(retained)) {
          expect([label, snapshot?.name, snapshot?.list]).toEqual([
            label,
            { S: "pre" },
            { L: base.list.map((value) => ({ S: value })) },
          ])
        } else {
          expect([label, snapshot]).toEqual([label, undefined])
        }
      }
    }).pipe(provideP133),
  )
})

// ---------------------------------------------------------------------------
// #133 — path operations on index composites and unique fields; condition
// failures; the exact retain post-image
//
// A top-level `pathSet` / `pathRemove` on a field that feeds an index key or a
// unique sentinel goes through the same logic as `.set()` / `.remove()`. Each
// case runs the record form against one table and the path form against a
// second table with the same definition: every item in the two tables — main
// items, index keys, version snapshots, unique sentinels — must be identical.
// Covers the six canonical GSI-composite shapes on a plain and a retain entity.
// ---------------------------------------------------------------------------

const G133Schema = DynamoSchema.make({ name: "edd133g", version: 1 })
const g133Variants = <M extends Schema.Top>(
  name: string,
  model: M,
  config: {
    readonly primaryKey: Record<string, any>
    readonly indexes?: Record<string, any>
    readonly unique?: Record<string, any>
  },
) =>
  ({
    plain: Entity.make({
      model,
      entityType: `G133${name}Plain`,
      ...config,
      timestamps: true,
      versioned: true,
    } as any),
    retained: Entity.make({
      model,
      entityType: `G133${name}Retained`,
      ...config,
      timestamps: true,
      versioned: { retain: true },
    } as any),
  }) as const
const g133IdKey = { pk: { field: "pk", composite: ["id"] }, sk: { field: "sk", composite: [] } }

class G133Device extends Schema.Class<G133Device>("G133Device")({
  id: Schema.String,
  owner: Schema.optional(Schema.String),
  reading: Schema.optional(Schema.String),
  seq: Schema.optional(Schema.Number),
  label: Schema.optional(Schema.String),
}) {}
const g133DeviceIndexes = {
  byOwner: {
    name: "gsi1",
    pk: { field: "gsi1pk", composite: ["owner"] },
    sk: { field: "gsi1sk", composite: ["reading", "seq"] },
  },
}
const G133Devices = g133Variants("Device", G133Device, {
  primaryKey: g133IdKey,
  indexes: g133DeviceIndexes,
})
class G133Port extends Schema.Class<G133Port>("G133Port")({
  channel: Schema.String,
  deviceId: Schema.String,
  label: Schema.optional(Schema.String),
}) {}
const G133Ports = g133Variants("Port", G133Port, {
  primaryKey: {
    pk: { field: "pk", composite: ["channel"] },
    sk: { field: "sk", composite: ["deviceId"] },
  },
  indexes: {
    byChannel: {
      name: "gsi2",
      pk: { field: "gsi2pk", composite: ["channel"] },
      sk: { field: "gsi2sk", composite: ["deviceId"] },
    },
  },
})
class G133Site extends Schema.Class<G133Site>("G133Site")({
  id: Schema.String,
  region: Schema.optional(Schema.String),
  country: Schema.optional(Schema.String),
  city: Schema.optional(Schema.String),
  site: Schema.optional(Schema.String),
}) {}
const G133Sites = g133Variants("Site", G133Site, {
  primaryKey: g133IdKey,
  indexes: {
    byLocation: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["region"] },
      sk: { field: "gsi1sk", composite: ["country", "city", "site"] },
    },
  },
})
class G133Slot extends Schema.Class<G133Slot>("G133Slot")({
  id: Schema.String,
  tenant: Schema.optional(Schema.String),
  lead: Schema.optional(Schema.String),
  trail: Schema.optional(Schema.String),
}) {}
const G133Slots = g133Variants("Slot", G133Slot, {
  primaryKey: g133IdKey,
  indexes: {
    byTenant: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["tenant"] },
      sk: { field: "gsi1sk", composite: ["lead", "trail"] },
      indexPolicy: { sk: "sparse" },
    },
  },
})
class G133Task extends Schema.Class<G133Task>("G133Task")({
  id: Schema.String,
  category: Schema.optional(Schema.String),
  priority: Schema.optional(Schema.String),
}) {}
const G133Tasks = g133Variants("Task", G133Task, {
  primaryKey: g133IdKey,
  indexes: {
    byCategory: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["category"] },
      sk: { field: "gsi1sk", composite: ["priority"] },
    },
  },
})
class G133Binding extends Schema.Class<G133Binding>("G133Binding")({
  id: Schema.String,
  deviceBinding: Schema.optional(Schema.String),
}) {}
const G133Bindings = g133Variants("Binding", G133Binding, {
  primaryKey: g133IdKey,
  indexes: {
    byDeviceBinding: {
      name: "gsi1",
      pk: { field: "gsi1pk", composite: ["deviceBinding"] },
      sk: { field: "gsi1sk", composite: [] },
    },
  },
})
class G133Account extends Schema.Class<G133Account>("G133Account")({
  id: Schema.String,
  email: Schema.optional(Schema.String),
  name: Schema.optional(Schema.String),
}) {}
const G133Accounts = g133Variants("Account", G133Account, {
  primaryKey: g133IdKey,
  unique: { email: ["email"] },
})
class G133Defaults extends Schema.Class<G133Defaults>("G133Defaults")({
  id: Schema.String,
  name: Schema.String,
  born: Schema.DateTimeUtcFromString.pipe(
    Schema.withDecodingDefault(Effect.succeed("1800-01-01T00:00:00.000Z")),
  ),
  when: Schema.DateTimeUtc.pipe(
    Schema.withDecodingDefault(Effect.succeed(DateTime.makeUnsafe(86_400_000))),
  ),
  tier: Schema.String.pipe(Schema.withDecodingDefaultKey(Effect.succeed("basic"))),
}) {}
class G133Wide extends Schema.Class<G133Wide>("G133Wide")({
  id: Schema.String,
  email: Schema.optional(Schema.String),
  totals: Schema.Record(Schema.String, Schema.Number),
}) {}
class G133Coded extends Schema.Class<G133Coded>("G133Coded")({
  id: Schema.String,
  name: Schema.String,
  // A unique field that is also an index composite, with a default.
  code: Schema.String.pipe(Schema.withDecodingDefault(Effect.succeed("none"))),
  // A unique field with a default that no index uses.
  handle: Schema.String.pipe(Schema.withDecodingDefault(Effect.succeed("anon"))),
}) {}
class G133Sparse extends Schema.Class<G133Sparse>("G133Sparse")({
  id: Schema.String,
  name: Schema.String,
  team: Schema.optional(Schema.String),
  rank: Schema.Number,
}) {}
class G133Ledger extends Schema.Class<G133Ledger>("G133Ledger")({
  id: Schema.String,
  book: Schema.String,
  note: Schema.optional(Schema.String),
}) {}
class G133Author extends Schema.Class<G133Author>("G133Author")({
  id: Schema.String.pipe(DynamoModel.identifier),
  name: Schema.String,
}) {}
class G133Article extends Schema.Class<G133Article>("G133Article")({
  articleId: Schema.String,
  author: G133Author.pipe(DynamoModel.ref),
}) {}
const G133Authors = Entity.make({
  model: G133Author,
  entityType: "G133Author",
  primaryKey: g133IdKey as any,
})
const G133Counters = Entity.make({
  model: G133Device,
  entityType: "G133Counter",
  primaryKey: g133IdKey as any,
  indexes: g133DeviceIndexes,
})

class G133Vec extends Schema.Class<G133Vec>("G133Vec")({
  id: Schema.String,
  vec: Schema.Array(Schema.Number),
}) {}

// Live rows whose keys look like history (#133).
class G133Reading extends Schema.Class<G133Reading>("G133Reading")({
  ch: Schema.String,
  dev: Schema.String,
  ts: Schema.DateTimeUtc,
  v: Schema.optional(Schema.Number),
}) {}
const G133ReadingInput = Schema.Struct({
  ch: Schema.String,
  dev: Schema.String,
  ts: Schema.DateTimeUtc,
  v: Schema.optional(Schema.Number),
})
class G133Sub extends Schema.Class<G133Sub>("G133Sub")({
  id: Schema.String,
  label: Schema.String,
}) {}
// A collection on the primary key: an order and its lines share a partition.
class G133POrder extends Schema.Class<G133POrder>("G133POrder")({
  orderId: Schema.String,
  label: Schema.String,
}) {}
class G133PLine extends Schema.Class<G133PLine>("G133PLine")({
  orderId: Schema.String,
  lineId: Schema.String,
  qty: Schema.Number,
}) {}
const g133OrderKey = (sk: ReadonlyArray<string>) => ({
  collection: "g133OrderAll",
  type: "isolated",
  pk: { field: "pk", composite: ["orderId"] },
  sk: { field: "sk", composite: sk },
})

// A number composite stored as a string (#133): rows written by 1.15 have an
// unpadded key (`#seq_5`) the current composer doesn't reproduce.
class G133Seq extends Schema.Class<G133Seq>("G133Seq")({
  tenant: Schema.String,
  seq: Schema.NumberFromString,
  label: Schema.String,
}) {}

// Names a placeholder may not hold, and renamed fields — on an entity, its GSI
// collection, and a second member storing the same field under another name.
const G133Named = Schema.Struct({
  grp: Schema.String,
  hid: Schema.String,
  "first-name": Schema.String,
  label: Schema.String,
  addr: Schema.Struct({ city: Schema.String, zip: Schema.String }),
  tags: Schema.Array(Schema.String),
})
const G133NamedOther = Schema.Struct({
  grp: Schema.String,
  nid: Schema.String,
  label: Schema.String,
})
// A third member whose DOMAIN field is named what Hyphens stores `label` as.
const G133NamedThird = Schema.Struct({
  grp: Schema.String,
  tid: Schema.String,
  label: Schema.String,
  "the-label": Schema.String,
})
const g133NamedIndex = (sk: string) => ({
  byGrp: {
    name: "gsi2",
    collection: "g133Named",
    pk: { field: "gsi2pk", composite: ["grp"] },
    sk: { field: "gsi2sk", composite: [sk] },
  },
})

// Two entities sharing a GSI partition: one's filter must never reach the other.
const G133OrA = Schema.Struct({
  owner: Schema.String,
  aid: Schema.String,
  n: Schema.Number,
  code: Schema.optional(Schema.String),
})
const G133OrB = Schema.Struct({
  owner: Schema.String,
  bid: Schema.String,
  n: Schema.Number,
  secret: Schema.String,
})
const g133OrIndex = (sk: string) => ({
  byOwner: {
    name: "gsi2",
    collection: "g133Or",
    pk: { field: "gsi2pk", composite: ["owner"] },
    sk: { field: "gsi2sk", composite: [sk] },
  },
})

// Several items per partition (#133): a primary sort key with a composite.
class G133Line extends Schema.Class<G133Line>("G133Line")({
  order: Schema.String,
  line: Schema.String,
  label: Schema.String,
}) {}
const g133LineKey = {
  pk: { field: "pk", composite: ["order"] },
  sk: { field: "sk", composite: ["line"] },
}

const g133Entities = {
  DevicesPlain: G133Devices.plain,
  DevicesRetained: G133Devices.retained,
  PortsPlain: G133Ports.plain,
  PortsRetained: G133Ports.retained,
  SitesPlain: G133Sites.plain,
  SitesRetained: G133Sites.retained,
  SlotsPlain: G133Slots.plain,
  SlotsRetained: G133Slots.retained,
  TasksPlain: G133Tasks.plain,
  TasksRetained: G133Tasks.retained,
  BindingsPlain: G133Bindings.plain,
  BindingsRetained: G133Bindings.retained,
  AccountsPlain: G133Accounts.plain,
  AccountsRetained: G133Accounts.retained,
  Counters: G133Counters,
  AccountsBare: Entity.make({
    model: G133Account,
    entityType: "G133AccountBare",
    primaryKey: g133IdKey as any,
    unique: { email: ["email"] },
    timestamps: true,
  }),
  SoftDevices: Entity.make({
    model: G133Device,
    entityType: "G133SoftDevice",
    primaryKey: g133IdKey as any,
    timestamps: true,
    versioned: { retain: true },
    softDelete: true,
  }),
  SoftAccounts: Entity.make({
    model: G133Account,
    entityType: "G133SoftAccount",
    primaryKey: g133IdKey as any,
    unique: { email: ["email"] },
    timestamps: true,
    softDelete: true,
  }),
  Ledgers: Entity.make({
    model: DynamoModel.configure(G133Ledger, { book: { immutable: true } }),
    entityType: "G133Ledger",
    primaryKey: g133IdKey as any,
    timestamps: true,
    versioned: true,
  }),
  LedgersRetained: Entity.make({
    model: DynamoModel.configure(G133Ledger, { book: { immutable: true } }),
    entityType: "G133LedgerRetained",
    primaryKey: g133IdKey as any,
    timestamps: true,
    versioned: { retain: true },
  }),
  Authors: G133Authors,
  Coded: Entity.make({
    model: G133Coded,
    entityType: "G133Coded",
    primaryKey: g133IdKey as any,
    indexes: {
      byCode: {
        name: "gsi1",
        pk: { field: "gsi1pk", composite: ["code"] },
        sk: { field: "gsi1sk", composite: [] },
      },
    },
    unique: { code: ["code"], handle: ["handle"] },
    timestamps: true,
  }),
  SoftVersioned: Entity.make({
    model: G133Account,
    entityType: "G133SoftVersioned",
    primaryKey: g133IdKey as any,
    versioned: true,
    softDelete: true,
  }),
  Defaults: Entity.make({
    model: G133Defaults,
    entityType: "G133Defaults",
    primaryKey: g133IdKey as any,
    indexes: {
      byTier: {
        name: "gsi1",
        pk: { field: "gsi1pk", composite: ["tier"] },
        sk: { field: "gsi1sk", composite: ["born"] },
      },
    },
    timestamps: true,
  }),
  WideStamped: Entity.make({
    model: DynamoModel.configure(G133Wide, { totals: { storedAs: DynamoModel.SparseMap() } }),
    entityType: "G133WideStamped",
    primaryKey: g133IdKey as any,
    unique: { email: ["email"] },
    timestamps: true,
    softDelete: true,
  }),
  WideBare: Entity.make({
    model: DynamoModel.configure(G133Wide, { totals: { storedAs: DynamoModel.SparseMap() } }),
    entityType: "G133WideBare",
    primaryKey: g133IdKey as any,
    unique: { email: ["email"] },
    softDelete: true,
  }),
  Sparse: Entity.make({
    model: G133Sparse,
    entityType: "G133Sparse",
    primaryKey: g133IdKey as any,
    indexes: {
      byTeam: {
        name: "gsi1",
        pk: { field: "gsi1pk", composite: ["team"] },
        sk: { field: "gsi1sk", composite: ["rank"] },
      },
    },
    timestamps: true,
    versioned: true,
  }),
  Vecs: Entity.make({ model: G133Vec, entityType: "G133Vec", primaryKey: g133IdKey as any }),
  Seqs: Entity.make({
    model: G133Seq,
    entityType: "G133Seq",
    primaryKey: {
      pk: { field: "pk", composite: ["tenant"] },
      sk: { field: "sk", composite: ["seq"] },
    } as any,
    versioned: { retain: true },
  }),
  Hyphens: Entity.make({
    model: DynamoModel.configure(G133Named, {
      label: { field: "the-label" },
      addr: { field: "ad" },
    } as any),
    entityType: "G133Hyphen",
    primaryKey: {
      pk: { field: "pk", composite: ["grp"] },
      sk: { field: "sk", composite: ["hid"] },
    } as any,
    indexes: g133NamedIndex("hid") as any,
  }),
  PlainHyphens: Entity.make({
    model: G133Named,
    entityType: "G133PlainHyphen",
    primaryKey: {
      pk: { field: "pk", composite: ["grp"] },
      sk: { field: "sk", composite: ["hid"] },
    } as any,
  }),
  NamedThirds: Entity.make({
    model: G133NamedThird,
    entityType: "G133NamedThird",
    primaryKey: {
      pk: { field: "pk", composite: ["grp"] },
      sk: { field: "sk", composite: ["tid"] },
    } as any,
    indexes: g133NamedIndex("tid") as any,
  }),
  NamedOthers: Entity.make({
    model: DynamoModel.configure(G133NamedOther, { label: { field: "nlabel" } } as any),
    entityType: "G133NamedOther",
    primaryKey: {
      pk: { field: "pk", composite: ["grp"] },
      sk: { field: "sk", composite: ["nid"] },
    } as any,
    indexes: g133NamedIndex("nid") as any,
  }),
  OrAs: Entity.make({
    model: G133OrA,
    entityType: "G133OrA",
    primaryKey: {
      pk: { field: "pk", composite: ["owner"] },
      sk: { field: "sk", composite: ["aid"] },
    } as any,
    indexes: g133OrIndex("aid") as any,
  }),
  OrBs: Entity.make({
    model: G133OrB,
    entityType: "G133OrB",
    primaryKey: {
      pk: { field: "pk", composite: ["owner"] },
      sk: { field: "sk", composite: ["bid"] },
    } as any,
    indexes: g133OrIndex("bid") as any,
  }),
  Readings: Entity.make({
    model: G133Reading,
    entityType: "G133Reading",
    primaryKey: {
      pk: { field: "pk", composite: ["ch"] },
      sk: { field: "sk", composite: ["dev"] },
    } as any,
    timeSeries: { orderBy: "ts", appendInput: G133ReadingInput },
  }),
  SubDeleted: Entity.make({
    model: G133Sub,
    entityType: "G133SubDel",
    primaryKey: {
      collection: ["G133SubDel", "deleted"],
      type: "clustered",
      pk: { field: "pk", composite: ["id"] },
      sk: { field: "sk", composite: [] },
    } as any,
    softDelete: true,
  }),
  SubVersioned: Entity.make({
    model: G133Sub,
    entityType: "G133SubV",
    primaryKey: {
      collection: ["G133SubV", "v"],
      type: "clustered",
      pk: { field: "pk", composite: ["id"] },
      sk: { field: "sk", composite: [] },
    } as any,
    versioned: { retain: true },
  }),
  POrders: Entity.make({
    model: G133POrder,
    entityType: "G133POrder",
    primaryKey: g133OrderKey([]) as any,
    timestamps: true,
    versioned: { retain: true },
    softDelete: true,
  }),
  PLines: Entity.make({
    model: G133PLine,
    entityType: "G133PLine",
    primaryKey: g133OrderKey(["lineId"]) as any,
  }),

  Lines: Entity.make({
    model: G133Line,
    entityType: "G133Line",
    primaryKey: g133LineKey as any,
    timestamps: true,
    versioned: { retain: true },
  }),
  SoftLines: Entity.make({
    model: G133Line,
    entityType: "G133SoftLine",
    primaryKey: g133LineKey as any,
    timestamps: true,
    versioned: { retain: true },
    softDelete: true,
  }),
  Articles: Entity.make({
    model: G133Article,
    entityType: "G133Article",
    primaryKey: {
      pk: { field: "pk", composite: ["articleId"] },
      sk: { field: "sk", composite: [] },
    } as any,
    indexes: {
      byAuthor: {
        name: "gsi1",
        pk: { field: "gsi1pk", composite: ["authorId"] },
        sk: { field: "gsi1sk", composite: ["articleId"] },
      },
    },
    refs: { author: { entity: G133Authors } },
  }),
}
const G133Table = Table.make({ schema: G133Schema, entities: g133Entities })
const g133Tables = {
  record: `edd133g-record-${Date.now()}`,
  path: `edd133g-path-${Date.now()}`,
} as const
/** A stream on the same table, to write guarded puts as `additionalItems`. */
class G133Noted extends Schema.Class<G133Noted>("G133Noted")({ note: Schema.String }) {}
const G133Feed = EventStore.makeStream({
  table: G133Table,
  streamName: "G133Feed",
  events: [G133Noted],
  streamId: { composite: ["feedId"] },
})
const g133Layer = (name: string) => Layer.mergeAll(ClientLayer, G133Table.layer({ name }))
const g133Client = DynamoClient.make({ entities: g133Entities, tables: { G133Table } })

/**
 * The bound accessors are driven through `any` (one table serves every shape),
 * which leaves the requirements `unknown`; the layers provide them.
 */
const g133Closed = <A, E>(effect: Effect.Effect<A, E, unknown>): Effect.Effect<A, E> =>
  effect as Effect.Effect<A, E>
/** A hook's effect with its layers' scope closed, so it can run anywhere. */
const g133Hook = (effect: Effect.Effect<void, unknown, any>): Effect.Effect<void, unknown> =>
  Effect.scoped(effect) as Effect.Effect<void, unknown>
/** One-shot hooks run around the next PutItem / UpdateItem / TransactWriteItems. */
const g133Inject: {
  before?: Effect.Effect<void, unknown> | undefined
  after?: Effect.Effect<void, unknown> | undefined
} = {}
const G133InjectingClient = Layer.effect(
  DynamoClient,
  Effect.gen(function* () {
    const real = yield* DynamoClient
    const around = <A, E>(call: Effect.Effect<A, E>) =>
      Effect.gen(function* () {
        const before = g133Inject.before
        g133Inject.before = undefined
        if (before) yield* Effect.orDie(before)
        const out = yield* call
        const after = g133Inject.after
        g133Inject.after = undefined
        if (after) yield* Effect.orDie(after)
        return out
      })
    return {
      ...real,
      putItem: (input) => around(real.putItem(input)),
      updateItem: (input) => around(real.updateItem(input)),
      transactWriteItems: (input) => around(real.transactWriteItems(input)),
    } satisfies DynamoClientService
  }),
).pipe(Layer.provide(ClientLayer))
const g133RaceLayer = Layer.mergeAll(
  G133InjectingClient,
  G133Table.layer({ name: g133Tables.record }),
)

// Items written before an entity was `versioned` (#133): the same entity type
// defined without and with versioning, against the same physical table.
class G133LegacyDoc extends Schema.Class<G133LegacyDoc>("G133LegacyDoc")({
  id: Schema.String,
  name: Schema.String,
  email: Schema.optional(Schema.String),
  tags: Schema.optional(Schema.Array(Schema.String)),
}) {}
const g133Legacy = (
  entityType: string,
  extra: Record<string, unknown>,
  versioned: boolean | { readonly retain: true },
) =>
  Entity.make({
    model: G133LegacyDoc,
    entityType,
    primaryKey: g133IdKey as any,
    indexes: {
      byName: {
        name: "gsi1",
        pk: { field: "gsi1pk", composite: ["name"] },
        sk: { field: "gsi1sk", composite: [] },
      },
    },
    ...extra,
    ...(versioned === false ? {} : { versioned }),
  } as any)
const g133LegacyPairs = {
  Plain: [g133Legacy("G133LegacyPlain", {}, false), g133Legacy("G133LegacyPlain", {}, true)],
  Ret: [g133Legacy("G133LegacyRet", {}, false), g133Legacy("G133LegacyRet", {}, { retain: true })],
  Uniq: [
    g133Legacy("G133LegacyUniq", { unique: { email: ["email"] } }, false),
    g133Legacy("G133LegacyUniq", { unique: { email: ["email"] } }, true),
  ],
  Soft: [
    g133Legacy("G133LegacySoft", { softDelete: true }, false),
    g133Legacy("G133LegacySoft", { softDelete: true }, { retain: true }),
  ],
} as const
/** A client for one side (0: before `versioned`, 1: after) of one pair. */
const g133LegacyClient = (pair: keyof typeof g133LegacyPairs, side: 0 | 1) => {
  const entity = g133LegacyPairs[pair][side]
  const table = Table.make({ schema: G133Schema, entities: { Doc: entity } as any })
  return {
    entity,
    layer: table.layer({ name: g133Tables.record }),
    client: DynamoClient.make({ entities: { Doc: entity }, tables: { T: table } } as any),
  }
}

describeConnected("#133 — path operations on index composites and unique fields", () => {
  beforeAll(async () => {
    for (const name of Object.values(g133Tables)) {
      await Effect.runPromise(
        Effect.gen(function* () {
          const db = yield* g133Client
          yield* db.tables.G133Table.create()
        }).pipe(Effect.provide(g133Layer(name)), Effect.scoped),
      )
    }
  }, 30000)

  afterAll(async () => {
    for (const name of Object.values(g133Tables)) {
      await Effect.runPromise(
        Effect.gen(function* () {
          const client = yield* DynamoClient
          yield* client.deleteTable({ TableName: name })
        }).pipe(
          Effect.provide(g133Layer(name)),
          Effect.scoped,
          Effect.catchTag("ResourceNotFoundError", () => Effect.void),
        ),
      )
    }
  }, 30000)

  type Build = (u: any) => any
  const pathSet =
    (field: string, value: unknown): Build =>
    (u) =>
      u.pathSet({ segments: [field], value, isPath: false })
  const shapes: ReadonlyArray<readonly [string, string, Record<string, unknown>, Build, Build]> = [
    [
      "1. multi-writer: PK-half composite",
      "Devices",
      { id: "mw", owner: "alice", reading: "r1", seq: 1, label: "l" },
      (u) => u.set({ owner: "bob" }),
      pathSet("owner", "bob"),
    ],
    [
      "1. multi-writer: numeric SK-half composite",
      "Devices",
      { id: "mwn", owner: "alice", reading: "r1", seq: 1 },
      (u) => u.add({ seq: 2 }),
      (u) => u.pathAdd({ segments: ["seq"], value: 2 }),
    ],
    [
      "2. PK-composites-only: a non-composite write",
      "Ports",
      { channel: "c1", deviceId: "x1", label: "l" },
      (u) => u.set({ label: "m" }),
      (u) => u.set({ label: "m" }),
    ],
    [
      "3. hierarchical: parents set, leaf removed",
      "Sites",
      { id: "h", region: "emea", country: "uk", city: "ldn", site: "dc1" },
      (u) => u.set({ region: "emea", country: "uk", city: "mcr" }).remove(["site"]),
      (u) =>
        u
          .pathSet({ segments: ["region"], value: "emea", isPath: false })
          .pathSet({ segments: ["country"], value: "uk", isPath: false })
          .pathSet({ segments: ["city"], value: "mcr", isPath: false })
          .pathRemove(["site"]),
    ],
    [
      "3. hierarchical: a lone leaf",
      "Sites",
      { id: "hl", region: "emea", country: "uk", city: "ldn", site: "dc1" },
      (u) => u.set({ city: "mcr" }),
      pathSet("city", "mcr"),
    ],
    [
      "4. hole pattern",
      "Slots",
      { id: "hp", tenant: "t1", trail: "z1" },
      (u) => u.set({ trail: "z2" }),
      pathSet("trail", "z2"),
    ],
    [
      "5. all composites mutable",
      "Tasks",
      { id: "am", category: "a", priority: "low" },
      (u) => u.set({ category: "b", priority: "high" }),
      (u) =>
        u
          .pathSet({ segments: ["category"], value: "b", isPath: false })
          .pathSet({ segments: ["priority"], value: "high", isPath: false }),
    ],
    [
      "6. empty-composite half: set",
      "Bindings",
      { id: "eh", deviceBinding: "db1" },
      (u) => u.set({ deviceBinding: "db2" }),
      pathSet("deviceBinding", "db2"),
    ],
    [
      "6. empty-composite half: remove",
      "Bindings",
      { id: "ehr", deviceBinding: "db1" },
      (u) => u.remove(["deviceBinding"]),
      (u) => u.pathRemove(["deviceBinding"]),
    ],
    [
      "unique constraint: set",
      "Accounts",
      { id: "us", email: "us@x.io", name: "n" },
      (u) => u.set({ email: "us2@x.io" }),
      pathSet("email", "us2@x.io"),
    ],
    [
      "unique constraint: remove",
      "Accounts",
      { id: "ur", email: "ur@x.io", name: "n" },
      (u) => u.remove(["email"]),
      (u) => u.pathRemove(["email"]),
    ],
  ]

  const scanAll = (name: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const { Items } = yield* client.scan({ TableName: name, ConsistentRead: true })
      return (
        (Items ?? [])
          .map((item) => item as Record<string, any>)
          // Each create draws its own random incarnation token.
          .map((item) =>
            item.__edd_i__ === undefined ? item : { ...item, __edd_i__: { S: "<incarnation>" } },
          )
          .sort((a, b) => `${a.pk.S}|${a.sk.S}`.localeCompare(`${b.pk.S}|${b.sk.S}`))
      )
    }).pipe(Effect.provide(g133Layer(name)))

  it.effect("pathSet / pathRemove store exactly what .set() / .remove() store", () =>
    Effect.gen(function* () {
      for (const variant of ["Plain", "Retained"] as const) {
        for (const [label, entity, seed, viaRecord, viaPath] of shapes) {
          for (const [form, build] of [
            ["record", viaRecord],
            ["path", viaPath],
          ] as const) {
            yield* Effect.gen(function* () {
              const db = yield* g133Client
              const bound = (db.entities as Record<string, any>)[`${entity}${variant}`]
              yield* bound.put(seed)
              const key = Object.fromEntries(
                Object.entries(seed).filter(([k]) => ["id", "channel", "deviceId"].includes(k)),
              )
              const out = yield* Effect.exit(
                Effect.suspend(() => build(bound.update(key)).asEffect() as Effect.Effect<unknown>),
              )
              expect([variant, label, form, out._tag]).toEqual([variant, label, form, "Success"])
            }).pipe(Effect.provide(g133Layer(g133Tables[form])))
          }
        }
      }
      const record = yield* scanAll(g133Tables.record)
      const path = yield* scanAll(g133Tables.path)
      expect(path).toEqual(record)
      // The keys moved: spot-check one per shape in the stored rows.
      const keysOf = (pk: string) =>
        record.filter((i) => i.pk.S === pk).map((i) => [i.sk.S, i.gsi1pk?.S, i.gsi1sk?.S])
      expect(keysOf("$edd133g#v1#g133deviceplain#id_mw")[0]![1]).toBe(
        "$edd133g#v1#g133deviceplain#owner_bob",
      )
      expect(keysOf("$edd133g#v1#g133deviceplain#id_mwn")[0]![2]).toMatch(/#seq_0+3$/)
      expect(keysOf("$edd133g#v1#g133siteplain#id_h")[0]![2]).toMatch(/#city_mcr$/)
      expect(keysOf("$edd133g#v1#g133bindingplain#id_eh")[0]![1]).toMatch(/#devicebinding_db2$/)
      expect(keysOf("$edd133g#v1#g133bindingplain#id_ehr")[0]![1]).toBeUndefined()
      const sentinels = record.filter((i) => String(i.__edd_e__?.S).includes("_unique"))
      expect(sentinels.map((i) => i.pk.S).filter((pk) => pk.includes("us"))).toEqual(
        expect.arrayContaining([expect.stringContaining("us2@x.io")]),
      )
      expect(sentinels.some((i) => String(i.pk.S).includes("us@x.io"))).toBe(false)
      expect(sentinels.some((i) => String(i.pk.S).includes("ur@x.io"))).toBe(false)
    }).pipe(g133Closed),
  )

  it.effect("refuses path operations DynamoDB would compute on a composite", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* db.entities.DevicesRetained.put({ id: "rf", owner: "alice", reading: "r1" } as any)
      const error = yield* (db.entities.DevicesRetained as any)
        .update({ id: "rf" })
        .pathIfNotExists({ segments: ["owner"], value: "bob" })
        .asEffect()
        .pipe(Effect.flip)
      expect(error._tag).toBe("ValidationError")
      expect(String(error.cause)).toContain('"owner"')
    }).pipe(Effect.provide(g133Layer(g133Tables.record)), g133Closed),
  )

  it.effect(".add() on a composite of an unversioned entity recomposes the index key", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* db.entities.Counters.put({ id: "cn", owner: "alice", reading: "r1", seq: 1 } as any)
      const updated = (yield* (db.entities.Counters as any)
        .update({ id: "cn" })
        .add({ seq: 2 })) as any
      expect(updated.seq).toBe(3)
      const hits = yield* (db.entities.Counters as any)
        .byOwner({ owner: "alice", reading: "r1", seq: 3 })
        .collect()
      expect(hits.map((h: any) => h.id)).toEqual(["cn"])
    }).pipe(Effect.provide(g133Layer(g133Tables.record)), g133Closed),
  )

  // ---- condition failures: a lost version race vs the user's condition ----

  const mainKey = (entityType: string, id: string) => ({
    pk: { S: `$edd133g#v1#${entityType.toLowerCase()}#id_${id}` },
    sk: { S: `$edd133g#v1#${entityType.toLowerCase()}` },
  })
  /** A concurrent writer: bumps the version (and label) behind our back. */
  const bumpVersion = (entityType: string, id: string, label = "l") =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      yield* client.updateItem({
        TableName: g133Tables.record,
        Key: mainKey(entityType, id),
        UpdateExpression: "SET #v = #v + :one, #l = :l",
        ExpressionAttributeNames: { "#v": "version", "#l": "label" },
        ExpressionAttributeValues: { ":one": { N: "1" }, ":l": { S: label } },
      })
    }).pipe(Effect.provide(ClientLayer))

  const raceCases: ReadonlyArray<
    readonly [string, "DevicesPlain" | "DevicesRetained", string, Build]
  > = [
    [
      "plain update with expectedVersion and a condition",
      "DevicesPlain",
      "G133DevicePlain",
      (u) =>
        u
          .set({ label: "m" })
          .expectedVersion(1)
          .condition({ eq: { label: "l" } }),
    ],
    [
      "retain record update with a condition",
      "DevicesRetained",
      "G133DeviceRetained",
      (u) => u.set({ label: "m" }).condition({ eq: { label: "l" } }),
    ],
    [
      "retain path update with a condition",
      "DevicesRetained",
      "G133DeviceRetained",
      (u) =>
        u
          .pathSet({ segments: ["label"], value: "m", isPath: false })
          .condition({ eq: { label: "l" } }),
    ],
  ]
  for (const [label, entity, entityType, build] of raceCases) {
    it.effect(`${label}: a concurrent version bump is an OptimisticLockError`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const id = `race-${entity.toLowerCase()}-${label.length}`
        yield* (db.entities[entity] as any).put({ id, owner: "o", reading: "r", label: "l" })
        g133Inject.before = g133Hook(bumpVersion(entityType, id))
        const error = yield* build((db.entities[entity] as any).update({ id }))
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("OptimisticLockError")
        expect([error.expectedVersion, error.actualVersion]).toEqual([1, 2])
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )

    it.effect(`${label}: a false condition without a race is a ConditionalCheckFailed`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const id = `cond-${entity.toLowerCase()}-${label.length}`
        yield* (db.entities[entity] as any).put({ id, owner: "o", reading: "r", label: "other" })
        const error = yield* build((db.entities[entity] as any).update({ id }))
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("ConditionalCheckFailed")
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  // ---- the retain path update returns its own post-image ----

  it.effect("returns our post-image although a later retain write replaced it", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const id = "post"
      yield* db.entities.DevicesRetained.put({
        id,
        owner: "alice",
        reading: "r1",
        seq: 1,
        label: "l",
      } as any)
      const client = yield* DynamoClient
      let ours: Record<string, any> | undefined
      g133Inject.after = g133Hook(
        Effect.gen(function* () {
          // The item exactly as our update left it, then another retain write.
          ours = (yield* client.getItem({
            TableName: g133Tables.record,
            Key: mainKey("G133DeviceRetained", id),
            ConsistentRead: true,
          })).Item
          const other = yield* g133Client
          yield* (other.entities.DevicesRetained as any).update({ id }).set({ label: "theirs" })
        }).pipe(Effect.provide(g133Layer(g133Tables.record))),
      )
      const native = yield* Entity.asNative(
        Entity.pathSet((G133Devices.retained as any).update({ id }), {
          segments: ["label"],
          value: "m",
          isPath: false,
        }),
      )
      expect(ours?.label).toEqual({ S: "m" })
      expect(native).toEqual(ours)
      const live = yield* db.entities.DevicesRetained.get({ id } as any)
      expect((live as any).label).toBe("theirs")
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("returns the model of our post-image, and fails clearly once it is gone", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const id = "post2"
      yield* db.entities.DevicesRetained.put({ id, owner: "alice", label: "l" } as any)
      g133Inject.after = g133Hook(
        Effect.gen(function* () {
          const other = yield* g133Client
          yield* (other.entities.DevicesRetained as any).update({ id }).set({ label: "theirs" })
        }).pipe(Effect.provide(g133Layer(g133Tables.record))),
      )
      const mine = (yield* (db.entities.DevicesRetained as any)
        .update({ id })
        .pathSet({ segments: ["label"], value: "m", isPath: false })) as any
      expect(mine.label).toBe("m")

      // A writer that leaves no snapshot (a raw write) — nothing exact to return.
      g133Inject.after = g133Hook(bumpVersion("G133DeviceRetained", id, "raw"))
      const error = yield* (db.entities.DevicesRetained as any)
        .update({ id })
        .pathSet({ segments: ["label"], value: "n", isPath: false })
        .asEffect()
        .pipe(Effect.flip)
      // The write WAS applied — a distinct error that says so, never a retry.
      expect(error._tag).toBe("UpdateAppliedButUnreadable")
      expect(error.version).toBe(4)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("returnValues('allOld') returns the item the update replaced", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      for (const [id, build] of [
        ["old-path", (u: any) => u.pathSet({ segments: ["label"], value: "m", isPath: false })],
        ["old-record", (u: any) => u.set({ label: "m" })],
      ] as const) {
        yield* db.entities.DevicesRetained.put({ id, owner: "alice", label: "l" } as any)
        const old = (yield* build((db.entities.DevicesRetained as any).update({ id })).returnValues(
          "allOld",
        )) as any
        expect([id, old.label, old.version]).toEqual([id, "l", 1])
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- read-then-write updates: guarded Update, never a whole-item Put ----

  const rawItem = (entityType: string, id: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      return (yield* client.getItem({
        TableName: g133Tables.record,
        Key: mainKey(entityType, id),
        ConsistentRead: true,
      })).Item as Record<string, any>
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)
  /** A concurrent raw write of one attribute behind the update's back. */
  const rawSet = (entityType: string, id: string, attr: string, value: Record<string, unknown>) =>
    g133Hook(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.updateItem({
          TableName: g133Tables.record,
          Key: mainKey(entityType, id),
          UpdateExpression: "SET #a = :v",
          ExpressionAttributeNames: { "#a": attr },
          ExpressionAttributeValues: { ":v": value as any },
        })
      }).pipe(Effect.provide(ClientLayer)),
    )

  it.effect("stores exactly the read item plus the update's changes", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* db.entities.AccountsBare.put({ id: "ex1", email: "ex1@x.io", name: "n" } as any)
      const before = yield* rawItem("G133AccountBare", "ex1")
      yield* (db.entities.AccountsBare as any).update({ id: "ex1" }).set({ email: "ex1b@x.io" })
      expect(yield* rawItem("G133AccountBare", "ex1")).toEqual({
        ...before,
        email: { S: "ex1b@x.io" },
      })

      yield* db.entities.Counters.put({ id: "ex2", owner: "o", reading: "r", seq: 1 } as any)
      const counter = yield* rawItem("G133Counter", "ex2")
      yield* (db.entities.Counters as any).update({ id: "ex2" }).add({ seq: 2 })
      expect(yield* rawItem("G133Counter", "ex2")).toEqual({
        ...counter,
        seq: { N: "3" },
        gsi1sk: { S: counter.gsi1sk.S.replace(/seq_0+1$/, "seq_0000000000000003") },
      })

      yield* db.entities.DevicesRetained.put({ id: "ex3", owner: "o", label: "l" } as any)
      const retained = yield* rawItem("G133DeviceRetained", "ex3")
      yield* (db.entities.DevicesRetained as any).update({ id: "ex3" }).set({ label: "m" })
      expect(yield* rawItem("G133DeviceRetained", "ex3")).toEqual({
        ...retained,
        label: { S: "m" },
        version: { N: "2" },
        // The owner composite composes the PK half of byOwner on update (the
        // put left both halves out: its SK composites are missing).
        gsi1pk: { S: "$edd133g#v1#g133deviceretained#owner_o" },
      })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  const readThenWrite: ReadonlyArray<
    readonly [
      string,
      "AccountsBare" | "Counters",
      string,
      Record<string, unknown>,
      Build,
      string,
      string,
    ]
  > = [
    [
      "unique-field change (transacted)",
      "AccountsBare",
      "G133AccountBare",
      { email: "rw@x.io", name: "n" },
      (u) => u.set({ email: "rw2@x.io" }),
      "name",
      "email",
    ],
    [
      "computed GSI composite (single UpdateItem)",
      "Counters",
      "G133Counter",
      { owner: "o", reading: "r", seq: 1, label: "l" },
      (u) => u.add({ seq: 2 }),
      "label",
      "reading",
    ],
  ]
  for (const [label, entity, entityType, seed, build, unrelated, input] of readThenWrite) {
    it.effect(`${label}: a concurrent change to an unrelated attribute is preserved`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const id = `un-${entity.toLowerCase()}`
        yield* (db.entities[entity] as any).put({ id, ...seed, email: `${id}@x.io` })
        g133Inject.before = rawSet(entityType, id, unrelated, { S: "theirs" })
        yield* build((db.entities[entity] as any).update({ id })).asEffect()
        const stored = yield* rawItem(entityType, id)
        expect(stored[unrelated]).toEqual({ S: "theirs" })
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )

    it.effect(`${label}: a concurrent change to an input is rejected`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const id = `in-${entity.toLowerCase()}`
        yield* (db.entities[entity] as any).put({ id, ...seed, email: `${id}@x.io` })
        g133Inject.before = rawSet(entityType, id, input, { S: "theirs" })
        const error = yield* build((db.entities[entity] as any).update({ id }))
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("ConcurrentModification")
        expect(error.attributes).toEqual([input])
        expect(Option.isSome(error.current)).toBe(true)
        const stored = yield* rawItem(entityType, id)
        expect(stored[input]).toEqual({ S: "theirs" })
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  // ---- returnValues: every mode on every update branch ----

  const branches: ReadonlyArray<
    readonly [
      string,
      keyof typeof g133Entities,
      Record<string, unknown>,
      Build,
      Record<string, unknown>,
      Record<string, unknown>,
    ]
  > = [
    [
      "plain UpdateItem",
      "DevicesPlain",
      { owner: "o", label: "l" },
      (u) => u.set({ label: "m" }),
      { label: "l" },
      { label: "m" },
    ],
    [
      "read-then-write UpdateItem",
      "Counters",
      { owner: "o", reading: "r", seq: 1, label: "l" },
      (u) => u.add({ seq: 2 }),
      { seq: 1 },
      { seq: 3 },
    ],
    [
      "read-then-write transaction",
      "AccountsBare",
      { email: "rv@x.io", name: "n" },
      (u) => u.set({ email: "rv2@x.io" }),
      { email: "rv@x.io" },
      { email: "rv2@x.io" },
    ],
    [
      "retain record update",
      "DevicesRetained",
      { owner: "o", label: "l" },
      (u) => u.set({ label: "m" }),
      { label: "l" },
      { label: "m" },
    ],
    [
      "retain path update",
      "DevicesRetained",
      { owner: "o", label: "l" },
      (u) => u.pathSet({ segments: ["label"], value: "m", isPath: false }),
      { label: "l" },
      { label: "m" },
    ],
  ]
  const modes = ["none", "allNew", "allOld", "updatedNew", "updatedOld"] as const
  for (const [index, [label, entity, seed, build, , newPart]] of branches.entries()) {
    it.effect(`returnValues on a ${label}`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const bound = db.entities[entity] as any
        for (const mode of modes) {
          const id = `rv-${index}-${mode.toLowerCase()}`
          const email =
            typeof seed.email === "string" ? `${mode.toLowerCase()}-${seed.email}` : undefined
          const nextEmail =
            typeof newPart.email === "string" ? `${mode.toLowerCase()}-${newPart.email}` : undefined
          yield* bound.put({ id, ...seed, ...(email !== undefined && { email }) })
          const before = yield* bound.get({ id })
          const update = build(bound.update({ id }))
          const result = yield* (
            nextEmail !== undefined ? update.set({ email: nextEmail }) : update
          )
            .returnValues(mode)
            .asEffect()
          const after = yield* bound.get({ id })
          // The written attributes: the update's own, plus the system fields
          // every update writes (the bound client decodes in record mode).
          const writtenFields = [
            ...Object.keys(nextEmail !== undefined ? { email: nextEmail } : newPart),
            ...["updatedAt", "version"].filter((field) => field in after),
          ]
          const pick = (item: Record<string, unknown>) =>
            Object.fromEntries(writtenFields.map((field) => [field, item[field]]))
          const expected = {
            none: undefined,
            allNew: after,
            allOld: before,
            updatedNew: pick(after),
            updatedOld: pick(before),
          }[mode]
          expect([label, mode, result]).toEqual([label, mode, expected])
        }
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  it.effect("returnValues partials in record and native modes", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* db.entities.DevicesPlain.put({ id: "rvn", owner: "o", label: "l" } as any)
      const record = (yield* Entity.asRecord(
        Entity.returnValues(
          (G133Devices.plain as any).update({ id: "rvn" }).pipe(Entity.set({ label: "m" } as any)),
          "updatedNew",
        ),
      )) as any
      expect(Object.keys(record).sort()).toEqual(["label", "updatedAt", "version"])
      expect([record.label, record.version]).toEqual(["m", 2])
      const native = yield* Entity.asNative(
        Entity.returnValues(
          (G133Devices.plain as any).update({ id: "rvn" }).pipe(Entity.set({ label: "n" } as any)),
          "updatedOld",
        ),
      )
      expect(Object.keys(native).sort()).toEqual(["label", "updatedAt", "version"])
      expect(native.label).toEqual({ S: "m" })
      const none = yield* Entity.asNative(
        Entity.returnValues(
          (G133Devices.plain as any).update({ id: "rvn" }).pipe(Entity.set({ label: "o" } as any)),
          "none",
        ),
      )
      expect(none).toBeUndefined()
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- incarnations: a deleted-and-recreated item is never mistaken for ours ----

  const rawDelete = (entityType: string, id: string) =>
    g133Hook(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteItem({
          TableName: g133Tables.record,
          Key: mainKey(entityType, id),
        })
      }).pipe(Effect.provide(ClientLayer)),
    )
  /**
   * A new incarnation of the item at version 1 — written raw, as an outside
   * writer (or a pre-#133 library) would: the library's own re-create continues
   * the version sequence past the earlier incarnation's history. `withHistory`
   * also writes this incarnation's own `v#0000001` snapshot.
   */
  const recreateRaw = (id: string, label: string, withHistory = false) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const item = {
        ...mainKey("G133DeviceRetained", id),
        __edd_e__: { S: "G133DeviceRetained" },
        __edd_i__: { S: crypto.randomUUID() },
        id: { S: id },
        owner: { S: "o" },
        label: { S: label },
        version: { N: "1" },
        createdAt: { S: "1970-01-01T00:00:00.000Z" },
        updatedAt: { S: "1970-01-01T00:00:00.000Z" },
      }
      yield* client.putItem({ TableName: g133Tables.record, Item: item })
      if (withHistory) {
        yield* client.putItem({
          TableName: g133Tables.record,
          Item: { ...item, sk: { S: "$edd133g#v1#g133deviceretained#v#0000001" } },
        })
      }
    }).pipe(Effect.provide(ClientLayer))
  const recreate = (id: string, label: string, withHistory = false) =>
    g133Hook(recreateRaw(id, label, withHistory))
  const snapshotKey = (id: string, version: number) => ({
    pk: { S: `$edd133g#v1#g133deviceretained#id_${id}` },
    sk: { S: `$edd133g#v1#g133deviceretained#v#${String(version).padStart(7, "0")}` },
  })
  const snapshotRow = (id: string, version: number) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      return (yield* client.getItem({
        TableName: g133Tables.record,
        Key: snapshotKey(id, version),
        ConsistentRead: true,
      })).Item as Record<string, any> | undefined
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)
  /** A snapshot gone — as a `versioned.ttl` expiry removes the oldest first. */
  const dropSnapshot = (id: string, version: number) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      yield* client.deleteItem({ TableName: g133Tables.record, Key: snapshotKey(id, version) })
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)

  for (const [label, after] of [
    ["a hard delete", (id: string) => rawDelete("G133DeviceRetained", id)],
    [
      "a delete and re-create",
      (id: string) => Effect.andThen(rawDelete("G133DeviceRetained", id), recreate(id, "LATER")),
    ],
  ] as const) {
    it.effect(
      `a retain path update never returns an earlier incarnation's snapshot (${label})`,
      () =>
        Effect.gen(function* () {
          const db = yield* g133Client
          const id = `stale-${label.length}`
          const docs = db.entities.DevicesRetained as any
          // Incarnation 1: versions 1..3 — its v#1 and v#2 snapshots outlive
          // the delete.
          yield* docs.put({ id, owner: "o", label: "OLD1" })
          yield* docs.update({ id }).set({ label: "OLD2" })
          yield* docs.update({ id }).set({ label: "OLD3" })
          yield* docs.delete({ id })
          // Incarnation 2 at version 1, written raw, with incarnation 1's v#1
          // expired: its update to version 2 snapshots v#1 and commits — while
          // v#2 still holds incarnation 1's history.
          yield* recreateRaw(id, "NEW").pipe(Effect.scoped)
          yield* dropSnapshot(id, 1)
          // Once the update commits, the item is deleted (and re-created), so
          // its post-image can only come from a v#2 snapshot of ITS incarnation
          // — and the v#2 there is incarnation 1's.
          g133Inject.after = after(id)
          const error = yield* docs
            .update({ id })
            .pathSet({ segments: ["label"], value: "mine", isPath: false })
            .asEffect()
            .pipe(Effect.flip)
          g133Inject.after = undefined
          expect([error._tag, error.version]).toEqual(["UpdateAppliedButUnreadable", 2])
          // The update was applied: its own snapshot of the item it replaced.
          expect((yield* snapshotRow(id, 1))?.label).toEqual({ S: "NEW" })
          expect((yield* snapshotRow(id, 2))?.label).toEqual({ S: "OLD2" })
        }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  for (const [label, build] of [
    ["record", (u: any) => u.set({ label: "mine" })],
    ["path", (u: any) => u.pathSet({ segments: ["label"], value: "mine", isPath: false })],
  ] as const) {
    it.effect(`a ${label} retain update rejects a re-created item at the same version`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const id = `aba-${label}`
        const docs = db.entities.DevicesRetained as any
        yield* docs.put({ id, owner: "o", label: "first" })
        // Between our read and our write: delete, and re-create at version 1 —
        // with the new incarnation's own v#1 snapshot.
        g133Inject.before = Effect.andThen(
          rawDelete("G133DeviceRetained", id),
          recreate(id, "second", true),
        )
        const error = yield* build(docs.update({ id })).asEffect().pipe(Effect.flip)
        expect(error._tag).toBe("OptimisticLockError")
        const live = yield* rawItem("G133DeviceRetained", id)
        expect([live.label, live.version]).toEqual([{ S: "second" }, { N: "1" }])
        // The new incarnation's v#1 is untouched: our snapshot of "first" was
        // never written over it.
        expect((yield* snapshotRow(id, 1))?.label).toEqual({ S: "second" })
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  it.effect(".set() of a primary-key composite or an immutable field is refused", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* db.entities.DevicesPlain.put({ id: "pkset", owner: "o", label: "l" } as any)
      const error = yield* (db.entities.DevicesPlain as any)
        .update({ id: "pkset" })
        .set({ id: "other", label: "m" })
        .asEffect()
        .pipe(Effect.flip)
      expect(error._tag).toBe("ValidationError")
      expect(String(error.cause)).toContain('"id"')
      // The same value is the key itself — a no-op, not a refusal.
      const same = (yield* (db.entities.DevicesPlain as any)
        .update({ id: "pkset" })
        .set({ id: "pkset", label: "m" })) as any
      expect(same.label).toBe("m")
      expect((yield* rawItem("G133DevicePlain", "pkset")).label).toEqual({ S: "m" })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- deletes and restore: guarded on what the tombstone / sentinels derive from ----

  const rawSetAttrs = (entityType: string, id: string, attrs: Record<string, any>, bump = false) =>
    g133Hook(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        const names: Record<string, string> = {}
        const values: Record<string, any> = {}
        const sets = Object.entries(attrs).map(([attr, value], i) => {
          names[`#a${i}`] = attr
          values[`:a${i}`] = value
          return `#a${i} = :a${i}`
        })
        if (bump) {
          names["#v"] = "version"
          values[":one"] = { N: "1" }
          sets.push("#v = #v + :one")
        }
        yield* client.updateItem({
          TableName: g133Tables.record,
          Key: mainKey(entityType, id),
          UpdateExpression: `SET ${sets.join(", ")}`,
          ExpressionAttributeNames: names,
          ExpressionAttributeValues: values,
        })
      }).pipe(Effect.provide(ClientLayer)),
    )
  const partition = (entityType: string, id: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const { Items } = yield* client.query({
        TableName: g133Tables.record,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: {
          ":pk": { S: `$edd133g#v1#${entityType.toLowerCase()}#id_${id}` },
        },
        ConsistentRead: true,
      })
      return (Items ?? []).map((item) => String(item.sk?.S)).sort()
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)
  const sentinelExists = (entityType: string, email: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      // Every page: wide sparse-map items fill a scan page quickly.
      let start: Record<string, any> | undefined
      do {
        const page: any = yield* client.scan({
          TableName: g133Tables.record,
          ConsistentRead: true,
          FilterExpression: "#e = :e",
          ExpressionAttributeNames: { "#e": "__edd_e__" },
          ExpressionAttributeValues: { ":e": { S: `${entityType}._unique.email` } },
          ...(start !== undefined && { ExclusiveStartKey: start }),
        })
        if (
          (page.Items ?? []).some((item: any) => String(item.pk?.S).includes(email.toLowerCase()))
        ) {
          return true
        }
        start = page.LastEvaluatedKey
      } while (start !== undefined)
      return false
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)

  const sentinelExistsFor = (entityType: string, constraint: string, value: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const { Item } = yield* client.getItem({
        TableName: g133Tables.record,
        Key: {
          pk: { S: `$edd133g#v1#${entityType.toLowerCase()}.${constraint}#${value}` },
          sk: { S: `$edd133g#v1#${entityType.toLowerCase()}.${constraint}` },
        },
        ConsistentRead: true,
      })
      return Item !== undefined
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)

  it.effect("a versioned soft delete loses no concurrent update into its tombstone", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const docs = db.entities.SoftDevices as any
      yield* docs.put({ id: "sdv", owner: "o", label: "l" })
      // With a `.condition()`, the read it was judged against has changed: refused.
      g133Inject.before = rawSetAttrs("G133SoftDevice", "sdv", { label: { S: "theirs" } }, true)
      const error = yield* docs
        .delete({ id: "sdv" })
        .condition({ attributeExists: "id" })
        .asEffect()
        .pipe(Effect.flip)
      expect(error._tag).toBe("OptimisticLockError")
      expect((yield* rawItem("G133SoftDevice", "sdv")).label).toEqual({ S: "theirs" })
      expect(yield* partition("G133SoftDevice", "sdv")).toEqual([
        "$edd133g#v1#g133softdevice",
        "$edd133g#v1#g133softdevice#v#0000001",
      ])
      // Without one, it reads the item again: the tombstone holds the update.
      g133Inject.before = rawSetAttrs("G133SoftDevice", "sdv", { label: { S: "later" } }, true)
      yield* docs.delete({ id: "sdv" })
      expect((yield* docs.deleted.get({ id: "sdv" })).label).toBe("later")
      const keys = yield* partition("G133SoftDevice", "sdv")
      expect(keys).toHaveLength(3)
      expect(keys[0]).toMatch(/^\$edd133g#v1#g133softdevice#deleted#/)
      // The snapshot is of the state deleted: version 3, after both bumps.
      expect(keys.slice(1)).toEqual([
        "$edd133g#v1#g133softdevice#v#0000001",
        "$edd133g#v1#g133softdevice#v#0000003",
      ])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("an unversioned soft delete refuses a concurrent update to any attribute", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const accounts = db.entities.SoftAccounts as any
      yield* accounts.put({ id: "sda", email: "sda@x.io", name: "n" })
      g133Inject.before = rawSetAttrs("G133SoftAccount", "sda", { name: { S: "theirs" } })
      const error = yield* accounts
        .delete({ id: "sda" })
        .condition({ attributeExists: "id" })
        .asEffect()
        .pipe(Effect.flip)
      expect(error._tag).toBe("ConcurrentModification")
      expect(error.attributes).toEqual(["name"])
      expect((yield* rawItem("G133SoftAccount", "sda")).name).toEqual({ S: "theirs" })
      expect(yield* sentinelExists("G133SoftAccount", "sda@x.io")).toBe(true)
      // Without a condition it reads the item again, and the tombstone holds the update.
      g133Inject.before = rawSetAttrs("G133SoftAccount", "sda", { name: { S: "later" } })
      yield* accounts.delete({ id: "sda" })
      expect((yield* accounts.deleted.get({ id: "sda" })).name).toBe("later")
      expect(yield* rawItem("G133SoftAccount", "sda")).toBeUndefined()
      expect(yield* sentinelExists("G133SoftAccount", "sda@x.io")).toBe(false)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a unique hard delete orphans no sentinel under a concurrent rotation", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const bare = db.entities.AccountsBare as any
      yield* bare.put({ id: "hd", email: "hd@x.io", name: "n" })
      // Another writer rotates the email (item + sentinels) between our read and write.
      g133Inject.before = g133Hook(
        Effect.gen(function* () {
          const other = yield* g133Client
          yield* (other.entities.AccountsBare as any)
            .update({ id: "hd" })
            .set({ email: "hd2@x.io" })
        }).pipe(Effect.provide(g133Layer(g133Tables.record))),
      )
      const error = yield* bare
        .delete({ id: "hd" })
        .condition({ attributeExists: "id" })
        .asEffect()
        .pipe(Effect.flip)
      expect(error._tag).toBe("ConcurrentModification")
      expect(error.attributes).toEqual(["email"])
      expect(yield* sentinelExists("G133AccountBare", "hd2@x.io")).toBe(true)
      // Without a condition the delete reads the item again: the rotated
      // sentinel is the one released.
      g133Inject.before = g133Hook(
        Effect.gen(function* () {
          const other = yield* g133Client
          yield* (other.entities.AccountsBare as any)
            .update({ id: "hd" })
            .set({ email: "hd3@x.io" })
        }).pipe(Effect.provide(g133Layer(g133Tables.record))),
      )
      yield* bare.delete({ id: "hd" })
      expect(yield* rawItem("G133AccountBare", "hd")).toBeUndefined()
      for (const email of ["hd@x.io", "hd2@x.io", "hd3@x.io"]) {
        expect(yield* sentinelExists("G133AccountBare", email)).toBe(false)
      }

      // Versioned: the version + incarnation condition.
      const plain = db.entities.AccountsPlain as any
      yield* plain.put({ id: "hdv", email: "hdv@x.io", name: "n" })
      g133Inject.before = rawSetAttrs("G133AccountPlain", "hdv", { name: { S: "x" } }, true)
      const versioned = yield* plain
        .delete({ id: "hdv" })
        .condition({ attributeExists: "id" })
        .asEffect()
        .pipe(Effect.flip)
      expect(versioned._tag).toBe("OptimisticLockError")
      expect(yield* sentinelExists("G133AccountPlain", "hdv@x.io")).toBe(true)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("restore never restores twice, nor over a live item", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const accounts = db.entities.SoftAccounts as any
      yield* accounts.put({ id: "rs", email: "rs@x.io", name: "n" })
      yield* accounts.delete({ id: "rs" })
      g133Inject.before = g133Hook(
        Effect.gen(function* () {
          const other = yield* g133Client
          yield* (other.entities.SoftAccounts as any).restore({ id: "rs" })
        }).pipe(Effect.provide(g133Layer(g133Tables.record))),
      )
      const twice = yield* accounts.restore({ id: "rs" }).pipe(Effect.flip)
      expect(twice._tag).toBe("ItemNotFound")
      expect(yield* partition("G133SoftAccount", "rs")).toEqual(["$edd133g#v1#g133softaccount"])

      yield* accounts.delete({ id: "rs" })
      g133Inject.before = g133Hook(
        Effect.gen(function* () {
          const other = yield* g133Client
          yield* (other.entities.SoftAccounts as any).put({
            id: "rs",
            email: "rs-new@x.io",
            name: "live",
          })
        }).pipe(Effect.provide(g133Layer(g133Tables.record))),
      )
      const overLive = yield* accounts.restore({ id: "rs" }).pipe(Effect.flip)
      expect(overLive._tag).toBe("ItemNotDeleted")
      expect((yield* rawItem("G133SoftAccount", "rs")).name).toEqual({ S: "live" })
      expect(yield* sentinelExists("G133SoftAccount", "rs-new@x.io")).toBe(true)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- update() of a missing item ----

  it.effect("update() of a missing item creates it only from a complete payload", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const devices = db.entities.DevicesPlain as any
      const partial = yield* devices
        .update({ id: "nx1" })
        .set({ label: "m" })
        .asEffect()
        .pipe(Effect.flip)
      expect(partial._tag).toBe("ItemNotFound")
      expect(yield* rawItem("G133DevicePlain", "nx1")).toBeUndefined()

      // Every required field (and the key composite) supplied: a real upsert.
      const created = (yield* devices.update({ id: "nx2" }).set({ id: "nx2", label: "m" })) as any
      expect([created.id, created.label, created.version]).toEqual(["nx2", "m", 1])
      const got = (yield* devices.get({ id: "nx2" })) as any
      expect([got.id, got.label, got.version]).toEqual(["nx2", "m", 1])
      expect((yield* rawItem("G133DevicePlain", "nx2")).__edd_e__).toEqual({ S: "G133DevicePlain" })
      // …and on an existing item the same update is an ordinary update.
      const again = (yield* devices.update({ id: "nx2" }).set({ id: "nx2", label: "n" })) as any
      expect([again.label, again.version]).toEqual(["n", 2])

      // patch keeps its contract: a missing item is ConditionalCheckFailed.
      const patched = yield* devices
        .patch({ id: "nx3" })
        .set({ label: "m" })
        .asEffect()
        .pipe(Effect.flip)
      expect(patched._tag).toBe("ConditionalCheckFailed")
      expect(yield* rawItem("G133DevicePlain", "nx3")).toBeUndefined()
      // The read-then-write paths report a missing item as before.
      const retained = yield* (db.entities.DevicesRetained as any)
        .update({ id: "nx4" })
        .set({ label: "m" })
        .asEffect()
        .pipe(Effect.flip)
      expect(retained._tag).toBe("ItemNotFound")
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- cascade with an old-image return mode ----

  it.effect("a cascade returning the old item cascades this write's new item exactly", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* (db.entities.Authors as any).put({ id: "au", name: "Old" })
      yield* (db.entities.Articles as any).put({ articleId: "ar", authorId: "au" })
      // A later write lands before the cascade runs.
      g133Inject.after = rawSetAttrs("G133Author", "au", { name: { S: "Later" } })
      const old = (yield* (db.entities.Authors as any)
        .update({ id: "au" })
        .set({ name: "New" })
        .cascade({ targets: [g133Entities.Articles] })
        .returnValues("allOld")) as any
      expect(old.name).toBe("Old")
      const article = (yield* (db.entities.Articles as any).get({ articleId: "ar" })) as any
      expect(article.author.name).toBe("New")
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- immutable fields: restating is fine, changing is refused ----

  for (const entity of ["Ledgers", "LedgersRetained"] as const) {
    it.effect(`${entity}: a spread record updates; a changed immutable value is refused`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const ledgers = db.entities[entity] as any
        const id = `lg-${entity.toLowerCase()}`
        const record = (yield* ledgers.put({ id, book: "b1", note: "n" })) as any
        const updated = (yield* ledgers.update({ id }).set({ ...record, note: "m" })) as any
        expect([updated.book, updated.note, updated.version]).toEqual(["b1", "m", 2])
        const error = yield* ledgers
          .update({ id })
          .set({ book: "b2", note: "x" })
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
        expect(String(error.cause)).toContain('"book"')
        const stored = (yield* ledgers.get({ id })) as any
        expect([stored.book, stored.note, stored.version]).toEqual(["b1", "m", 2])
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  // ---- wide unversioned soft delete: the strongest guard that fits ----

  for (const [entity, entityType, stamped] of [
    ["WideStamped", "G133WideStamped", true],
    ["WideBare", "G133WideBare", false],
  ] as const) {
    it.effect(`${entity}: soft-deletes 50 / 150 / 300 sparse entries, never refusing`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const wide = db.entities[entity] as any
        for (const n of [50, 150, 300]) {
          const id = `w${n}`
          const totals = Object.fromEntries(Array.from({ length: n }, (_, i) => [`k${i}`, i]))
          yield* wide.put({ id, email: `${entity}${n}@x.io`, totals })
          yield* wide.delete({ id })
          expect([n, yield* rawItem(entityType, id)]).toEqual([n, undefined])
          expect(yield* sentinelExists(entityType, `${entity}${n}@x.io`)).toBe(false)
          const keys = yield* partition(entityType, id)
          expect(keys).toHaveLength(1)
          expect(keys[0]).toMatch(/#deleted#/)
        }
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )

    it.effect(`${entity}: an update too wide for one expression writes the whole item`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const wide = db.entities[entity] as any
        const id = "wupd"
        yield* wide.put({ id, email: `${entity}upd@x.io`, totals: { a: 1 } })
        const totals = Object.fromEntries(Array.from({ length: 300 }, (_, i) => [`k${i}`, i]))
        const updated = (yield* wide
          .update({ id })
          .set({ email: `${entity}upd2@x.io`, totals })) as any
        expect(Object.keys(updated.totals)).toHaveLength(301)
        const got = (yield* wide.get({ id })) as any
        expect([got.email, Object.keys(got.totals).length]).toEqual([`${entity}upd2@x.io`, 301])
        expect(yield* sentinelExists(entityType, `${entity}upd2@x.io`)).toBe(true)
        expect(yield* sentinelExists(entityType, `${entity}upd@x.io`)).toBe(false)
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )

    it.effect(`${entity}: a wide soft delete still refuses a concurrent update it can see`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const wide = db.entities[entity] as any
        const id = "wrace"
        const totals = Object.fromEntries(Array.from({ length: 300 }, (_, i) => [`k${i}`, i]))
        yield* wide.put({ id, email: `${entity}race@x.io`, totals })
        // With timestamps a library update always rewrites updatedAt; without,
        // the unique field (guarded first) is what a sentinel derives from.
        g133Inject.before = stamped
          ? g133Hook(
              Effect.gen(function* () {
                const other = yield* g133Client
                yield* TestClock.adjust("1 second")
                yield* (other.entities[entity] as any).update({ id }).set({ totals: { k5: 500 } })
              }).pipe(Effect.provide(g133Layer(g133Tables.record))),
            )
          : rawSetAttrs(entityType, id, { email: { S: `${entity}race2@x.io` } })
        const error = yield* wide
          .delete({ id })
          .condition({ attributeExists: "id" })
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("ConcurrentModification")
        expect(error.attributes).toEqual([stamped ? "updatedAt" : "email"])
        expect(yield* rawItem(entityType, id)).toBeDefined()
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  // ---- a complete-payload update of a missing item writes what put writes ----

  it.effect("update() of a missing item with a sparse index writes exactly put's item", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const sparse = db.entities.Sparse as any
      const strip = (item: Record<string, any>, id: string) =>
        JSON.parse(
          JSON.stringify({ ...item, __edd_i__: undefined })
            .replaceAll(id, "ID")
            .replaceAll(id.toLowerCase(), "id"),
        )
      for (const [label, payload] of [
        ["no team (sparse index omitted)", { name: "n", rank: 3 }],
        ["team present", { name: "n", rank: 3, team: "t" }],
      ] as const) {
        const viaPut = `sp-put-${label.length}`
        const viaUpdate = `sp-upd-${label.length}`
        yield* sparse.put({ id: viaPut, ...payload })
        const created = (yield* sparse
          .update({ id: viaUpdate })
          .set({ id: viaUpdate, ...payload })) as any
        expect([label, created.version]).toEqual([label, 1])
        expect([label, strip(yield* rawItem("G133Sparse", viaUpdate), viaUpdate)]).toEqual([
          label,
          strip(yield* rawItem("G133Sparse", viaPut), viaPut),
        ])
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- decoding defaults: what put writes and what it returns agree ----

  it.effect("put / create / upsert / batch / transaction honour decoding defaults", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const defaults = db.entities.Defaults as any
      const expectDefaults = (value: any) => {
        expect(DateTime.formatIso(value.born)).toBe("1800-01-01T00:00:00.000Z")
        expect(DateTime.formatIso(value.when)).toBe("1970-01-02T00:00:00.000Z")
        expect(value.tier).toBe("basic")
      }
      // The defaults are stored — so the index they compose is written too.
      const expectStored = (id: string) =>
        Effect.gen(function* () {
          const raw = yield* rawItem("G133Defaults", id)
          // `tier` and `born` compose the byTier index: stored. `when` is
          // not part of any key: not stored, defaulted on read.
          expect([id, raw.born, raw.when, raw.tier]).toEqual([
            id,
            { S: "1800-01-01T00:00:00.000Z" },
            undefined,
            { S: "basic" },
          ])
          expect(raw.gsi1pk).toEqual({ S: "$edd133g#v1#g133defaults#tier_basic" })
        })
      const put = yield* defaults.put({ id: "df1", name: "n" })
      expectDefaults(put)
      expectDefaults(yield* defaults.get({ id: "df1" }))
      yield* expectStored("df1")
      expectDefaults(yield* defaults.create({ id: "df2", name: "n" }))
      expectDefaults(yield* defaults.get({ id: "df2" }))
      yield* expectStored("df2")
      expectDefaults(yield* defaults.upsert({ id: "df3", name: "n" }))
      expectDefaults(yield* defaults.get({ id: "df3" }))
      yield* expectStored("df3")
      yield* Batch.write([g133Entities.Defaults.put({ id: "df4", name: "n" } as any)])
      expectDefaults(yield* defaults.get({ id: "df4" }))
      yield* expectStored("df4")
      yield* Transaction.transactWrite([g133Entities.Defaults.put({ id: "df5", name: "n" } as any)])
      expectDefaults(yield* defaults.get({ id: "df5" }))
      yield* expectStored("df5")
      // Every one of them is in the index its decoded record names.
      const inIndex = (yield* defaults.byTier({ tier: "basic" }).collect()).map((d: any) => d.id)
      expect(inIndex).toEqual(expect.arrayContaining(["df1", "df2", "df3", "df4", "df5"]))
      // A value supplied is stored and returned as given.
      const given = (yield* defaults.put({
        id: "df6",
        name: "n",
        born: DateTime.makeUnsafe(1000),
        when: DateTime.makeUnsafe(2000),
        tier: "gold",
      })) as any
      expect([DateTime.formatIso(given.born), DateTime.formatIso(given.when), given.tier]).toEqual([
        "1970-01-01T00:00:01.000Z",
        "1970-01-01T00:00:02.000Z",
        "gold",
      ])
      expect((yield* rawItem("G133Defaults", "df6")).born).toEqual({
        S: "1970-01-01T00:00:01.000Z",
      })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a soft delete of an item written before the entity was versioned", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const client = yield* DynamoClient
      // As `put` wrote it before `versioned` was enabled: no version, no token.
      yield* client.putItem({
        TableName: g133Tables.record,
        Item: {
          ...mainKey("G133SoftVersioned", "legacy"),
          __edd_e__: { S: "G133SoftVersioned" },
          id: { S: "legacy" },
          name: { S: "n" },
        },
      })
      yield* (db.entities.SoftVersioned as any).delete({ id: "legacy" })
      expect(yield* rawItem("G133SoftVersioned", "legacy")).toBeUndefined()
      const keys = yield* partition("G133SoftVersioned", "legacy")
      expect(keys).toHaveLength(1)
      expect(keys[0]).toMatch(/#deleted#/)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("update() of a missing item may omit a field that has a decoding default", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const defaults = db.entities.Defaults as any
      yield* defaults.put({ id: "dp", name: "n" })
      const created = (yield* defaults.update({ id: "du" }).set({ id: "du", name: "n" })) as any
      expect(created.tier).toBe("basic")
      expect((yield* defaults.byTier({ tier: "basic" }).collect()).map((d: any) => d.id)).toContain(
        "du",
      )
      const strip = (item: Record<string, any>, id: string) =>
        JSON.parse(
          JSON.stringify(item).replaceAll(`id_${id}`, "id_ID").replaceAll(`"${id}"`, '"ID"'),
        )
      expect(strip(yield* rawItem("G133Defaults", "du"), "du")).toEqual(
        strip(yield* rawItem("G133Defaults", "dp"), "dp"),
      )
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- items written before the entity was `versioned` ----

  /** Write `id` through the unversioned definition, then run `use` through the versioned one. */
  const legacy = <A>(
    pair: keyof typeof g133LegacyPairs,
    seeds: ReadonlyArray<Record<string, unknown>>,
    use: (db: any, entity: any) => Effect.Effect<A, any, any>,
  ) =>
    Effect.gen(function* () {
      const before = g133LegacyClient(pair, 0)
      yield* Effect.gen(function* () {
        const db: any = yield* before.client
        for (const seed of seeds) yield* db.entities.Doc.put(seed)
      }).pipe(Effect.provide(before.layer))
      const after = g133LegacyClient(pair, 1)
      return yield* Effect.gen(function* () {
        const db: any = yield* after.client
        return yield* use(db, after.entity)
      }).pipe(Effect.provide(after.layer))
    })
  const legacyRaw = (entityType: string, id: string) => rawItem(entityType, id)

  it.effect("a pre-versioning item reads as version 0 on every read path", () =>
    Effect.gen(function* () {
      yield* legacy(
        "Ret",
        [
          { id: "lr1", name: "legacy-read" },
          { id: "lr2", name: "legacy-read" },
        ],
        (db, entity) =>
          Effect.gen(function* () {
            const docs = db.entities.Doc
            expect((yield* docs.get({ id: "lr1" })).version).toBe(0)
            const byName = yield* docs.byName({ name: "legacy-read" }).collect()
            expect(byName.map((d: any) => d.version)).toEqual([0, 0])
            const scanned = (yield* docs.scan().collect()).filter((d: any) =>
              ["lr1", "lr2"].includes(d.id),
            )
            expect(scanned.map((d: any) => d.version)).toEqual([0, 0])
            const [batch] = yield* Batch.get([entity.get({ id: "lr1" })])
            expect((batch as any).version).toBe(0)
            const [tx] = yield* Transaction.transactGet([entity.get({ id: "lr2" })])
            expect((tx as any).version).toBe(0)
            const item = (yield* Entity.asItem(entity.get({ id: "lr1" }))) as any
            expect(item.version).toBe(0)
          }),
      )
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("the first versioned plain update writes version 1", () =>
    Effect.gen(function* () {
      yield* legacy(
        "Plain",
        [
          { id: "lp1", name: "a" },
          { id: "lp2", name: "a" },
          { id: "lp3", name: "a" },
        ],
        (db) =>
          Effect.gen(function* () {
            const docs = db.entities.Doc
            const updated = yield* docs.update({ id: "lp1" }).set({ tags: ["x"] })
            expect(updated.version).toBe(1)
            expect((yield* legacyRaw("G133LegacyPlain", "lp1")).version).toEqual({ N: "1" })
            const pinned = yield* docs
              .update({ id: "lp2" })
              .set({ tags: ["y"] })
              .expectedVersion(0)
            expect(pinned.version).toBe(1)
            const stale = yield* docs
              .update({ id: "lp3" })
              .set({ tags: ["z"] })
              .expectedVersion(1)
              .asEffect()
              .pipe(Effect.flip)
            expect([stale._tag, stale.expectedVersion, stale.actualVersion]).toEqual([
              "OptimisticLockError",
              1,
              0,
            ])
            // A path update is a plain update too.
            const path = yield* docs
              .update({ id: "lp3" })
              .pathSet({ segments: ["name"], value: "p", isPath: false })
            expect([path.name, path.version]).toEqual(["p", 1])
          }),
      )
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("the first versioned read-then-write update writes version 1", () =>
    Effect.gen(function* () {
      yield* legacy("Uniq", [{ id: "lu1", name: "a", email: "lu1@x.io" }], (db) =>
        Effect.gen(function* () {
          const updated = yield* db.entities.Doc.update({ id: "lu1" }).set({ email: "lu1b@x.io" })
          expect([updated.version, updated.email]).toEqual([1, "lu1b@x.io"])
          const raw = yield* legacyRaw("G133LegacyUniq", "lu1")
          expect(raw.version).toEqual({ N: "1" })
          expect(raw.__edd_i__?.S).toMatch(/^[0-9a-f-]{36}$/)
        }),
      )
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  for (const [label, build] of [
    ["record", (u: any) => u.set({ name: "b" })],
    ["path", (u: any) => u.pathSet({ segments: ["name"], value: "b", isPath: false })],
  ] as const) {
    it.effect(`the first versioned retain ${label} update snapshots it as v#0000000`, () =>
      Effect.gen(function* () {
        const id = `lret-${label}`
        yield* legacy("Ret", [{ id, name: "a" }], (db) =>
          Effect.gen(function* () {
            const docs = db.entities.Doc
            const updated = yield* build(docs.update({ id }))
            expect([updated.name, updated.version]).toEqual(["b", 1])
            const v0 = yield* docs.getVersion({ id }, 0)
            expect([v0.name, v0.version]).toEqual(["a", 0])
            const all = yield* docs.versions({ id }).collect()
            expect(all.map((v: any) => v.version)).toEqual([0])
            expect(yield* partition("G133LegacyRet", id)).toEqual([
              "$edd133g#v1#g133legacyret",
              "$edd133g#v1#g133legacyret#v#0000000",
            ])
            // …and from there on, ordinary versioning.
            const next = yield* build(docs.update({ id }))
            expect(next.version).toBe(2)
          }),
        )
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )

    it.effect(
      `a race on the first versioned retain ${label} update is an OptimisticLockError`,
      () =>
        Effect.gen(function* () {
          const id = `lrace-${label}`
          yield* legacy("Ret", [{ id, name: "a" }], (db) =>
            Effect.gen(function* () {
              // Another writer versions the item between our read and write.
              g133Inject.before = rawSetAttrs("G133LegacyRet", id, {
                version: { N: "1" },
                name: { S: "theirs" },
              })
              const error = yield* build(db.entities.Doc.update({ id }))
                .asEffect()
                .pipe(Effect.flip)
              expect([error._tag, error.expectedVersion, error.actualVersion]).toEqual([
                "OptimisticLockError",
                0,
                1,
              ])
              expect((yield* legacyRaw("G133LegacyRet", id)).name).toEqual({ S: "theirs" })
            }),
          )
        }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  it.effect("a race on the first versioned plain update with expectedVersion(0)", () =>
    Effect.gen(function* () {
      yield* legacy("Plain", [{ id: "lpr", name: "a" }], (db) =>
        Effect.gen(function* () {
          g133Inject.before = rawSetAttrs("G133LegacyPlain", "lpr", { version: { N: "1" } })
          const error = yield* db.entities.Doc.update({ id: "lpr" })
            .set({ name: "b" })
            .expectedVersion(0)
            .asEffect()
            .pipe(Effect.flip)
          expect([error._tag, error.actualVersion]).toEqual(["OptimisticLockError", 1])
        }),
      )
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a pre-versioning item soft-deletes and restores through the versioned entity", () =>
    Effect.gen(function* () {
      yield* legacy("Soft", [{ id: "lsd", name: "a" }], (db) =>
        Effect.gen(function* () {
          const docs = db.entities.Doc
          yield* docs.delete({ id: "lsd" })
          const tombstone = yield* docs.deleted.get({ id: "lsd" })
          expect(tombstone.version).toBe(0)
          const restored = yield* docs.restore({ id: "lsd" })
          expect([restored.name, restored.version]).toEqual(["a", 1])
          const keys = yield* partition("G133LegacySoft", "lsd")
          expect(keys).toEqual([
            "$edd133g#v1#g133legacysoft",
            "$edd133g#v1#g133legacysoft#v#0000000",
          ])
        }),
      )
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- a condition too large to share with the library's guard ----

  it.effect("a pathological .condition() is refused before writing, naming its size", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const huge = (t: any, { or, eq }: any) =>
        or(...Array.from({ length: 160 }, (_, i) => eq(t.name, `value-${i}`)))
      const accounts = db.entities.SoftAccounts as any
      yield* accounts.put({ id: "huge", email: "huge@x.io", name: "n" })
      const del = yield* accounts
        .delete({ id: "huge" })
        .condition(huge)
        .asEffect()
        .pipe(Effect.flip)
      expect(del._tag).toBe("ValidationError")
      expect(String(del.cause)).toMatch(/\.condition\(\) alone is \d+ characters \/ \d+ operators/)
      expect(yield* rawItem("G133SoftAccount", "huge")).toBeDefined()

      const retained = db.entities.DevicesRetained as any
      yield* retained.put({ id: "huge", owner: "o", label: "l" })
      for (const build of [
        (u: any) => u.set({ label: "m" }),
        (u: any) => u.pathSet({ segments: ["label"], value: "m", isPath: false }),
      ]) {
        const error = yield* build(retained.update({ id: "huge" }))
          .condition(huge)
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
      }
      const plain = yield* (db.entities.DevicesPlain as any)
        .update({ id: "huge" })
        .set({ label: "m" })
        .condition(huge)
        .asEffect()
        .pipe(Effect.flip)
      expect(plain._tag).toBe("ValidationError")
      expect((yield* rawItem("G133DeviceRetained", "huge")).label).toEqual({ S: "l" })

      // A large condition that fits beside the guard still runs, and a wide
      // item's fallback guard shrinks to leave it room.
      const wide = db.entities.WideBare as any
      const totals = Object.fromEntries(Array.from({ length: 300 }, (_, i) => [`k${i}`, i]))
      yield* wide.put({ id: "wcond", email: "wcond@x.io", totals })
      const sized = (t: any, { or, eq }: any) =>
        or(...Array.from({ length: 60 }, (_, i) => eq(t.email, i === 0 ? "wcond@x.io" : `x${i}`)))
      yield* wide.delete({ id: "wcond" }).condition(sized)
      expect(yield* rawItem("G133WideBare", "wcond")).toBeUndefined()
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- a removed version is corruption, never "pre-versioning" ----

  const removeAttr = (entityType: string, id: string, attr: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      yield* client.updateItem({
        TableName: g133Tables.record,
        Key: mainKey(entityType, id),
        UpdateExpression: "REMOVE #a",
        ExpressionAttributeNames: { "#a": attr },
      })
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)
  const snapshotOf = (entityType: string, id: string, version: number) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const { Item } = yield* client.getItem({
        TableName: g133Tables.record,
        Key: {
          pk: mainKey(entityType, id).pk,
          sk: {
            S: `$edd133g#v1#${entityType.toLowerCase()}#v#${String(version).padStart(7, "0")}`,
          },
        },
        ConsistentRead: true,
      })
      return Item as Record<string, any> | undefined
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)
  const snapshotLabel = (entityType: string, id: string, version: number) =>
    Effect.map(snapshotOf(entityType, id, version), (item) => item?.label)

  it.effect("an item whose version was removed outside the library is refused everywhere", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const docs = db.entities.DevicesRetained as any
      // The reviewer's repro: v1 → v2 → v3, then the version is removed.
      yield* docs.put({ id: "corrupt", owner: "o", label: "v1" })
      yield* docs.update({ id: "corrupt" }).set({ label: "v2" })
      yield* docs.update({ id: "corrupt" }).set({ label: "v3" })
      yield* removeAttr("G133DeviceRetained", "corrupt", "version")
      const refused = (effect: Effect.Effect<unknown, any>) =>
        Effect.gen(function* () {
          const error = yield* effect.pipe(Effect.flip)
          expect(error._tag).toBe("ValidationError")
          expect(String(error.cause)).toContain("incarnation token")
        })
      yield* refused(docs.get({ id: "corrupt" }).asEffect())
      yield* refused(docs.update({ id: "corrupt" }).set({ label: "x1" }).asEffect())
      yield* refused(
        docs
          .update({ id: "corrupt" })
          .pathSet({ segments: ["label"], value: "x1", isPath: false })
          .asEffect(),
      )
      yield* refused(docs.scan().collect())
      // History untouched: no v#0000000, v#0000001 still "v1".
      expect(yield* snapshotLabel("G133DeviceRetained", "corrupt", 1)).toEqual({ S: "v1" })
      expect(yield* snapshotLabel("G133DeviceRetained", "corrupt", 0)).toBeUndefined()

      // A plain (unread) update refuses it too, through its condition.
      const plain = db.entities.DevicesPlain as any
      yield* plain.put({ id: "corrupt2", owner: "o", label: "l" })
      yield* removeAttr("G133DevicePlain", "corrupt2", "version")
      yield* refused(plain.update({ id: "corrupt2" }).set({ label: "m" }).asEffect())
      expect((yield* rawItem("G133DevicePlain", "corrupt2")).version).toBeUndefined()
      // A plain upsert (no read) would version it as 1 — refused by its condition.
      yield* refused(plain.upsert({ id: "corrupt2", owner: "o", label: "u" }).asEffect())
      const afterUpsert = yield* rawItem("G133DevicePlain", "corrupt2")
      expect([afterUpsert.version, afterUpsert.label]).toEqual([undefined, { S: "l" }])
      // A plain hard delete reads and writes no history: it removes the item.
      yield* plain.delete({ id: "corrupt2" })
      expect(yield* rawItem("G133DevicePlain", "corrupt2")).toBeUndefined()
      // …and so do soft delete and restore.
      const soft = db.entities.SoftDevices as any
      yield* soft.put({ id: "corrupt3", owner: "o", label: "l" })
      yield* removeAttr("G133SoftDevice", "corrupt3", "version")
      yield* refused(soft.delete({ id: "corrupt3" }).asEffect())
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a version snapshot is never overwritten", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const docs = db.entities.DevicesRetained as any
      // Distinct clock readings: each version's updatedAt differs.
      yield* docs.put({ id: "hist", owner: "o", label: "v1" })
      yield* TestClock.adjust("1 second")
      yield* docs.update({ id: "hist" }).set({ label: "v2" })
      yield* TestClock.adjust("1 second")
      yield* docs.update({ id: "hist" }).set({ label: "v3" })
      // An outside write rewinds the version: the next snapshot would be v#0000001.
      yield* rawSetAttrs("G133DeviceRetained", "hist", { version: { N: "1" } }).pipe(Effect.orDie)
      for (const build of [
        (u: any) => u.set({ label: "x" }),
        (u: any) => u.pathSet({ segments: ["label"], value: "x", isPath: false }),
      ]) {
        const error = yield* build(docs.update({ id: "hist" }))
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
        expect(String(error.cause)).toContain("version 1 snapshot already exists")
      }
      expect(yield* snapshotLabel("G133DeviceRetained", "hist", 1)).toEqual({ S: "v1" })
      expect((yield* rawItem("G133DeviceRetained", "hist")).label).toEqual({ S: "v3" })

      const soft = db.entities.SoftDevices as any
      yield* soft.put({ id: "hist2", owner: "o", label: "v1" })
      yield* TestClock.adjust("1 second")
      yield* soft.update({ id: "hist2" }).set({ label: "v2" })
      yield* rawSetAttrs("G133SoftDevice", "hist2", { version: { N: "1" } }).pipe(Effect.orDie)
      const del = yield* soft.delete({ id: "hist2" }).asEffect().pipe(Effect.flip)
      expect(del._tag).toBe("ValidationError")
      expect(yield* rawItem("G133SoftDevice", "hist2")).toBeDefined()
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("an item written by a versioned entity before #133 (version, no token) works", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const client = yield* DynamoClient
      yield* client.putItem({
        TableName: g133Tables.record,
        Item: {
          ...mainKey("G133DeviceRetained", "pre133"),
          __edd_e__: { S: "G133DeviceRetained" },
          id: { S: "pre133" },
          owner: { S: "o" },
          label: { S: "l" },
          version: { N: "4" },
          createdAt: { S: "1970-01-01T00:00:00.000Z" },
          updatedAt: { S: "1970-01-01T00:00:00.000Z" },
        },
      })
      const docs = db.entities.DevicesRetained as any
      expect((yield* docs.get({ id: "pre133" })).version).toBe(4)
      const updated = yield* docs.update({ id: "pre133" }).set({ label: "m" })
      expect(updated.version).toBe(5)
      expect((yield* rawItem("G133DeviceRetained", "pre133")).__edd_i__?.S).toMatch(
        /^[0-9a-f-]{36}$/,
      )
      expect(yield* snapshotLabel("G133DeviceRetained", "pre133", 4)).toEqual({ S: "l" })
      yield* docs.delete({ id: "pre133" })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- a default never creates a unique sentinel ----

  it.effect("defaulted unique fields: indexed and stored, but no sentinel until supplied", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const coded = db.entities.Coded as any
      // Two items leave both unique fields to their defaults: both succeed.
      yield* coded.put({ id: "cd1", name: "a" })
      yield* coded.put({ id: "cd2", name: "b" })
      const raw1 = yield* rawItem("G133Coded", "cd1")
      expect([raw1.code, raw1.handle, raw1.__edd_d__]).toEqual([
        { S: "none" },
        undefined,
        { SS: ["code"] },
      ])
      expect(
        (yield* coded.byCode({ code: "none" }).collect()).map((c: any) => c.id).sort(),
      ).toEqual(["cd1", "cd2"])
      expect(yield* sentinelExistsFor("G133Coded", "code", "none")).toBe(false)
      expect(yield* sentinelExistsFor("G133Coded", "handle", "anon")).toBe(false)
      // A third item SUPPLIES "none": its sentinel is created, and it is its own.
      yield* coded.put({ id: "cd3", name: "c", code: "none" })
      expect(yield* sentinelExistsFor("G133Coded", "code", "none")).toBe(true)
      // cd1 moves to a real value: its sentinel is created, cd3's survives.
      const moved = yield* coded.update({ id: "cd1" }).set({ code: "c1" })
      expect(moved.code).toBe("c1")
      expect((yield* rawItem("G133Coded", "cd1")).__edd_d__).toBeUndefined()
      expect(yield* sentinelExistsFor("G133Coded", "code", "c1")).toBe(true)
      expect(yield* sentinelExistsFor("G133Coded", "code", "none")).toBe(true)
      // Deleting cd2 (still on the default) leaves cd3's sentinel alone too.
      yield* coded.delete({ id: "cd2" })
      expect(yield* sentinelExistsFor("G133Coded", "code", "none")).toBe(true)
      // And the supplied value is enforced.
      const taken = yield* coded
        .put({ id: "cd4", name: "d", code: "c1" })
        .asEffect()
        .pipe(Effect.flip)
      expect(taken._tag).toBe("UniqueConstraintViolation")
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- operator counting against DynamoDB's own ----

  it.effect("a condition at DynamoDB's operator limit is sent; one over is refused first", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const counters = db.entities.Counters as any
      yield* counters.put({ id: "ops", owner: "o", label: "l0" })
      // The plain update adds `AND attribute_exists(#exists)`: 2 operators.
      const equals =
        (k: number) =>
        (t: any, { or, eq }: any) =>
          or(...Array.from({ length: k }, (_, i) => eq(t.label, i === 0 ? "l0" : `x${i}`)))
      const betweens =
        (k: number) =>
        (t: any, { or, between }: any) =>
          or(...Array.from({ length: k }, (_, i) => between(t.label, i === 0 ? "a" : `x${i}`, "z")))
      // 149 comparisons → 149 + 148 OR + 2 = 299: within the limit.
      yield* counters.update({ id: "ops" }).set({ label: "l0" }).condition(equals(149))
      // BETWEEN's own AND is part of it: 101 BETWEEN → 101 + 100 OR + 2 = 203,
      // sent and accepted (counting that AND would make it 304, refused).
      yield* counters.update({ id: "ops" }).set({ label: "l0" }).condition(betweens(101))
      // 150 comparisons → 301: refused before anything is sent.
      for (const cond of [equals(150)]) {
        const error = yield* counters
          .update({ id: "ops" })
          .set({ label: "changed" })
          .condition(cond)
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("ValidationError")
        expect(String(error.cause)).toContain("301 operators")
      }
      expect((yield* rawItem("G133Counter", "ops")).label).toEqual({ S: "l0" })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- upsert of a unique-constrained entity: guarded read-then-write ----

  for (const [entity, entityType, versioned] of [
    ["AccountsBare", "G133AccountBare", false],
    ["AccountsPlain", "G133AccountPlain", true],
  ] as const) {
    it.effect(`${entity}: upsert creates, rotates, keeps, and refuses duplicates`, () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const accounts = db.entities[entity] as any
        const id = `ups-${entity.toLowerCase()}`
        const email = (v: string) => `${v}-${entity.toLowerCase()}@x.io`
        // Missing: a create, with its sentinel.
        yield* accounts.upsert({ id, email: email("a"), name: "n1" })
        expect(yield* sentinelExistsFor(entityType, "email", email("a"))).toBe(true)
        // Existing, changed value: the sentinel rotates.
        const rotated = yield* accounts.upsert({ id, email: email("b"), name: "n2" })
        expect([rotated.email, rotated.name]).toEqual([email("b"), "n2"])
        expect(yield* sentinelExistsFor(entityType, "email", email("a"))).toBe(false)
        expect(yield* sentinelExistsFor(entityType, "email", email("b"))).toBe(true)
        if (versioned) expect(rotated.version).toBe(2)
        // Existing, unchanged value: left alone (no self-collision).
        const same = yield* accounts.upsert({ id, email: email("b"), name: "n3" })
        expect(same.name).toBe("n3")
        expect(yield* sentinelExistsFor(entityType, "email", email("b"))).toBe(true)
        // A value another item holds: refused, nothing written.
        const other = `${id}-other`
        const dup = yield* accounts
          .upsert({ id: other, email: email("b"), name: "x" })
          .asEffect()
          .pipe(Effect.flip)
        expect(dup._tag).toBe("UniqueConstraintViolation")
        expect(yield* rawItem(entityType, other)).toBeUndefined()
        // A concurrent change between the read and the write.
        g133Inject.before = rawSetAttrs(
          entityType,
          id,
          versioned ? { name: { S: "theirs" } } : { email: { S: email("c") } },
          versioned,
        )
        const race = yield* accounts
          .upsert({ id, email: email("d"), name: "mine" })
          .asEffect()
          .pipe(Effect.flip)
        expect(race._tag).toBe(versioned ? "OptimisticLockError" : "ConcurrentModification")
        expect(yield* sentinelExistsFor(entityType, "email", email("d"))).toBe(false)
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
    )
  }

  // ---- a put over an existing versioned item continues it ----

  it.effect("a replacing put continues the version, incarnation and history", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const plain = db.entities.DevicesPlain as any
      const first = yield* plain.put({ id: "rp1", owner: "o", label: "a" })
      expect(first.version).toBe(1)
      const firstRaw = yield* rawItem("G133DevicePlain", "rp1")
      yield* TestClock.adjust("1 second")
      const again = yield* plain.put({ id: "rp1", owner: "o", label: "b" })
      expect(again.version).toBe(2)
      const againRaw = yield* rawItem("G133DevicePlain", "rp1")
      expect(againRaw.__edd_i__).toEqual(firstRaw.__edd_i__)
      expect(againRaw.createdAt).toEqual(firstRaw.createdAt)
      expect(againRaw.updatedAt).not.toEqual(firstRaw.updatedAt)

      const retained = db.entities.DevicesRetained as any
      yield* retained.put({ id: "rp2", owner: "o", label: "v1" })
      yield* TestClock.adjust("1 second")
      yield* retained.update({ id: "rp2" }).set({ label: "v2" })
      yield* TestClock.adjust("1 second")
      const replaced = yield* retained.put({ id: "rp2", owner: "o", label: "v3" })
      expect(replaced.version).toBe(3)
      expect(yield* snapshotLabel("G133DeviceRetained", "rp2", 1)).toEqual({ S: "v1" })
      expect(yield* snapshotLabel("G133DeviceRetained", "rp2", 2)).toEqual({ S: "v2" })
      expect(yield* snapshotLabel("G133DeviceRetained", "rp2", 3)).toBeUndefined()

      // A race on the replace: the put is planned again from the item as the
      // other writer left it, and the last writer wins — as a plain PutItem.
      g133Inject.before = rawSetAttrs("G133DeviceRetained", "rp2", { label: { S: "x" } }, true)
      const raced = yield* retained.put({ id: "rp2", owner: "o", label: "v4" })
      expect([raced.label, raced.version]).toEqual(["v4", 5])
      expect(yield* snapshotLabel("G133DeviceRetained", "rp2", 4)).toEqual({ S: "x" })

      // create still fails on an existing item.
      const dup = yield* retained
        .create({ id: "rp2", owner: "o", label: "new" })
        .asEffect()
        .pipe(Effect.flip)
      expect(dup._tag).toBe("ConditionalCheckFailed")

      // A put over a unique entity's item with the same value replaces it,
      // keeping its createdAt as a versioned item's does.
      const bare = db.entities.AccountsBare as any
      yield* bare.put({ id: "rp3", email: "rp3@x.io", name: "a" })
      const bareFirst = yield* rawItem("G133AccountBare", "rp3")
      yield* TestClock.adjust("1 second")
      yield* bare.put({ id: "rp3", email: "rp3@x.io", name: "b" })
      const bareAgain = yield* rawItem("G133AccountBare", "rp3")
      expect(bareAgain.createdAt).toEqual(bareFirst.createdAt)
      expect(bareAgain.updatedAt).not.toEqual(bareFirst.updatedAt)
      yield* bare.put({ id: "rp3", email: "rp3b@x.io", name: "c" })
      expect(yield* sentinelExistsFor("G133AccountBare", "email", "rp3@x.io")).toBe(false)
      expect(yield* sentinelExistsFor("G133AccountBare", "email", "rp3b@x.io")).toBe(true)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("transactions replace a versioned item; batches may only create one", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* (db.entities.DevicesPlain as any).put({ id: "tx1", owner: "o", label: "a" })
      yield* Transaction.transactWrite([
        g133Entities.DevicesPlain.put({ id: "tx1", owner: "o", label: "b" } as any),
      ])
      const replaced = yield* rawItem("G133DevicePlain", "tx1")
      expect([replaced.label, replaced.version]).toEqual([{ S: "b" }, { N: "2" }])
      // A new item is still created through a transaction.
      yield* Transaction.transactWrite([
        g133Entities.DevicesPlain.put({ id: "tx2", owner: "o", label: "b" } as any),
      ])
      expect((yield* rawItem("G133DevicePlain", "tx2")).version).toEqual({ N: "1" })
      // Batch.write: new versioned items are created through create-only
      // transactions; one that exists cancels its whole chunk.
      yield* Batch.write([
        g133Entities.DevicesPlain.put({ id: "tx3", owner: "o", label: "b" } as any),
      ])
      expect((yield* rawItem("G133DevicePlain", "tx3")).version).toEqual({ N: "1" })
      const batch = yield* Batch.write([
        g133Entities.DevicesPlain.put({ id: "tx4", owner: "o", label: "b" } as any),
        g133Entities.DevicesPlain.put({ id: "tx3", owner: "o", label: "c" } as any),
      ]).pipe(Effect.flip)
      expect(batch._tag).toBe("ValidationError")
      expect(String((batch as any).cause)).toContain("would replace an existing")
      expect(yield* rawItem("G133DevicePlain", "tx4")).toBeUndefined()
      expect((yield* rawItem("G133DevicePlain", "tx3")).label).toEqual({ S: "b" })
      // More than 4 MB of versioned puts is split under DynamoDB's transaction cap.
      const big = "x".repeat(380_000)
      yield* Batch.write(
        Array.from({ length: 12 }, (_, i) =>
          g133Entities.DevicesPlain.put({ id: `big${i}`, owner: "o", label: big } as any),
        ),
      )
      expect((yield* rawItem("G133DevicePlain", "big11")).version).toEqual({ N: "1" })
      // A delete and a versioned put of one item can't keep their order: refused.
      const reordered = yield* Batch.write([
        g133Entities.DevicesPlain.delete({ id: "tx2" } as any),
        g133Entities.DevicesPlain.put({ id: "tx2", owner: "o", label: "z" } as any),
      ]).pipe(Effect.flip)
      expect(reordered._tag).toBe("ValidationError")
      expect((yield* rawItem("G133DevicePlain", "tx2")).label).toEqual({ S: "b" })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect(".remove() of a defaulted index composite re-materialises its default", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const coded = db.entities.Coded as any
      yield* coded.put({ id: "rm1", name: "a", code: "real" })
      expect(yield* sentinelExistsFor("G133Coded", "code", "real")).toBe(true)
      const removed = yield* coded.update({ id: "rm1" }).remove(["code"])
      expect(removed.code).toBe("none")
      const raw = yield* rawItem("G133Coded", "rm1")
      expect([raw.code, raw.gsi1pk, raw.__edd_d__]).toEqual([
        { S: "none" },
        { S: "$edd133g#v1#g133coded#code_none" },
        { SS: ["code"] },
      ])
      expect(yield* sentinelExistsFor("G133Coded", "code", "real")).toBe(false)
      expect((yield* coded.byCode({ code: "none" }).collect()).map((c: any) => c.id)).toContain(
        "rm1",
      )
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- a sentinel another item owns is never released (#133) ----

  const sentinelKey = (entityType: string, constraint: string, value: string) => ({
    pk: { S: `$edd133g#v1#${entityType.toLowerCase()}.${constraint}#${value}` },
    sk: { S: `$edd133g#v1#${entityType.toLowerCase()}.${constraint}` },
  })
  const sentinelOwner = (entityType: string, constraint: string, value: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const { Item } = yield* client.getItem({
        TableName: g133Tables.record,
        Key: sentinelKey(entityType, constraint, value),
        ConsistentRead: true,
      })
      return Item?._entity_pk?.S
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)
  const dropSentinel = (entityType: string, constraint: string, value: string) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      yield* client.deleteItem({
        TableName: g133Tables.record,
        Key: sentinelKey(entityType, constraint, value),
      })
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)

  for (const [entity, entityType] of [
    ["AccountsBare", "G133AccountBare"],
    ["AccountsPlain", "G133AccountPlain"],
    ["AccountsRetained", "G133AccountRetained"],
    ["SoftAccounts", "G133SoftAccount"],
  ] as const) {
    for (const [label, release] of [
      ["put", (acc: any, id: string, to: string) => acc.put({ id, email: to, name: "moved" })],
      ["update", (acc: any, id: string, to: string) => acc.update({ id }).set({ email: to })],
      ["upsert", (acc: any, id: string, to: string) => acc.upsert({ id, email: to, name: "m" })],
      ["delete", (acc: any, id: string) => acc.delete({ id })],
      ["purge", (acc: any, id: string) => acc.purge({ id })],
      [
        "transactWrite put",
        (_: any, id: string, to: string) =>
          Transaction.transactWrite([
            (g133Entities as any)[entity].put({ id, email: to, name: "moved" }),
          ]),
      ],
    ] as const) {
      it.effect(`${entity}: ${label} never releases a sentinel another item owns`, () =>
        Effect.gen(function* () {
          const db = yield* g133Client
          const accounts = db.entities[entity] as any
          const tag = `${entity}-${label.replace(" ", "-")}`.toLowerCase()
          const [a, b, c] = [`${tag}-a`, `${tag}-b`, `${tag}-c`]
          const held = `${tag}@x.io`
          const moved = `${tag}-moved@x.io`
          // `a` holds the value without its sentinel (an expired or later-added
          // constraint); `b` then claimed the value and owns its sentinel.
          yield* accounts.put({ id: a, email: held, name: "a" })
          yield* dropSentinel(entityType, "email", held)
          yield* accounts.put({ id: b, email: held, name: "b" })
          const owner = mainKey(entityType, b).pk.S
          expect(yield* sentinelOwner(entityType, "email", held)).toBe(owner)

          yield* release(accounts, a, moved)

          // `b` still owns its sentinel, and the value stays taken.
          expect(yield* sentinelOwner(entityType, "email", held)).toBe(owner)
          const dup = yield* accounts
            .create({ id: c, email: held, name: "c" })
            .asEffect()
            .pipe(Effect.flip)
          expect(dup._tag).toBe("UniqueConstraintViolation")
          if (label !== "delete" && label !== "purge") {
            expect(yield* sentinelOwner(entityType, "email", moved)).toBe(
              mainKey(entityType, a).pk.S,
            )
          }
        }).pipe(Effect.provide(g133RaceLayer), g133Closed),
      )
    }
  }

  // ---- a release whose reservation changes hands is planned again (#133) ----

  /** Another item takes over a sentinel — between a write's ownership read and its write. */
  const handOver = (entityType: string, constraint: string, value: string, to: string) =>
    g133Hook(
      Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.updateItem({
          TableName: g133Tables.record,
          Key: sentinelKey(entityType, constraint, value),
          UpdateExpression: "SET #epk = :epk",
          ExpressionAttributeNames: { "#epk": "_entity_pk" },
          ExpressionAttributeValues: { ":epk": mainKey(entityType, to).pk },
        })
      }).pipe(Effect.provide(ClientLayer)),
    )

  for (const [entity, entityType] of [
    ["AccountsBare", "G133AccountBare"],
    ["AccountsPlain", "G133AccountPlain"],
    ["AccountsRetained", "G133AccountRetained"],
  ] as const) {
    for (const [label, write] of [
      ["put", (acc: any, id: string, to: string) => acc.put({ id, email: to, name: "m" })],
      ["upsert", (acc: any, id: string, to: string) => acc.upsert({ id, email: to, name: "m" })],
      [
        "transactWrite put",
        (_: any, id: string, to: string) =>
          Transaction.transactWrite([
            (g133Entities as any)[entity].put({ id, email: to, name: "m" }),
          ]),
      ],
    ] as const) {
      it.effect(`${entity}: ${label} is written again when a release races`, () =>
        Effect.gen(function* () {
          const db = yield* g133Client
          const accounts = db.entities[entity] as any
          const tag = `${entity}-hand-${label.replace(" ", "-")}`.toLowerCase()
          const [a, b] = [`${tag}-a`, `${tag}-b`]
          const held = `${tag}@x.io`
          const moved = `${tag}-moved@x.io`
          yield* accounts.put({ id: a, email: held, name: "a" })
          // The reservation passes to `b` after `a`'s write read it as `a`'s.
          g133Inject.before = handOver(entityType, "email", held, b)
          yield* write(accounts, a, moved)
          // Planned again from a fresh read: `b`'s sentinel is left alone.
          expect(yield* sentinelOwner(entityType, "email", held)).toBe(mainKey(entityType, b).pk.S)
          expect(yield* sentinelOwner(entityType, "email", moved)).toBe(mainKey(entityType, a).pk.S)
          expect((yield* rawItem(entityType, a)).email).toEqual({ S: moved })
        }).pipe(Effect.provide(g133RaceLayer), g133Closed),
      )
    }
  }

  // ---- transactions write existing versioned and unique items (#133) ----

  it.effect("transactWrite replaces an existing item exactly as its own put does", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const retained = db.entities.DevicesRetained as any
      yield* retained.put({ id: "txr1", owner: "o", label: "v1" })
      const firstRaw = yield* rawItem("G133DeviceRetained", "txr1")
      yield* TestClock.adjust("1 second")
      yield* Transaction.transactWrite([
        g133Entities.DevicesRetained.put({ id: "txr1", owner: "o", label: "v2" } as any),
      ])
      const raw = yield* rawItem("G133DeviceRetained", "txr1")
      expect([raw.label, raw.version]).toEqual([{ S: "v2" }, { N: "2" }])
      expect(raw.__edd_i__).toEqual(firstRaw.__edd_i__)
      expect(raw.createdAt).toEqual(firstRaw.createdAt)
      expect(raw.updatedAt).not.toEqual(firstRaw.updatedAt)
      expect(yield* snapshotLabel("G133DeviceRetained", "txr1", 1)).toEqual({ S: "v1" })

      // A race between the read and the transaction: written again, last writer wins.
      g133Inject.before = rawSetAttrs("G133DeviceRetained", "txr1", { label: { S: "x" } }, true)
      yield* Transaction.transactWrite([
        g133Entities.DevicesRetained.put({ id: "txr1", owner: "o", label: "v4" } as any),
      ])
      const after = yield* rawItem("G133DeviceRetained", "txr1")
      expect([after.label, after.version]).toEqual([{ S: "v4" }, { N: "4" }])
      expect(yield* snapshotLabel("G133DeviceRetained", "txr1", 3)).toEqual({ S: "x" })

      // A unique entity: the changed value's sentinel rotates; createdAt is kept.
      const bare = db.entities.AccountsBare as any
      yield* bare.put({ id: "txr2", email: "txr2@x.io", name: "a" })
      const bareFirst = yield* rawItem("G133AccountBare", "txr2")
      yield* TestClock.adjust("1 second")
      yield* Transaction.transactWrite([
        g133Entities.AccountsBare.put({ id: "txr2", email: "txr2b@x.io", name: "b" } as any),
      ])
      expect(yield* sentinelExistsFor("G133AccountBare", "email", "txr2@x.io")).toBe(false)
      expect(yield* sentinelOwner("G133AccountBare", "email", "txr2b@x.io")).toBe(
        mainKey("G133AccountBare", "txr2").pk.S,
      )
      expect((yield* rawItem("G133AccountBare", "txr2")).createdAt).toEqual(bareFirst.createdAt)

      // A value another item holds: UniqueConstraintViolation, nothing written.
      yield* bare.put({ id: "txr3", email: "txr3@x.io", name: "c" })
      const taken = yield* Transaction.transactWrite([
        g133Entities.AccountsBare.put({ id: "txr3", email: "txr2b@x.io", name: "c" } as any),
      ]).pipe(Effect.flip)
      expect(taken._tag).toBe("UniqueConstraintViolation")
      expect((yield* rawItem("G133AccountBare", "txr3")).email).toEqual({ S: "txr3@x.io" })

      // The caller's own condition: TransactionCancelled, as for any op.
      const refused = yield* Transaction.transactWrite([
        g133Entities.DevicesRetained.put({ id: "txr1", owner: "o", label: "v5" } as any).pipe(
          g133Entities.DevicesRetained.condition({ label: "nope" }),
        ),
      ]).pipe(Effect.flip)
      expect(refused._tag).toBe("TransactionCancelled")
      expect((yield* rawItem("G133DeviceRetained", "txr1")).label).toEqual({ S: "v4" })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("EventStore.append writes a guarded additional put again after a lost race", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const docs = db.entities.DevicesRetained as any
      yield* docs.put({ id: "esr1", owner: "o", label: "v1" })
      // Another writer changes the item between the append's read and its
      // transaction: the transaction is cancelled, built again and written.
      g133Inject.before = rawSetAttrs("G133DeviceRetained", "esr1", { label: { S: "x" } }, true)
      const appended = yield* G133Feed.append(
        { feedId: "esr-feed" },
        [new G133Noted({ note: "n" })],
        0,
        {
          additionalItems: [
            g133Entities.DevicesRetained.put({ id: "esr1", owner: "o", label: "mine" } as any),
          ],
        },
      )
      expect(appended.version).toBe(1)
      // Last writer wins, past the concurrent write — which is kept as history.
      const raw = yield* rawItem("G133DeviceRetained", "esr1")
      expect([raw.label, raw.version]).toEqual([{ S: "mine" }, { N: "3" }])
      expect(yield* snapshotLabel("G133DeviceRetained", "esr1", 2)).toEqual({ S: "x" })
      // The event was written exactly once.
      expect(yield* G133Feed.read({ feedId: "esr-feed" })).toHaveLength(1)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("transactWrite refuses two ops on one item, including swapped unique values", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const bare = db.entities.AccountsBare as any
      yield* bare.put({ id: "sw1", email: "sw1@x.io", name: "a" })
      yield* bare.put({ id: "sw2", email: "sw2@x.io", name: "b" })

      // Swapping values releases and claims each sentinel in the same transaction.
      const swapped = yield* Transaction.transactWrite([
        g133Entities.AccountsBare.put({ id: "sw1", email: "sw2@x.io", name: "a" } as any),
        g133Entities.AccountsBare.put({ id: "sw2", email: "sw1@x.io", name: "b" } as any),
      ]).pipe(Effect.flip)
      // Refused before anything is sent, naming the entity.
      expect(swapped._tag).toBe("ValidationError")
      expect((swapped as ValidationError).entityType).toBe("G133AccountBare")
      expect(String(swapped.cause)).toContain("touches one item more than once")

      // Two puts of one item.
      const twice = yield* Transaction.transactWrite([
        g133Entities.AccountsBare.put({ id: "sw1", email: "sw1@x.io", name: "c" } as any),
        g133Entities.AccountsBare.put({ id: "sw1", email: "sw1@x.io", name: "d" } as any),
      ]).pipe(Effect.flip)
      expect(twice._tag).toBe("ValidationError")
      expect(String(twice.cause)).toContain("touches one item more than once")

      // Two puts of one versioned retain item: refused too — not judged a lost
      // race, retried and reported as `OptimisticLockError`.
      const retained = db.entities.DevicesRetained as any
      yield* retained.put({ id: "sw3", owner: "o", label: "v1" })
      const retainedTwice = yield* Transaction.transactWrite([
        g133Entities.DevicesRetained.put({ id: "sw3", owner: "o", label: "a" } as any),
        g133Entities.DevicesRetained.put({ id: "sw3", owner: "o", label: "b" } as any),
      ]).pipe(Effect.flip)
      expect(retainedTwice._tag).toBe("ValidationError")
      expect((retainedTwice as ValidationError).entityType).toBe("G133DeviceRetained")
      const sw3 = yield* rawItem("G133DeviceRetained", "sw3")
      expect([sw3.label, sw3.version]).toEqual([{ S: "v1" }, { N: "1" }])
      expect(yield* snapshotLabel("G133DeviceRetained", "sw3", 2)).toBeUndefined()

      // Nothing was written: both items and their sentinels are as they were.
      expect((yield* rawItem("G133AccountBare", "sw1")).email).toEqual({ S: "sw1@x.io" })
      expect((yield* rawItem("G133AccountBare", "sw1")).name).toEqual({ S: "a" })
      expect((yield* rawItem("G133AccountBare", "sw2")).email).toEqual({ S: "sw2@x.io" })
      expect(yield* sentinelOwner("G133AccountBare", "email", "sw1@x.io")).toBe(
        mainKey("G133AccountBare", "sw1").pk.S,
      )
      expect(yield* sentinelOwner("G133AccountBare", "email", "sw2@x.io")).toBe(
        mainKey("G133AccountBare", "sw2").pk.S,
      )
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a transaction over 4 MB is refused before it is sent", () =>
    Effect.gen(function* () {
      // Six 380 KB retain puts: 2.3 MB of items, 4.6 MB with their snapshots.
      const big = "x".repeat(380_000)
      const ids = Array.from({ length: 6 }, (_, i) => `big-${i}`)
      const error = yield* Transaction.transactWrite(
        ids.map((id) => g133Entities.DevicesRetained.put({ id, owner: "o", label: big } as any)),
      ).pipe(Effect.flip)
      expect(error._tag).toBe("ValidationError")
      expect((error as ValidationError).entityType).toBe("G133DeviceRetained")
      expect(String(error.cause)).toContain("4194304 bytes (4 MB)")
      for (const id of ids) expect(yield* rawItem("G133DeviceRetained", id)).toBeUndefined()
      // The same items fit as two transactions.
      yield* Transaction.transactWrite(
        ids
          .slice(0, 3)
          .map((id) => g133Entities.DevicesRetained.put({ id, owner: "o", label: big } as any)),
      )
      expect((yield* rawItem("G133DeviceRetained", "big-0")).label.S).toHaveLength(380_000)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a transaction of number-heavy items DynamoDB accepts is not refused", () =>
    Effect.gen(function* () {
      // 25 lists of 12,000 floats: ~2.9 MB as DynamoDB stores numbers.
      const vec = Array.from({ length: 12_000 }, (_, i) => 0.1234567890123 + i * 1e-13)
      yield* Transaction.transactWrite(
        Array.from({ length: 25 }, (_, i) => g133Entities.Vecs.put({ id: `vec-${i}`, vec } as any)),
      )
      const db = yield* g133Client
      expect(
        ((yield* (db.entities.Vecs as any).get({ id: "vec-24" })).vec as Array<number>).length,
      ).toBe(12_000)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- a deleted retain item's key is reused: its history continues (#133) ----

  it.effect("a hard-deleted retain item is created again past its retained history", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const docs = db.entities.DevicesRetained as any
      for (const [id, recreate] of [
        ["rc-put", (doc: any) => docs.put(doc)],
        ["rc-create", (doc: any) => docs.create(doc)],
        ["rc-upsert", (doc: any) => docs.upsert(doc)],
        ["rc-tx", (doc: any) => Transaction.transactWrite([g133Entities.DevicesRetained.put(doc)])],
      ] as const) {
        yield* docs.put({ id, owner: "o", label: "first" })
        yield* docs.update({ id }).set({ label: "second" })
        const firstRaw = yield* rawItem("G133DeviceRetained", id)
        yield* docs.delete({ id })
        // The delete snapshots the final state (v#2) beside the history (v#1).
        expect(yield* snapshotLabel("G133DeviceRetained", id, 1)).toEqual({ S: "first" })
        expect(yield* snapshotLabel("G133DeviceRetained", id, 2)).toEqual({ S: "second" })
        expect(yield* rawItem("G133DeviceRetained", id)).toBeUndefined()
        yield* recreate({ id, owner: "o", label: "again" })
        const raw = yield* rawItem("G133DeviceRetained", id)
        // Past the highest version retained (2), with a new incarnation.
        expect([raw.label, raw.version]).toEqual([{ S: "again" }, { N: "3" }])
        expect(raw.__edd_i__).not.toEqual(firstRaw.__edd_i__)
        expect(yield* snapshotLabel("G133DeviceRetained", id, 2)).toEqual({ S: "second" })
        expect(yield* snapshotLabel("G133DeviceRetained", id, 3)).toEqual({ S: "again" })
        // …and it goes on from there.
        const next = yield* docs.update({ id }).set({ label: "more" })
        expect(next.version).toBe(4)
        // A query of its partition returns the item, never its snapshots.
        expect((yield* docs.primary({ id }).collect()).map((r: any) => r.label)).toEqual(["more"])
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect(
    "a retain hard delete with no condition is written again after a concurrent update",
    () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        for (const [entity, entityType] of [
          ["DevicesRetained", "G133DeviceRetained"],
          ["AccountsRetained", "G133AccountRetained"],
        ] as const) {
          const docs = db.entities[entity] as any
          const id = `hdr-${entity.toLowerCase()}`
          const field = entity === "AccountsRetained" ? "name" : "label"
          yield* docs.put(
            entity === "AccountsRetained"
              ? { id, email: `${id}@x.io`, name: "first" }
              : { id, owner: "o", label: "first" },
          )
          // Another writer updates it between the delete's read and its write.
          g133Inject.before = rawSetAttrs(entityType, id, { [field]: { S: "theirs" } }, true)
          yield* docs.delete({ id })
          expect(yield* rawItem(entityType, id)).toBeUndefined()
          // The final state snapshotted is the one the concurrent write left.
          expect((yield* snapshotOf(entityType, id, 2))?.[field]).toEqual({ S: "theirs" })
          // With a `.condition()`, the race still fails.
          yield* docs.put(
            entity === "AccountsRetained"
              ? { id, email: `${id}@x.io`, name: "again" }
              : { id, owner: "o", label: "again" },
          )
          g133Inject.before = rawSetAttrs(entityType, id, { [field]: { S: "x" } }, true)
          const refused = yield* docs
            .delete({ id })
            .condition({ attributeExists: "id" })
            .asEffect()
            .pipe(Effect.flip)
          expect(refused._tag).toBe("OptimisticLockError")
        }
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect(
    "a delete racing a concurrent delete reports what a delete of a missing item does",
    () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        // Retain only: a delete of a missing item succeeds — so does the race.
        const retained = db.entities.DevicesRetained as any
        yield* retained.put({ id: "rdd1", owner: "o", label: "l" })
        g133Inject.before = rawDelete("G133DeviceRetained", "rdd1")
        yield* retained.delete({ id: "rdd1" })
        yield* retained.delete({ id: "rdd1" })
        // Soft delete: a missing item is ItemNotFound, raced or not.
        const soft = db.entities.SoftDevices as any
        yield* soft.put({ id: "rdd2", owner: "o", label: "l" })
        g133Inject.before = rawDelete("G133SoftDevice", "rdd2")
        expect((yield* soft.delete({ id: "rdd2" }).asEffect().pipe(Effect.flip))._tag).toBe(
          "ItemNotFound",
        )
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect(
    "deleteIfExists racing an update reads the item again; a missing one fails its condition",
    () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        for (const [entity, entityType, doc] of [
          ["DevicesRetained", "G133DeviceRetained", { owner: "o", label: "l" }],
          ["AccountsBare", "G133AccountBare", { email: "die@x.io", name: "l" }],
          ["SoftDevices", "G133SoftDevice", { owner: "o", label: "l" }],
        ] as const) {
          const docs = db.entities[entity] as any
          const id = `die-${entity.toLowerCase()}`
          yield* docs.put({ id, ...doc })
          g133Inject.before = rawSetAttrs(
            entityType,
            id,
            entity === "AccountsBare" ? { name: { S: "x" } } : { label: { S: "x" } },
            entity !== "AccountsBare",
          )
          yield* docs.deleteIfExists({ id })
          expect(yield* rawItem(entityType, id)).toBeUndefined()
          const gone = yield* docs.deleteIfExists({ id }).asEffect().pipe(Effect.flip)
          expect(gone._tag).toBe("ConditionalCheckFailed")
        }
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("delete().returnValues('allOld') returns the deleted item on every path", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      for (const [entity, doc] of [
        ["Authors", { name: "plain" }],
        ["DevicesPlain", { owner: "o", label: "versioned" }],
        ["AccountsBare", { email: "rv@x.io", name: "unique" }],
        ["DevicesRetained", { owner: "o", label: "retained" }],
        ["SoftDevices", { owner: "o", label: "soft" }],
      ] as const) {
        const docs = db.entities[entity] as any
        const id = `rv-${entity.toLowerCase()}`
        yield* docs.put({ id, ...doc })
        const old = yield* docs.delete({ id }).returnValues("allOld")
        expect(old).toMatchObject({ id, ...doc })
        // Nothing to delete: nothing returned. "none" returns nothing either.
        if (entity === "Authors" || entity === "DevicesRetained") {
          expect(yield* docs.delete({ id }).returnValues("allOld")).toBeUndefined()
        }
        yield* docs.put({ id, ...doc })
        expect(yield* docs.delete({ id }).returnValues("none")).toBeUndefined()
        // A mode DeleteItem doesn't support is refused, and nothing is deleted.
        yield* docs.put({ id, ...doc })
        const refused = yield* docs
          .delete({ id })
          .returnValues("allNew")
          .asEffect()
          .pipe(Effect.flip)
        expect(refused._tag).toBe("ValidationError")
        expect(yield* docs.get({ id })).toMatchObject({ id })
        // deleteIfExists with a condition asserts both.
        const unmet = yield* docs
          .deleteIfExists({ id })
          .condition({ id: "someone-else" })
          .asEffect()
          .pipe(Effect.flip)
        expect(unmet._tag).toBe("ConditionalCheckFailed")
        yield* docs.deleteIfExists({ id }).condition({ id })
        const missing = yield* docs
          .deleteIfExists({ id })
          .condition({ id })
          .asEffect()
          .pipe(Effect.flip)
        expect(missing._tag).toBe("ConditionalCheckFailed")
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a stale writer cannot overwrite a hard-deleted and re-created retain item", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      for (const [entity, entityType, extra] of [
        ["DevicesRetained", "G133DeviceRetained", { owner: "o" }],
        ["AccountsRetained", "G133AccountRetained", {}],
      ] as const) {
        const docs = db.entities[entity] as any
        const id = `stale-${entity.toLowerCase()}`
        const doc = (label: string) =>
          entity === "AccountsRetained"
            ? { id, email: `${id}-${label}@x.io`, name: label }
            : { id, ...extra, label }
        const field = entity === "AccountsRetained" ? "name" : "label"
        yield* docs.put(doc("first"))
        yield* docs.update({ id }).set({ [field]: "second" })
        // A writer reads the item at version 2…
        const seen = yield* docs.get({ id })
        expect(seen.version).toBe(2)
        // …it is hard-deleted (one transaction: the delete and the v#2 snapshot)…
        yield* docs.delete({ id })
        expect((yield* snapshotOf(entityType, id, 2))?.[field]).toEqual({ S: "second" })
        // …and created again, past its history, under a new incarnation.
        const again = yield* docs.create(doc("again"))
        expect(again.version).toBe(3)
        // The stale writer's expectedVersion(2) no longer matches anything live.
        const stale = yield* docs
          .update({ id })
          .set({ [field]: "stale" })
          .expectedVersion(2)
          .asEffect()
          .pipe(Effect.flip)
        expect(stale._tag).toBe("OptimisticLockError")
        expect((yield* rawItem(entityType, id))[field]).toEqual({ S: "again" })
        // `purge` still removes all of it — the delete-time snapshot included.
        yield* docs.purge({ id })
        expect(yield* rawItem(entityType, id)).toBeUndefined()
        for (const version of [1, 2, 3]) {
          expect(yield* snapshotOf(entityType, id, version)).toBeUndefined()
        }
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a soft-deleted retain item is created again; restore then refuses the live one", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const docs = db.entities.SoftDevices as any
      yield* docs.put({ id: "rs1", owner: "o", label: "first" })
      yield* docs.update({ id: "rs1" }).set({ label: "second" })
      yield* docs.delete({ id: "rs1" })
      // A soft-deleted item is missing: created again, past its history (v#2).
      const again = yield* docs.create({ id: "rs1", owner: "o", label: "again" })
      expect(again.version).toBe(3)
      expect(yield* snapshotLabel("G133SoftDevice", "rs1", 2)).toEqual({ S: "second" })
      expect(yield* snapshotLabel("G133SoftDevice", "rs1", 3)).toEqual({ S: "again" })
      // Restoring the tombstone over the live item is refused, clearly.
      const refused = yield* docs.restore({ id: "rs1" }).pipe(Effect.flip)
      expect(refused._tag).toBe("ItemNotDeleted")
      expect((yield* rawItem("G133SoftDevice", "rs1")).label).toEqual({ S: "again" })
      // Once the live item is deleted too, restore brings back the latest one.
      yield* TestClock.adjust("1 second")
      yield* docs.delete({ id: "rs1" })
      const restored = yield* docs.restore({ id: "rs1" })
      expect([restored.label, restored.version]).toEqual(["again", 4])
      expect(yield* snapshotLabel("G133SoftDevice", "rs1", 3)).toEqual({ S: "again" })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- several items per partition: each has its own history (#133) ----

  /** Every row of an order's partition, by sort key. */
  const linePartition = (order: string, entityType = "g133line") =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      const { Items } = yield* client.query({
        TableName: g133Tables.record,
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: { "#pk": "pk" },
        ExpressionAttributeValues: { ":pk": { S: `$edd133g#v1#${entityType}#order_${order}` } },
        ConsistentRead: true,
      })
      return (Items ?? []).map((item) => item.sk!.S!)
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)

  it.effect("sibling retain items keep independent version histories", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const lines = db.entities.Lines as any
      const [a, b] = [
        { order: "o1", line: "a" },
        { order: "o1", line: "b" },
      ]
      yield* lines.put({ ...a, label: "a1" })
      // A sibling is created at version 1 — not after the other's history.
      expect((yield* lines.put({ ...b, label: "b1" })).version).toBe(1)
      yield* lines.update(a).set({ label: "a2" })
      expect((yield* lines.update(a).set({ label: "a3" })).version).toBe(3)
      expect((yield* lines.update(b).set({ label: "b2" })).version).toBe(2)
      // Each item's snapshots are its own, under its own key.
      expect(yield* linePartition("o1")).toEqual([
        "$edd133g#v1#g133line#line_a",
        "$edd133g#v1#g133line#line_b",
        "$edd133g#v1#g133line#v#line_a#0000001",
        "$edd133g#v1#g133line#v#line_a#0000002",
        "$edd133g#v1#g133line#v#line_b#0000001",
      ])
      const labels = (rows: ReadonlyArray<any>) => rows.map((r) => [r.version, r.label])
      expect(labels(yield* lines.versions(a).collect())).toEqual([
        [1, "a1"],
        [2, "a2"],
      ])
      expect(labels(yield* lines.versions(b).collect())).toEqual([[1, "b1"]])
      expect((yield* lines.getVersion(b, 1)).label).toBe("b1")
      expect(yield* lines.getVersion(b, 2).pipe(Effect.flip)).toMatchObject({
        _tag: "ItemNotFound",
      })

      // Delete and re-create one: it continues past its own history only.
      yield* lines.delete(a)
      expect((yield* lines.create({ ...a, label: "a4" })).version).toBe(4)
      expect((yield* lines.update(b).set({ label: "b3" })).version).toBe(3)

      // A query of the partition by sort key composite never meets history.
      expect(
        (yield* lines.primary({ order: "o1", line: "a" }).collect()).map((r: any) => r.label),
      ).toEqual(["a4"])

      // Nor does a query of the whole partition, or a scan: history rows keep
      // the entity's type, but they are not its items.
      const live = (rows: ReadonlyArray<any>) => rows.map((r) => r.label).sort()
      expect(live(yield* lines.primary({ order: "o1" }).collect())).toEqual(["a4", "b3"])
      expect(live(yield* lines.scan().collect())).toEqual(["a4", "b3"])
      expect(yield* lines.primary({ order: "o1" }).count()).toBe(2)
      // Purging one leaves its sibling and the sibling's history.
      yield* lines.purge(a)
      expect(yield* linePartition("o1")).toEqual([
        "$edd133g#v1#g133line#line_b",
        "$edd133g#v1#g133line#v#line_b#0000001",
        "$edd133g#v1#g133line#v#line_b#0000002",
      ])
      expect((yield* lines.get(b)).label).toBe("b3")
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("sibling soft-deleted items are read, restored and purged one by one", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const lines = db.entities.SoftLines as any
      const [a, b] = [
        { order: "o2", line: "a" },
        { order: "o2", line: "b" },
      ]
      yield* lines.put({ ...a, label: "a1" })
      yield* lines.put({ ...b, label: "b1" })
      yield* lines.update(b).set({ label: "b2" })
      yield* lines.delete(a)
      yield* TestClock.adjust("1 second")
      yield* lines.delete(b)
      // Tombstones are not items: a query of the partition finds none.
      expect(yield* lines.primary({ order: "o2" }).collect()).toEqual([])
      // Each tombstone is found by its own key — not the partition's latest.
      expect((yield* lines.deleted.get(a)).label).toBe("a1")
      expect((yield* lines.deleted.get(b)).label).toBe("b2")
      // The partition's tombstones are still listed together.
      expect((yield* lines.deleted.list(a).collect()).map((r: any) => r.label).sort()).toEqual([
        "a1",
        "b2",
      ])
      // Restoring one restores that one, at its own next version.
      const restored = yield* lines.restore(a)
      expect([restored.line, restored.label, restored.version]).toEqual(["a", "a1", 2])
      expect(yield* lines.deleted.get(b).pipe(Effect.map((r: any) => r.label))).toBe("b2")
      const restoredB = yield* lines.restore(b)
      expect([restoredB.line, restoredB.label, restoredB.version]).toEqual(["b", "b2", 3])
      // Purge removes one item, its tombstones and history only.
      yield* lines.delete(a)
      yield* lines.purge(a)
      const left = yield* linePartition("o2", "g133softline")
      expect(left.filter((sk) => sk.includes("line_a"))).toEqual([])
      expect(left).toContain("$edd133g#v1#g133softline#line_b")
      expect((yield* lines.versions(b).collect()).map((r: any) => r.version)).toEqual([1, 2])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- history written before items got their own keeps working (#133) ----

  /** A row as an earlier release stored it, written raw. */
  const putRaw = (item: Record<string, unknown>) =>
    Effect.gen(function* () {
      const client = yield* DynamoClient
      yield* client.putItem({ TableName: g133Tables.record, Item: toAttributeMap(item) })
    }).pipe(Effect.provide(ClientLayer), Effect.scoped)
  const lineRow = (
    entityType: string,
    order: string,
    line: string,
    sk: string,
    fields: Record<string, unknown>,
  ) => ({
    pk: `$edd133g#v1#${entityType.toLowerCase()}#order_${order}`,
    sk,
    __edd_e__: entityType,
    order,
    line,
    createdAt: "2024-01-01T00:00:00.000Z",
    updatedAt: "2024-01-01T00:00:00.000Z",
    ...fields,
  })

  it.effect("sibling items read their legacy version history, segmented first", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const lines = db.entities.Lines as any
      const at = (sk: string) => `$edd133g#v1#g133line#${sk}`
      const row = (line: string, sk: string, version: number, label: string) =>
        lineRow("G133Line", "o3", line, at(sk), { version, label })
      // The shared sequence earlier releases wrote: a v1, b v2, a v3.
      yield* putRaw(row("a", "v#0000001", 1, "a1"))
      yield* putRaw(row("b", "v#0000002", 2, "b2"))
      yield* putRaw(row("a", "v#0000003", 3, "a3-legacy"))
      yield* putRaw(row("a", "line_a", 3, "a3"))
      yield* putRaw(row("b", "line_b", 2, "b2"))
      const a = { order: "o3", line: "a" }
      const b = { order: "o3", line: "b" }
      const history = (key: any) =>
        Effect.map(lines.versions(key).collect(), (rows: any) =>
          rows.map((r: any) => [r.version, r.label]),
        )
      expect(yield* history(a)).toEqual([
        [1, "a1"],
        [3, "a3-legacy"],
      ])
      expect(yield* history(b)).toEqual([[2, "b2"]])
      expect((yield* lines.getVersion(a, 1)).label).toBe("a1")
      expect((yield* lines.getVersion(b, 2)).label).toBe("b2")
      // v#0000001 is a's, not b's.
      expect(((yield* Effect.flip(lines.getVersion(b, 1))) as any)._tag).toBe("ItemNotFound")

      // An update snapshots v3 again, under the item's own key: that one wins.
      expect((yield* lines.update(a).set({ label: "a4" })).version).toBe(4)
      expect(yield* history(a)).toEqual([
        [1, "a1"],
        [3, "a3"],
      ])
      expect((yield* lines.getVersion(a, 3)).label).toBe("a3")
      expect((yield* lines.versions(a).reverse().collect()).map((r: any) => r.version)).toEqual([
        3, 1,
      ])

      // b was hard-deleted by an earlier release (no final snapshot): created
      // again, it continues past its legacy history, not at version 1.
      yield* Effect.gen(function* () {
        const client = yield* DynamoClient
        yield* client.deleteItem({
          TableName: g133Tables.record,
          Key: toAttributeMap({ pk: "$edd133g#v1#g133line#order_o3", sk: at("line_b") }),
        })
      }).pipe(Effect.provide(ClientLayer), Effect.scoped)
      expect((yield* lines.create({ ...b, label: "b3" })).version).toBe(3)
      expect(yield* history(b)).toEqual([
        [2, "b2"],
        [3, "b3"],
      ])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("sibling items find and restore their legacy tombstones", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const lines = db.entities.SoftLines as any
      const tomb = (line: string, ts: string, label: string, version: number) =>
        lineRow("G133SoftLine", "o4", line, `$edd133g#v1#g133softline#deleted#${ts}`, {
          version,
          label,
          deletedAt: ts,
        })
      yield* putRaw(tomb("a", "2024-01-02T00:00:00.000Z", "a1", 1))
      yield* putRaw(tomb("b", "2024-01-03T00:00:00.000Z", "b2", 2))
      const a = { order: "o4", line: "a" }
      const b = { order: "o4", line: "b" }
      // Each item finds its own — not the partition's latest.
      expect((yield* lines.deleted.get(a)).label).toBe("a1")
      expect((yield* lines.deleted.get(b)).label).toBe("b2")
      expect((yield* lines.deleted.list(a).collect()).map((r: any) => r.label).sort()).toEqual([
        "a1",
        "b2",
      ])
      // Restored from the legacy tombstone, which is consumed.
      const restored = yield* lines.restore(a)
      expect([restored.line, restored.label, restored.version]).toEqual(["a", "a1", 2])
      expect(((yield* Effect.flip(lines.deleted.get(a))) as any)._tag).toBe("ItemNotFound")
      expect((yield* lines.deleted.get(b)).label).toBe("b2")
      const restoredB = yield* lines.restore(b)
      expect([restoredB.line, restoredB.label, restoredB.version]).toEqual(["b", "b2", 3])
      // Deleted again: the newer, segmented tombstone is the one found.
      yield* TestClock.adjust("1 second")
      yield* lines.update(a).set({ label: "a3" })
      yield* lines.delete(a)
      expect((yield* lines.deleted.get(a)).label).toBe("a3")
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a collection's fetch, paginate, count and select return its members' items", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* (db.entities.OrAs as any).put({ owner: "cq1", aid: "a1", n: 1 })
      yield* (db.entities.OrAs as any).put({ owner: "cq1", aid: "a2", n: 2 })
      yield* (db.entities.OrBs as any).put({ owner: "cq1", bid: "b1", n: 3, secret: "s" })
      const q = () => db.collections.g133Or!({ owner: "cq1" })
      const page = yield* q().fetch()
      expect(page.items.OrAs.map((a: any) => a.aid)).toEqual(["a1", "a2"])
      expect(page.items.OrBs.map((b: any) => b.bid)).toEqual(["b1"])
      const streamed = [...(yield* Stream.runCollect(q().paginate()))] as Array<any>
      expect(streamed.map((s) => [s.member, s.item.aid ?? s.item.bid]).sort()).toEqual([
        ["OrAs", "a1"],
        ["OrAs", "a2"],
        ["OrBs", "b1"],
      ])
      expect(yield* q().count()).toBe(3)
      const selected = yield* q().select(["n"]).collect()
      expect(selected).toEqual({ OrAs: [{ n: 1 }, { n: 2 }], OrBs: [{ n: 3 }] })
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect(
    "a collection filter on a field only some members have reads it as absent on the rest",
    () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const as = db.entities.OrAs as any
        const bs = db.entities.OrBs as any
        yield* as.put({ owner: "part", aid: "a1", n: 1, code: "c1" })
        yield* as.put({ owner: "part", aid: "a2", n: 2, code: "c2" })
        yield* bs.put({ owner: "part", bid: "b1", n: 3, secret: "s" })
        yield* bs.put({ owner: "part", bid: "b2", n: 4, secret: "t" })
        const q = () => db.collections.g133Or!({ owner: "part" })
        const ids = (r: any) => [r.OrAs.map((a: any) => a.aid), r.OrBs.map((b: any) => b.bid)]
        // What main returns: DynamoDB evaluates an absent attribute.
        const either = (t: any, { or, eq }: any) => or(eq(t.secret, "s"), eq(t.code, "c1"))
        expect(ids(yield* q().filter(either).collect())).toEqual([["a1"], ["b1"]])
        expect(yield* q().filter(either).count()).toBe(2)
        const notS = (t: any, { not, eq }: any) => not(eq(t.secret, "s"))
        expect(ids(yield* q().filter(notS).collect())).toEqual([["a1", "a2"], ["b2"]])
        const noSecret = (t: any, { notExists }: any) => notExists(t.secret)
        expect(ids(yield* q().filter(noSecret).collect())).toEqual([["a1", "a2"], []])
        // A field no member has matches nothing.
        expect(ids(yield* q().filter({ nope: "x" }).collect())).toEqual([[], []])
        // A GSI is read only eventually consistently: refused before sending.
        expect((yield* q().consistentRead().collect().pipe(Effect.flip))._tag).toBe(
          "ValidationError",
        )
        const byOwner = as.byOwner({ owner: "part" }).consistentRead()
        expect((yield* byOwner.collect().pipe(Effect.flip))._tag).toBe("ValidationError")
        // A row of no member's in the partition (seen once ownership is
        // ignored) is never streamed as a member's item.
        const a1 = yield* Effect.gen(function* () {
          const client = yield* DynamoClient
          return (yield* client.getItem({
            TableName: g133Tables.record,
            Key: toAttributeMap({
              pk: "$edd133g#v1#g133ora#owner_part",
              sk: "$edd133g#v1#g133ora#aid_a1",
            }),
          })).Item!
        }).pipe(Effect.provide(ClientLayer), Effect.scoped)
        yield* putRaw({
          ...fromAttributeMap(a1),
          pk: "$edd133g#v1#foreign#x",
          sk: "$edd133g#v1#foreign",
          __edd_e__: "Foreign",
        })
        const streamed = [...(yield* Stream.runCollect(q().ignoreOwnership().paginate()))]
        expect(streamed.map((s) => s.member).sort()).toEqual(["OrAs", "OrAs", "OrBs", "OrBs"])
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- an op's own guard is never replaced by .condition() (#133) ----

  it.effect("create of an existing item fails whatever condition is added", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      for (const [entity, entityType, doc] of [
        ["Authors", "G133Author", { name: "first" }],
        ["DevicesPlain", "G133DevicePlain", { owner: "o", label: "first" }],
      ] as const) {
        const docs = db.entities[entity] as any
        const id = `cg-${entity.toLowerCase()}`
        yield* docs.put({ id, ...doc })
        const field = entity === "Authors" ? "name" : "label"
        const again = { id, ...doc, [field]: "second" }
        for (const condition of [{}, { [field]: "first" }]) {
          const bound = yield* docs.create(again).condition(condition).asEffect().pipe(Effect.flip)
          expect(bound._tag).toBe("ConditionalCheckFailed")
          const unbound = yield* (g133Entities[entity] as any)
            .create(again)
            .pipe((g133Entities[entity] as any).condition(condition))
            .asEffect()
            .pipe(Effect.flip)
          expect(unbound._tag).toBe("ConditionalCheckFailed")
          const inTransaction = yield* Transaction.transactWrite([
            (g133Entities[entity] as any)
              .create(again)
              .pipe((g133Entities[entity] as any).condition(condition)),
          ]).pipe(Effect.flip)
          expect(inTransaction._tag).toBe("TransactionCancelled")
          expect((yield* rawItem(entityType, id))[field]).toEqual({ S: "first" })
        }
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("patch of a missing item fails its existence check whatever condition is added", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const authors = db.entities.Authors as any
      for (const condition of [{}, { name: "x" }]) {
        const missing = yield* authors
          .patch({ id: "pg-missing" })
          .set({ name: "n" })
          .condition(condition)
          .asEffect()
          .pipe(Effect.flip)
        expect(missing._tag).toBe("ConditionalCheckFailed")
        expect(yield* rawItem("G133Author", "pg-missing")).toBeUndefined()
      }
      // A patch that reads first (retain) reports the same, writing nothing (#134).
      const retained = db.entities.DevicesRetained as any
      for (const condition of [{}, { label: "x" }]) {
        const missing = yield* retained
          .patch({ id: "pg-missing-r" })
          .set({ label: "n" })
          .condition(condition)
          .asEffect()
          .pipe(Effect.flip)
        expect(missing._tag).toBe("ConditionalCheckFailed")
        expect(yield* rawItem("G133DeviceRetained", "pg-missing-r")).toBeUndefined()
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect(
    "a read-first patch that loses its item to a delete fails, writing nothing (#134)",
    () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const accounts = db.entities.AccountsBare as any
        yield* accounts.put({ id: "pr-race", email: "pr-race@x.io", name: "a" })
        // Deleted between the patch's read and its write.
        g133Inject.before = g133Hook(
          Effect.gen(function* () {
            const client = yield* DynamoClient
            yield* client.deleteItem({
              TableName: g133Tables.record,
              Key: mainKey("G133AccountBare", "pr-race"),
            })
          }).pipe(Effect.provide(ClientLayer)),
        )
        const error = yield* accounts
          .patch({ id: "pr-race" })
          .set({ email: "pr-race-2@x.io" })
          .asEffect()
          .pipe(Effect.flip)
        expect(error._tag).toBe("ConditionalCheckFailed")
        expect(yield* rawItem("G133AccountBare", "pr-race")).toBeUndefined()
        // The new value's sentinel was not written: another item can take it.
        yield* accounts.put({ id: "pr-race-other", email: "pr-race-2@x.io" })
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("deleteIfExists of a missing item fails whatever condition is added", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const devices = db.entities.DevicesPlain as any
      const Devices = g133Entities.DevicesPlain as any
      for (const condition of [{}, { label: "first" }]) {
        const key = { id: "dg-missing" }
        const bound = yield* devices
          .deleteIfExists(key)
          .condition(condition)
          .asEffect()
          .pipe(Effect.flip)
        expect(bound._tag).toBe("ConditionalCheckFailed")
        const unbound = yield* Devices.deleteIfExists(key)
          .pipe(Devices.condition(condition))
          .asEffect()
          .pipe(Effect.flip)
        expect(unbound._tag).toBe("ConditionalCheckFailed")
        const inTransaction = yield* Transaction.transactWrite([
          Devices.deleteIfExists(key).pipe(Devices.condition(condition)),
        ]).pipe(Effect.flip)
        expect(inTransaction._tag).toBe("TransactionCancelled")
      }
      // A later `.condition({})` replaces an earlier one; existence stays.
      yield* devices.put({ id: "dg-present", owner: "o", label: "first" })
      yield* devices
        .deleteIfExists({ id: "dg-present" })
        .condition({ label: "other" })
        .condition({})
      expect(yield* rawItem("G133DevicePlain", "dg-present")).toBeUndefined()
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- an empty part under or / not is refused, never dropped (#133) ----

  it.effect("an empty part under or or not is refused before anything is sent", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const devices = db.entities.DevicesPlain as any
      const refused = [
        (t: any, { or, eq, and }: any) => or(eq(t.label, "first"), and()),
        (_: any, { not, and }: any) => not(and()),
        (_: any, { or }: any) => or(),
        (t: any, { isIn }: any) => isIn(t.label, []),
      ]
      yield* devices.put({ id: "ep-1", owner: "ep", label: "first" })
      for (const cond of refused) {
        const put = yield* devices
          .put({ id: "ep-2", owner: "ep", label: "x" })
          .condition(cond)
          .asEffect()
          .pipe(Effect.flip)
        expect(put._tag).toBe("ValidationError")
        expect(yield* rawItem("G133DevicePlain", "ep-2")).toBeUndefined()
        const del = yield* devices
          .delete({ id: "ep-1" })
          .condition(cond)
          .asEffect()
          .pipe(Effect.flip)
        expect(del._tag).toBe("ValidationError")
        expect(yield* rawItem("G133DevicePlain", "ep-1")).toBeDefined()
        const filtered = yield* devices.scan().filter(cond).collect().pipe(Effect.flip)
        expect(filtered._tag).toBe("ValidationError")
      }
      // Directly under and, or as the whole filter, an empty part is none.
      const kept = (t: any, { and, eq }: any) => and(eq(t.owner, "ep"), and())
      expect((yield* devices.scan().filter(kept).collect()).map((d: any) => d.id)).toEqual(["ep-1"])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- a filter never widens the ownership check (#133) ----

  it.effect("a top-level OR filter returns only the entity's own rows", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const as = db.entities.OrAs as any
      const bs = db.entities.OrBs as any
      yield* as.put({ owner: "or1", aid: "a1", n: 1 })
      yield* bs.put({ owner: "or1", bid: "b1", n: 5, secret: "s" })
      const either = (t: any, { or, eq }: any) => or(eq(t.n, 1), eq(t.n, 5))
      const q = () => as.byOwner({ owner: "or1" }).filter(either)
      expect((yield* q().collect()).map((a: any) => a.aid)).toEqual(["a1"])
      expect(yield* q().count()).toBe(1)
      expect(yield* q().select(["n"]).collect()).toEqual([{ n: 1 }])
      const scanned = yield* as.scan().filter({ owner: "or1" }).filter(either).collect()
      expect(scanned.map((a: any) => a.aid)).toEqual(["a1"])
      expect(yield* as.scan().filter(either).count()).toBeGreaterThanOrEqual(1)
      const leaked = yield* as.scan().filter(either).collect()
      expect(leaked.every((a: any) => a.aid !== undefined)).toBe(true)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- names a placeholder may not hold, and renamed fields (#133) ----

  it.effect("select and filter field names that need placeholders, plain or renamed", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      for (const entity of ["Hyphens", "PlainHyphens"] as const) {
        const docs = db.entities[entity] as any
        const grp = `pl-${entity.toLowerCase()}`
        yield* docs.put({
          grp,
          hid: "1",
          "first-name": "Ann",
          label: "L1",
          addr: { city: "C1", zip: "Z1" },
          tags: ["a", "b"],
        })
        yield* docs.put({
          grp,
          hid: "2",
          "first-name": "Bob",
          label: "L2",
          addr: { city: "C2", zip: "Z2" },
          tags: ["a"],
        })
        // A scan reads the whole shared table, so it gets the names that need
        // placeholders and a nested renamed path, not every query below.
        const scan = () => docs.scan().filter({ grp })
        expect(yield* scan().select(["first-name"]).collect()).toEqual([
          { "first-name": "Ann" },
          { "first-name": "Bob" },
        ])
        expect(
          yield* scan()
            .select((x: any) => [x.addr.city])
            .collect(),
        ).toEqual([{ addr: { city: "C1" } }, { addr: { city: "C2" } }])
        expect(
          (yield* scan().filter({ "first-name": "Ann" }).collect()).map((r: any) => r.hid),
        ).toEqual(["1"])
        const queries = [
          () => docs.primary({ grp }),
          ...(entity === "Hyphens" ? [() => docs.byGrp({ grp })] : []),
        ]
        for (const q of queries) {
          expect(yield* q().select(["first-name"]).collect()).toEqual([
            { "first-name": "Ann" },
            { "first-name": "Bob" },
          ])
          expect(
            yield* q()
              .select((x: any) => [x["first-name"], x.label])
              .collect(),
          ).toEqual([
            { "first-name": "Ann", label: "L1" },
            { "first-name": "Bob", label: "L2" },
          ])
          expect(yield* q().select(["label"]).collect()).toEqual([{ label: "L1" }, { label: "L2" }])
          // A nested path under a renamed field.
          expect(
            yield* q()
              .select((x: any) => [x.addr.city])
              .collect(),
          ).toEqual([{ addr: { city: "C1" } }, { addr: { city: "C2" } }])
          const page = yield* q().select(["label"]).limit(1).fetch()
          expect(page.items).toEqual([{ label: "L1" }])
          const ids = (rows: ReadonlyArray<any>) => rows.map((r) => r.hid)
          expect(ids(yield* q().filter({ label: "L2" }).collect())).toEqual(["2"])
          expect(ids(yield* q().filter({ "first-name": "Ann" }).collect())).toEqual(["1"])
          expect(
            ids(
              yield* q()
                .filter((x: any, { eq }: any) => eq(x.addr.city, "C2"))
                .collect(),
            ),
          ).toEqual(["2"])
          expect(
            ids(
              yield* q()
                .filter((x: any, { gt }: any) => gt(x.label.size(), 1))
                .collect(),
            ),
          ).toEqual(["1", "2"])
          expect(
            ids(
              yield* q()
                .filter((x: any, { gt }: any) => gt(x.tags.size(), 1))
                .collect(),
            ),
          ).toEqual(["1"])
        }
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a collection filter names each member's domain fields, never stored names", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      yield* (db.entities.Hyphens as any).put({
        grp: "dom",
        hid: "1",
        "first-name": "Ann",
        label: "L1",
        addr: { city: "C1", zip: "Z1" },
        tags: [],
      })
      // Its `the-label` is a domain field, holding what Hyphens stores under that name.
      yield* (db.entities.NamedThirds as any).put({
        grp: "dom",
        tid: "t1",
        label: "X",
        "the-label": "L1",
      })
      const named = (db.collections as any).g133Named
      const byLabel = yield* named({ grp: "dom" }).filter({ label: "L1" }).collect()
      expect(byLabel.Hyphens.map((h: any) => h.hid)).toEqual(["1"])
      expect(byLabel.NamedThirds).toEqual([])
      const byStoredName = yield* named({ grp: "dom" }).filter({ "the-label": "L1" }).collect()
      expect(byStoredName.Hyphens).toEqual([])
      expect(byStoredName.NamedThirds.map((t: any) => t.tid)).toEqual(["t1"])
      // A field only one member stores, under a name that is no member's
      // domain field: nothing.
      expect(yield* named({ grp: "dom" }).filter({ nlabel: "L1" }).count()).toBe(0)
      // select reads each member's own domain fields — never a stored name.
      const selected = yield* named({ grp: "dom" }).select(["label", "the-label"]).collect()
      expect(selected.Hyphens).toEqual([{ label: "L1" }])
      expect(selected.NamedThirds).toEqual([{ label: "X", "the-label": "L1" }])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a collection filters and selects each member's renamed fields", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const hyphens = db.entities.Hyphens as any
      const others = db.entities.NamedOthers as any
      yield* hyphens.put({
        grp: "col",
        hid: "1",
        "first-name": "Ann",
        label: "L1",
        addr: { city: "C1", zip: "Z1" },
        tags: [],
      })
      yield* hyphens.put({
        grp: "col",
        hid: "2",
        "first-name": "Bob",
        label: "L2",
        addr: { city: "C2", zip: "Z2" },
        tags: [],
      })
      yield* others.put({ grp: "col", nid: "n1", label: "L1" })
      const named = (db.collections as any).g133Named
      const filtered = yield* named({ grp: "col" }).filter({ label: "L1" }).collect()
      expect(filtered.Hyphens.map((h: any) => h.hid)).toEqual(["1"])
      expect(filtered.NamedOthers.map((o: any) => o.nid)).toEqual(["n1"])
      const selected = yield* named({ grp: "col" }).select(["label"]).collect()
      expect(selected.Hyphens).toEqual([{ label: "L1" }, { label: "L2" }])
      expect(selected.NamedOthers).toEqual([{ label: "L1" }])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- live rows are never mistaken for history (#133) ----

  it.effect("a time-series item whose key holds the event marker is still an item", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const readings = db.entities.Readings as any
      for (const dev of ["d1", "x#e#y"]) {
        for (let day = 1; day <= 3; day++) {
          yield* readings.append({
            ch: "lr",
            dev,
            ts: DateTime.makeUnsafe(Date.UTC(2024, 0, day)),
            v: day,
          })
        }
      }
      const devs = (rows: ReadonlyArray<any>) => rows.map((r) => r.dev).sort()
      expect(devs(yield* readings.primary({ ch: "lr" }).collect())).toEqual(["d1", "x#e#y"])
      expect(yield* readings.primary({ ch: "lr" }).count()).toBe(2)
      expect(devs(yield* readings.scan().filter({ ch: "lr" }).collect())).toEqual(["d1", "x#e#y"])
      expect(yield* readings.history({ ch: "lr", dev: "x#e#y" }).collect()).toHaveLength(3)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("a row the composer can't reproduce is kept unless it is shaped like history", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const seqs = db.entities.Seqs as any
      // As 1.15 wrote it: the key from the encoded string, unpadded.
      yield* putRaw({
        pk: "$edd133g#v1#g133seq#tenant_t1",
        sk: "$edd133g#v1#g133seq#seq_5",
        __edd_e__: "G133Seq",
        tenant: "t1",
        seq: "5",
        label: "from-1.15",
        version: 1,
      })
      yield* seqs.put({ tenant: "t1", seq: 42, label: "new" })
      yield* seqs.update({ tenant: "t1", seq: 42 }).set({ label: "newer" })
      const labels = (rows: ReadonlyArray<any>) => rows.map((r) => r.label).sort()
      expect(labels(yield* seqs.primary({ tenant: "t1" }).collect())).toEqual([
        "from-1.15",
        "newer",
      ])
      expect(yield* seqs.primary({ tenant: "t1" }).count()).toBe(2)
      expect(labels(yield* seqs.scan().filter({ tenant: "t1" }).collect())).toEqual([
        "from-1.15",
        "newer",
      ])
      // A malformed live row (its composite is missing) is read as before:
      // the query fails to decode it rather than hiding it.
      yield* putRaw({
        pk: "$edd133g#v1#g133seq#tenant_t2",
        sk: "$edd133g#v1#g133seq#seq_0000000000000007",
        __edd_e__: "G133Seq",
        tenant: "t2",
        label: "malformed",
        version: 1,
      })
      const failed = yield* Effect.flip(seqs.primary({ tenant: "t2" }).collect())
      expect((failed as any)._tag).toBe("ValidationError")
      // Rows under the snapshot or tombstone prefix that aren't shaped like a
      // snapshot (`…#<7+ digits>`) or a tombstone (`…#<ISO timestamp>`) are kept.
      yield* putRaw({
        pk: "$edd133g#v1#g133seq#tenant_t3",
        sk: "$edd133g#v1#g133seq#v#seq_5",
        __edd_e__: "G133Seq",
        tenant: "t3",
        seq: "5",
        label: "under-v",
        version: 1,
      })
      expect(labels(yield* seqs.primary({ tenant: "t3" }).collect())).toEqual(["under-v"])
      const soft = db.entities.SoftLines as any
      yield* putRaw({
        pk: "$edd133g#v1#g133softline#order_o9",
        sk: "$edd133g#v1#g133softline#deleted#not-a-time",
        __edd_e__: "G133SoftLine",
        order: "o9",
        line: "zz",
        label: "under-deleted",
        version: 1,
        createdAt: "2024-01-01T00:00:00.000Z",
        updatedAt: "2024-01-01T00:00:00.000Z",
      })
      expect(labels(yield* soft.primary({ order: "o9" }).collect())).toEqual(["under-deleted"])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("select on items sharing a partition reads the composites it judges rows by", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const lines = db.entities.Lines as any
      for (const line of ["a", "b"]) {
        yield* lines.put({ order: "o5", line, label: `${line}1` })
        yield* lines.update({ order: "o5", line }).set({ label: `${line}2` })
      }
      const labels = (rows: ReadonlyArray<any>) => rows.map((r) => r.label).sort()
      expect(labels(yield* lines.primary({ order: "o5" }).select(["label"]).collect())).toEqual([
        "a2",
        "b2",
      ])
      expect(
        labels(yield* lines.scan().filter({ order: "o5" }).select(["label"]).collect()),
      ).toEqual(["a2", "b2"])
      const streamed = yield* Stream.runCollect(
        lines.primary({ order: "o5" }).select(["label"]).limit(5).paginate(),
      )
      expect(labels([...(streamed as any)])).toEqual(["a2", "b2"])

      // A live time-series item whose composite holds the event marker is told
      // from an event only by composing its key — which needs its composites.
      const readings = db.entities.Readings as any
      for (const dev of ["s1", "x#e#z"]) {
        for (let day = 1; day <= 2; day++) {
          yield* readings.append({
            ch: "sel",
            dev,
            ts: DateTime.makeUnsafe(Date.UTC(2024, 0, day)),
            v: day,
          })
        }
      }
      const values = (rows: ReadonlyArray<any>) => rows.map((r) => r.v)
      expect(values(yield* readings.primary({ ch: "sel" }).select(["v"]).collect())).toEqual([2, 2])
      expect(values(yield* readings.scan().filter({ ch: "sel" }).select(["v"]).collect())).toEqual([
        2, 2,
      ])
      const paged = yield* Stream.runCollect(
        readings.primary({ ch: "sel" }).select(["v"]).limit(5).paginate(),
      )
      expect(values([...(paged as any)])).toEqual([2, 2])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("an item in a sub-collection named like a history marker is still an item", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      for (const entity of ["SubDeleted", "SubVersioned"] as const) {
        const subs = db.entities[entity] as any
        yield* subs.put({ id: "sc1", label: "one" })
        expect((yield* subs.primary({ id: "sc1" }).collect()).map((r: any) => r.label)).toEqual([
          "one",
        ])
        expect((yield* subs.scan().collect()).map((r: any) => r.label)).toEqual(["one"])
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect(
    "a collection on the primary key returns items, and purge keeps other types' rows",
    () =>
      Effect.gen(function* () {
        const db = yield* g133Client
        const orders = db.entities.POrders as any
        const lines = db.entities.PLines as any
        yield* orders.put({ orderId: "po1", label: "v1" })
        yield* orders.update({ orderId: "po1" }).set({ label: "v2" })
        yield* orders.update({ orderId: "po1" }).set({ label: "v3" })
        yield* lines.put({ orderId: "po1", lineId: "a", qty: 1 })
        yield* lines.put({ orderId: "po1", lineId: "b", qty: 2 })
        const all = yield* (db.collections as any).g133OrderAll({ orderId: "po1" }).collect()
        expect(all.POrders.map((o: any) => o.label)).toEqual(["v3"])
        expect(all.PLines).toHaveLength(2)
        // Counting judges each row by its member — which reads its entity type.
        expect(yield* (db.collections as any).g133OrderAll({ orderId: "po1" }).count()).toBe(3)
        // purge removes the order, its history and its tombstones — not the lines.
        yield* orders.delete({ orderId: "po1" })
        yield* orders.purge({ orderId: "po1" })
        expect(yield* orders.versions({ orderId: "po1" }).collect()).toEqual([])
        expect(
          (yield* lines.primary({ orderId: "po1" }).collect()).map((l: any) => l.lineId),
        ).toEqual(["a", "b"])
      }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- upsert validates the whole input, and stores defaults only on create ----

  it.effect("upsert of an existing unique item still requires every required field", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const coded = db.entities.Coded as any
      yield* coded.put({ id: "uv1", name: "a", code: "uv1" })
      const missing = yield* coded.upsert({ id: "uv1", code: "uv1b" }).asEffect().pipe(Effect.flip)
      expect(missing._tag).toBe("ValidationError")
      expect(missing.operation).toMatch(/^upsert/)
      const raw = yield* rawItem("G133Coded", "uv1")
      expect([raw.name, raw.code]).toEqual([{ S: "a" }, { S: "uv1" }])
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  it.effect("upsert stores an omitted default on create and keeps the stored value on update", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      const defaults = db.entities.Defaults as any
      const coded = db.entities.Coded as any
      // Created: the default is stored and indexed.
      expect((yield* defaults.upsert({ id: "ud1", name: "n" })).tier).toBe("basic")
      expect((yield* rawItem("G133Defaults", "ud1")).tier).toEqual({ S: "basic" })
      // Existing: an upsert that omits it keeps what is stored — unique or not.
      yield* defaults.put({ id: "ud2", name: "n", tier: "gold" })
      const kept = yield* defaults.upsert({ id: "ud2", name: "m" })
      expect([kept.tier, kept.name]).toEqual(["gold", "m"])
      expect((yield* defaults.byTier({ tier: "gold" }).collect()).map((d: any) => d.id)).toContain(
        "ud2",
      )
      yield* coded.put({ id: "ud3", name: "n", code: "ud3" })
      const codedKept = yield* coded.upsert({ id: "ud3", name: "m" })
      expect([codedKept.code, codedKept.name]).toEqual(["ud3", "m"])
      expect(yield* sentinelExistsFor("G133Coded", "code", "ud3")).toBe(true)
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )

  // ---- concurrent puts of a missing versioned item: the last writer wins ----

  it.effect("a put that loses the race to create an item replaces it", () =>
    Effect.gen(function* () {
      const db = yield* g133Client
      for (const [entity, entityType] of [
        ["DevicesPlain", "G133DevicePlain"],
        ["DevicesRetained", "G133DeviceRetained"],
      ] as const) {
        const docs = db.entities[entity] as any
        const id = `lww-${entity.toLowerCase()}`
        // Another writer creates the item between our read and our write.
        g133Inject.before = g133Hook(
          Effect.asVoid(
            (g133Entities[entity] as any)
              .put({ id, owner: "o", label: "theirs" })
              .asEffect()
              .pipe(Effect.provide(g133Layer(g133Tables.record))),
          ),
        )
        const mine = yield* docs.put({ id, owner: "o", label: "mine" })
        expect([mine.label, mine.version]).toEqual(["mine", 2])
        expect((yield* rawItem(entityType, id)).label).toEqual({ S: "mine" })
      }
    }).pipe(Effect.provide(g133RaceLayer), g133Closed),
  )
})
