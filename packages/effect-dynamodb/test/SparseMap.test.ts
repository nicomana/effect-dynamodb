/**
 * Unit tests for the SparseMap storage primitive.
 *
 * Covers:
 *   - Configuration parsing (configure with storedAs: DynamoModel.SparseMap())
 *   - Entity.make() validation (EDD-9020..9023)
 *   - Marshaller encode/decode round-trip
 *   - Record-style writes (whole-bucket replace)
 *   - Path-style writes via .pathSet / .pathAdd through .entry(key)
 *   - removeEntries explicit REMOVE
 *   - clearMap composition with other combinators
 *   - null in input is NOT REMOVE
 *   - Conditional ops via attribute_exists on entry path
 *   - Lifecycle: snapshots, soft-delete, time-series event item stripping
 */

import { describe, expect, it } from "@effect/vitest"
import * as DynamoModel from "@effect-dynamodb/schema/DynamoModel.js"
import * as DynamoSchema from "@effect-dynamodb/schema/DynamoSchema.js"
import { DynamoError, ValidationError } from "@effect-dynamodb/schema/Errors.js"
import { DateTime, Effect, Layer, Schema } from "effect"
import { vi } from "vitest"
import { DynamoClient } from "../src/DynamoClient.js"
import * as Entity from "../src/Entity.js"
import { decodeSparseFields, encodeSparseFields } from "../src/Marshaller.js"
import * as Table from "../src/Table.js"
import { mockDynamoClient } from "./helpers/MockDynamoClient.js"

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const AppSchema = DynamoSchema.make({ name: "sparseapp", version: 1 })

class Page extends Schema.Class<Page>("Page")({
  pageId: Schema.String,
  status: Schema.optional(Schema.String),
  metrics: Schema.Record(
    Schema.String,
    Schema.Struct({ views: Schema.Number, clicks: Schema.Number }),
  ),
  totals: Schema.Record(Schema.String, Schema.Number),
}) {}

const PageModel = DynamoModel.configure(Page, {
  metrics: { storedAs: DynamoModel.SparseMap() },
  totals: { storedAs: DynamoModel.SparseMap() },
})

// ---------------------------------------------------------------------------
// 1. Configuration parsing
// ---------------------------------------------------------------------------

describe("SparseMap — DynamoModel.SparseMap() helper", () => {
  it("SparseMap() returns a tagged config object", () => {
    const cfg = DynamoModel.SparseMap()
    expect(DynamoModel.isSparseMapConfig(cfg)).toBe(true)
    expect(cfg.prefix).toBeUndefined()
  })

  it("SparseMap({ prefix }) carries the prefix through", () => {
    const cfg = DynamoModel.SparseMap({ prefix: "foo" })
    expect(DynamoModel.isSparseMapConfig(cfg)).toBe(true)
    expect(cfg.prefix).toBe("foo")
  })

  it("isSparseMapConfig returns false for non-SparseMap values", () => {
    expect(DynamoModel.isSparseMapConfig(undefined)).toBe(false)
    expect(DynamoModel.isSparseMapConfig(null)).toBe(false)
    expect(DynamoModel.isSparseMapConfig("sparse")).toBe(false)
    expect(DynamoModel.isSparseMapConfig({ prefix: "x" })).toBe(false)
    expect(DynamoModel.isSparseMapConfig({})).toBe(false)
  })

  it("the legacy 'sparse' string literal is no longer recognised", () => {
    // Pre-1.6 used the string literal "sparse"; under v2 the union
    // `Schema.Schema<A> | SparseMapConfig` excludes the string literal at the
    // type level. At runtime, isSparseMapConfig returns false for the string,
    // so configure() falls into the schema branch and throws when it tries
    // to read the (non-existent) DynamoEncoding annotation. The exact error
    // message isn't load-bearing; the important contract is that the magic
    // string no longer activates the sparse-map path.
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      metrics: Schema.Record(Schema.String, Schema.Number),
    }) {}
    expect(() =>
      DynamoModel.configure(P, {
        // The magic string where a storage schema is expected — the cast is the
        // point of the test: `"sparse"` must no longer activate the sparse-map path.
        metrics: {
          storedAs: "sparse" as unknown as Schema.Schema<{ readonly [x: string]: number }>,
        },
      }),
    ).toThrow()
    // And the Schema's getSparseFields would NOT report metrics as sparse if
    // configure had recognised it (it didn't — it threw above), proving the
    // magic string is dead.
  })
})

describe("SparseMap — configuration", () => {
  it("DynamoModel.configure attaches sparse config with default prefix", () => {
    const sparse = DynamoModel.getSparseFields(PageModel as unknown as Schema.Top)
    expect(Object.keys(sparse).sort()).toEqual(["metrics", "totals"])
    expect(sparse.metrics).toEqual({ prefix: "metrics" })
    expect(sparse.totals).toEqual({ prefix: "totals" })
  })

  it("respects an explicit prefix override", () => {
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      metrics: Schema.Record(Schema.String, Schema.Number),
    }) {}
    const M = DynamoModel.configure(P, {
      metrics: { storedAs: DynamoModel.SparseMap({ prefix: "m" }) },
    })
    const sparse = DynamoModel.getSparseFields(M as unknown as Schema.Top)
    expect(sparse.metrics).toEqual({ prefix: "m" })
  })

  it("rejects an empty prefix override at configure time", () => {
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      metrics: Schema.Record(Schema.String, Schema.Number),
    }) {}
    expect(() =>
      DynamoModel.configure(P, {
        metrics: { storedAs: DynamoModel.SparseMap({ prefix: "" }) },
      }),
    ).toThrow(/non-empty/)
  })

  it("rejects a prefix containing '#' at configure time", () => {
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      metrics: Schema.Record(Schema.String, Schema.Number),
    }) {}
    expect(() =>
      DynamoModel.configure(P, {
        metrics: { storedAs: DynamoModel.SparseMap({ prefix: "bad#prefix" }) },
      }),
    ).toThrow(/must not contain '#'/)
  })
})

// ---------------------------------------------------------------------------
// 2. Entity.make() validation
// ---------------------------------------------------------------------------

describe("SparseMap — Entity.make() validation", () => {
  it("EDD-9020: storedAs: DynamoModel.SparseMap() rejected on non-Record fields", () => {
    class Bad extends Schema.Class<Bad>("Bad")({
      id: Schema.String,
      notARecord: Schema.String,
    }) {}
    const Configured = DynamoModel.configure(Bad, {
      // Cast — at configure time we accept the SparseMap config broadly;
      // EDD-9020 fires at Entity.make() time on schema-shape mismatch.
      notARecord: { storedAs: DynamoModel.SparseMap() } as any,
    })
    expect(() =>
      Entity.make({
        model: Configured,
        entityType: "Bad",
        primaryKey: {
          pk: { field: "pk", composite: ["id"] },
          sk: { field: "sk", composite: [] },
        },
      }),
    ).toThrow(/EDD-9020/)
  })

  it("EDD-9021: nested sparse Record (Record-of-Record) is rejected", () => {
    class Nested extends Schema.Class<Nested>("Nested")({
      id: Schema.String,
      doubleNested: Schema.Record(Schema.String, Schema.Record(Schema.String, Schema.Number)),
    }) {}
    const Configured = DynamoModel.configure(Nested, {
      doubleNested: { storedAs: DynamoModel.SparseMap() },
    })
    expect(() =>
      Entity.make({
        model: Configured,
        entityType: "Nested",
        primaryKey: {
          pk: { field: "pk", composite: ["id"] },
          sk: { field: "sk", composite: [] },
        },
      }),
    ).toThrow(/EDD-9021/)
  })

  it("EDD-9022: sparse field cannot be a primary-key composite", () => {
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      tags: Schema.Record(Schema.String, Schema.String),
    }) {}
    const Configured = DynamoModel.configure(P, {
      tags: { storedAs: DynamoModel.SparseMap() },
    })
    expect(() =>
      Entity.make({
        model: Configured,
        entityType: "P",
        primaryKey: {
          pk: { field: "pk", composite: ["tags"] },
          sk: { field: "sk", composite: [] },
        },
      }),
    ).toThrow(/EDD-9022/)
  })

  it("EDD-9022: sparse field cannot be a GSI composite", () => {
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      tags: Schema.Record(Schema.String, Schema.String),
    }) {}
    const Configured = DynamoModel.configure(P, {
      tags: { storedAs: DynamoModel.SparseMap() },
    })
    expect(() =>
      Entity.make({
        model: Configured,
        entityType: "P",
        primaryKey: {
          pk: { field: "pk", composite: ["id"] },
          sk: { field: "sk", composite: [] },
        },
        indexes: {
          byTag: {
            name: "gsi1",
            pk: { field: "gsi1pk", composite: ["tags"] },
            sk: { field: "gsi1sk", composite: [] },
          },
        },
      }),
    ).toThrow(/EDD-9022/)
  })

  it("EDD-9022: sparse field cannot be in a unique constraint", () => {
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      flags: Schema.Record(Schema.String, Schema.Boolean),
    }) {}
    const Configured = DynamoModel.configure(P, {
      flags: { storedAs: DynamoModel.SparseMap() },
    })
    expect(() =>
      Entity.make({
        model: Configured,
        entityType: "P",
        primaryKey: {
          pk: { field: "pk", composite: ["id"] },
          sk: { field: "sk", composite: [] },
        },
        unique: { byFlags: ["flags"] },
      }),
    ).toThrow(/EDD-9022/)
  })

  it("EDD-9023: distinct prefixes required", () => {
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      a: Schema.Record(Schema.String, Schema.Number),
      b: Schema.Record(Schema.String, Schema.Number),
    }) {}
    const Configured = DynamoModel.configure(P, {
      a: { storedAs: DynamoModel.SparseMap({ prefix: "x" }) },
      b: { storedAs: DynamoModel.SparseMap({ prefix: "x" }) },
    })
    expect(() =>
      Entity.make({
        model: Configured,
        entityType: "P",
        primaryKey: {
          pk: { field: "pk", composite: ["id"] },
          sk: { field: "sk", composite: [] },
        },
      }),
    ).toThrow(/EDD-9023/)
  })

  it("EDD-9023: prefix cannot collide with a non-sparse model field", () => {
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      name: Schema.String,
      counts: Schema.Record(Schema.String, Schema.Number),
    }) {}
    const Configured = DynamoModel.configure(P, {
      counts: { storedAs: DynamoModel.SparseMap({ prefix: "name" }) },
    })
    expect(() =>
      Entity.make({
        model: Configured,
        entityType: "P",
        primaryKey: {
          pk: { field: "pk", composite: ["id"] },
          sk: { field: "sk", composite: [] },
        },
      }),
    ).toThrow(/EDD-9023/)
  })

  it("valid sparse entity makes successfully", () => {
    expect(() =>
      Entity.make({
        model: PageModel,
        entityType: "Page",
        primaryKey: {
          pk: { field: "pk", composite: ["pageId"] },
          sk: { field: "sk", composite: [] },
        },
      }),
    ).not.toThrow()
  })
})

// ---------------------------------------------------------------------------
// 3. Marshaller encode/decode round-trip
// ---------------------------------------------------------------------------

describe("SparseMap — encode/decode round-trip", () => {
  const sparse = { metrics: { prefix: "metrics" }, totals: { prefix: "totals" } }

  it("encodes a Record into per-entry top-level attrs", () => {
    const item: Record<string, unknown> = {
      pageId: "p1",
      metrics: { "2026-01": { views: 5, clicks: 2 } },
      totals: { "2026-01": 7 },
    }
    encodeSparseFields(item, sparse)
    expect(item.metrics).toBeUndefined()
    expect(item.totals).toBeUndefined()
    expect(item["metrics#2026-01"]).toEqual({ views: 5, clicks: 2 })
    expect(item["totals#2026-01"]).toEqual(7)
    expect(item.pageId).toEqual("p1")
  })

  it("encodes an empty Record as no attributes (field is dropped)", () => {
    const item: Record<string, unknown> = { pageId: "p1", metrics: {}, totals: {} }
    encodeSparseFields(item, sparse)
    expect(item.metrics).toBeUndefined()
    expect(item.totals).toBeUndefined()
    expect(Object.keys(item)).toEqual(["pageId"])
  })

  it("rejects a key containing '#' on encode", () => {
    const item: Record<string, unknown> = {
      pageId: "p1",
      metrics: { "bad#key": { views: 1, clicks: 0 } },
    }
    expect(() => encodeSparseFields(item, sparse)).toThrow(/must not contain '#'/)
  })

  it("rejects an empty key on encode", () => {
    const item: Record<string, unknown> = {
      pageId: "p1",
      totals: { "": 1 },
    }
    expect(() => encodeSparseFields(item, sparse)).toThrow(/non-empty/)
  })

  it("decodes flattened attrs back into a Record", () => {
    const raw: Record<string, unknown> = {
      pageId: "p1",
      "metrics#2026-01": { views: 5, clicks: 2 },
      "metrics#2026-02": { views: 12, clicks: 4 },
      "totals#2026-01": 7,
    }
    decodeSparseFields(raw, sparse)
    expect(raw.metrics).toEqual({
      "2026-01": { views: 5, clicks: 2 },
      "2026-02": { views: 12, clicks: 4 },
    })
    expect(raw.totals).toEqual({ "2026-01": 7 })
    expect(raw["metrics#2026-01"]).toBeUndefined()
    expect(raw.pageId).toEqual("p1")
  })

  it("decodes an item with no sparse attrs into empty Records", () => {
    const raw: Record<string, unknown> = { pageId: "p1" }
    decodeSparseFields(raw, sparse)
    expect(raw.metrics).toEqual({})
    expect(raw.totals).toEqual({})
  })

  it("round-trips encode → decode", () => {
    const original = {
      pageId: "p1",
      metrics: { "2026-01": { views: 5, clicks: 2 }, "2026-12": { views: 0, clicks: 0 } },
      totals: { "2026-01": 7, "2026-12": 3 },
    }
    const item: Record<string, unknown> = {
      ...original,
      metrics: { ...original.metrics },
      totals: { ...original.totals },
    }
    encodeSparseFields(item, sparse)
    decodeSparseFields(item, sparse)
    expect(item).toEqual(original)
  })

  it("longer prefix wins over shorter when prefixes happen to overlap", () => {
    const fields = { metric: { prefix: "metric" }, metrics: { prefix: "metrics" } }
    const raw: Record<string, unknown> = {
      "metrics#x": 1,
      "metric#y": 2,
    }
    decodeSparseFields(raw, fields)
    expect(raw.metrics).toEqual({ x: 1 })
    expect(raw.metric).toEqual({ y: 2 })
  })
})

// ---------------------------------------------------------------------------
// 4. Mock DynamoClient + Entity execution tests
// ---------------------------------------------------------------------------

const mockPutItem = vi.fn()
const mockUpdateItem = vi.fn()
const mockGetItem = vi.fn()
const mockDeleteItem = vi.fn()
const mockTransactWriteItems = vi.fn()

const mockService = mockDynamoClient({
  putItem: (input) =>
    Effect.tryPromise({
      try: () => mockPutItem(input),
      catch: (e) => new DynamoError({ operation: "PutItem", cause: e }),
    }),
  getItem: (input) =>
    Effect.tryPromise({
      try: async () => (await mockGetItem(input)) ?? {},
      catch: (e) => new DynamoError({ operation: "GetItem", cause: e }),
    }),
  updateItem: (input) =>
    Effect.tryPromise({
      try: () => mockUpdateItem(input),
      catch: (e) => new DynamoError({ operation: "UpdateItem", cause: e }),
    }),
  deleteItem: (input) =>
    Effect.tryPromise({
      try: () => mockDeleteItem(input),
      catch: (e) => new DynamoError({ operation: "DeleteItem", cause: e }),
    }),
  transactWriteItems: (input) =>
    Effect.tryPromise({
      try: () => mockTransactWriteItems(input),
      catch: (e) => new DynamoError({ operation: "TransactWriteItems", cause: e }),
    }),
})

const TestDynamoClient = Layer.succeed(DynamoClient, mockService)

const Pages = Entity.make({
  model: PageModel,
  entityType: "Page",
  primaryKey: {
    pk: { field: "pk", composite: ["pageId"] },
    sk: { field: "sk", composite: [] },
  },
})
/** Same model, versioned — the clearMap read must CAS on the version it found. */
const VersionedPages = Entity.make({
  model: PageModel,
  entityType: "VersionedPage",
  primaryKey: {
    pk: { field: "pk", composite: ["pageId"] },
    sk: { field: "sk", composite: [] },
  },
  versioned: true,
})
const PagesTable = Table.make({ schema: AppSchema, entities: { Pages, VersionedPages } })
;(Pages as any)._configure(AppSchema, PagesTable.Tag)
;(VersionedPages as any)._configure(AppSchema, PagesTable.Tag)
const PagesTableLayer = PagesTable.layer({ name: "test-pages" })
const TestLayers = Layer.mergeAll(TestDynamoClient, PagesTableLayer)

// ---------------------------------------------------------------------------
// 5. Put — domain Record flattens to per-entry attrs
// ---------------------------------------------------------------------------

describe("SparseMap — put", () => {
  it("flattens domain Record into per-entry top-level attributes", async () => {
    mockPutItem.mockReset()
    mockPutItem.mockImplementation(() => Promise.resolve({}))
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Pages.put({
          pageId: "p1",
          metrics: { "2026-01": { views: 5, clicks: 2 } },
          totals: { "2026-01": 1 },
        }).asEffect()
      }).pipe(Effect.provide(TestLayers, { local: true })),
    )
    expect(mockPutItem).toHaveBeenCalledOnce()
    const call = mockPutItem.mock.calls[0]![0]
    const attrs = call.Item
    expect(attrs["metrics#2026-01"]).toBeDefined()
    expect(attrs["totals#2026-01"]).toBeDefined()
    expect(attrs.metrics).toBeUndefined()
    expect(attrs.totals).toBeUndefined()
  })

  it("rejects a key containing '#' with a tagged ValidationError", async () => {
    mockPutItem.mockReset()
    mockPutItem.mockImplementation(() => Promise.resolve({}))
    const result = await Effect.runPromise(
      Pages.put({
        pageId: "p1",
        metrics: { "bad#key": { views: 1, clicks: 1 } },
        totals: {},
      })
        .asEffect()
        .pipe(Effect.flip, Effect.provide(TestLayers, { local: true })),
    )
    expect(result).toBeInstanceOf(ValidationError)
    expect((result as ValidationError).operation).toMatch(/sparse/)
  })
})

// ---------------------------------------------------------------------------
// 6. Update — record-style decomposes per-bucket
// ---------------------------------------------------------------------------

describe("SparseMap — update record-style", () => {
  it(".set({ metrics: { ... } }) emits one SET per bucket", async () => {
    mockUpdateItem.mockReset()
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({ Attributes: { pk: { S: "x" }, sk: { S: "x" }, pageId: { S: "p1" } } }),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Pages.update({ pageId: "p1" })
          .pipe(
            Entity.set({
              metrics: {
                "2026-01": { views: 5, clicks: 2 },
                "2026-02": { views: 10, clicks: 3 },
              },
            }),
          )
          .pipe(Entity.asRecord)
      }).pipe(Effect.provide(TestLayers, { local: true })),
    )
    const call = mockUpdateItem.mock.calls[0]![0]
    // Two SET clauses, each pointing at a flattened attr name
    const expr: string = call.UpdateExpression
    const names: Record<string, string> = call.ExpressionAttributeNames
    expect(expr).toMatch(/^SET /)
    const flattenedNames = Object.values(names).filter((n) => n.startsWith("metrics#"))
    expect(flattenedNames.sort()).toEqual(["metrics#2026-01", "metrics#2026-02"])
  })

  it(".set with scalar bucket Record emits one SET per bucket", async () => {
    mockUpdateItem.mockReset()
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({ Attributes: { pk: { S: "x" }, sk: { S: "x" }, pageId: { S: "p1" } } }),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Pages.update({ pageId: "p1" })
          .pipe(Entity.set({ totals: { "2026-01": 5, "2026-02": 7 } }))
          .pipe(Entity.asRecord)
      }).pipe(Effect.provide(TestLayers, { local: true })),
    )
    const names = mockUpdateItem.mock.calls[0]![0].ExpressionAttributeNames as Record<
      string,
      string
    >
    const flattenedNames = Object.values(names).filter((n) => n.startsWith("totals#"))
    expect(flattenedNames.sort()).toEqual(["totals#2026-01", "totals#2026-02"])
  })
})

// ---------------------------------------------------------------------------
// 7. Path-style writes via .entry(key)
// ---------------------------------------------------------------------------

describe("SparseMap — path-style writes", () => {
  it("pathAdd on scalar bucket via BoundUpdate compiles to ADD <prefix>#<key> :v (counter case)", async () => {
    mockUpdateItem.mockReset()
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({ Attributes: { pk: { S: "x" }, sk: { S: "x" }, pageId: { S: "p1" } } }),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { Pages },
          tables: { PagesTable },
        })
        yield* db.entities.Pages.update({ pageId: "p1" }).pathAdd({
          segments: [`totals#2026-01`],
          value: 1,
        })
      }).pipe(Effect.provide(TestLayers, { local: true })),
    )
    const call = mockUpdateItem.mock.calls[0]![0]
    expect(call.UpdateExpression).toMatch(/ADD /)
    // The pathAdd path expression resolves to a single ExpressionAttributeNames
    // entry whose value is the literal flattened attr name.
    const namesValues = Object.values(call.ExpressionAttributeNames as Record<string, string>)
    expect(namesValues).toContain("totals#2026-01")
  })
})

// ---------------------------------------------------------------------------
// 8. removeEntries
// ---------------------------------------------------------------------------

describe("SparseMap — removeEntries", () => {
  it("compiles to REMOVE <prefix>#k1, <prefix>#k2", async () => {
    mockUpdateItem.mockReset()
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({ Attributes: { pk: { S: "x" }, sk: { S: "x" }, pageId: { S: "p1" } } }),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Pages.update({ pageId: "p1" })
          .pipe(Entity.removeEntries("metrics", ["2026-01", "2026-02"]))
          .pipe(Entity.asRecord)
      }).pipe(Effect.provide(TestLayers, { local: true })),
    )
    const call = mockUpdateItem.mock.calls[0]![0]
    const expr: string = call.UpdateExpression
    expect(expr).toMatch(/REMOVE /)
    const removedNames = Object.values(
      call.ExpressionAttributeNames as Record<string, string>,
    ).filter((n) => n.startsWith("metrics#"))
    expect(removedNames.sort()).toEqual(["metrics#2026-01", "metrics#2026-02"])
  })

  it("rejects keys containing '#' with a tagged ValidationError", async () => {
    mockUpdateItem.mockReset()
    mockUpdateItem.mockImplementation(() => Promise.resolve({ Attributes: {} }))
    const result = await Effect.runPromise(
      Pages.update({ pageId: "p1" })
        .pipe(Entity.removeEntries("metrics", ["bad#key"]))
        .pipe(Entity.asRecord, Effect.flip, Effect.provide(TestLayers, { local: true })),
    )
    expect(result).toBeInstanceOf(ValidationError)
  })

  it("rejects calling removeEntries on a non-sparse field", async () => {
    mockUpdateItem.mockReset()
    mockUpdateItem.mockImplementation(() => Promise.resolve({ Attributes: {} }))
    const result = await Effect.runPromise(
      Pages.update({ pageId: "p1" })
        .pipe(Entity.removeEntries("status", ["x"]))
        .pipe(Entity.asRecord, Effect.flip, Effect.provide(TestLayers, { local: true })),
    )
    expect(result).toBeInstanceOf(ValidationError)
  })
})

// ---------------------------------------------------------------------------
// 9. clearMap composes with other combinators
// ---------------------------------------------------------------------------

describe("SparseMap — clearMap", () => {
  it("issues a Get + Update; folds REMOVE into the same final UpdateItem", async () => {
    mockGetItem.mockReset()
    mockUpdateItem.mockReset()
    // The Get returns existing flattened attrs to clear.
    mockGetItem.mockImplementation(() =>
      Promise.resolve({
        Item: {
          pk: { S: "x" },
          sk: { S: "x" },
          pageId: { S: "p1" },
          "metrics#2026-01": { M: { views: { N: "5" }, clicks: { N: "2" } } },
          "metrics#2026-02": { M: { views: { N: "1" }, clicks: { N: "0" } } },
        },
      }),
    )
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({
        Attributes: {
          pk: { S: "x" },
          sk: { S: "x" },
          pageId: { S: "p1" },
          status: { S: "reset" },
        },
      }),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Pages.update({ pageId: "p1" })
          .pipe(Entity.clearMap("metrics"))
          .pipe(Entity.set({ status: "reset" }))
          .pipe(Entity.asRecord)
      }).pipe(Effect.provide(TestLayers, { local: true })),
    )
    expect(mockGetItem).toHaveBeenCalledOnce()
    expect(mockGetItem.mock.calls[0]![0].ConsistentRead).toBe(true)
    expect(mockUpdateItem).toHaveBeenCalledOnce()
    const call = mockUpdateItem.mock.calls[0]![0]
    const expr: string = call.UpdateExpression
    // Both SET (status) and REMOVE (cleared metrics) clauses must appear in
    // the same final UpdateItem.
    expect(expr).toMatch(/SET /)
    expect(expr).toMatch(/REMOVE /)
    const removedNames = Object.values(
      call.ExpressionAttributeNames as Record<string, string>,
    ).filter((n) => n.startsWith("metrics#"))
    expect(removedNames.sort()).toEqual(["metrics#2026-01", "metrics#2026-02"])
  })

  const versionedRead = () =>
    Promise.resolve({
      Item: {
        pk: { S: "x" },
        sk: { S: "x" },
        pageId: { S: "p1" },
        version: { N: "3" },
        "metrics#2026-01": { M: { views: { N: "5" }, clicks: { N: "2" } } },
      },
    })

  it("on a versioned entity, conditions the Update on the version the read found", async () => {
    mockGetItem.mockReset()
    mockUpdateItem.mockReset()
    mockGetItem.mockImplementation(versionedRead)
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({
        Attributes: { pk: { S: "x" }, sk: { S: "x" }, pageId: { S: "p1" }, version: { N: "4" } },
      }),
    )
    await Effect.runPromise(
      VersionedPages.update({ pageId: "p1" })
        .pipe(Entity.clearMap("metrics"))
        .pipe(Entity.asRecord)
        .pipe(Effect.provide(TestLayers, { local: true })),
    )
    const call = mockUpdateItem.mock.calls[0]![0]
    // Without it, a bucket added between the Get and the Update survives the clear.
    expect(call.ConditionExpression).toContain("#casVer = :casVer")
    expect(call.ExpressionAttributeValues[":casVer"]).toEqual({ N: "3" })
  })

  it("on a versioned entity, a concurrent write fails the clear instead of surviving it", async () => {
    mockGetItem.mockReset()
    mockUpdateItem.mockReset()
    mockGetItem.mockImplementation(versionedRead)
    const casError = new Error("conditional")
    ;(casError as any).name = "ConditionalCheckFailedException"
    // The stored item now: a writer bumped the version and added a bucket.
    ;(casError as any).Item = {
      pk: { S: "x" },
      sk: { S: "x" },
      pageId: { S: "p1" },
      version: { N: "4" },
      "metrics#2026-01": { M: { views: { N: "5" }, clicks: { N: "2" } } },
      "metrics#2026-02": { M: { views: { N: "9" }, clicks: { N: "9" } } },
    }
    mockUpdateItem.mockImplementation(() => Promise.reject(casError))
    const error = await Effect.runPromise(
      VersionedPages.update({ pageId: "p1" })
        .pipe(Entity.clearMap("metrics"))
        .pipe(Entity.asRecord)
        .pipe(Effect.flip, Effect.provide(TestLayers, { local: true })),
    )
    expect(error._tag).toBe("OptimisticLockError")
  })

  it("on a versioned entity, a stale expectedVersion is refused before sending", async () => {
    mockGetItem.mockReset()
    mockUpdateItem.mockReset()
    mockGetItem.mockImplementation(versionedRead)
    const error = await Effect.runPromise(
      VersionedPages.update({ pageId: "p1" })
        .pipe(Entity.clearMap("metrics"))
        .pipe(Entity.expectedVersion(2))
        .pipe(Entity.asRecord)
        .pipe(Effect.flip, Effect.provide(TestLayers, { local: true })),
    )
    expect(error._tag).toBe("OptimisticLockError")
    expect((error as { actualVersion: number }).actualVersion).toBe(3)
    expect(mockUpdateItem).not.toHaveBeenCalled()
  })

  it("clearMap on missing item is a no-op (the subsequent Update may still create the row)", async () => {
    mockGetItem.mockReset()
    mockUpdateItem.mockReset()
    mockGetItem.mockImplementation(() => Promise.resolve({}))
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({
        Attributes: { pk: { S: "x" }, sk: { S: "x" }, pageId: { S: "p1" } },
      }),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Pages.update({ pageId: "p1" })
          .pipe(Entity.clearMap("metrics"))
          .pipe(Entity.set({ status: "reset" }))
          .pipe(Entity.asRecord)
      }).pipe(Effect.provide(TestLayers, { local: true })),
    )
    const call = mockUpdateItem.mock.calls[0]![0]
    const expr: string = call.UpdateExpression
    // No REMOVE because there were no matching attrs.
    expect(expr).not.toMatch(/REMOVE /)
    expect(expr).toMatch(/SET /)
  })

  it("clearMap rejects an unknown field at run time", async () => {
    mockGetItem.mockReset()
    mockUpdateItem.mockReset()
    mockGetItem.mockImplementation(() => Promise.resolve({}))
    mockUpdateItem.mockImplementation(() => Promise.resolve({ Attributes: {} }))
    const result = await Effect.runPromise(
      Pages.update({ pageId: "p1" })
        .pipe(Entity.clearMap("not-a-sparse-field"))
        .pipe(Entity.asRecord, Effect.flip, Effect.provide(TestLayers, { local: true })),
    )
    expect(result).toBeInstanceOf(ValidationError)
  })
})

// ---------------------------------------------------------------------------
// 10. null in record input is NOT REMOVE
// ---------------------------------------------------------------------------

describe("SparseMap — null is not REMOVE", () => {
  it("encodeSparseFields drops a null field rather than treating it as REMOVE", () => {
    const item: Record<string, unknown> = {
      pageId: "p1",
      metrics: null,
      totals: { "2026-01": 1 },
    }
    encodeSparseFields(item, {
      metrics: { prefix: "metrics" },
      totals: { prefix: "totals" },
    })
    // metrics is dropped — no flattened attrs emitted, no REMOVE marker.
    // The downstream UpdateItem path will not emit a REMOVE for any
    // metrics#* attribute. Removal is always explicit via removeEntries.
    expect(item.metrics).toBeUndefined()
    expect(Object.keys(item).filter((k) => k.startsWith("metrics#"))).toEqual([])
    // The other field is processed as normal.
    expect(item["totals#2026-01"]).toBe(1)
  })

  it("update path: null on a sparse field is skipped (no SET, no REMOVE) — not interpreted as clear", async () => {
    // Model with optional Record so the schema layer accepts null at the
    // input boundary; the runtime layer must then NOT decompose it.
    class P extends Schema.Class<P>("P")({
      id: Schema.String,
      name: Schema.optional(Schema.String),
      tags: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    }) {}
    const PM = DynamoModel.configure(P, { tags: { storedAs: DynamoModel.SparseMap() } })
    const Ent = Entity.make({
      model: PM,
      entityType: "P",
      primaryKey: {
        pk: { field: "pk", composite: ["id"] },
        sk: { field: "sk", composite: [] },
      },
    })
    const T = Table.make({ schema: AppSchema, entities: { Ent } })
    ;(Ent as any)._configure(AppSchema, T.Tag)
    const Layers = Layer.mergeAll(TestDynamoClient, T.layer({ name: "p-table" }))

    mockUpdateItem.mockReset()
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({ Attributes: { pk: { S: "x" }, sk: { S: "x" }, id: { S: "p1" } } }),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Ent.update({ id: "p1" })
          .pipe(Entity.set({ name: "ok", tags: undefined as any }))
          .pipe(Entity.asRecord)
      }).pipe(Effect.provide(Layers, { local: true })),
    )
    const call = mockUpdateItem.mock.calls[0]![0]
    if (call.ExpressionAttributeNames) {
      const flattenedNames = Object.values(
        call.ExpressionAttributeNames as Record<string, string>,
      ).filter((n) => n.startsWith("tags#"))
      expect(flattenedNames).toEqual([])
    }
    expect(call.UpdateExpression).not.toMatch(/REMOVE /)
  })
})

// ---------------------------------------------------------------------------
// 11. Conditional ops via attribute_exists on entry
// ---------------------------------------------------------------------------

describe("SparseMap — conditional ops", () => {
  it("attribute_exists(metrics#2026-01) compiles via BoundUpdate.condition shorthand", async () => {
    mockUpdateItem.mockReset()
    mockUpdateItem.mockImplementation(() =>
      Promise.resolve({ Attributes: { pk: { S: "x" }, sk: { S: "x" }, pageId: { S: "p1" } } }),
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* DynamoClient.make({
          entities: { Pages },
          tables: { PagesTable },
        })
        // Use Pages.condition (entity-scoped pipeable form) so the shorthand
        // can include a literal flattened attribute name with `#`.
        yield* db.entities.Pages.update({ pageId: "p1" })
          .set({ status: "active" })
          .condition((t, { exists }) => exists((t.metrics as any).entry("2026-01")))
      }).pipe(Effect.provide(TestLayers, { local: true })),
    )
    const call = mockUpdateItem.mock.calls[0]![0]
    expect(call.ConditionExpression).toMatch(/attribute_exists\(/)
    const namesValues = Object.values(call.ExpressionAttributeNames as Record<string, string>)
    expect(namesValues).toContain("metrics#2026-01")
  })
})

// ---------------------------------------------------------------------------
// 12. Time-series append strips sparse attrs from event items
// ---------------------------------------------------------------------------

describe("SparseMap — time-series interaction", () => {
  it("event items do NOT carry sparse-map attributes (Reading A from issue #31)", async () => {
    // Define a sparse-aware time-series entity.
    class TM extends Schema.Class<TM>("TM")({
      device: Schema.String,
      timestamp: Schema.DateTimeUtc,
      reading: Schema.optional(Schema.Number),
      counters: Schema.Record(Schema.String, Schema.Number),
    }) {}
    const TMModel = DynamoModel.configure(TM, {
      counters: { storedAs: DynamoModel.SparseMap() },
    })
    const TMSchema = DynamoSchema.make({ name: "tm", version: 1 })
    const Telemetries = Entity.make({
      model: TMModel,
      entityType: "TM",
      primaryKey: {
        pk: { field: "pk", composite: ["device"] },
        sk: { field: "sk", composite: [] },
      },
      timestamps: { created: "createdAt" },
      timeSeries: {
        orderBy: "timestamp",
        appendInput: Schema.Struct({
          device: Schema.String,
          timestamp: Schema.DateTimeUtc,
          reading: Schema.optional(Schema.Number),
        }),
      },
    })
    const TMTable = Table.make({ schema: TMSchema, entities: { Telemetries } })
    ;(Telemetries as any)._configure(TMSchema, TMTable.Tag)
    const TMTableLayer = TMTable.layer({ name: "tm-table" })
    const TMLayers = Layer.mergeAll(TestDynamoClient, TMTableLayer)

    mockTransactWriteItems.mockReset()
    mockGetItem.mockReset()
    mockTransactWriteItems.mockImplementation(() => Promise.resolve({}))
    // The follow-up GetItem after append returns a current-item shape with
    // a counters bucket — used to assert the rebuilt domain Record on the
    // current item alongside the absence on the event item.
    mockGetItem.mockImplementation(() =>
      Promise.resolve({
        Item: {
          pk: { S: "$tm#v1#tm#dev-1" },
          sk: { S: "$tm#v1#tm" },
          device: { S: "dev-1" },
          timestamp: { S: "2026-04-22T10:00:00.000Z" },
          reading: { N: "42" },
          "counters#hits": { N: "100" },
          __edd_e__: { S: "TM" },
          createdAt: { S: "2026-04-22T10:00:00.000Z" },
        },
      }),
    )

    const r = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* Telemetries.append({
          device: "dev-1",
          timestamp: DateTime.makeUnsafe("2026-04-22T10:00:00.000Z"),
          reading: 42,
        })
      }).pipe(Effect.provide(TMLayers, { local: true })),
    )
    // Stale-as-error contract: success returns `{ current }` directly with the
    // domain-shaped model. The current item carries the sparse field as a
    // rebuilt domain Record.
    expect(r.current).toBeDefined()
    expect(r.current.counters).toEqual({ hits: 100 })

    // Inspect the event-item Put: must NOT carry counters or counters#*.
    const txCall = mockTransactWriteItems.mock.calls[0]![0]
    const eventPut = txCall.TransactItems[1].Put
    const eventAttrs = eventPut.Item as Record<string, unknown>
    expect(eventAttrs.counters).toBeUndefined()
    for (const k of Object.keys(eventAttrs)) {
      expect(k.startsWith("counters#")).toBe(false)
    }
  })
})
