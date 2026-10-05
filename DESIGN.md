# Effect DynamoDB ORM - Design Specification

## 1. Philosophy & Principles

### Motivation

effect-dynamodb provides a type-safe, Effect-native DynamoDB ORM that makes single-table design first-class. The library bridges the gap between Effect's composable programming model and DynamoDB's access-pattern-driven data modeling, delivering an API where domain models are portable, storage concerns are declarative, and queries compose via pipes.

### Six Principles

1. **Domain models are portable.** A `User` schema should work with DynamoDB, SQL, or an API response — no storage concepts leak into the model.
2. **Entity owns storage concerns.** Key composition, timestamps, versioning, soft delete — all configured at the Entity level, not annotated on model fields.
3. **Convention over configuration.** The system owns key format, delimiters, and serialization. The developer declares *which* attributes compose each key, not *how*.
4. **Composable queries.** Queries are pipeable data types with combinators, not builder patterns. They follow Effect TS idioms.
5. **Type safety from declarations.** Seven types are derived automatically from Model + Table + Entity — zero manual type maintenance.
6. **Client is the gateway.** `DynamoClient.make(table)` is the sole execution gateway — it resolves infrastructure dependencies, binds all entities and aggregates registered on the table, and returns a typed client where every operation has `R = never`. This matches `HttpApiClient.make(api)` from Effect v4 and enables clean service boundaries with layer-based testing.

### Design Evolution

The API went through two significant redesigns:

| Concern | v1 | v2 (bind pattern) | v3 (client gateway) |
|---------|----|--------------------|---------------------|
| Model base class | `DynamoModel.Class` (VariantSchema) | Standard `Schema.Class` | Standard `Schema.Class` |
| Key composition | Template strings: `"USER#${userId}"` | Attribute lists: `composite: ["userId"]` | Attribute lists: `composite: ["userId"]` |
| Entity definition | `Entity.make({ model, table, ... })` | `Entity.make({ model, table, ... })` | `Entity.make({ model, ... })` — no `table` |
| Table definition | `Table.make({ schema })` | `Table.make({ schema })` | `Table.make({ schema, entities, aggregates })` |
| Execution gateway | `repo.put`, `repo.get` (flat) | `yield* Entity.bind(e)` → `BoundEntity` | `yield* DynamoClient.make(table)` → typed client |
| Aggregate internals | N/A | Composes Entity, Collection, Transaction | Composes Entity, Collection, Transaction |
| Aggregate edges | N/A | Explicit first-class entities | Explicit first-class entities |

The v3 redesign moved the `table` parameter out of `Entity.make()` (entities are now pure definitions), had `Table.make()` declare its members (entities + aggregates) up front, and established `DynamoClient.make(table)` as the typed execution gateway — matching `HttpApiClient.make(api)` from Effect v4 where the API definition describes the shape, and the client factory returns typed access to every group and operation.

---

## 2. Architecture

### Module Structure

```
packages/effect-dynamodb/src/
├── DynamoModel.ts      # Schema annotations (Hidden, identifier, ref) and configure() for field overrides (immutable, field rename, storedAs)
├── DynamoSchema.ts     # Application namespace (name + version) for key prefixing
├── Table.ts            # Table definition: { schema, entities, aggregates } — declares members up front
├── Entity.ts           # Entity definition (pure, no table ref) + typed operations
├── Aggregate.ts        # Aggregate definition — composes Entity, Collection, Transaction
├── EventStore.ts       # EventStream definition — event sourcing on DynamoDB
├── KeyComposer.ts      # Composite key composition from index definitions
├── Collection.ts       # Multi-entity queries with per-entity Schema decode
├── Expression.ts       # Condition, filter, and update expression builders (ConditionInput / UpdateInput)
├── Transaction.ts      # TransactGetItems + TransactWriteItems (atomic multi-item ops)
├── Projection.ts       # ProjectionExpression builder for selecting specific attributes
├── DynamoClient.ts     # Context.Service wrapping AWS SDK + DynamoClient.make(table) typed gateway
├── Marshaller.ts       # Thin wrapper around @aws-sdk/util-dynamodb
├── Errors.ts           # Tagged errors
├── internal/           # Decomposed internals
│   ├── Expr.ts         # Expr ADT — type-safe expression nodes, ConditionOps, compileExpr
│   ├── PathBuilder.ts  # PathBuilder — recursive Proxy for type-safe attribute path access
│   ├── EntityOps.ts    # Entity operation intermediates (EntityGet, EntityPut, EntityUpdate, EntityDelete)
│   ├── EntityTypes.ts  # Type-level computations for Entity derived types
│   ├── EntitySchemas.ts # Schema derivation (7 derived schemas)
│   ├── EntityCombinators.ts # Terminal functions and update combinators (record + path-based)
│   └── ...             # Other internal modules
└── index.ts            # Public API barrel export

packages/effect-dynamodb-geo/src/
├── GeoIndex.ts         # GeoIndex definition — geospatial indexing on Entity
├── GeoSearch.ts        # Internal search orchestration (H3 multi-cell parallel query)
├── H3.ts               # H3 hexagonal grid utilities
├── Spherical.ts        # Great-circle distance calculations
└── index.ts            # Public API barrel export
```

### Data Flow

```
User code → yield* DynamoClient.make(MainTable)  // typed execution gateway
  → resolves DynamoClient service + TableConfig from context
  → binds ALL entities and aggregates registered on the table
  → returns typed client: { Users, Tasks, Matches, createTable, ... }

db.Users.put(inputData)
  → Schema.decode(Entity.Input) — validate input
  → compose keys (KeyComposer) for all indexes using composite attributes
  → add __edd_e__ + timestamps + version
  → marshall to DynamoDB format (Marshaller)
  → versioned / unique entities: consistent read of the current item first, to
    continue its version and rotate its sentinels (#133)
  → DynamoClient.putItem (or transactWriteItems for unique constraints / retain)
  → Schema.decode(Entity.Record) — decode full item for return

db.Users.get(key)
  → compose primary key → DynamoClient.getItem
  → unmarshall → Schema.decode(Entity.Record) — validate & type

db.Users.execute(Users.query.indexName({ pk composites }))
  → compose PK/SK from composite attributes (KeyComposer)
  → build KeyConditionExpression + __edd_e__ FilterExpression
  → Stream.paginate (automatic DynamoDB pagination)
  → unmarshall → Schema.decode(Entity.Record) per item

db.Matches.get({ matchId: "m-1" })
  → internally uses Collection query to fetch all items in partition
  → discriminate by __edd_e__ into edge entity buckets
  → assemble into domain object
```

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Native build (not wrapping ElectroDB) | Full control over Effect integration, no impedance mismatch |
| Raw AWS SDK (not @effect-aws) | Avoid extra dependency; thin wrapper is simple enough |
| Effect Schema as sole schema system | Native Effect integration, bidirectional transforms, branded types |
| Schema.Class/Struct for models | Pure domain schemas — no DynamoDB concepts in models. Entity derives DynamoDB types |
| DynamoClient.make(table) as typed gateway | `Table.make({ entities, aggregates })` declares members; `DynamoClient.make(table)` binds them all and returns typed access. Matches `HttpApiClient.make(api)` pattern from Effect v4 |
| Entities are pure definitions | `Entity.make()` has no `table` parameter — entities carry only model, indexes, and config. Table association happens at `Table.make()` time |
| Table declares its members | `Table.make({ schema, entities: { Users, Tasks }, aggregates: { Matches } })` — the named record provides property names on the typed client |
| Aggregates compose entity operations | Aggregates never touch DynamoClient. They orchestrate Entity, Collection, and Transaction primitives |
| ElectroDB-style composite indexes | `{ pk: { field, composite }, sk: { field, composite } }` — attribute lists not templates |
| DynamoSchema for key namespacing | `$schema#v1#entity#attrs` format with `$` sentinel prefix for ORM/non-ORM coexistence |
| `__edd_e__` entity type attribute | Ugly name convention (like ElectroDB's `__edb_e__`) avoids collisions with user model fields |
| Single-table first | Most impactful DynamoDB pattern; multi-table is simpler subset |
| @aws-sdk/util-dynamodb for marshalling | Proven, maintained; Effect Schema handles validation layer above |

### Module Dependencies

```
Aggregate → Entity, Collection, Transaction, Errors (never DynamoClient directly)
Entity → DynamoClient, DynamoSchema, Table, KeyComposer, Marshaller, Expression, Errors
Collection → DynamoClient, Entity, Table, Marshaller, Errors
Transaction → DynamoClient, Entity, KeyComposer, Marshaller, Expression, Errors
Projection → (standalone, no internal deps)
Expression → Marshaller
Table → DynamoSchema, Entity (type-level for member registration)
DynamoSchema → (standalone, no internal deps)
DynamoModel → effect (Schema) — provides annotations (Hidden, identifier, ref) and configure()
DynamoClient → effect (Context, Layer), @aws-sdk/client-dynamodb, Entity (for make() binding)
KeyComposer → (standalone, no internal deps)
Marshaller → @aws-sdk/util-dynamodb
Errors → effect (Data)
```

### Layering Principle

Higher-level constructs compose lower-level primitives. No layer may bypass the one below:

```
┌─────────────────────────────────────┐
│  DynamoClient.make(table)           │  ← typed gateway (binds all members, R = never)
├─────────────────────────────────────┤
│  Aggregate / GeoIndex / EventStore  │  ← orchestration (decompose, assemble, diff)
├─────────────────────────────────────┤
│  Collection / Transaction / Batch   │  ← multi-entity coordination
├─────────────────────────────────────┤
│  Entity                             │  ← single-item CRUD, keys, validation, versioning
├─────────────────────────────────────┤
│  DynamoClient (raw service)         │  ← raw AWS SDK operations
└─────────────────────────────────────┘
```

---

## 3. Model Layer

### Pure Domain Models

Models use standard Effect Schema definitions — `Schema.Class` for class instances or `Schema.Struct` for plain objects. No DynamoDB concepts appear in the model definition. Models are portable across storage backends.

```typescript
import { Schema } from "effect"
import { DynamoModel } from "effect-dynamodb"

class User extends Schema.Class<User>("User")({
  userId:      Schema.String,
  email:       Schema.String,
  displayName: Schema.NonEmptyString,
  role:        Schema.Literals(["admin", "member"]),
}) {}
```

### DynamoModel.configure — Immutable Fields

`DynamoModel.configure` wraps a model with per-field DynamoDB overrides, keeping ORM concerns separate from pure domain models. The `immutable` option marks a field as read-only after creation — excluded from `Entity.Update<E>` alongside key-referenced fields.

```typescript
import { Schema } from "effect"
import { DynamoModel } from "effect-dynamodb"

// Pure domain model — no DynamoDB concepts
class User extends Schema.Class<User>("User")({
  userId:      Schema.String,
  email:       Schema.String,
  displayName: Schema.NonEmptyString,
  createdBy:   Schema.String,
}) {}

// DynamoDB-specific configuration — separate from model
const UserModel = DynamoModel.configure(User, {
  createdBy: { immutable: true },  // never changes after creation
})
```

The Entity reads the `immutable` flag from the configured model's attributes and excludes that field from `Entity.Update<E>`.

---

## 4. Application Namespace (DynamoSchema)

### Namespace and Versioning

`DynamoSchema` is a top-level construct that defines the application namespace. It prefixes every generated key in the system, enabling multiple applications to share the same DynamoDB table with complete isolation.

```typescript
import { DynamoSchema } from "effect-dynamodb"

const AppSchema = DynamoSchema.make({
  name: "myapp",
  version: 1,
  casing: "lowercase",  // default
})
```

### Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `string` | *required* | Application name, used as key prefix |
| `version` | `number` | *required* | Schema version number |
| `casing` | `"lowercase" \| "uppercase" \| "preserve"` | `"lowercase"` | Casing for the whole composed key, composite values included |

### Casing Rules

Casing applies to the **entire composed key** (ElectroDB parity):
- Schema name
- Entity type / collection name
- Composite attribute names
- **Composite attribute values** — under the default `"lowercase"`, `"Dev-A"` and `"dev-a"` compose the same key. Ids that must stay distinct by case need `"preserve"`.

The stored attribute keeps its original value; only the key string is cased.

**Index-level override.** `primaryKey.casing` and `indexes.<name>.casing` override the schema's casing for that index's keys: entity type / collection name and composites, both halves. The `$<schema>#v<n>` prefix always uses the schema's casing (`KeyComposer.effectiveCasing` = index's, else schema's). Every composition path goes through `KeyComposer` with the index definition, so put, query accessors (PK, `begins_with`, `.where()` operands), policy-aware updates and collection queries all honour it. Collection members must agree on the collection index's effective casing — `EDD-9055` at `DynamoClient.make()` / `Collection.make()`, since keys composed under different casings never meet in the shared partition. (Before 1.22 `normalizeGsiConfig` dropped `casing` from `indexes` entries, so it was accepted but ignored on GSIs; `primaryKey.casing` always worked.)

**Time-series exception.** The `#e#` infix and the serialized `orderBy` value in an event-item SK follow the **schema's** casing even when `primaryKey.casing` overrides it. Event SKs are already stored under that rule, so it is pinned (`TimeSeries.test.ts`) rather than aligned.

**Fixed markers are never cased:** the `v` in `#v<n>`, the `#v#` (version snapshot) and `#deleted#` (soft delete) infixes, and the `_<n>` entity/event version suffix. The time-series `#e#` infix is the exception and follows the casing. These are storage format — pinned by `packages/schema/test/DynamoSchema.test.ts` — and must not change.

**Casing is storage format.** Changing it on a populated table moves every composed key.

**EventStore streams** lower-case `streamName` in their keys by default, regardless of `casing`. `makeStream({ casing })` sets the stream's key casing like an index's `casing` does. See §12.

### Key Prefix Format

Every generated key starts with a `$` sentinel followed by the schema prefix. The `$` sentinel identifies ORM-managed keys, enabling coexistence with non-ORM items on the same table:

```
$<schema>#<version>#<entityType|collection>#<...composites>
```

Examples with `name: "myapp"`, `version: 1`, `casing: "lowercase"`:

| Context | Generated key |
|---------|---------------|
| User entity, pk `["userId"]`, value `"abc-123"` | `$myapp#v1#user#userid_abc-123` |
| User entity, sk `[]` (empty) | `$myapp#v1#user` |
| Clustered collection "TenantItems", pk `["tenantId"]` | `$myapp#v1#tenantitems#tenantid_t-1` |
| Unique constraint sentinel (email) | `$myapp#v1#user.email#foo@bar.com` |
| Version snapshot (v7) | `$myapp#v1#user#v#0000007` |
| Soft-deleted item | `$myapp#v1#user#deleted#2024-01-15T10:30:00Z` |
| Version snapshot (v7), sk `["line"]`, value `"a"` | `$myapp#v1#user#v#line_a#0000007` |

### The `$` Sentinel

Every ORM-generated key starts with `$`. This serves two purposes:

1. **Coexistence** — A scan or stream consumer can immediately identify ORM-managed items by the `$` prefix without needing to know the schema name.
2. **Collision avoidance** — The `$` separates ORM-managed structural prefixes from user-provided attribute values, preventing ambiguity during key parsing.

### Multi-Application Isolation

Two applications sharing the same table produce completely independent key spaces:

```
$myapp#v1#user#abc-123     ← Application A
$billing#v1#user#abc-123   ← Application B (different schema name)
```

### Schema Versioning for Migration

Schema version enables blue/green deployments and gradual migration:

```
$myapp#v1#user#abc-123     ← Current production
$myapp#v2#user#abc-123     ← New version (migration in progress)
```

---

## 5. Table & Entity

### Entity — Pure Definition

An Entity binds a domain model to key composition rules, system field configuration, unique constraints, and collection membership. **Entities do not reference a Table** — they are pure definitions carrying only model, indexes, and config.

```typescript
import { Duration } from "effect"
import { Entity } from "effect-dynamodb"

const UserEntity = Entity.make({
  model: User,
  entityType: "User",
  indexes: {
    primary: {
      pk: { field: "pk", composite: ["userId"] },
      sk: { field: "sk", composite: [] },
    },
    byTenant: {
      index: "gsi1",
      collection: "TenantItems",
      type: "clustered",
      pk: { field: "gsi1pk", composite: ["tenantId"] },
      sk: { field: "gsi1sk", composite: ["createdAt"] },
    },
    byEmail: {
      index: "gsi2",
      pk: { field: "gsi2pk", composite: ["email"] },
      sk: { field: "gsi2sk", composite: [] },
    },
  },
  unique: { email: ["email"] },
  timestamps: true,
  versioned: { retain: true },
  softDelete: true,
})
```

### Table — Declares Members

`Table` groups entities and aggregates that share a physical DynamoDB table and application namespace. It carries the `DynamoSchema` reference used for key prefix generation and the named records of its members. The physical table name is provided at runtime via `Table.layer()`.

```typescript
import { Table } from "effect-dynamodb"

const MainTable = Table.make({
  schema: AppSchema,
  entities: { Users: UserEntity, Tasks: TaskEntity },
  aggregates: { Matches: MatchAggregate },
})
```

The named record keys (`Users`, `Tasks`, `Matches`) become the property names on the typed client returned by `DynamoClient.make()`.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `schema` | `DynamoSchema` | Yes | The application schema (provides key prefixing) |
| `entities` | `Record<string, Entity>` | No | Named entity definitions |
| `aggregates` | `Record<string, Aggregate>` | No | Named aggregate definitions |

### Runtime Configuration

The physical table name is injected at runtime via Effect Layers, keeping definitions pure and environment-independent:

```typescript
// Provide physical table name at the edge
MainTable.layer({ name: "my-prod-table" })

// Or from environment variables via Effect Config
MainTable.layerConfig({ name: Config.String("TABLE_NAME") })
```

### DynamoClient.make(table) — Typed Execution Gateway

`DynamoClient.make(table)` is the sole gateway for executing operations. It resolves infrastructure dependencies (`DynamoClient` service + `TableConfig`), binds all entities and aggregates registered on the table, and returns a typed client where every operation has `R = never`.

This follows the `HttpApiClient.make(api)` pattern from Effect v4: the table definition describes the shape (like `HttpApi` describes endpoints), and the client factory returns typed access to every member (like `HttpApiClient` returns typed access to every group).

```typescript
const program = Effect.gen(function* () {
  const db = yield* DynamoClient.make(MainTable)

  // Entity operations — typed, R = never
  const user = yield* db.Users.get({ userId: "123" })
  yield* db.Users.put({ userId: "456", ... })

  // Aggregate operations — typed, R = never
  const match = yield* db.Matches.get({ matchId: "m-1" })

  // Table management
  yield* db.createTable()
  yield* db.deleteTable
  const info = yield* db.describeTable
})
```

The typed client provides:

| Property | Type | Description |
|----------|------|-------------|
| `db.<EntityName>` | `BoundEntity<...>` | Bound entity operations (get, put, create, update, delete, query, etc.) |
| `db.<AggregateName>` | `BoundAggregate<...>` | Bound aggregate operations (get, create, update, delete, list) |
| `db.createTable(options?)` | `Effect<void, DynamoClientError>` | Create the physical table (derives schema from members) |
| `db.deleteTable` | `Effect<void, DynamoClientError>` | Delete the physical table |
| `db.describeTable` | `Effect<DescribeTableOutput, DynamoClientError>` | Describe the table |

### Service Pattern

Wrap `DynamoClient.make(table)` in `Context.Service` for dependency injection and testability. Destructure to access only the entities you need:

```typescript
export class TeamService extends Context.Service<TeamService>()("@gamemanager/TeamService", {
  make: Effect.gen(function* () {
    const { Teams } = yield* DynamoClient.make(MainTable)
    return {
      create: Effect.fn(function* (input: CreateTeamInput) {
        const id = ulid() as TeamId
        return yield* Teams.put({ ...input, id })
      }),
      get: (id: TeamId) => Teams.get({ id }),
      update: (id: TeamId, updates: UpdateTeamInput) => Teams.update({ id }, updates),
      delete: (id: TeamId) => Teams.delete({ id }),
      list: (filter: TeamListFilter = {}, pagination?: PaginationOptions) =>
        Teams.execute(applyPagination(Teams.query.byAll(filter), pagination)).pipe(
          Effect.map((page) => ({
            data: page.items,
            count: page.items.length,
            cursor: page.cursor,
          })),
        ),
    }
  }),
}) {}
```

Testing — mock at the service level, no DynamoDB needed:

```typescript
program.pipe(
  Effect.provide(Layer.succeed(TeamService, {
    get: () => Effect.succeed(fakeTeam),
    create: () => Effect.succeed(fakeTeam),
    list: () => Effect.succeed({ data: [], count: 0, cursor: null }),
  }))
)
```

The entity definition still provides type derivation (`Entity.Record<typeof UserEntity>`, `Entity.Key<typeof UserEntity>`, etc.) without the client.

### Index Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `pk.field` | `string` | Yes | Physical DynamoDB attribute name |
| `pk.composite` | `string[]` | Yes | Ordered list of model attributes |
| `sk.field` | `string` | Yes | Physical DynamoDB attribute name |
| `sk.composite` | `string[]` | Yes | Ordered list of model attributes |
| `index` | `string` | No (primary only) | Physical GSI/LSI name from Table definition |
| `collection` | `string \| string[]` | No | Collection name(s) for cross-entity queries |
| `type` | `"isolated" \| "clustered"` | No | Collection type (default: `"clustered"`) |
| `casing` | `"lowercase" \| "uppercase" \| "preserve"` | No | Casing override for this index |

### System Fields

| Config | Type | Fields Added |
|--------|------|-------------|
| `timestamps: true` | `boolean \| { created?: TimestampFieldConfig, updated?: TimestampFieldConfig }` | `createdAt`, `updatedAt` (or custom names) |
| `versioned: true` | `boolean \| { field?: string, retain?: boolean, ttl?: Duration \| string }` | `version` (or custom name) |
| `softDelete: true` | `boolean \| { ttl?: Duration \| string, preserveUnique?: boolean }` | `deletedAt` (when soft-deleted) |

#### Timestamp field configuration (`TimestampFieldConfig`)

```ts
type TimestampFieldConfig =
  | string                                              // field name only
  | Schema.Top                                          // storage only (default field name)
  | { field?: string; schema?: Schema.Top }             // both
```

The `schema` half is **a storage descriptor, not a codec**. System timestamps are
*generated* by the library at write time (`generateTimestampPrimitive` over the
Clock-backed `DateTime.now`), never decoded from caller input, so the only thing
the write path can read off the supplied schema is its `DynamoEncoding`
annotation — which storage form to emit (`string` / `epochMs` / `epochSeconds`).

**Make-time validation (EDD-9044).** A `schema` that carries no `DynamoEncoding`
annotation is rejected at `Entity.make()` / `Aggregate.make()` time. Before this
rule such a schema was silently discarded and the field fell back to an ISO
string — the type accepted `Schema.Number` and `Schema.DateTimeUtcFromMillis`,
both of which then stored an `S` (#97). Storing the wrong wire type is not merely
an unused attribute: a GSI declaring `AttributeType: 'N'` over that field makes
DynamoDB reject the write outright with `ValidationException: Type mismatch for
Index Key`, so the failure has to surface at definition time, not at the write.

| `schema` | Result |
|---|---|
| `DynamoModel.DateString` | ISO 8601 string (`S`) |
| `DynamoModel.DateEpochMs` | epoch millis (`N`) |
| `DynamoModel.DateEpochSeconds` / `DynamoModel.TTL` | epoch seconds (`N`) |
| `X.pipe(DynamoModel.storedAs(Y))` | `Y`'s storage form |
| `Schema.Number`, `Schema.DateTimeUtcFromMillis`, any un-annotated schema | **EDD-9044** at make() time |

A *model-declared* field of the same name still wins over `timestamps.schema`
(the existing collision rule): a date-compatible model field supplies the
encoding, and a non-date model field means the user owns the attribute outright
and the library skips timestamp management for it.

#### TTL configuration (`versioned.ttl`, `softDelete.ttl`, `timeSeries.ttl`, `unique[].ttl`)

Every framework TTL accepts a `Duration.Duration` **or** a humanized string
(`"7 days"`, `"24 hours"`, `"30 minutes"`), parsed via Effect's `Duration` input
grammar. Two rules are enforced:

- **No bare `number`** — the type rejects it. Effect's `Duration` grammar treats
  a number as *milliseconds*, so `3600` would mean 3.6 s, not an hour (a 1000×
  footgun). Pass `Duration.seconds(3600)` or `"3600 seconds"`.
- **No infinite / unparseable value** — an infinite TTL epoch is nonsensical;
  these (and unparseable strings) fail at `Entity.make()` time with **EDD-9005**,
  so the write path never observes an invalid TTL.

The stored TTL attribute is an absolute epoch-seconds expiry computed from the
**Clock-backed `DateTime.now`** at write time (deterministic under `TestClock`)
plus the configured duration, written to the table's configured TTL attribute
name (`TableConfig.ttlAttributeName`, default `_ttl`).

### Generated IDs

Opt into auto-generated UUID primary keys with `generatedId`:

```ts
Entity.make({ …, generatedId?: { field: string; version?: "v4" | "v7" } })  // default version: "v4"
```

| Config | Behavior |
|--------|----------|
| `generatedId: { field: "id" }` | On `put`/`create`/`upsert`, fill `id` with a UUIDv4 when the caller omits it. A caller-supplied value is always respected (never overwritten). |
| `generatedId: { field: "id", version: "v7" }` | As above, but time-ordered UUIDv7. |

**Make-time validation (EDD-9008).** The named `field` MUST exist in the model **AND** participate in the primary key (pk or sk composite). Generating an id that doesn't compose into the primary key would be a silent no-op, so both conditions are enforced at `Entity.make()` time with a thrown `[EDD-9008]` error. EDD-9008 was the next free code (9006/9007 in use; 9008/9009 free; 9010–9016 are the timeSeries range).

**Schema split — input-optional, record-required.** In the derived `inputSchema` the generated-id field is marked **optional** (caller MAY omit it and let the library fill it), reusing the same `applySystemCollisionAdjustments` / `optionalOnCollide` path that makes colliding `createdAt`/`updatedAt` optional. In the `recordSchema` it stays **required** — a decoded record always carries the id. The type-level optionality is mirrored by `WithGeneratedId<Input, TGeneratedId>` so `Entity.inputSchema.Type` and the `put`/`create`/`upsert` parameter types have the field optional.

**Injection point.** The id is filled on the raw `input` **before** the encode in `Entity.put` (the `encodeOrDecodeEncode` call). `create`/`upsert` inherit via delegation. The filled id then flows through input validation → key composition (PK/SK + any GSI composites it participates in) → stored item → returned record in a single pass.

**`R = never` is preserved — the crux.** `DynamoClient.make(...)` returns bound methods with `R = never`, enforced by hard-typed `provide` helpers. Filling the id requires the cryptographically-secure `Crypto` service (`effect/Crypto`), and `effect-core` ships **no default Crypto layer**. Yielding `Crypto.Crypto` in `put` would naively widen `R` to `… | Crypto.Crypto` and fail at runtime with "Service not found".

The chosen solution (Option 1, the issue's recommendation): **bundle a default `Crypto` service into the context the typed client already captures, and widen the `provide` helpers to admit `Crypto.Crypto`.** Concretely:

- A thin default service (`internal/DefaultCrypto.ts`) is built from `Crypto.make({ randomBytes, digest })` over `globalThis.crypto.getRandomValues` / `crypto.subtle.digest` — **no new dependency** (Web Crypto is available on Node 18+, browsers, and edge runtimes).
- `Entity.bind` (where bound `put` actually runs) and `DynamoClient.makeFromConfig` each add the default via `Context.add(ctx, Crypto.Crypto, makeDefaultCrypto())` and widen their `provide` helper's input type from `Effect<A, E, DynamoClient | TableConfig>` to `… | Crypto.Crypto`. `Entity.bind` only fills the default when Crypto is absent (`Context.getOption`), so an override is respected.
- Bound `put` stays `R = never`; the public method signatures are unchanged. Entities **without** `generatedId` never yield `Crypto` (the fill helper returns early), so their unbound ops keep `R = DynamoClient | TableConfig` exactly as before.
- **Optional platform override:** `DynamoClient.make({ …, crypto?: Crypto.Crypto })` accepts a platform implementation (e.g. from `@effect/platform-node`) which takes precedence over the default. Crypto is **never** surfaced in the public `R` (the rejected Option 2).

### Unique Constraints

```typescript
unique: {
  email: ["email"],                        // single-field uniqueness
  tenantEmail: ["tenantId", "email"],       // compound uniqueness
  idempotencyKey: { fields: ["idempotencyKey"], ttl: Duration.hours(1) },  // time-bounded
  reservation: { fields: ["code"], ttl: "30 minutes" },                    // string form
}
```

When a unique constraint declares a `ttl`, the **sentinel item** carries the TTL
attribute and auto-expires, releasing the uniqueness reservation (e.g. a
time-bounded hold), whether a put or an update wrote it. Without a `ttl`,
sentinels are permanent. A sentinel names the item that reserved it
(`_entity_pk` / `_entity_sk`), and a write releases only sentinels its item
owns, since an expired reservation may have been claimed by another item (see
"Sentinel ownership" in §10).

Constraints are **sparse**: a sentinel is only written when every composing
field is present on the record. Mirrors GSI sparse semantics — a record with a
missing optional composite is silently excluded from the constraint, allowing
multiple records to coexist with the field unset (no false collision on a
literal `"undefined"` key). Update transitions claim/release the sentinel as
the field becomes set/unset.

**A default never creates a sentinel (#133).** A field with
`withDecodingDefault` that a write omits holds only its default, so no sentinel
is composed for it, even when it is stored. A defaulted unique field that is
also an index composite is stored (its index keys need it) and listed in a
hidden string-set attribute, `__edd_d__` (`UNSENTINELED_DEFAULTS`), which is
stripped from decoded models. `composeUniqueSentinel` returns nothing for a
constraint over a listed field, so no sentinel is composed, rotated or deleted
for it. A write that supplies the value drops the field from `__edd_d__` and
claims its sentinel. A `.remove()` of a defaulted index composite stores the
default again (`rematerializeRemovedDefaults`), keeps the item indexed under
it, re-lists a unique field in `__edd_d__` and releases the old value's
sentinel.

---

## 6. Entity-Derived Types

Seven types are automatically derived from the Model + Table + Entity declarations. Zero manual type maintenance.

### Type Hierarchy

```
Entity.Model<E>        Pure domain object (the Schema.Class itself)
    ↓ + system fields
Entity.Record<E>       Domain + system metadata (what Entity operations return)
    ↓ + key attributes
Entity.Item<E>         Full unmarshalled DynamoDB item
    ↓ + DynamoDB encoding
Entity.Marshalled<E>   DynamoDB AttributeValue format
```

### All Seven Types

```typescript
// Given UserEntity with timestamps: true, versioned: true

Entity.Model<typeof UserEntity>
// { userId: string, email: string, displayName: string, role: "admin" | "member" }

Entity.Record<typeof UserEntity>
// { userId: string, email: string, displayName: string, role: "admin" | "member",
//   version: number, createdAt: DateTime.Utc, updatedAt: DateTime.Utc }

Entity.Input<typeof UserEntity>
// { userId: string, email: string, displayName: string, role: "admin" | "member" }
// (no system fields — they are auto-managed)

Entity.Update<typeof UserEntity>
// { email?: string, displayName?: string, role?: "admin" | "member" }
// (keys excluded, immutable fields excluded, all optional)

Entity.Key<typeof UserEntity>
// { userId: string }
// (primary key composite attributes)

Entity.Item<typeof UserEntity>
// { pk: string, sk: string, gsi1pk: string, gsi1sk: string, gsi2pk: string, gsi2sk: string,
//   __edd_e__: string, userId: string, email: string, displayName: string, role: string,
//   version: number, createdAt: string, updatedAt: string }

Entity.Marshalled<typeof UserEntity>
// { pk: { S: string }, sk: { S: string }, gsi1pk: { S: string }, gsi1sk: { S: string },
//   __edd_e__: { S: string }, userId: { S: string }, email: { S: string },
//   version: { N: string }, createdAt: { S: string }, ... }
```

### Schema Accessors for Raw Data

For consuming DynamoDB Streams or working with raw items:

```typescript
Entity.itemSchema(UserEntity)
// Schema<Entity.Record<typeof UserEntity>, Entity.Item<typeof UserEntity>>

Entity.marshalledSchema(UserEntity)
// Schema<Entity.Record<typeof UserEntity>, Entity.Marshalled<typeof UserEntity>>
```

---

## 7. Key Composition

### Format Convention

**Format:** `${schema}#{version}#{prefix}#{attr1}#{attr2}`

- **Schema + version:** From `DynamoSchema` — e.g., `$myapp#v1`
- **Prefix:** Entity type (for entity keys) or collection name (for collection partition keys)
- **Attributes:** Values from composite array, in declared order, separated by `#`
- **Delimiter:** Always `#`

### Key Generation Rules

Given `DynamoSchema({ name: "myapp", version: 1, casing: "lowercase" })` and `entityType: "User"`:

| pk composite | sk composite | Generated pk | Generated sk |
|-------------|-------------|-------------|-------------|
| `[]` | `[]` | `$myapp#v1#user` | `$myapp#v1#user` |
| `["userId"]` | `[]` | `$myapp#v1#user#abc-123` | `$myapp#v1#user` |
| `[]` | `["userId"]` | `$myapp#v1#user` | `$myapp#v1#user#abc-123` |
| `["tenantId"]` | `["userId"]` | `$myapp#v1#user#t-1` | `$myapp#v1#user#abc-123` |
| `["tenantId"]` | `["status", "createdAt"]` | `$myapp#v1#user#t-1` | `$myapp#v1#user#active#2024-01-15` |

### Attribute Serialization

| Type | Serialization |
|------|---------------|
| `string` | As-is |
| `number` | Zero-padded to fixed width |
| `DateTime.Utc` | ISO 8601 string |
| `boolean` | `"true"` / `"false"` |
| Branded string | Underlying string value |

### Composite key form

Every path composes a composite through ONE function
(`internal/CompositeCodec.ts`) — the write path, `composePrimaryKey`, the query
accessors, `.where()` operands, and the aggregate composer. An operand and a
stored key must be produced by the same code; two call sites disagreeing about
which form to use has been the root cause of every key bug in this area.

**The rule.** Compose from the **Encoded** form, EXCEPT when the domain type is
numeric (`number` / `bigint`) and the encoded form is a **string** — then
compose from the numeric **Type** form so `serializeValue` pads it.

| composite | Type | Encoded | key uses |
|---|---|---|---|
| `Schema.Number` | number | number | encoded (already padded) |
| `Schema.BigInt` | bigint | bigint | encoded (already padded) |
| `Schema.BigIntFromString` | bigint | string | **Type** — a string would not pad |
| `Schema.NumberFromString` | number | string | **Type** — same shape |
| `DynamoModel.DateEpochMs` | DateTime | number | encoded — epoch, padded |
| `Schema.Date` / `DateTimeUtc` | Date | ISO string | encoded — ISO sorts correctly |
| untransformed string | string | string | encoded (identical) |

The exception exists because `serializeValue` pads numbers to 16 digits and
bigints to 38 so they sort lexicographically in numeric order, and leaves a
string alone. Composing the encoded `"42"` of a `BigIntFromString` stored
`txn_42` beside `txn_100` and `txn_5`, which DynamoDB orders 100 < 42 < 5 — so
`gte(42n)` returned 42 and 5 instead of 42 and 100.

Resolution per attribute:

| case | behaviour |
|---|---|
| Not a model field (ref-derived `<ref>Id`) | Pass through — already wire-shaped. |
| Numeric Type, string Encoded | Keep a `number` / `bigint`; otherwise `decode` to reach it. |
| No encoding transformation (`ast.encoding === undefined`, documented as "type and encoded forms are identical") | Pass through WITHOUT encoding, so an open bound like `gte(t.status, "d")` on a `Schema.Literals` composite is not rejected by a codec. |
| Has an encoding transformation | `encode`, with `decode -> encode` as fallback so an already-encoded value round-trips to itself. |
| Resolves to neither | **EDD-9050**, naming the attribute. |

Public key and composite input is always the **Type** side — the value the
domain model holds and `.where()` accepts. Internal paths (retain, restore,
soft-delete) hold wire-shaped records read back from DynamoDB and reach the
composer directly.

### Isolated vs Clustered Key Prefixes

**Isolated:**
```
SK = ${schema}#{version}#{entityType}_{entityVersion}#{composites}
```

**Clustered:**
```
SK = ${schema}#{version}#{collectionName}#{entityType}_{entityVersion}#{composites}
```

**Clustered with sub-collections:**
```
SK = ${schema}#{version}#{parentCollection}#{childCollection}#{entityType}_{entityVersion}#{composites}
```

### Special Key Patterns

**Unique constraint sentinel:**
```
PK: ${schema}#{version}#{entityType}.{constraintName}#{fieldValues}
SK: ${schema}#{version}#{entityType}.{constraintName}
```

**Version snapshot:**
```
PK: (same as current item)
SK: ${schema}#{version}#{entityType}#v#[{item}#]{zeroPaddedVersion}
```

**Soft-deleted item:**
```
PK: (same as current item)
SK: ${schema}#{version}#{entityType}#deleted#[{item}#]{isoTimestamp}
```

**`{item}` — history of items that share a partition (#133).** An entity whose
primary sort key has composites keeps several items in one partition, so each
item's history keys carry its identity: the composite part of its live sort key
(`KeyComposer.composeHistoryItemSegment`, e.g. `line_a`), inserted after the
marker (`DynamoSchema.HistoryKeyOptions.item`). Siblings therefore never share a
version sequence, a snapshot row or a tombstone; `versions`, `getVersion`,
`deleted.get`, `restore`, `highestRetainedVersion` and `purge` key by the item,
and `deleted.list` keeps the partition-wide prefix (every item's tombstones).
Without sort key composites there is no segment, and the keys are byte-identical
to every earlier release's. The segment sits after the marker, not after the
live sort key as time-series events do (`<currentSk>#e#…`): the marker keeps the
type-wide `begins_with` prefixes valid, so a primary-key query narrowed by sort
key composites (`begins_with($app#v1#line#line_a)`) can never reach a history
row, and a single-item entity's keys stay unchanged. `purge` of such an item
removes its live row, rows nested under it, its own history, and history an
earlier release wrote without a segment whose stored composites compose its
live key; siblings are untouched. An entity without sort key composites purges
every row of its own in the partition. Either way, only rows of its own entity
type (`__edd_e__`): another entity sharing the partition through a collection on
the primary key keeps its rows (purge used to delete every row in the
partition).

**History written before the segment stays readable.** Rows an earlier release
wrote for such an entity sit under the partition-wide keys (`#v#0000003`,
`#deleted#<ts>`), all items' in one sequence. A row there belongs to the item
whose live key its stored composites compose (`isItemsRow`), and every reader
of one item's history reads them too (`legacyHistory`: a `BETWEEN` on
`<prefix>0`…`<prefix>:` — a version or timestamp starts with a digit, a
segment with a composite name; a segment can still start with a digit if a
composite's NAME does, so rows with a `#` past the prefix are dropped. A
keys-only `Limit 1` probe of the range runs first, and only a hit reads it in
full, so a partition with no such history costs one small read):
`getVersion` falls back to the unsegmented key when the item's own is missing;
`deleted.get` / `restore` take the later of the item's own latest tombstone and
its latest unsegmented one (`latestTombstone`); `highestRetainedVersion` takes
the higher of both; `deleted.list` is partition-wide and lists them anyway.
`versions` stays a lazy `Query`: its `prepare` hook (run once per terminal)
looks for the item's unsegmented snapshots and, if there are any, widens the
`begins_with` to the partition's history and keeps the item's own rows plus
those not already held under its own key. **Precedence:** a version held both
ways is read from the segmented row; of two tombstones, the later wins (the
segmented one on a tie).

**History rows are not items.** Snapshots, tombstones and time-series event
items keep their entity's `__edd_e__` (the history readers filter on it), so the
ownership filter alone admits them to a primary-key query, a scan, or a
collection on the primary key. A row is dropped only when it is positively
history: the sort key composed from its own stored composites
(`liveSkOf(toDomainView(composites))`, only those attributes unmarshalled) is not
its stored sort key, or they don't compose, AND the sort key has a history
layout — `#v#[…#]<7+ digits>` (retain), `#deleted#[…#]<ISO timestamp>`
(softDelete), or `#e#` after the live key less its composites (timeSeries). A
live row composes its own key, whatever its values or collection names hold
(`#e#`, `deleted`, `v`); a row the current composer can't reproduce (an unpadded
number composite written by 1.15, a row missing a composite) isn't history-shaped
and is read as on main. `Entity._liveRows` (for `retain` / `softDelete` /
`timeSeries`; otherwise `undefined`, and the query is unchanged) drives
`Query`'s `liveRows`, applied to rows as they arrive, before decoding or
counting, for queries and scans alike (a query can't name a key attribute in its
filter, and a scan's filter saves no read capacity). `limit` is sent as `Limit`
on the first request and each later request asks for twice the last
(`computeRequestLimit`, capped at 100,000; `pageSize`, when set, is used as is):
a run of `n` history rows costs about `log2(n / limit)` requests, where sending
the remainder cost one per row for `limit(1)`. The surplus past `limit` is
discarded and the cursor rebuilt from the last item returned. `maxPages` still
bounds requests, so a capped query can return fewer items than `limit` when rows
are dropped — as with a filter. `count()` reads only the sort key and composites (`reads`) of each row; a
`select` also reads them. A collection on the primary key judges each row by the member its
`__edd_e__` names. GSI queries never meet history rows, which carry no index keys.

### Policy-Aware GSI Composition (update & append)

**Problem.** GSI composite attributes can be owned by different writers ("hybrid GSIs"). A device-ingest writer owns `alertState` + `timestamp`; an enrichment writer owns `accountId`. A GSI with `pk.composite = [accountId]` and `sk.composite = [alertState, timestamp]` is touched by *every* ingest event (via `timestamp`), but the ingest writer can't supply `accountId` without an extra read. The library needs a way to express what an *update payload* means for the GSI's stored keys when only some composites are in scope — *and* must not let one writer's update silently corrupt the half another writer owns.

**Mental model — three contracts.** v1.7.1 expresses GSI maintenance as three independent contracts, all per-half:

> `'preserve'` is a contract with **other writers** ("don't disturb my key when you fire"); `'sparse'` is a contract with **yourself as the half's owner** ("drop my key if I touch this half but can't compose it"); `Entity.remove([attr])` is the explicit signal that a composite is gone — the library REMOVEs the half(s) containing the cleared attribute.

Per-half is the unifying property: declaration (`{ pk, sk }`), evaluation gate (per-half "touched?"), outcome (per-half SET / noop / REMOVE), and cascade (per-half via `removedSet`). There is no GSI-wide cascade left in the model — the v1.6 holdover that bug-1 of v1.7.0 inherited is gone.

**API — per-half policy declaration (unchanged from v1.7.0).** Each GSI may declare an `indexPolicy`:

```ts
indexes: {
  byAccountAlert: {
    name: "gsi6",
    pk: { field: "gsi6pk", composite: ["accountId"] },
    sk: { field: "gsi6sk", composite: ["alertState", "timestamp"] },
    indexPolicy: { pk: "preserve", sk: "sparse" },
  },
}
```

- `indexPolicy: { pk: 'sparse' | 'preserve', sk: 'sparse' | 'preserve' }`. Both halves default to `'preserve'` if `indexPolicy` is omitted entirely or either half is unspecified.
- The standard composition path has no per-composite information to discriminate within a half (it has only the merged payload, no read-before-write), so per-attribute policy callbacks were removed. A half is a single concatenated string; per-attribute mixing within a half has no coherent semantic.

**Two-way payload classification.** v1.7.x collapses the v1.6 three-way classification (present / explicit-clear / omitted) to two states: **present** or **absent**. `attr: null`, `attr: undefined`, and "key omitted from payload" all mean "absent" for GSI-composition purposes.

| Payload state | What library does for GSI composition |
|---|---|
| `attr: <value>` | Use the value as a slot in the composed half |
| `attr: null` *or* `attr: undefined` *or* (key omitted) | Treat as **absent** — the composition is built from the leading prefix of present values |

`set({ attr: null })` is no longer a separate "drop signal" — it still REMOVEs the attribute from the item (the data-attribute REMOVE clause), but it does not separately cascade-drop the GSI keys. Drop-via-cascade goes through `Entity.remove([attr])` instead.

**Per-half evaluation gate (reframed in v1.7.3 as a skip-predicate — closes the empty-composite-half regression AND the class of degenerate-case bugs).** Before the structural rule even runs, each half is asked a single question: *can this half be safely skipped?* The answer depends only on whether the gate's purpose — **multi-writer protection** — actually applies. **If the half cannot be safely skipped, it is evaluated** (SET / REMOVE / noop per the structural rule). The skip predicate is:

```ts
// Skip evaluation only when multi-writer protection actually applies:
// composites exist (otherwise the half value is a constant prefix and
// nothing to multi-writer-clobber); no explicit removal of one of the
// half's composites; AND every composite is absent from both
// `updatePayload` and `keyRecord` (so this writer is genuinely not
// claiming ownership of this half on this call).
const shouldSkip =
  halfComposites.length > 0 &&
  !halfHasRemoved &&
  halfComposites.every((c) => !(c in updatePayload) && !(c in keyRecord))
if (shouldSkip) continue  // leave this half's key attribute alone
```

**Why a skip-predicate, not a touched-predicate.** The gate exists for exactly one reason: to prevent writer A from clobbering keys for halves it doesn't own when writer B does own them. The skip-predicate states that purpose *directly* — "skip iff multi-writer protection applies." Every degenerate case (empty composite list, composites entirely entity-PK, future shapes) negates `shouldSkip` for an obvious structural reason, without enumeration. The pre-v1.7.3 framing inverted the question into a "touched" predicate that had to enumerate every case for which the half *should* be evaluated as a chain of `||` clauses. Each missed shape required another tactical patch — v1.7.1 missed the multi-writer leak (#41), v1.7.2 missed the PK-composites-only case (#43), v1.7.2 then missed the empty-composite-half case (#46). The skip-predicate closes the entire class structurally.

**Walk-through against the canonical shapes.** Each row confirms the skip-predicate is observably equivalent to the cumulative tactical fixes of v1.7.1 + v1.7.2 (no API changes, no behavior changes for existing inputs):

| Half shape | Skip-predicate evaluates to | Outcome |
|---|---|---|
| Empty composite list (`composite: []`) | `length > 0` is false → not skipped | Evaluate (constant entity prefix) — **closes #46** |
| PK-composites-only (composite ∈ keyRecord) | `every(NOT in keyRecord)` is false → not skipped | Evaluate (idempotent SET) — preserves v1.7.2 #43 fix |
| Multi-writer not touching this half | `every()` true, length > 0, !hasRemoved → **skipped** | Skip — preserves v1.7.1 #41 fix |
| Caller asserts authority via payload | `every(NOT in payload)` is false → not skipped | Evaluate |
| Caller invalidates via `Entity.remove([...])` | `!halfHasRemoved` is false → not skipped | Evaluate (cascade override may apply under preserve) |

**Why two input sources are checked, not one** (preserved from v1.7.2). Composites that are *also* entity primary-key composites (e.g. `byChannel: { pk: [channel], sk: [deviceId] }` on an entity with `primaryKey: [channel, deviceId]`) arrive through `keyRecord`, never through `updatePayload` — the writer addresses the row by key, doesn't restate those values in `.set({...})`, and `.append()` deliberately separates structural fields from the SET clause. The skip-predicate's `every(NOT in keyRecord)` clause keeps these halves un-skipped on every write, and the structural rule composes them from the always-present PK values. The behavior is idempotent — re-SETting the same composed value from immutable PK composites produces the same key — so there is no multi-writer regression (see issue #43).

**Why empty composite lists are always evaluated** (NEW in v1.7.3). For a half with `composite: []`, the value is a *constant* (the bare entity / collection prefix). There is nothing for another writer to clobber, no per-call decision to make. The pre-v1.7.3 touched-predicate's `.some(...)` clauses all returned `false` for an empty array, classifying the half as untouched and skipping it — leaving items with one half SET and the other missing, invisible to the GSI (#46). The skip-predicate's leading `length > 0` guard short-circuits before `every()` is ever asked, and `classifyHalf` (which already correctly handled empty composites as `{ kind: 'set', length: 0 }`) is finally reached.

**Why `'sparse'` still works.** `'sparse'`'s contract is *with the half's owner*. If the writer doesn't touch any of the half's composites — neither in payload, nor through the key-addressed structural inputs — they aren't claiming ownership of this half on this call, and the skip-predicate skips the half exactly as before. The writer-scope concept #38 was reaching for falls out of payload-plus-keyRecord contents naturally.

**Design history.** The gate has been progressively reframed:
- **v1.7.0** introduced the touched-predicate (`updatePayload`-only) to fix the v1.6 GSI-wide cascade. Closed the original GSI-blast bug but introduced the multi-writer leak.
- **v1.7.1** added the `removedSet` arm to the touched-predicate (multi-writer leak fix — #41).
- **v1.7.2** added the `keyRecord` arm to the touched-predicate (PK-composites-only fix — #43).
- **v1.7.3** reframed the predicate from "list cases for which to evaluate" to "list the single condition for which it is safe to skip." Closes the empty-composite-half regression (#46) and any future degenerate-case gap of the same shape — the skip predicate's negation is the cumulative `||`-chain of all prior tactical fixes plus the structural `length === 0` short-circuit.

End-to-end consequences for the canonical multi-writer scenarios:

- **Stamps** writing only `{ published: {...} }` → no half is touched → both halves left alone. (v1.7.0 bug-3: stamps blew away the sparse GSI half.)
- **Enrichment** writing `{ accountId: 'X' }` → pk touched, sk untouched.
- **Telemetry** writing `{ alertState: 'active', timestamp: T }` → pk untouched, sk touched.

**Structural composition — longest valid leading prefix.** For each *touched* GSI half (PK and SK independently), walk the composite list left-to-right and build the longest valid leading prefix from values present in the merged record (`{ ...storedKeyAttrs, ...payload }`). The rule is **identical for PK and SK** — the v1.6 PK-clear-degrades-to-sparse asymmetry is gone. The composed half is whatever's reachable from the leading run of present composites:

| Composite state | Result for that half |
|---|---|
| All composites present | Recompose the half with all values |
| Trailing composites absent (`[A, B, _, _]`) | Truncate to the leading prefix `[A, B]` |
| Whole half empty (no leading composites available) | Empty prefix → can't compose → see "Per-half outcome" below |
| Hole pattern (`[A, _, C]`) | Leading prefix is `[A]`, but a present trailing composite would be silently dropped → treated as can't-compose → see "Per-half outcome" below |

Truncation on PK is the same hierarchical demotion as truncation on SK — an item with `pk.composite = ['accountId', 'fleetId']` and `fleetId` cleared composes a partition key of `account#A` and stays queryable at the account scope.

#### Per-half outcome (unified can't-compose rule — NEW in v1.7.1)

When the structural rule **can compose** (leading prefix is non-empty and there's no information loss), the half SETs its key — full or truncated. When the structural rule **can't compose** (empty leading prefix, OR a hole pattern that would lose trailing data), the per-half outcome is decided by policy + cascade:

| Outcome for this half's key attribute | Conditions |
|---|---|
| **noop** (leave key alone) | No composite of this half is in the payload AND none in `Entity.remove([...])` (per-half evaluation gate) |
| **SET full** | Half touched + all composites have values supplied in payload (or available from primary key) |
| **SET truncated** to leading prefix | Half touched + some leading composites have values, trailing absent, no hole |
| **REMOVE** this half's key | Half touched + can't compose (empty leading prefix OR hole pattern), AND policy is `'sparse'` OR a composite is in `Entity.remove([...])` (cascade override) |
| **noop** (stored key may go stale) | Half touched + can't compose, policy is `'preserve'`, AND no composite is in `Entity.remove` |

The roll-up is **per-key** — each half's outcome applies to its own key attribute only. PK dropping doesn't drop SK; SK dropping doesn't drop PK. The item may be invisible in the GSI during a single-half-dropped period (DDB needs both for projection — the projection invariant for readers, not a library behavior), but the surviving half's value persists. When the missing half is later composed again (e.g. telemetry writes `{ alertState, timestamp }` after a clear), the item rejoins the GSI under the still-current other half *without that other writer needing to re-fire* — the v1.7.0 multi-writer bug (#41 bug-1) is closed.

**Cascade override under preserve.** If a half is touched via `removedSet` AND the structural rule would no-op (preserve + can't-compose), the outcome is **REMOVE** instead of noop. Rationale: the consumer's explicit signal trumps stale-data preservation. `Entity.remove(["alertState"])` means "alertState is gone" — preserving the stored key with a value derived from the now-removed composite would lie to readers.

**Hole patterns collapse into can't-compose.** A "hole" (composite at position `i` absent while a composite at position `j > i` is present, e.g. `[A, _, C]`) was a separate v1.7.0 outcome (truncate-or-throw). Under v1.7.1, holes follow the unified rule above: drop under sparse (or cascade), noop under preserve (no cascade). The previous "truncate to `[A]` and silently discard `C`" sparse behavior is gone — silent data loss is a worse failure mode than dropping the half. The previous "throw EDD-9024 under preserve" is also gone — see "EDD-9024 deprecation" below.

#### The set/remove asymmetry (worth memorising)

> `set` provides values to compose with. `remove` invalidates a composite without providing a replacement. The library has no read-before-write — so `remove` without surrounding `set` context can't truncate (it doesn't know what to truncate *to*) and instead REMOVEs the half's key entirely.

```ts
// Truncation works — surviving composites supplied via set, leaf invalidated via remove
update(key).set({ region, country, city }).remove(["site"])
// → SET gsi1sk truncated to "region#APAC#country#AU#city#Sydney"
// (site is in removedSet → cascade override fires, but the surviving leading prefix
// is non-empty → the structural rule composes the prefix and SETs.)

// REMOVE — surviving composites not supplied
update(key).remove(["site"])
// → REMOVE gsi1sk entirely (library has no way to compose [region, country, city] —
// no values supplied, and the library does not read-before-write to discover them).
```

If you want to demote (truncate) hierarchically, you must `set` the surviving composites in the same call — the `remove` invalidates the leaf without providing a replacement, but the surviving prefix is non-empty so the structural rule composes it.

#### How to drop a half (call-site syntax)

| Composite is... | Method 1: `Entity.remove` (always works) | Method 2: `set` with `undefined` |
|---|---|---|
| **Required** in model (e.g. `Schema.String`) | `update(key).remove(["composite"])` | not available — TS rejects `undefined` for non-optional fields under `exactOptionalPropertyTypes` (the v1.7.0 NullishOr revert ensures payload types match model declarations) |
| **Optional** in model (`Schema.optional(...)`) | `update(key).remove(["composite"])` | `update(key).set({ composite: undefined })` |

Naming any one composite of a half is enough — `Entity.remove` doesn't require enumerating all of them. `null` is **never** valid for a composite (EDD-9025 rejects nullable composites at make-time).

**`Entity.remove([attr])` cascade is per-half (NEW in v1.7.1).** When an update's REMOVE list contains a composite attribute, the cascade applies only to the half(s) containing that attribute. Other halves follow the per-half evaluation gate (untouched → noop). v1.7.0 issued a GSI-wide REMOVE (both `gsiNpk` AND `gsiNsk`) for any composite in the cascade set — that GSI-wide blast radius is gone in v1.7.1. The cascade still REMOVEs the affected half's key; if the surrounding `set` provides values for the half's other composites, the structural rule may still compose a truncated prefix (see set/remove asymmetry).

**Decision algorithm (per GSI).** Given merged record `M = { ...storedKeyAttrs, ...payload }` (treating `null`/`undefined` payload values as absent) and `removedSet` (composites named in `Entity.remove([...])`):

For each half (PK then SK), independently:

1. **Evaluation gate (skip-predicate, v1.7.3).** If the half's composite list is non-empty AND no composite is in `removedSet` AND every composite is absent from both `payload` AND `keyRecord` → `noop` (skip — leave the stored key untouched, multi-writer protection applies). Otherwise the half is **evaluated**, continue. (Halves with empty composite lists short-circuit on the leading `length > 0` guard and are always evaluated — the value is a constant prefix; closes #46. Halves whose composites are entity-PK composites fail the `every(NOT in keyRecord)` clause and are always evaluated — the structural rule composes idempotently from the immutable PK values; closes #43.)
2. **Structural rule.** Walk the half's composite list left-to-right. Find the longest leading prefix of present values in `M`.
3. **Classify the outcome:**
   - **Compose succeeded** (leading prefix is non-empty AND no hole — i.e. all absent composites are at trailing positions): SET this half's key from the leading prefix. Done.
   - **Compose failed** (empty leading prefix OR hole pattern):
     - Policy is `'sparse'` → `REMOVE` this half's key.
     - Policy is `'preserve'` AND any composite of this half is in `removedSet` → `REMOVE` this half's key (cascade override).
     - Policy is `'preserve'` AND no composite of this half is in `removedSet` → `noop` (preserve preservation; stored key may be stale).

The two halves' outcomes are emitted independently — each affects only its own key attribute. There is no GSI-wide cascade. There is no longer a `'hole-throw'` outcome (EDD-9024 deprecated — see below).

**`put()` semantics (unchanged).** `put()` does not consult `indexPolicy`. It writes a complete item from scratch — any missing composite means "this item is not in that GSI." The existing `tryComposeIndexKeys` path (omit GSI keys when any composite is absent) is preserved as-is. `indexPolicy` exists specifically to resolve the update/append ambiguity, not the put case.

**`.append()` semantics (time-series) — same composer.** v1.7.x unifies append with update. `.append(input)` calls the same composer as `.update(...).set(...)`, with the encoded append input passed as both `updatePayload` and `keyRecord` (v1.7.2 — see #43; the v1.7.1 path filtered PK composites out of the payload, which combined with the gate-only-checks-payload bug to skip GSI evaluation for any half whose composites are entirely entity-PK composites). Composites outside `appendInput` are simply absent under the structural rule, and the per-half evaluation gate applies — halves whose composites are entirely outside `appendInput` AND outside the entity primary key are untouched and follow the noop branch.

For sparse GSIs whose composites are entirely outside `appendInput` *and* outside the entity primary key, the half is untouched → noop. Sparse only fires when the writer touches the half but can't compose it. This matches the consumer-side multi-writer recommendation: sparse GSIs should be single-writer-per-half by design.

For GSIs whose composites are entirely entity-PK composites (e.g. `byChannel: { pk: [channel], sk: [deviceId] }` on a Telemetry entity with `primaryKey: [channel, deviceId]`), the half is always touched (PK composites are always in `keyRecord`), the structural rule always composes from the immutable PK values, and the resulting SET is idempotent — every write re-emits the same composed key. This is the correct behavior; v1.7.0 / v1.7.1 silently skipped these GSIs, leaving items invisible to channel-scoped queries.

**EDD-9024 (`CompositeKeyHoleError`) deprecation — runtime-irrelevant in v1.7.1.** The class is kept exported for back-compat but no longer thrown at runtime. The original v1.7.0 throw protected against an `[A, _, C]` shape under preserve — but the type system already catches the only "wrong" case the throw was guarding (required composites can't be omitted under `exactOptionalPropertyTypes` since v1.7.0 reverted the NullishOr widening; the only legitimate runtime hole is "optional leading composite absent + present trailing composite," which is a normal write the consumer expressly chose). Under v1.7.1 the hole pattern collapses into the unified can't-compose rule (drop under sparse, noop-or-cascade-override under preserve). See `Errors.ts` for the deprecation note on the class.

**EDD-9025 — composite attribute schemas must not include `null` (unchanged from v1.7.0).** At `Entity.make()` time, the library walks every composite across `primaryKey`, every entry in `indexes`, and every entry in `unique` constraints. For each composite attribute it inspects the field's Schema AST and throws `CompositeNullableError` (EDD-9025) if `null` is reachable in the type union (`Schema.NullOr`, `Schema.NullishOr`, `Schema.Union` with a Null branch, custom-named field via `DynamoModel.configure({ field })` rename that resolves to a nullable schema, etc.).

The semantic justification: composites participate in string composition (`acc#X#alert#Y`); null is not a meaningful slot value, only present-with-value or absent. Allowing `Schema.NullOr` on a composite would let `set({ composite: null })` typecheck and then immediately blow up at runtime (or worse, silently produce a key with the literal string `"null"` as a slot). EDD-9025 catches this at make-time.

The sparse pattern is still expressible — use `Schema.optional(...)`, which produces `T | undefined` (without `null`). `undefined` is "absent" under the two-way classification.

**Footgun closed at the type level — `set({ composite: null })` no longer compiles.** Two changes work together (both shipped in v1.7.0, kept in v1.7.1):

1. v1.7.x reverts the v1.6 update-payload type widening. The v1.6 schemas wrapped each update field in `Schema.optional(Schema.NullishOr(field))` to support the v1.6 three-way classification. v1.7.x drops the `NullishOr` wrap — update payload field types match the model's declarations exactly (just wrapped in `Schema.optional` so the key can be omitted entirely).
2. EDD-9025 prevents the model from declaring a composite as nullable in the first place.

Together: `set({ composite: null })` for a composite is a TypeScript error two ways — the model can't widen the composite to include `null`, and the update payload type isn't widened beyond the model. The stale-GSI-keys-via-`set-null` concern dissolves entirely at the type level. No runtime path is reachable from typed callers.

**Migration of v1.6 `set({attr: null})` patterns:**

```typescript
// v1.6 — `null` in payload was a per-composite drop signal under sparse,
// or an SK-truncate signal under preserve.
entity.update(key).set({ alertState: null }).asEffect()

// v1.7.x — explicit per-attribute drop (per call) — atomic remove + per-half cascade.
entity.update(key).remove(['alertState']).asEffect()

// v1.7.x — for "drop when this index has nothing to compose" intent,
// declare the GSI half as 'sparse' once at the index definition.
//   indexPolicy: { pk: 'sparse', sk: 'preserve' }
// Then any update that touches the PK half but can't compose it drops the GSI
// implicitly per the per-half can't-compose rule.
```

**Final per-half decision table (worked).** GSI with `pk.composite = [accountId]`, `sk.composite = [alertState, timestamp]`, `indexPolicy = { pk: 'preserve', sk: 'sparse' }`. Item exists with `accountId = "acme"`, `alertState = "active"`, `timestamp = T1`. Both GSI keys composed.

| Writer | Payload | pk outcome | sk outcome | Item state in GSI |
|---|---|---|---|---|
| Enrichment | `{ accountId: "newAcct" }` | touched, SET full → new `account#newAcct` | untouched, **noop** | Visible under new account, sk position unchanged |
| Telemetry (active alert) | `{ alertState: "active", timestamp: T2 }` | untouched, **noop** | touched, SET full → fresh `alert#active#ts#T2` | Visible under last accountId, fresh sk |
| Telemetry (no alert) | `{ timestamp: T2 }` (alertState omitted, optional) | untouched, **noop** | touched (timestamp), can't compose (hole) + sparse → REMOVE gsi1sk | Invisible (DDB needs both); gsi1pk preserved |
| Telemetry (explicit clear) | `{ alertState: undefined, timestamp: T2 }` | untouched, **noop** | touched, can't compose + sparse → REMOVE gsi1sk | Same — invisible, gsi1pk preserved |
| Stamp (unrelated write) | `{ published: {...} }` | untouched, **noop** | untouched, **noop** | Unchanged — both halves preserved (the v1.7.0 leak is closed) |
| Telemetry (rejoin) | `{ alertState: "active", timestamp: T3 }` | untouched, **noop** | touched, SET full → `alert#active#ts#T3` | Re-visible under preserved gsi1pk + new gsi1sk; **no enrichment re-fire needed** |
| Hierarchical demote (different entity, multi-composite SK) | `update(key).set({ region, country, city }).remove(["site"])` | n/a | touched (set + removedSet), trailing-absent → SET truncated to `region#APAC#country#AU#city#Sydney` | Re-indexed at city scope |
| Explicit drop | `update(key).remove(["alertState"])` | untouched, **noop** | touched (alertState in removedSet), can't compose → REMOVE gsi1sk | Invisible; gsi1pk preserved (per-half cascade — no longer GSI-wide) |
| Cascade override under preserve | `{ pk: "preserve" }`, `update(key).remove(["accountId"])` (no surviving pk composites) | touched (cascade), can't compose + preserve + cascade override → REMOVE gsi1pk | untouched, **noop** | gsi1pk REMOVE'd via cascade override |

**Multi-writer entity design rule.** Each GSI half should be entirely owned by a single writer's domain. The library does not paper over cross-writer composite ownership — that's a consumer-side modeling discipline. With v1.7.1's per-half evaluation gate and the per-half outcome rule, a writer that doesn't touch a GSI's composites simply doesn't supply them, and the half no-ops regardless of policy. There is no per-composite leakage across writers like in v1.6, and no GSI-wide blast radius like in v1.7.0.

**v1.7.0 / v1.7.1 → v1.7.2 PK-composites-only regression callout (closes #43).** The v1.7.1 per-half gate consulted only `updatePayload`. For GSI halves whose composites are entity-PK composites (a common pattern: tenant-scoped queries, entity-key-projected GSIs), `updatePayload` never carried those composites — the writer addresses the row by key, never restates them in `.set({...})`, and `.append()` further filtered PK composites out of its payload before passing to the composer. The gate saw both halves as untouched, skipped GSI evaluation entirely, and never wrote `gsiNpk` / `gsiNsk`. Items written under v1.7.0 / v1.7.1 against such GSIs were invisible to GSI queries. v1.7.2 fixes this in two places: the gate now also counts `keyRecord` membership (so PK composites carried alongside the payload count as "touched"), AND `Entity.append()` no longer filters PK composites out of the payload it passes to the composer (the filter never solved a real problem and silently broke this pattern). Affected items repair on the next `Entity.update()` against them — the gate now fires, the structural rule composes the immutable PK values, and the missing GSI keys are SET. No data migration needed; reads via the GSI start returning these items as their next update lands.

**v1.7.0 / v1.7.1 / v1.7.2 → v1.7.3 empty-composite-half regression callout (closes #46).** The v1.7.2 per-half gate, even after the `keyRecord` broadening, still classified halves with **empty composite lists** as untouched. For an empty composite array, every `.some(...)` clause in the touched-predicate trivially returned `false`, so `Entity.update()` would skip composing the half entirely — leaving items with one half SET and the other missing, invisible to the GSI. The shape is common in single-table designs: a sparse "lookup" GSI like `byDeviceBinding: { pk: [deviceBinding], sk: { composite: [] } }` writes the SK as a constant entity prefix on every visible item, but the v1.7.0–v1.7.2 gate skipped the SK because no composite of an empty list can be "in" anything. v1.7.3 reframes the gate as a skip-predicate keyed on the gate's actual purpose (multi-writer protection); the leading `length > 0` guard short-circuits empty-composite halves to *always evaluated*, and `classifyHalf` (which already handled empty composites correctly as `{ kind: 'set', length: 0 }`) is finally reached. Affected items repair on the next `Entity.update()` — the next write composes the missing half from the constant prefix and the item rejoins the GSI. No data migration needed.

#### Canonical GSI-composite test-fixture shapes

Test coverage for the policy-aware composer must span the canonical GSI-composite shapes. Missing one shape (as the v1.7.1 fixture matrix did with PK-composites-only, and as the v1.7.2 matrix did with empty-composite halves) leads to consumer-facing regressions. Future work in this area must verify each shape:

1. **Multi-writer GSI** — composites split across writers (e.g. enrichment-owned PK composite + telemetry-owned SK composites). The per-half gate must skip halves the current writer doesn't touch. Anchor scenario for the `'preserve'` contract.
2. **PK-composites-only GSI** — composites entirely subset of the entity primary key (e.g. `byChannel: { pk: [channel], sk: [deviceId] }` on `primaryKey: [channel, deviceId]`). The per-half gate must fire via `keyRecord` membership and SET on every write. Regression scenario for #43 — must be present in unit, entity-level integration, and connected suites.
3. **Hierarchical GSI** — composites form a parent → child hierarchy (e.g. `[region, country, city, site]`). The structural rule must truncate via `set({ parents }).remove(["leaf"])`.
4. **Hole pattern GSI** — optional leading composite + present trailing composite. Must collapse into the unified can't-compose rule (drop under `'sparse'`, noop or cascade-override under `'preserve'`).
5. **All composites mutable** — every composite is a non-PK model field, and `appendInput` / update payloads carry them all. The standard case; the gate fires through the payload.
6. **Empty-composite half** — at least one half is `composite: []` (e.g. `byDeviceBinding: { pk: [deviceBinding], sk: { composite: [] } }`, common in single-table designs where the SK is just the entity prefix). The per-half gate must always evaluate the empty half (the value is a constant prefix; multi-writer protection does not apply). Regression scenario for #46 — must be present in unit, entity-level integration, and connected suites.

### Sparse Map Storage (`storedAs: DynamoModel.SparseMap()`)

> **Naming disambiguation.** This "sparse" is the *Sparse Map storage primitive* — flattening a logical `Record<K, V>` into per-entry top-level attributes. It is unrelated to the `'sparse'` value of `indexPolicy` (§7 Policy-Aware GSI Composition), which controls whether an *update* drops a *GSI* membership when an entire half's composites are absent. The two were spelled the same in 1.5.0 and that spelling collision was a frequent source of confusion. 1.6.0 renames the storage opt-in to `DynamoModel.SparseMap()` (a typed callable) so the two concepts no longer share a string.

**Problem.** A logical `Record<K, V>` field on a domain model maps awkwardly to DynamoDB. Stored as a single Map (`M`) attribute, every entry must be addressed via nested-Map syntax (`metrics.2026-01.views`) — which requires the parent attribute to exist. There is no `if_not_exists()` ergonomic for adding the first entry to an empty map: concurrent writers race to create the parent, and a fresh item demands a read-modify-write to materialise it.

**Solution.** A field annotated `storedAs: DynamoModel.SparseMap()` is *flattened* — each map entry becomes a top-level DynamoDB attribute named `<prefix>#<key>`. Each entry is independently addressable; no parent ceremony is required.

`metrics: Record<string, { views: number; clicks: number }>` storing `{ "2026-01": { views: 5, clicks: 2 } }` is laid out on disk as:

```
metrics#2026-01 = M { views: N(5), clicks: N(2) }
```

A counter `Record<string, number>` is even simpler — the bucket attribute *is* the scalar:

```
totals#2026-01 = N(1)
```

**One level deep.** Sparseness is *exactly* one layer. The value at each entry is a normal DynamoDB attribute (scalar, `M`, `L`, `SS`, `NS`). Nested sparse Records are rejected at `Entity.make()` time.

#### Configuration

```ts
class Page extends Schema.Class<Page>('Page')({
  pageId: Schema.String,
  metrics: Schema.Record({
    key: Schema.String,
    value: Schema.Struct({ views: Schema.Number, clicks: Schema.Number }),
  }),
  totals: Schema.Record({ key: Schema.String, value: Schema.Number }),
}) {}

const PageModel = DynamoModel.configure(Page, {
  metrics: { storedAs: DynamoModel.SparseMap() },
  totals: { storedAs: DynamoModel.SparseMap({ prefix: 't' }) }, // optional prefix override
})
```

- `storedAs: DynamoModel.SparseMap(options?)` is only valid on a `Schema.Record` field (validated at `make()`). The callable form lets options like `prefix` (and any future options such as `trackKeys`) live inside the SparseMap declaration where they belong, rather than as siblings on `ConfigureAttributes` that are only meaningful when paired with the right `storedAs` value.
- `prefix` defaults to the field name. Distinct sparse fields must have distinct prefixes; prefixes must not collide with non-sparse top-level attribute names.
- Inner value schema can be any DynamoDB-native shape (scalar / `Schema.Struct` / `Schema.Array` / `Schema.Set`). Nested `storedAs: DynamoModel.SparseMap()` is rejected.
- Sparse fields **cannot** participate in primary-key composites, GSI composites, or unique constraints — keys are not statically known at `make()` time.

#### Wire format

| Domain | Storage |
|---|---|
| `{ pageId: 'p1', metrics: {} }` | `pk, sk, __edd_e__, pageId` (no `metrics#*` attrs) |
| `{ ..., metrics: { '2026-01': { views: 5, clicks: 2 } } }` | `..., metrics#2026-01 = M { views: 5, clicks: 2 }` |
| `{ ..., totals: { '2026-01': 1, '2026-02': 3 } }` | `..., totals#2026-01 = 1, totals#2026-02 = 3` |

The `#` delimiter matches the rest of the library's key-composition convention. Keys flow through `ExpressionAttributeNames` aliasing in every read/write path so there is no lexical collision risk with user attributes. **User keys must not contain `#`** — validated at write time with a clear error (no silent escaping).

#### Reads — transparent

`get`, `query`, `scan`, batch, and stream paths all rebuild the domain `Record<K, V>` from flattened attributes by walking the marshalled item once and grouping attributes matching `<prefix>#*`. Domain consumers see the field as a normal Record.

#### Writes — record-style (whole-bucket replace)

```ts
db.entities.Pages.update({ pageId: 'p1' })
  .set({ metrics: { '2026-01': { views: 5, clicks: 2 } } })
```

Compiles to **one `SET` per bucket**. The above produces `SET #m_2026_01 = :map` (one clause). For a payload of N buckets the UpdateExpression has N `SET` clauses. There is no leaf-merging within a bucket — the whole bucket value replaces.

- Concurrent writes to **different** buckets are safe.
- Concurrent writes to the **same** bucket race (last-write-wins on that bucket).
- For finer-grained merge within a bucket, drop to path-style.

`null` in record-style input is **NOT** interpreted as REMOVE. Removal is always explicit via `removeEntries`. The `null`-as-REMOVE shortcut is too footgunny — a domain model that genuinely uses `null` as a value would lose data on every write.

#### Writes — path-style (per-leaf within a bucket)

```ts
// Counter — bucket attribute IS the scalar; works on a fresh item with no parent ceremony.
db.entities.Pages.update({ pageId: 'p1' })
  .pathAdd((t) => t.totals.entry('2026-01'), 1)
  // → ADD totals#2026-01 :1

// Inner-field update on a struct bucket — uses native DynamoDB nested-Map syntax.
db.entities.Pages.update({ pageId: 'p1' })
  .pathAdd((t) => t.metrics.entry('2026-01').views, 1)
  // → ADD metrics#2026-01.views :1
```

`PathBuilder<Model>` exposes `.entry(key)` on sparse Record fields, returning a path typed by the inner value schema. The path compiles to `<prefix>#<key>` for the bucket itself, and `<prefix>#<key>.<field>` for nested-Map field access using DynamoDB's native nested-map syntax. `ExpressionAttributeNames` aliasing handles the `#` literal.

**Caveat.** Nested-Map operations on inner fields (`metrics#2026-01.views`) require the bucket attribute to exist. Use record-style for new buckets, path-style for buckets known to exist. This mirrors DynamoDB's native semantics — the library does not paper over it.

For scalar-valued sparse maps (counter use case), there is no inner field — the bucket attribute itself is the scalar, so `ADD totals#2026-01 :1` works on a fresh item with no parent. This is the headline win.

#### Removal — explicit

```ts
db.entities.Pages.update({ pageId: 'p1' }).removeEntries('metrics', ['2026-01', '2026-02'])
// → REMOVE metrics#2026-01, metrics#2026-02
```

Compiles to a single `REMOVE` clause per call. Removing an entry that does not exist is a no-op (DynamoDB's REMOVE semantics).

#### Clearing — `clearMap(field)`

DynamoDB has no `REMOVE prefix#*` syntax, and the library does not statically know which bucket keys exist. `clearMap` is a **two-op helper**, presented as a single API call:

1. `GetItem` (consistent read, projection narrowed to the prefix where possible — falls back to full item)
2. `UpdateItem` with an explicit `REMOVE <prefix>#k1, <prefix>#k2, ...` clause derived from the read

```ts
db.entities.Pages.update({ pageId: 'p1' }).clearMap('metrics')
```

`clearMap` **chains** with other update combinators — the REMOVE list folds into the same `UpdateItem` that performs other SETs/ADDs:

```ts
db.entities.Pages.update({ pageId: 'p1' })
  .clearMap('metrics')
  .set({ status: 'reset' })
  .expectedVersion(7)
// → 1 GetItem + 1 UpdateItem (REMOVE metrics#... + SET status, with version condition)
```

**Race window.** Between read and update, a concurrent writer may add a new bucket. The new bucket survives the clear.

- For `versioned: { retain: true }` entities, the existing optimistic-lock CAS closes the race automatically — clear fails on stale version, retry resolves.
- For non-versioned entities, clear is **best-effort** (documented). If atomic clear is critical for a non-versioned entity, the user can read+update at the call site or opt into versioning.

A future enhancement (out of scope) could add an opt-in sidecar keys-set (`storedAs: { kind: 'sparse', trackKeys: true }`) to make clear a single op. The per-write attribute overhead isn't worth paying by default.

#### Conditional ops

`attribute_exists(<prefix>#<key>)` and `attribute_not_exists(<prefix>#<key>)` work natively because each entry is a top-level attribute. Exposed via the path API:

```ts
db.entities.Pages.update({ pageId: 'p1' })
  .condition((t, { exists }) => exists(t.metrics.entry('2026-01')))
  .set({ status: 'updated' })
```

#### Lifecycle interactions

- **`versioned: { retain: true }`** — snapshots preserve flattened attributes verbatim.
- **`softDelete`** — GSI keys are stripped; sparse attributes are domain data and are **preserved**. Restore is a no-op for sparse attributes.
- **Unique constraints** — sparse fields cannot be referenced. Same reason as keys — composite values aren't known at `make()` time.
- **`timeSeries`** — sparse fields are aggregate state, not event state. They live on the **current item only** and are preserved across `.append()` (untouched, since they're outside `appendInput`). Event items (`#e#<orderBy>`) **DO NOT** carry sparse attributes — same treatment as enrichment fields outside `appendInput`. Per-event snapshots of aggregate state would multiply storage by `(events × sparse-keys)` — a real cost on long event streams (e.g. 10s heartbeats × 7d TTL ≈ 60K events per device) with no read-side benefit.

#### Constraints (enforced at `Entity.make()`)

| Code | Constraint |
|---|---|
| EDD-9020 | `storedAs: DynamoModel.SparseMap()` is only valid on `Schema.Record` fields. |
| EDD-9021 | Inner value schema must be DynamoDB-native; **nested sparse Records are rejected**. |
| EDD-9022 | Sparse fields cannot participate in primary key, GSI composites, or unique constraints. |
| EDD-9023 | Multiple sparse fields on the same entity must have distinct prefixes (and not collide with non-sparse attribute names). |

User key validation at write time (no error code — runtime `ValidationError`):

- Map keys must serialize to strings.
- Keys must not contain `#` (silent escaping rejected — explicit error wins).
- `<prefix>#<key>` must satisfy DynamoDB attribute-name rules (1–255 bytes after concatenation).

#### Worked example — counter

```ts
class Page extends Schema.Class<Page>('Page')({
  pageId: Schema.String,
  views: Schema.Record({ key: Schema.String, value: Schema.Number }),
}) {}
const PageModel = DynamoModel.configure(Page, { views: { storedAs: DynamoModel.SparseMap() } })

const Pages = Entity.make({
  model: PageModel,
  entityType: 'Page',
  primaryKey: {
    pk: { field: 'pk', composite: ['pageId'] },
    sk: { field: 'sk', composite: [] },
  },
})

// Create with no buckets — `views` is just absent on disk.
yield* db.entities.Pages.put({ pageId: 'p1', views: {} })

// First view — atomic counter on a fresh item, no parent-map dance.
yield* db.entities.Pages.update({ pageId: 'p1' })
  .pathAdd((t) => t.views.entry('2026-04'), 1)
// On disk: views#2026-04 = N(1)

// Concurrent writers to different months never race.
// Concurrent writers to the same month race (last-write-wins on the increment? no —
// ADD is atomic, so concurrent ADDs on the same bucket sum correctly. Concurrent SETs race.)

// Read — transparent rebuild.
const page = yield* db.entities.Pages.get({ pageId: 'p1' })
// page.views === { '2026-04': 1 }
```

#### Worked example — struct buckets with clear

```ts
class Page extends Schema.Class<Page>('Page')({
  pageId: Schema.String,
  status: Schema.String,
  metrics: Schema.Record({
    key: Schema.String,
    value: Schema.Struct({ views: Schema.Number, clicks: Schema.Number }),
  }),
}) {}
const PageModel = DynamoModel.configure(Page, { metrics: { storedAs: DynamoModel.SparseMap() } })
// versioned: { retain: true } makes clearMap atomic.
const Pages = Entity.make({
  model: PageModel,
  entityType: 'Page',
  primaryKey: { pk: { field: 'pk', composite: ['pageId'] }, sk: { field: 'sk', composite: [] } },
  versioned: { retain: true },
})

// Write an initial bucket.
yield* db.entities.Pages.update({ pageId: 'p1' })
  .set({ metrics: { '2026-04': { views: 100, clicks: 10 } } })
// On disk: metrics#2026-04 = M { views: 100, clicks: 10 }

// Atomic per-leaf update within a known bucket.
yield* db.entities.Pages.update({ pageId: 'p1' })
  .pathAdd((t) => t.metrics.entry('2026-04').views, 1)
// On disk: metrics#2026-04.views = 101

// Reset — two-op helper, atomic via the version CAS.
yield* db.entities.Pages.update({ pageId: 'p1' })
  .clearMap('metrics')
  .set({ status: 'reset' })
// → 1 GetItem + 1 UpdateItem (REMOVE metrics#2026-04 + SET status with version CAS)
```

### Hierarchical Key Truncation

Hierarchical truncation — leaf composites being *refinements* that should *demote* the item, not *evict* it — is the unifying property across many real-world domains:

| Domain | Composite hierarchy | Trailing-absent meaning |
|---|---|---|
| Geographic | `[region, country, city, site]` | Asset leaves a site but stays queryable at city/country/region |
| Org | `[division, department, team, squad]` | Engineer rotates off a squad, stays queryable at team/department/division |
| Workflow | `[stage, subStage, step]` | Approval step retracted; item stays queryable at parent stage |
| Content | `[category, subcategory, tag]` | Leaf tag dropped; item stays in subcategory listings |
| Permission | `[org, project, resource]` | Resource access lost; project-level access preserved |
| Order grouping | `[customerId, orderId]` | After clearing `orderId`, group-by-customer queries still work |
| Multi-tenant fleet | `[accountId, fleetId]` (PK) | Vehicle leaves a fleet but stays queryable at account scope |

Under v1.7.1 (§7), trailing-absent truncation is part of the structural composition rule — there is no separate "pruning" code path. A trailing-absent composite simply truncates the half to its leading prefix, regardless of which half (PK or SK). PK and SK behave identically. The set/remove asymmetry (§7) governs how to invoke truncation: `set` provides surviving composites; `remove` invalidates the leaf — both must appear in the same call for truncation to happen, otherwise `remove` alone REMOVEs the half (no read-before-write).

**Worked example — geographic asset hierarchy (PK + SK, both preserve).**

```ts
indexes: {
  byLocation: {
    name: 'gsi1',
    pk: { field: 'gsi1pk', composite: ['region'] },
    sk: { field: 'gsi1sk', composite: ['country', 'city', 'site'] },
    indexPolicy: { pk: 'preserve', sk: 'preserve' },
  },
}

// Initial state: asset is at /americas/us/sf/datacenter-1
// Stored: gsi1pk = "$app#v1#asset#region_americas",
//         gsi1sk = "$app#v1#asset#country_us#city_sf#site_datacenter-1"

// Asset leaves the datacenter — *demote* (truncate, stay queryable at city scope).
// Supply the surviving composites via `set` AND invalidate the leaf via `remove`
// in the same call — the structural rule then composes the truncated leading prefix.
yield* db.entities.Assets.update(key).set({ country: 'us', city: 'sf' }).remove(['site'])
// gsi1pk unchanged (region untouched — pk half is not in the payload).
// gsi1sk truncated to "$app#v1#asset#country_us#city_sf"
// begins_with(gsi1sk, "$app#v1#asset#country_us#city_sf") still finds this asset.

// Asset leaves the datacenter — *evict* (drop the whole half).
// Without surviving composites in `set`, the library can't compose anything;
// the cascade fires and REMOVEs gsi1sk entirely.
yield* db.entities.Assets.update(key).remove(['site'])
// pk untouched, noop. sk touched via removedSet, can't-compose (no surviving values),
// preserve + removedSet → cascade override → REMOVE gsi1sk.
// gsi1pk preserved; the per-half cascade no longer drops gsi1pk.

// PK-side demotion — multi-composite PK example.
//   pk.composite = ['accountId', 'fleetId']  (preserve on both halves)
// Vehicle leaves a fleet but stays under the account scope:
yield* db.entities.Vehicles.update(key).set({ accountId: 'acct-1' }).remove(['fleetId'])
// PK truncated to "$app#v1#vehicle#accountid_acct-1" — vehicle still queryable
// by account, just not by the prior fleet. Same hierarchical demotion as SK,
// applied symmetrically to PK.

// Decommission — drop from the index entirely (sparse policy on the relevant half),
// or use Entity.remove on every composite of the half to force the cascade.
yield* db.entities.Assets.update(key).remove(['country', 'city', 'site'])
// sk touched (multiple composites in removedSet), no surviving composites → REMOVE gsi1sk.
// pk untouched (no pk composites in removedSet), noop. Item invisible in the GSI
// (DDB projection rule: needs both keys), gsi1pk value retained for future rejoin.

// Hole pattern under preserve — collapses into can't-compose, no throw, no SET.
// `city` invalidated, `site` supplied → would compose `[country_us, _, site_dc2]` (hole).
// Per v1.7.1: hole = can't-compose; preserve + removedSet contains city → cascade
// override → REMOVE gsi1sk.
// yield* db.entities.Assets.update(key).remove(['city']).set({ country: 'us', site: 'datacenter-2' })
// → REMOVE gsi1sk. (v1.7.0 would have thrown EDD-9024; that throw is now deprecated.)

// Hole pattern under sparse — same outcome, REMOVE gsi1sk:
//   indexPolicy: { pk: 'preserve', sk: 'sparse' }
// yield* db.entities.Assets.update(key).remove(['city']).set({ country: 'us', site: 'datacenter-2' })
// → REMOVE gsi1sk. Note: site value is invalidated (data loss in the index) —
// this is the unified rule's "no silent partial composition on holes" intent.
```

---

## 8. Date & Time Handling

### Three-Layer Model

Every date field passes through three representations:

```
Wire (external)  →  decode  →  Domain (application)  →  encode  →  Storage (DynamoDB)
```

| Layer | What it is | Who controls it |
|-------|-----------|-----------------|
| Wire | JSON-compatible format clients send/receive | Consumer (via schema's Encoded type) |
| Domain | Rich type the application works with | Schema's Type |
| Storage | DynamoDB attribute format | Library (via annotation) |

### Domain Types

| Domain type | What it carries | Use case |
|-------------|----------------|----------|
| `DateTime.Utc` | UTC instant (immutable, Effect-native) | Default for all UTC date fields |
| `DateTime.Zoned` | UTC instant + timezone (immutable) | Scheduling, audit, TZ-aware display |
| `Date` | UTC instant (mutable, native JS) | Interop with non-Effect libraries |

### Consumer API

#### Date Schemas (domain type: `DateTime.Utc`)

```typescript
import { DynamoModel } from "effect-dynamodb"

DynamoModel.DateString              // Wire: ISO string ↔ Domain: DateTime.Utc
DynamoModel.DateEpochMs             // Wire: epoch milliseconds ↔ Domain: DateTime.Utc
DynamoModel.DateEpochSeconds        // Wire: epoch seconds ↔ Domain: DateTime.Utc
```

#### Unsafe Date Schemas (domain type: `Date`)

```typescript
DynamoModel.UnsafeDateString        // Wire: ISO string ↔ Domain: Date (mutable)
DynamoModel.UnsafeDateEpochMs       // Wire: epoch milliseconds ↔ Domain: Date (mutable)
DynamoModel.UnsafeDateEpochSeconds  // Wire: epoch seconds ↔ Domain: Date (mutable)
```

#### Timezone-Aware Schemas (domain type: `DateTime.Zoned`)

```typescript
DynamoModel.DateTimeZoned           // Wire: ISO string with offset/zone ↔ Domain: DateTime.Zoned
```

### Storage Override

When wire format ≠ storage format, use `storedAs` with a target schema:

```typescript
// Wire: ISO string, DynamoDB: epoch seconds (for TTL)
DynamoModel.DateString.pipe(DynamoModel.storedAs(DynamoModel.DateEpochSeconds))

// Wire: epoch ms, DynamoDB: ISO string
DynamoModel.DateEpochMs.pipe(DynamoModel.storedAs(DynamoModel.DateString))
```

**Type safety:** `storedAs` constrains the storage schema to have the same domain type (`A`) as the field schema. Incompatible combinations are rejected at compile time.

### Auto-Detecting Epoch Schema

```typescript
DynamoModel.DateEpoch(options: {
  minimum: string | DateTime.DateTime.Input
  encode?: typeof DynamoModel.DateEpochMs | typeof DynamoModel.DateEpochSeconds
})
```

### TTL Alias

```typescript
DynamoModel.TTL                     // alias for DateEpochSeconds
```

### Usage Examples

```typescript
class Order extends Schema.Class<Order>("Order")({
  orderId: Schema.String,
  placedAt: DynamoModel.DateString,
  expiresAt: DynamoModel.DateString.pipe(
    DynamoModel.storedAs(DynamoModel.DateEpochSeconds)
  ),
  timestamp: DynamoModel.DateEpochMs,
  ttl: DynamoModel.TTL,
  clientTimestamp: DynamoModel.DateEpoch({ minimum: "2020-01-01" }).pipe(
    DynamoModel.storedAs(DynamoModel.DateEpochSeconds)
  ),
  scheduledAt: DynamoModel.DateTimeZoned,
}) {}
```

### Sort Key Behavior

**Rule: Keys always normalize to UTC. Attributes preserve the original format.**

| Schema | In sort key | In attribute |
|--------|------------|--------------|
| `DateString` | UTC ISO string | UTC ISO string |
| `DateEpochMs` | Epoch ms number | Epoch ms number |
| `DateEpochSeconds` | Epoch seconds number | Epoch seconds number |
| `DateTimeZoned` | UTC ISO string (normalized) | Extended ISO with zone |

The extended ISO form round-trips the zone: a named zone as `…+09:00[Asia/Tokyo]`,
an offset zone as `…+05:00` (rebuilt with that offset since #133, for both
`DynamoModel.DateTimeZoned` and a self `Schema.DateTimeZoned`; earlier versions
read it back as UTC).

### Domain Model Purity

The library supports two patterns for where storage configuration lives:

**Pattern A: Annotated Model (Inline)** — DynamoModel schemas carry invisible annotations. Everything in one place.

```typescript
class Order extends Schema.Class<Order>("Order")({
  orderId: Schema.String,
  placedAt: DynamoModel.DateString,
  expiresAt: DynamoModel.DateString.pipe(
    DynamoModel.storedAs(DynamoModel.DateEpochSeconds)
  ),
}) {}
```

**Pattern B: Pure Model + Configured Model** — Domain model uses standard Effect schemas. Storage mapping is separate.

```typescript
class Order extends Schema.Class<Order>("Order")({
  orderId: Schema.String,
  placedAt: Schema.DateTimeUtcFromString,
  expiresAt: Schema.DateTimeUtcFromString,
}) {}

const OrderModel = DynamoModel.configure(Order, {
  expiresAt: { storedAs: DynamoModel.DateEpochSeconds },
})

const OrderEntity = Entity.make({
  model: OrderModel,
  entityType: "Order",
  indexes: { ... },
})
```

### Complete API Surface

#### Schemas

| Export | Wire (Encoded) | Domain (Type) | Default Storage |
|--------|---------------|---------------|-----------------|
| `DynamoModel.DateString` | `string` | `DateTime.Utc` | ISO string |
| `DynamoModel.DateEpochMs` | `number` | `DateTime.Utc` | epoch ms |
| `DynamoModel.DateEpochSeconds` | `number` | `DateTime.Utc` | epoch seconds |
| `DynamoModel.DateEpoch(opts)` | `number` | `DateTime.Utc` | matches `encode` option |
| `DynamoModel.DateTimeZoned` | `string` | `DateTime.Zoned` | extended ISO with zone |
| `DynamoModel.UnsafeDateString` | `string` | `Date` | ISO string |
| `DynamoModel.UnsafeDateEpochMs` | `number` | `Date` | epoch ms |
| `DynamoModel.UnsafeDateEpochSeconds` | `number` | `Date` | epoch seconds |
| `DynamoModel.TTL` | `number` | `DateTime.Utc` | epoch seconds |

#### Modifiers

| Export | Description |
|--------|-------------|
| `DynamoModel.storedAs(schema)` | Override DynamoDB storage format via schema annotation (Pattern A) |
| `DynamoModel.configure(model, attributes)` | Create a configured model with per-field storage overrides and field renaming (Pattern B) |
| `DynamoModel.configure({ immutable: true })` | Mark field as read-only after creation |

#### Self dates nested in containers (#133)

Entity derivation substitutes every **self** date (`Schema.DateTimeUtc`,
`Schema.Date`, `storedAs(...)`) with a transform to its wire primitive. Before
#133 the walk entered Struct / Class / Array but stopped at `Union`, `Record`,
`Tuple`, `TupleWithRest` and `StructWithRest`, so `NullOr(Schema.DateTimeUtc)`,
`NullOr(ClassWithDate)`, `Array(NullOr(date))` and `Record(_, date)` stored the
`DateTime` instance itself, a marshalled `{ epochMilliseconds, … }` map.
`substituteSchemaDeep` now walks those containers in every mode
(`walkedContainer`), rebuilding each container kind with the original node's
annotations, checks and context (`withMetadataOf`). A container with its own
encoding chain is left as declared. Without `tolerantTransforms` only self-date
and `Redacted` leaves are substituted, so a transform (Pattern B) inside a
container keeps owning its wire form. A `TupleWithRest` is now derived as a
tuple; it was previously treated as an array.

**Container checks on writes (#133).** Rebuilding an `Array`, a `Struct` or a
class around substituted children with a plain constructor dropped the original
node's `.check()` refinements, so a write that broke one
(`Schema.Array(Schema.DateTimeUtc).check(Schema.isMaxLength(1))` given two
dates) was accepted. WRITE schemas are now built with `enforceChecks`, which
restores those checks (`withMetadataOf`; for a class over a checked Struct, on
the Struct its encoding leads to, `classStructAst`). That covers the entity
input / create / update / key schemas (so `put`, `create`, `update`, Batch and
Transaction), `appendInput`, the path-value schemas (`writeModelSchema`) and the
aggregate's `writeSchema`, used by `create` and `update`. READ schemas leave it
off, so a row written while the check was not enforced still reads. On an
aggregate every `update` of such a row fails until the same update makes the
value valid, because the whole mutated state is decoded through `writeSchema`;
an entity `.set()` of other fields still succeeds. The repairing `update` works
because it converts the current state through the read
schema when `toIso` rejects it (`toPlainState`), and `keyRecord` normalises only
key composites. `Union` / `Record` / `Tuple` rebuilds (`walkedContainer`) keep
their metadata in every mode. A container that holds no substituted value is
never rebuilt and keeps its own checks on reads and writes, as before.

Date leaves inside a `Union` follow the union rules below, and also rebuild
legacy maps, so rows written before #133 read back as real `DateTime`s.

**Union rules (#133).** Every member of a union learns which primitive kinds
the *other* members are stored as (`memberWireKinds`), including, for a union
nested in another (`Union([NullOr(DateTimeUtc), String])`), the outer union's
other members. A self-date member then:

- accepts only its own wire kind, its own domain, or a legacy map
  (`strictWireKind`), so it never claims a value of another kind;
- when its storage kind **collides** with another member's (ISO storage next to
  a `String`), accepts only the exact canonical string `toWirePrimitive` writes
  (`canonicalOnly`), and is decoded **first** whatever the declared order. So
  `"2000-01-01T00:00:00.000Z"` in `Union([DateTimeUtc, String])` reads back as a
  `DateTime`, while `"2020"`, `"5"` and `"hello"` stay strings. A string field
  that may legitimately hold canonical ISO instants needs a tagged or
  discriminated shape;
- when it is stored as an epoch number next to a member also stored as a number
  (`Number`, a number literal, `BigInt`, another epoch date), cannot be told
  apart from it at all: rejected at `make()` with **EDD-9058**. There is no
  fallback to ISO storage. On an entity, `NumberFromString` and
  `BigIntFromString` are stored as strings and do not collide. Under
  `tolerantTransforms` (aggregates) a member's **domain** kinds count too
  (`memberWireKinds(..., { domainSide })`), because `update` re-decodes domain
  values: their domain `5` competes with an epoch date, so aggregates reject
  them with EDD-9058 as well.

A transform date member (`DateTimeUtcFromString`) keeps the transform's own
decode inside a union, since the generic date transform accepts more than the
user's transform. A `DynamoModel.configure(..., { f: { storedAs } })` override
on a top-level union field applies to the union's single self-date member; with
several date members it is rejected with **EDD-9057**.

**Legacy reads on entities.** Entity READ schemas (model, record, item,
deleted, history) are built with `legacyReads`: a transform field also accepts
the domain-form value older path updates wrote (a `number` on a
`NumberFromString`, a safe-integer `number` or `bigint` on a `BigIntFromString`,
a marshalled map on a date transform), and a plain `Schema.BigInt` lifts the
unmarshalled `number` back to `bigint`. Never inside a union, where a lenient
member could claim another member's value. Write schemas and keys use the
strict schemas, so nothing written changes.

**Zoned offsets.** A zoned date stored as `…+05:00` (an offset zone, no
bracket) is rebuilt with that offset; before #133 it was rebuilt as UTC. One
parser (`internal/ZonedIso.ts`, `parseZonedIso`) serves both the
`DynamoModel.DateTimeZoned` transform and the substituted date transform, so the
two cannot drift. Named zones (`…[Europe/London]`) and UTC round-trip as before,
and the stored form is unchanged. Offsets that are not whole minutes (historical
LMT offsets, a sub-minute `zoneMakeOffset`) are rounded to the minute by
`formatIsoZoned`, so the instant read back moves by the rounding difference, as
in earlier versions.

Key composition is unchanged for every
existing shape (primary, GSI, unique, version, soft-delete, time-series keys);
`Entity.nestedSelfDates.test.ts` snapshots the key attributes written.

---

## 9. Collections

### Overview

Collections group multiple entity types for cross-entity queries. Two collection modes are supported:

| Mode | SK ownership | Query mechanism | Use case |
|------|-------------|-----------------|----------|
| **Isolated** | Each entity owns its SK prefix | PK match only (no SK condition) | High-volume single-entity queries |
| **Clustered** (default) | Collection owns SK prefix | `begins_with` on collection prefix | Cross-entity queries, relationship-dense data |

### Isolated Collections

In isolated mode, each entity's sort key starts with its own entity type prefix. The collection query uses only the partition key — no sort key condition.

### Clustered Collections

In clustered mode, the collection name sits at the top of the sort key. All entity types share this prefix, enabling efficient cross-entity queries with `begins_with`.

### Sub-Collections (Clustered Only)

Sub-collections create a hierarchy within the sort key, enabling queries at any depth. An entity declares membership in multiple collections via an array:

```typescript
collection: ["contributions", "assignments"],  // sub-collection
```

### Collection Definition

```typescript
import { Collection } from "effect-dynamodb"

const TenantItems = Collection.make("TenantItems", {
  users: UserEntity,
  orders: OrderEntity,
})
```

### Validation Rules

- All entities in a collection must share the same PK composite on that index.
- All entities sharing an index must agree on the type (cannot mix isolated and clustered).
- Sub-collection members must include parent: `["contributions", "assignments"]` means the entity is in both.
- All collection members must be on the same table.

---

## 10. Queries & Operations

### Pipeable Query API

Queries are composable data types following Effect TS idioms. A `Query<A>` is a pure description — no DynamoDB calls occur until a terminal combinator (`execute` or `paginate`) interprets it.

```typescript
// 1. Construct — sets partition key
TenantItems.query({ tenantId: "t-1" })

// 2. Narrow — entity selector
TenantItems.users

// 3. Key condition — KeyConditionExpression (efficient, uses the index)
Query.where({ status: "active", createdAt: { gte: someDate } })

// 4. Filter — FilterExpression (post-scan, doesn't reduce read capacity)
Query.filter({ email: { contains: "@company.com" } })

// 5. Shape — pagination, ordering
Query.limit(10)    // at most 10 ITEMS (accumulates across requests)
Query.pageSize(10) // 10 rows examined per REQUEST (DynamoDB `Limit`)
Query.reverse      // scanForward = false
Query.consistentRead // the table; refused on a GSI (#133) — entity indexes are always GSIs

// 6. Execute — terminal, crosses into Effect
Query.execute    // Query<A> => Effect<A, DynamoError, DynamoClient>
Query.paginate   // Query<A> => Effect<Stream<A>, DynamoError, DynamoClient>
```

### Entity Operations (via typed client)

All operations are accessed through the typed client returned by `DynamoClient.make(table)`. The client binds all entities and aggregates, providing operations with `R = never`.

#### Read Operations

```typescript
const db = yield* DynamoClient.make(MainTable)

yield* db.Users.get({ userId: "abc-123" })

const results = yield* db.Users.execute(
  Users.query.byTenant({ tenantId: "t-1" }).pipe(
    Query.where({ createdAt: { gte: lastWeek } }),
    Query.limit(25),
  )
)
```

#### Write Operations

```typescript
yield* db.Users.put({ userId: "abc-123", email: "alice@example.com", displayName: "Alice", role: "admin" })
yield* db.Users.update({ userId: "abc-123" }).set({ displayName: "Alice B" })
```

#### Fluent bound-CRUD builders

Bound-client CRUD methods return **fluent builders** that mirror the `BoundQuery` contract on the read side. This replaces the variadic `...combinators` form used prior to v0.9.

```typescript
// Update + optimistic lock
yield* db.entities.Tasks.update({ taskId: "t-1" })
  .set({ status: "done" })
  .expectedVersion(3)

// Put with a condition
yield* db.entities.Users.put(input)
  .condition({ status: "active" })

// Create (attribute_not_exists) with a callback condition
yield* db.entities.Users.create(input)
  .condition((t, { eq }) => eq(t.status, "active"))

// Delete with a condition
yield* db.entities.Products.delete({ productId: "p-1" })
  .condition({ status: "archived" })

// Upsert — same shape as put
yield* db.entities.Counters.upsert({ counterId: "c-1", total: 0 })

// Patch — update with attribute_exists guard
yield* db.entities.Tasks.patch({ taskId: "t-1" })
  .set({ status: "blocked" })

// deleteIfExists — delete with attribute_exists guard
yield* db.entities.Tasks.deleteIfExists({ taskId: "t-1" })

// Composed update
yield* db.entities.Products.update({ productId: "p-1" })
  .set({ name: "Updated", price: 24.99 })
  .add({ viewCount: 1 })
  .subtract({ stock: 3 })
  .append({ tags: ["clearance"] })
  .remove(["temporaryFlag"])
  .expectedVersion(5)
```

**An op's own guard is structural (#133).** `create`'s
`attribute_not_exists(pk) AND attribute_not_exists(sk)`, and `patch`'s and
`deleteIfExists`'s `attribute_exists(pk)`, are not held as the op's condition:
they come from the op's kind (`putKind: "create"`, `patch: true`,
`_mustExist`) and are ANDed with the caller's condition where the request is
built — the entity's own write, `Entity.extractTransactable` (transactions,
`EventStore` additional items) and so `Batch.write`, which refuses any op with
a condition. Guard and condition compile as one `and` expression, so their
placeholders never collide; with no condition the guard is sent alone, byte
for byte as before. As on every op, a later `.condition()` replaces an earlier
one; the guard always stays. (The version, unique-sentinel and retain guards
were already separate from the caller's condition.)

**Empty conditions and filters (#133).** The `condition` combinator is the one
place an empty condition becomes none (`nonEmptyCondition`; `append`, which
takes its condition as an argument, applies it on entry): a condition that asserts nothing (`{}`, `and()`, an `and` of only such
parts — `isEmptyExpr`) is dropped, leaving the op's own guard alone; likewise
`Query.filterExpr` drops an empty filter. An empty part directly under `and` is
left out when compiled. Anywhere else — under `or` (which it would make match
everything) or `not` (nothing), or an `or()` with no parts (nothing) — it has
no reading that keeps what the caller wrote, so `emptyPartProblem` refuses it
with a `ValidationError` before anything is sent: in `EntityPut` / `Update` /
`Delete` when they run, per op in `transactWrite`, in `append`, for filters
when a query runs, and for aggregate `list` filters. An `isIn` with no values
(`IN ()`, which DynamoDB rejects) is refused the same way, and a
`Transaction.check()` with an empty condition is refused before sending.
`compileExpr` itself throws on anything `emptyPartProblem` refuses, so no
caller can compile one by skipping the check — reaching it is a library bug.

**`patch` and missing items (#134).** `patch()` of a missing item fails with
`ConditionalCheckFailed` on every path — including those that read first
(retain, a unique-field change), which used to report `ItemNotFound`. A plain
(unread) patch sends one exists clause: the plain write's own
`attribute_exists(#exists)` already covers patch's guard.

**`expectedVersion` needs a version (#134).** On an entity without
`versioned`, `.expectedVersion(n)` is refused with a `ValidationError` before
anything is read or sent — a silently skipped concurrency check is worse than
none.

**Yieldable, not Effect.** The *write* builders implement `Pipeable.Pipeable` and `[Symbol.iterator]` (via `Utils.SingleShotGen`) — the same contract as the unbound `EntityOp` and `EntityDelete` intermediates. You execute them by `yield*`ing inside `Effect.gen`. For interop with Effect combinators (`Effect.map`, `Effect.flip`, etc.) use `.asEffect()`.

**`BoundGet` is the exception, and must remain one.** `db.entities.X.get(key)` has returned a real `Effect` since the client gateway existed, and call sites depend on that: `db.entities.X.get(k).pipe(Effect.catchTag("ItemNotFound", …))`, `Effect.map`, `Effect.all`. So `BoundGet` is built on `Effectable.Prototype` and **is** an `Effect<A, E, never>` — the same mechanism `effect`'s own `Statement` uses for a builder that is also an Effect. It additionally carries the bound-op marker, which is what lets it be handed to `Batch.get`, `Transaction.transactGet` and `Transaction.check` (#108). Downgrading it to merely-yieldable would be a silent, wide breaking change.

**One unwrap protocol, both directions.** A bound op joins the multi-item paths by carrying `BoundOpTypeId` plus `_op`; `Entity.extractTransactable` unwraps it to the underlying `EntityOp` / `EntityDelete`. That is the whole mechanism, for reads and writes alike — there is no second one to add. Because `BoundGet` wraps the entity's own `EntityGet`, the key it composes is by construction identical to the one `db.entities.X.get(key)` composes (both go through `entity._keyForm`; see §7). A value that is not a get descriptor is rejected with a `ValidationError` carrying **EDD-9052** on the error channel.

**Immutable accumulator.** Every chainable call returns a new builder — same semantics as `BoundQuery`.

**Method surface per builder**

| Builder | Method | Accepts |
|---|---|---|
| `BoundGet` | *(no combinators — it is an `Effect`)* | — |
| `BoundPut` / `BoundCreate` / `BoundUpsert` | `.condition(cond)` | callback `(t, ops) => Expr` or equality shorthand record (`{ status: "active" }`) |
| `BoundDelete` | `.condition(cond)` | same as above |
| `BoundDelete` | `.returnValues(mode)` | `"none"` or `"allOld"` |
| `BoundUpdate` / `BoundPatch` | `.set(updates)` | partial record |
| `BoundUpdate` / `BoundPatch` | `.remove(fields)` | `ReadonlyArray<string>` |
| `BoundUpdate` / `BoundPatch` | `.add(values)` | `Record<string, number>` |
| `BoundUpdate` / `BoundPatch` | `.subtract(values)` | `Record<string, number>` |
| `BoundUpdate` / `BoundPatch` | `.append(values)` | `Record<string, ReadonlyArray<unknown>>` |
| `BoundUpdate` / `BoundPatch` | `.deleteFromSet(values)` | `Record<string, unknown>` |
| `BoundUpdate` / `BoundPatch` | `.expectedVersion(n)` | `number` |
| `BoundUpdate` / `BoundPatch` | `.condition(cond)` | callback or equality shorthand record |
| `BoundUpdate` / `BoundPatch` | `.returnValues(mode)` | any `ReturnValuesMode` |
| `BoundUpdate` / `BoundPatch` | `.cascade(config)` | cascade targets |
| `BoundUpdate` / `BoundPatch` | `.pathSet(op)` / `.pathRemove(segs)` / `.pathAdd(op)` / `.pathSubtract(op)` / `.pathAppend(op)` / `.pathPrepend(op)` / `.pathIfNotExists(op)` / `.pathDelete(op)` | same payloads as the unbound `Entity.path*` combinators |
| all builders | `.asEffect()` | — |

**Implementation strategy.** The builders are thin wrappers. Internally each holds an `EntityOp` (or `EntityDelete`) from the unbound entity plus a pre-resolved `provide` for `DynamoClient + TableConfig`. Every chainable method forwards into the existing `Entity.set/remove/add/condition/…` combinators. On `yield*` (or `.asEffect()`) the builder calls `op._run("record")` (or `op.asEffect()` for deletes) and pipes through `provide` so the final `Effect` has `R = never`.

**Path-addressed values are encoded (#133).** `pathSet`, `pathAppend`,
`pathPrepend`, `pathIfNotExists` and record-based `append` (including the
versioned-retain path) bypass the update schema, and used to marshal their
value as given: a `DateTime` became a map even on a plain date field, and a
`NumberFromString` value was stored as a number. `makePathValueEncoder` resolves
the schema the path addresses (`childAtSegment` through struct fields, union
members, array / tuple elements and record values; a top-level field uses the
record schema's own, so `storedAs` applies) and encodes the value through it;
list operations encode element by element. Values go through `encode`, then
`decode → encode` as `.set()` does, so a plain object on a class-typed field is
encoded as that class, and a value already in wire form is normalised
(`NumberFromString` `"05"` → `"5"`, `DateTimeUtcFromString` `"2000-01-01"` →
`"2000-01-01T00:00:00.000Z"`, `Schema.Trim`): read-back values are identical,
only stored bytes differ. A class instance is always encoded whole. A plain
object or array that neither encodes nor decode→encodes whole (it mixes wire and
domain parts), or that holds an ambiguous wire leaf, is encoded part by part
(`encodeByParts`, `holdsAmbiguousLeaf`), so every `DateTime` / `Date` /
`Redacted` inside is stored in wire form. The one pass-through is a LEAF
transform with a primitive wire form, given a value that genuinely decodes as
wire AND validates as the domain type (`StringFromBase64` given `"aGk="`,
`fromJsonString`), whose encode would double-encode it (`makeAmbiguityCheck`);
`"hi"` on `StringFromBase64` is not valid wire and is encoded. Paths into and
under a TOP-LEVEL `DynamoModel.ref` field follow the ref target's model
(`refTargets`), whose read schema is substituted, so they are encoded like any
other path. An opaque class (built with `.check()` or `.annotate()`, or a
`DynamoModel.ref` nested inside a ref target) is deliberately not followed by
`childAtSegment`: its `.fields` are gone, so the read schema keeps it
unsubstituted and decodes its leaves exactly as `put` stores them, and a path
value under it is passed through as given so the item stays readable. Known
limitation: plain dates inside such a class are still stored as maps (also by
`put` / `.set()`), as on 1.22.0. Only those and paths no schema describes (under
a dynamic key of an untyped value) are passed through.

**Retain entities.** The `versioned: { retain: true }` update branch builds the
new item in memory and writes it in a transaction with the snapshot; it used to
ignore path operations entirely (success, nothing written). An update that
carries path operations now reads the current item (consistent read) for the
snapshot, then sends the non-retain branch's own `UpdateExpression` (version
bump, timestamps, GSI recomposition, `expectedVersion`, user condition) as the
`Update` in one `TransactWriteItems` with the snapshot `Put`, conditioned on
the version read. DynamoDB therefore applies the path semantics itself — parity
by construction, never emulated — and a rejected expression writes no snapshot.
Path operations combined with a unique-constraint change, or with a computed
change to an index composite, are rejected with a `ValidationError`: both are
read-then-write updates that cannot carry path expressions. Record retain
updates are guarded read-then-write updates (below).

Each encoded path value is then **validated** against the write schema at its
path (`validate` / `validateElements`), as `.set()` validates its payload: a
literal outside its set, a string under `minLength` or a broken container check
fails with a `ValidationError` instead of being stored. `undefined` object
entries are dropped first (`asStored`), since the marshaller drops them too.
List `append` / `prepend` validate each element, but cannot enforce list-level
checks such as `maxLength`: DynamoDB builds the list server-side. `ADD`,
`DELETE` and `SUBTRACT` are unchanged.

**Path operations on key and unique fields (#133).** DynamoDB evaluates a path
operation, so compiling one on an index composite or unique field would change
the attribute while its keys and sentinel stayed put. `normalizeDerivedPathOps`
rewrites the ones whose result is known client-side into record operations: a
top-level `pathSet` of a value / `pathRemove` into `.set()` / `.remove()`, a
numeric `pathAdd` / `pathSubtract` into `.add()` / `.subtract()`. Those then go
through the key composer and the sentinel rotation. It refuses, with a
`ValidationError` naming the field: copies, `pathIfNotExists`, list and set
operations on such a field (DynamoDB computes their result at write time); a
path below such a field; any path operation on a primary-key composite or an
immutable field; and a second operation on the same field. `.add()` /
`.subtract()` / `.append()` / `.deleteFromSet()` on a GSI composite recompose
the index key on every entity.

**Guarded read-then-write (#133).** Updates that read first (a unique-field
change, a computed change to an index composite, every retain update) write a
guarded `Update` of only the attributes that changed, conditioned on what they
read. A concurrent change to an unrelated attribute is preserved. A race on an
input fails without writing: `OptimisticLockError` (with the real
`actualVersion`) on a versioned entity, `ConcurrentModification` (naming the
changed `attributes`, with `current`) on an unversioned one. Soft delete, and a
hard delete of an entity with unique constraints or `retain`, are guarded the
same way: by version, or for an unversioned entity by a condition over every
attribute read. `restore` fails with `ItemNotFound` when the tombstone is gone (a
concurrent restore won) and `ItemNotDeleted` when a live item already exists.

**Wide items.** Guards are sized against DynamoDB's real limits on one
expression (`EXPRESSION_LIMIT` 4,096 characters, `OPERATOR_LIMIT` 300 operators
and functions), computed on the actual condition, including the caller's
`.condition()` (`expressionFits`). An unversioned delete is never refused for
width: when the full guard does not fit, `deleteGuard` falls back to the
strongest guard that fits. That is `attribute_exists(pk)`, then `updatedAt`
unchanged (with timestamps), then as many attributes as fit, unique-constraint
fields first, then the model's fields, then the rest. With timestamps, every
library write changes `updatedAt`, so any concurrent library update is detected
except one in the same millisecond with an identical `updatedAt`. A writer
outside the library that leaves `updatedAt` alone can change unguarded
attributes undetected. Without timestamps only the guarded attributes are
protected. An update too wide for one expression writes the whole item, under
the version condition (versioned) or the same fallback guard (unversioned). A
concurrent write from outside the library to an attribute the guard does not
cover is lost, as 1.22.0 lost it for every such update. A caller's
`.condition()` too large to fit beside the guard fails before writing
(`oversizedCondition`), with a `ValidationError` stating both sizes.

Operators are counted as DynamoDB counts them (`countOperators`, measured
against DynamoDB). In a condition: each comparison (`=`, `<>`, `<`, `<=`, `>`,
`>=`), `AND` / `OR` / `NOT`, `IN`, and each function; `BETWEEN` counts once,
because its own `AND` is part of it. In an update expression: each `+` / `-`
and each function (`if_not_exists`, `list_append`); a `SET` clause's `=` is not
an operator.

**Pre-versioning items.** An item written before the entity was `versioned` has
no version attribute. It reads as version 0 on every path, and
`expectedVersion(0)` addresses it. The first versioned write conditions on
`attribute_not_exists(version)` (plus the incarnation token, which it adds) and
writes version 1; the retain snapshot is `v#0000000`. A race on that first
write is an `OptimisticLockError`. Soft delete and restore handle it too.

**Items whose version was removed.** A versioned entity stamps the incarnation
token when it creates an item, so only a pre-versioning item may lack a version,
and it lacks the token too. An item with the token and no version had its
version removed outside the library. Reading it as version 0 would let the next
update rewrite its history, so `versionCorruption` refuses it with a
`ValidationError`: on every decode (`get`, queries, the `deleted` views,
`decodeMarshalledItem`), so a query over a partition holding one fails as a
whole; on the read of every read-then-write path (updates, soft delete,
hard delete with unique constraints or `retain`, `restore`, versioned `put`
other than `create`,
read-first `upsert`, transaction puts); and on a plain update and a plain
`upsert`, through a
`attribute_exists(version) OR attribute_not_exists(__edd_i__)` condition with
`ALL_OLD` on failure. A plain hard delete (no unique constraints, no `retain`)
deliberately doesn't check: it reads nothing and writes no history, so it is the
safe way to remove such an item; `purge` removes it on any entity.

**Version history is never overwritten.** Every `v#N` snapshot `Put`
(`snapshotPut`) is conditioned on `attribute_not_exists(pk)` OR the existing
row holding the same version, incarnation token and (with timestamps)
`updatedAt`. Rewriting the same state is legitimate: a retain `put` writes
`v#0000001` and the first update snapshots that same version-1 state again, and
a restore rewrites the delete-time snapshot. Any other row is a different
history, so the update, soft or hard delete, restore or replacing `put` fails with a
`ValidationError` (`historyConflict`) and writes nothing. A new item's snapshot
(`v#0000001`, or the version after the history retained at its key) carries a
fresh incarnation token, so in effect it requires the row to be missing; one
already there means the read missed retained history, and the put is planned
again. Caveat: a write from outside the library that
changes an item without bumping its version can be captured into the next
`v#N` snapshot, because the snapshot copies the item read.

**Decoding defaults.** Read schemas keep `withDecodingDefault`, so a `put` that
omits a defaulted field returns and reads back the default (it used to write the
item and then fail with a `ValidationError`). A defaulted self date is stored as
an ISO string. A defaulted primary-key or index composite that a write omits is
stored with its default, and keys are composed from it; other defaulted fields
are not stored. A default never creates a unique sentinel: see §5 Unique
Constraints for `__edd_d__` and `.remove()` of a defaulted index composite.

**Error mapping.** Failed conditions ask DynamoDB for `ALL_OLD` and classify by
the stored item. A newer stored version is `OptimisticLockError` with the real
`actualVersion`; the same version is the user's `.condition()`, so
`ConditionalCheckFailed`; no item is `ItemNotFound`. That holds on every update
path. Before, three paths had it the other way round:

- a plain versioned update with `expectedVersion` + `.condition()` reported a
  failed condition as `OptimisticLockError(-1)`;
- a retain record update with `.condition()` did the same;
- a retain path update with `.condition()` reported a lost version race as
  `ConditionalCheckFailed`.

A versioned update of a missing item with `expectedVersion` also gave
`OptimisticLockError(-1)`. `patch()` keeps its contract: a missing item is
`ConditionalCheckFailed`.

**Incarnation token.** A version alone cannot tell an item from one deleted and
recreated at the same version. Versioned entities carry `__edd_i__`
(`INCARNATION_TOKEN`), a random UUID stamped on create (put, create, upsert,
batch / transaction put) and backfilled on the next guarded write. Every
version-checked write also checks the token (`incarnationGuard`;
`attribute_not_exists` for an item that has none yet). It is stripped from
decoded models and appears only in `asNative` / raw items.

**Return values.** `returnValues` is honoured on every update path: `"none"` →
`undefined`; `"updatedOld"` / `"updatedNew"` → only the top-level attributes
the update wrote, decoded as a partial; `"allOld"` / `"allNew"` → the whole
item. The type follows the mode (`UpdateReturn<A, M>`); repeated
`Entity.returnValues` calls are typed by the last one (`UpdateBase`). A retain
update returns exactly the item it wrote even if another writer replaced it
since. When that cannot be proven from a snapshot it fails with
`UpdateAppliedButUnreadable` (the write was applied; do not retry). `allOld`
returns the replaced item; the record retain / unique branch used to return the
new one. A cascade with `allOld` / `updatedOld` cascades exactly what the update
wrote, which needs both images; with path operations on an unversioned entity
that combination is refused.

**Missing items and refusals.** `update()` of a missing item no longer leaves an
undecodable partial row. A plain update always requires the item to exist. When
it is missing and the update is a plain `.set()` of a complete item (every
required field and primary-key composite, where a field with a decoding default
is not required) with no other operation, `expectedVersion`, `.condition()`,
cascade, `withVector` or old-image return mode, the library creates it through
`create` with the same payload (`MissingForCreate` → `updateOrCreate`), so the
item is exactly what `put` writes. If another writer creates it in between, the
update re-runs once on that item. Anything else fails with `ItemNotFound` and
writes nothing, as do retain entities and updates that read first. `.set()` of a changed primary-key composite is refused (it was silently
ignored). An immutable field may be restated with its stored value (spread
records), while a different value is refused.

**Guarded puts.** A `put` of a versioned or unique-constrained entity is planned
from a consistent read of the item (`planPut`), and written by `runGuardedPut`.
Over an existing item it continues that item: the next version, the same
incarnation token, the stored `createdAt` (unless the input supplies one — on
unique-only entities too), a retain snapshot of the replaced item at its version,
and sentinel rotation (a changed value takes the new sentinel and releases the
old one; an unchanged value is left alone). It never resets to version 1 or
orphans a sentinel. The main `Put` is guarded by `attribute_not_exists(pk)` when
the item was missing, and otherwise by `deleteGuard` over what was read (version
and incarnation when versioned, the unique attributes otherwise), with
`ReturnValuesOnConditionCheckFailure: ALL_OLD`. A soft-deleted item counts as
missing, since its tombstone has a different sort key. Entities with neither
feature keep the single `PutItem`, with no read.

A put replaces the whole item, so a lost race is retried rather than reported:
a concurrent create, replace or delete of the item between the read and the
write cancels it, and it is planned again from a fresh read — the last writer
wins, as with a plain `PutItem` (`GUARDED_PUT_ATTEMPTS` = 3; a race lost on every
attempt is an `OptimisticLockError` when versioned, else `ConcurrentModification`).
A `.condition()` failure is `ConditionalCheckFailed`, a taken unique value
`UniqueConstraintViolation`, a replaced item whose `v#N` snapshot holds other
history a `ValidationError`. `create` does not read: the item must be missing,
so its `Put` is guarded by `attribute_not_exists(pk)` and carries a fresh
incarnation; an existing item is `ConditionalCheckFailed`. A retain `create`
runs only the one `Limit: 1` history query below.

**Re-creating a deleted retain item.** A deleted (or soft-deleted) retain item's
`v#N` snapshots outlive it, and its final state is one of them: a hard delete,
like a soft delete, reads the item and snapshots it at its own version in the
same transaction as the `Delete` (guarded on the version and incarnation read,
under the `snapshotPut` guard). Without that snapshot an item deleted at `vN`
(N ≥ 2) had history only up to `v(N−1)`, came back at exactly `vN`, and a writer
still holding `vN` could overwrite the new incarnation. A retain hard delete
therefore costs a `GetItem` and a two-item `TransactWriteItems`, not one
`DeleteItem`; deleting a missing retain item writes nothing (with a
`.condition()`, a `DeleteItem` conditioned on `attribute_not_exists(pk)` and the
condition, so the condition is judged against no item and an item created since
the read is never removed unsnapshotted). A delete that reads first — retain,
unique or soft — with no `.condition()` is retried from a fresh read when it
loses a race (the item changed, was deleted — `DeletedConcurrently` — or a
sentinel it releases changed hands), up to `GUARDED_PUT_ATTEMPTS`, as a put is:
the caller asserted nothing the race could break, and the guard still keeps a
concurrent write out of the snapshot or tombstone. A retry that finds the item
gone reports what a delete of a missing item does: nothing for retain only,
`ItemNotFound` with unique constraints or soft delete. `deleteIfExists`'s
`attribute_exists(pk)` (`assertsExistenceOnly`) is already implied by these
guards, so it is judged against the read — a missing item is
`ConditionalCheckFailed` — and retried like an unconditioned delete. With any
other `.condition()`, the read is what the condition was judged against, and the
race fails. Every delete path returns, under `returnValues("allOld")`, the item
it removed: the one read (the delete is guarded on it), or `ALL_OLD` from a
plain `DeleteItem`. A put, `create`, `upsert` or transaction put of a
missing retain item reads the highest version retained for its key (a `Query` on
the `v#` prefix, reversed, `Limit: 1`) and continues after it: the new item takes
that version + 1, a new incarnation token, and its snapshot at that version.
History is never overwritten, and the key is reusable without `purge`. If
snapshots appear between the read and the write, the snapshot guard cancels the
write and it is planned again. `restore` of a tombstone while a live item exists
at the key is refused with `ItemNotDeleted`; once that item is deleted too,
`restore` brings back the latest tombstone.

**Sentinel ownership.** A sentinel names the item that reserved it
(`_entity_pk` / `_entity_sk`). An item can hold a unique value without owning its
sentinel: the constraint was added after the item was written, the item was
written outside the library, or a `ttl`'d reservation expired and another item
claimed the value. Releasing that sentinel by key would delete the other item's
reservation and let the value be taken twice. So every path that releases a
sentinel — a replacing put, an update or upsert that changes the value, a hard or
soft delete, `purge`, a transaction put — first reads it consistently
(`ownedSentinels`) and releases only those this item owns, each with a `Delete`
conditioned on `_entity_pk` / `_entity_sk` still naming it (`sentinelRelease`). A
release cancelled because the reservation changed hands in between is planned
again from a fresh read by a put, an `upsert` and a transaction put (bounded by
the same attempts), and is a `ConcurrentModification` on the unique fields from
an update or a delete with a `.condition()` (an unconditioned delete is retried
too). (`releaseRaced` marks those errors, `releaseRaces`, so
`guardedUpsert` can tell them from a change of the item itself, which fails an
upsert as it fails an update.) Each sentinel a write would release costs one consistent
`GetItem` (projecting only `_entity_pk` / `_entity_sk`). `purge` releases them
one by one with a conditional `DeleteItem`, since a batch delete cannot carry
the condition, collects them from the live item and every tombstone, and skips
a release whose reservation changed hands in between. A sentinel an update
writes for a changed value carries its constraint's `ttl`, as a put's does.

**`upsert` that reads first.** One `UpdateItem` cannot write, rotate or check a
sentinel, snapshot the replaced item, or tell whether to store a default or keep
the stored value. So an entity with `unique` constraints or `versioned: { retain:
true }`, or an input that omits a defaulted index composite, takes
`guardedUpsert`: it validates the whole input (required fields included) and
reads the item once. Missing: `create`, sentinels guarded by
`attribute_not_exists`, the retain snapshot written, omitted defaults stored.
Present: an update of the upserted fields (primary-key composites, immutable
fields, `createdAt` and the version dropped, so they keep their stored values,
as do fields the input omits) from that same read, with sentinels rotated for
changed values only and the replaced item snapshotted, under the update's
version / incarnation guard (versioned) or attribute guard (unversioned). A
concurrent create or delete between the read and the write is retried the other
way, and a sentinel release that raced is planned again from a fresh read
(`GUARDED_PUT_ATTEMPTS` in all); a race lost on every attempt is an
`OptimisticLockError` / `ConcurrentModification`, never a
`ConditionalCheckFailed` the caller didn't ask for, and every error names the
`upsert`. Other entities keep the single `if_not_exists` `UpdateItem`.

**Transaction puts.** `transactWrite` and `EventStore.append`'s
`additionalItems` plan a put of a versioned or unique-constrained entity with
the same `planPut` (`Entity._planPut`), so it creates or replaces exactly as the
entity's own `put`: it reads the item, guards the `Put` on what it read,
continues a replaced item's version, incarnation and `createdAt`, snapshots it,
rotates its sentinels (releasing only owned ones) and continues a re-created
retain item past its history. Each guarded put's items carry `"guarded"`
provenance and are judged by `judgeCancellation`: a taken unique value
(`UniqueConstraintViolation`) or a history conflict (`ValidationError`) is
final; the caller's own condition is `TransactionCancelled` from
`transactWrite` and `AdditionalItemConditionFailed` from `append` — never
reported for an op the caller set no condition on; a race with the read cancels
the transaction, which is built and written again
(`GUARDED_TRANSACTION_ATTEMPTS` = 3, then `OptimisticLockError` /
`ConcurrentModification`).

Two checks run on the compiled transaction before it is sent, in
`transactWrite` and `EventStore.append`. **One op per item**
(`refuseRepeatedItems`): DynamoDB allows one operation per item in a
transaction, and for a guarded put the reasons it reports for a repeated item
can read as a lost race (`[None, ConditionalCheckFailed]` on DynamoDB Local),
which would be retried and misreported. Every compiled item carries a target —
its table and primary key, and the caller op it came from
(`BuiltTransactWriteItems.targets`) — including the sentinels and snapshots a
guarded put adds, so two puts that swap unique values (releasing and reserving
the same sentinels) are caught too; `append` adds targets for its contiguity
check, event puts and idempotency sentinel. A repeat is a `ValidationError`
naming the entity and both sources. **Size** (`refuseOversizedTransaction`):
a LOWER bound on the items' sizes by DynamoDB's item-size rules
(`internal/ItemSize.ts`, `"lower"`: numbers a byte per two significant digits
plus one, no list or map overhead; a `Put`'s item, a `Delete`'s or
`ConditionCheck`'s or `Update`'s key — an Update's values may be its
condition's; zero is one byte), so a transaction DynamoDB
would accept is never refused, must not pass 4 MB =
4,194,304 bytes, DynamoDB's documented aggregate limit for one transaction (in
the binary megabytes of all its size limits). A retain put counts twice: its
item and its snapshot. An oversized transaction is a `ValidationError` naming
its largest item, not DynamoDB's bare `ValidationException`. Deletes of `unique` / retain / `softDelete`
entities, and updates, are planned by `Transaction.transactWrite` from the
entity's own op (`Entity._planUpdate` / `_planDelete`): the op runs with its
write recorded instead of sent (`internal/TransactPlan.planWrite`), and its
items join the transaction as a guarded write, read by the same verdict
contract as a guarded put. `EventStore.append` plans none, and still refuses
those deletes (EDD-9048).

**`Batch.write` of a `versioned` entity.** A `PutRequest` would reset an
existing item to version 1 under a new incarnation. So `Batch.write` sends these
puts first, before any other request, as create-only `TransactWriteItems` Puts
(`attribute_not_exists(pk)`) in chunks of up to 100 items, each closed before
its item size, by an upper bound of DynamoDB's item-size rules (`"upper"`), passes 3.5 MB (the cap is 4 MB). There
is no read, and so no window between a check and the write. A batch that
touches a versioned put's item more than once — a delete and a put, or two
puts — is refused before anything is sent: the put runs in its own
transaction, so the order couldn't be kept (and two puts of one item in one
transaction would be misreported as a replace). Each chunk is atomic: a put that would
replace an existing item cancels it, nothing in the chunk is written, and the
batch fails with a `ValidationError` without sending later chunks or the plain
requests. Earlier chunks may already have been written; `Batch.write` was never
atomic across chunks. A cancellation whose reasons are only
`TransactionConflict` / throttling is retried with the batch's `maxRetries` /
`baseDelayMs` backoff, and any other cancellation is a `DynamoError`. Each
chunk costs twice the write capacity of a batch write. Puts of other entities,
and deletes, stay plain `BatchWriteItem` requests. (`unique` and retain
entities are still refused by EDD-9049.)

**Known limitations** (inherent):

- On unversioned entities, nothing can prove an unguarded attribute unchanged.
  So the item a unique-field update returns may show stale values for
  attributes it neither reads nor writes, wide items use the fallback guard,
  and the wide-update whole-item write can overwrite outside writers.
- A plain `.expectedVersion(n)` cannot detect a delete-and-recreate that has
  climbed back to version `n` on an entity without `retain` (its versions
  restart at 1, so it takes `n − 1` updates; a retain item continues after its
  retained history).

**Why hard-break over dual.** Carrying both the variadic overload and the fluent builder would double the surface area of `BoundEntity`, degrade hover tooltips, and force contributors to remember two shapes. The read side settled on builders for the same reasons. The change is batched into the next major alongside other breaking changes.

#### Lifecycle Operations

```typescript
yield* db.Users.delete({ userId: "abc-123" })     // soft delete (when enabled)
yield* db.Users.restore({ userId: "abc-123" })    // restore soft-deleted item
yield* db.Users.purge({ userId: "abc-123" })      // permanent delete
yield* db.Users.getVersion({ userId: "abc-123" }, 3)  // get specific version
yield* db.Users.versions({ userId: "abc-123" })   // query version history
yield* db.Users.deleted.get({ userId: "abc-123" })    // get soft-deleted item
yield* db.Users.deleted.list()                        // list all soft-deleted items
```

### Data Integrity

#### Unique Constraints

Enforcement uses sentinel items with transactional writes. **Sparse** — a
sentinel is only written when every composing field is present on the record;
constraints whose fields are unset are silently skipped (mirrors GSI sparse
semantics):

| Operation | Transaction Items |
|-----------|-------------------|
| Put — new item | Entity item + sentinel per unique field whose composites are all set (`condition: attribute_not_exists(pk)`) |
| Put — over an existing item | Entity item (guarded by what was read) + for each changed value, release old sentinel + put new sentinel |
| Update — composites unchanged | Entity item only (no sentinel ops) |
| Update — undefined → defined | Entity item + put new sentinel |
| Update — defined → undefined | Entity item + release old sentinel |
| Update — defined → defined (changed) | Entity item + release old sentinel + put new sentinel |
| Delete | Entity item + release sentinel per unique field whose composites were set |

A release is a `Delete` conditioned on the sentinel still naming this item
(`_entity_pk` / `_entity_sk`), emitted only for a sentinel this item owns — see
"Sentinel ownership" under §10 Fluent bound-CRUD builders.

#### Optimistic Concurrency

When `versioned` is enabled, updates can include an expected version:

```typescript
db.Users.update(key, changes, { expectedVersion: 5 })
// Adds ConditionExpression: version = :expected
// Fails with OptimisticLockError if version doesn't match
```

### Entity Lifecycle

#### Soft Delete

When `softDelete` is configured, `db.Users.delete()` performs a logical deletion:

1. Modifies the sort key: `$myapp#v1#user` → `$myapp#v1#user#deleted#<timestamp>`
2. Removes all GSI key attributes (item falls out of all indexes)
3. Adds `deletedAt` timestamp
4. Optionally sets DynamoDB TTL for auto-purge

#### Version Retention

When `versioned: { retain: true }`, every mutation stores a snapshot of the previous state as a separate item. All versions are co-located with the current item (same partition key).

### DynamoClient

The `DynamoClient` service provides:

| Operation | Used By |
|-----------|---------|
| `putItem` | Entity writes |
| `getItem` | Entity reads |
| `deleteItem` | Entity deletes |
| `query` | Entity queries, version history, soft-deleted list |
| `updateItem` | Entity updates (partial, atomic version increment) |
| `transactWriteItems` | Unique constraints, versioned writes |
| `transactGetItems` | Batch reads with consistency |
| `batchGetItem` | Batch operations |
| `batchWriteItem` | Batch operations |

Runtime configuration via Effect Layers:

```typescript
// Direct configuration
DynamoClient.layer({ region: "us-east-1" })
MainTable.layer({ name: "my-prod-table" })

// Config-based (reads from environment variables)
DynamoClient.layerConfig({ region: Config.String("AWS_REGION") })
MainTable.layerConfig({ name: Config.String("TABLE_NAME") })
```

---

## 11. Aggregates & Relational Patterns

### Problem

DynamoDB single-table designs frequently model rich domain objects as multiple denormalized items sharing a partition key. Building and maintaining these structures requires enormous manual effort:

1. **Denormalized references** — Junction items embed full copies of related entities. Creating/updating requires manual hydration.
2. **Context attribute propagation** — Parent-level attributes are copied into every child item to enable sort-key queries.
3. **Aggregate assembly** — Reading an aggregate requires a collection query followed by manual discrimination, reduction, and deep-merge.
4. **Aggregate mutation** — Updating a nested field requires deep destructuring, manual array manipulation, reconstruction, validation, and transactional write.
5. **Cascade updates** — When denormalized data changes at the source, all items that embed that entity must be found and updated.

A production cricket match management system built on ElectroDB demonstrates these patterns at scale — 17 model files, 16 service files, a ~1,100 line MatchService, and ~120 lines to update one player within a match. These patterns are universal to DynamoDB single-table designs.

### Concepts

**Edge Entity** — A first-class entity representing a relationship within an aggregate's partition. For example, `MatchVenueEntity` represents the Match<>Venue relationship, with `matchId` + `venueId` in its primary key. Edge entities may embed denormalized data from referenced entities (e.g., venue name, city). They are real entities with their own models, indexes, and configuration — not implicit decomposition targets.

**Ref** — A reference to an external entity whose data is denormalized into an edge entity at write time. The aggregate framework handles hydration: on create/update it fetches the referenced entity (e.g., `VenueEntity.get({ venueId })`) and embeds its domain data into the edge entity (e.g., `MatchVenue`). On read, the data is already materialized — no ref lookups needed.

**Context** — Fields on the aggregate's domain schema that must be propagated to every edge entity item in DynamoDB for query support. Defined once at the aggregate level.

**Aggregate** — A domain object composed of multiple entity types that share a partition key. The aggregate orchestrates Entity, Collection, and Transaction primitives — it never touches DynamoClient directly. The underlying structure is a directed acyclic graph (DAG) where nodes are entity types and edges are relationships with cardinality.

**Optics** — Effect v4's `effect/Optic` library solves aggregate mutation: instead of manual destructuring, an optic navigates to the target and produces an updated aggregate immutably.

### Edge Entities

Edges in an aggregate are **explicit first-class entities**, not implicit constructs. Each edge entity has its own model, primary key, indexes, and configuration:

```typescript
// Edge entity model — includes relationship keys + denormalized ref data
class MatchVenue extends Schema.Class<MatchVenue>("MatchVenue")({
  matchId: Schema.String,
  venueId: Schema.String,
  name: Schema.String,        // denormalized from Venue
  city: Schema.String,        // denormalized from Venue
  capacity: Schema.Number,    // denormalized from Venue
}) {}

// Edge entity — real entity with keys, timestamps, versioning
const MatchVenueEntity = Entity.make({
  model: MatchVenue,
  entityType: "MatchVenue",
  indexes: {
    primary: {
      pk: { field: "pk", composite: ["matchId"] },
      sk: { field: "sk", composite: ["venueId"] },
    },
  },
  timestamps: true,
})
```

DynamoDB partition layout for a Match aggregate:

```
PK = $cricket#v1#match#m-1

  SK = $cricket#v1#match                          → MatchEntity (root)
  SK = $cricket#v1#match_venue#v-1                → MatchVenueEntity (one-edge)
  SK = $cricket#v1#match_team#teamNumber#1        → MatchTeamEntity (one-edge, discriminated)
  SK = $cricket#v1#match_team#teamNumber#2        → MatchTeamEntity (one-edge, discriminated)
  SK = $cricket#v1#match_player#p-1               → MatchPlayerEntity (many-edge)
  SK = $cricket#v1#match_player#p-2               → MatchPlayerEntity (many-edge)
```

### DynamoModel.ref — Denormalized Reference Annotation

`DynamoModel.ref` marks a field as a denormalized reference in edge entity models:

```typescript
class MatchPlayer extends Schema.Class<MatchPlayer>("MatchPlayer")({
  matchId: Schema.String,
  playerId: Schema.String,
  player: Player.pipe(DynamoModel.ref),   // denormalized Player data
  isCaptain: Schema.Boolean,
}) {}
```

When Entity encounters a `ref`-annotated field:

| Derived Type | Behavior |
|---------|----------|
| `Entity.Input<E>` | Ref field becomes its ID type (`player: Player` → `playerId: string`) |
| `Entity.Record<E>` | Ref field is the full entity domain type (`player: Player`) |
| `Entity.Update<E>` | Ref field becomes optional ID (`playerId?: string`) |
| DynamoDB storage | Core domain data stored as embedded map attribute |
| Create/Put | Entity auto-hydrates: receives ID → fetches entity → embeds domain data |

### Aggregate.make() — Graph-Based Composite Domain Model

The consumer defines the aggregate's domain shape as a pure Schema.Class hierarchy, then `Aggregate.make` binds it to a graph of underlying edge entities:

```typescript
const MatchAggregate = Aggregate.make(Match, {
  schema: AppSchema,
  pk: { field: "pk", composite: ["matchId"] },
  collection: { index: "lsi1", name: "match", sk: { field: "lsi1sk", composite: [...] } },
  context: ["name", "gender", "matchType", "league", "series", "season", "startDate"],
  root: MatchEntity,

  edges: {
    venue:   Aggregate.one(MatchVenueEntity, { ref: VenueEntity }),
    team1:   TeamSheetAggregate.with({ discriminator: { teamNumber: 1 } }),
    team2:   TeamSheetAggregate.with({ discriminator: { teamNumber: 2 } }),
    umpires: Aggregate.many(MatchUmpireEntity, { ref: UmpireEntity }),
  },
})
```

**Edge types:**
- `Aggregate.one(EdgeEntity, { ref? })` — one-to-one edge entity. Optional `ref` specifies the external entity to hydrate/denormalize from.
- `Aggregate.many(EdgeEntity, { ref? })` — one-to-many edge entities. One DynamoDB item per element.
- **BoundSubAggregate** — sub-tree with discriminator for reuse (e.g., `TeamSheetAggregate.with(...)`)

### Aggregate Operations via Typed Client

Aggregates registered on a table are accessible through the typed client, alongside entities:

```typescript
const MainTable = Table.make({
  schema: AppSchema,
  entities: { Teams: TeamEntity, Players: PlayerEntity, Venues: VenueEntity },
  aggregates: { Matches: MatchAggregate },
})

const db = yield* DynamoClient.make(MainTable)

// Aggregate operations — typed, R = never
const match = yield* db.Matches.get({ matchId: "m-1" })
yield* db.Matches.create({ matchId: "m-2", venueId: "v-1", ... })
```

Internally, `DynamoClient.make` resolves `DynamoClient` service + `TableConfig` once and binds:
- All entities (root + edge + ref entities from all aggregates)
- All aggregates

As a service for testability:

```typescript
class MatchService extends Context.Service<MatchService>()("@gamemanager/MatchService", {
  make: Effect.gen(function* () {
    const { Matches } = yield* DynamoClient.make(MainTable)
    return {
      get: (matchId: string) => Matches.get({ matchId }),
      create: Effect.fn(function* (input: CreateMatchInput) {
        return yield* Matches.create(input)
      }),
    }
  }),
}) {}
```

### Assembly (Read Path)

Aggregates compose entity operations for reads — they never query DynamoDB directly:

```
db.Matches.get({ matchId: "m-1" })
  → Collection query (all items in partition, decoded per entity schema)
  → Discriminate by __edd_e__ + discriminator into edge entity buckets
  → Assemble in topological order (leaves first) into domain object
  → Return as Schema.Class instance
  // No ref lookups — data already denormalized in edge entities
```

### Decomposition (Write Path)

On create/update, the aggregate decomposes the domain object into entity operations with write-time ref hydration:

```
db.Matches.create({ matchId: "m-2", venueId: "v-1", teams: [...], players: [...] })
  → Ref hydration: VenueEntity.get({ venueId: "v-1" }) → { name: "MCG", city: "Melbourne", ... }
  → Denormalize: MatchVenue = { matchId, venueId, name: "MCG", city: "Melbourne", capacity: 100000 }
  → Decompose all edges into entity inputs
  → Transaction.transactWrite(
      MatchEntity.create(rootItem),          // attribute_not_exists — first item of the first transaction
      MatchVenueEntity.put({ matchId, venueId, name: "MCG", city: "Melbourne", capacity: 100000 }),
      MatchTeamEntity.put(team1),
      MatchTeamEntity.put(team2),
      MatchPlayerEntity.put(player1),
      ...
    )
```

**`create` is guarded on the root (#134).** The root item is the first Put of
the first transaction, conditioned on `attribute_not_exists(pk)`. An existing
aggregate cancels that transaction — nothing is written — and its cancellation
reason maps to `ConditionalCheckFailed` (the root's `entityType`, `key`
`{ pk, sk }`), never a raw `TransactionCancelled`. Edge, `many` and nested
sub-aggregate rows carry no guard: they are written only after the root's
transaction commits. Each sub-aggregate group remains its own transaction, so a
later group that fails leaves the earlier groups written — exactly as before;
the guard only makes the first transaction refuse. `update` and `delete` are
unchanged. The guard sees the root only: orphan edge rows (a root-less
partition left by a partial earlier write) don't stop `create`, and `get`
merges them in; detecting them would cost a partition read on every create, so
it is documented instead (`delete` the key first). `delete` retries the
`UnprocessedItems` of its `BatchWriteItem`s with `Batch.write`'s backoff and
bound (5 retries), then fails with a `DynamoError`.

**Update with diff:**

```
db.Matches.update({ matchId: "m-1" }, mutation)
  → current = db.Matches.get({ matchId: "m-1" })       // entity-based fetch
  → next = mutation(current)                              // optic-powered mutation
  → diff(current, next)
  → Re-hydrate changed refs (e.g., venueId changed → fetch new Venue)
  → Transaction.transactWrite(
      MatchVenueEntity.delete(oldVenueKey),              // removed edge
      MatchVenueEntity.put(newVenueItem),                // added edge (with denormalized data)
      MatchPlayerEntity.update(changedPlayerKey, changes), // modified edge
    )
```

**Transaction Decomposition:** Each sub-aggregate is a transactional unit, keeping transactions well within DynamoDB's 100-item limit.

### Attribute Encoding (#72, #133)

Decomposition works from the schema-decoded domain object, so every attribute it
produces is a Type-side value. Each attribute is put into its schema's **wire
form** before marshalling, through per-attribute encoders built at `make()` time.
Marshalling a domain value directly stores a shape the read path cannot decode:
a `DateTime` becomes a `{ epochMilliseconds, <type-id>, _tag }` map, a `Date`
becomes `{M:{}}`, and a `bigint` becomes `{N:"5"}`.

| Rule | Behaviour |
|------|-----------|
| **Which attributes** | Any field holding a wire transform **at any depth** (`containsWireTransform`): a leaf transform, a `Schema.Class`, a self date or `Redacted`, or an `Array` / `Struct` / `Union` / `Record` / `Tuple` containing one. Gating on the top-level AST (pre-#133) skipped every container, since an `Arrays` / `Objects` / `Union` node carries no encoding of its own; that included `Schema.optional(X)` and `NullOr(X)` around a transform. Fields with nothing to encode get no encoder and their bytes are unchanged. |
| **One encoder per field** | A `storedAs` annotation or inferred date default wins, except for a union mixing a date with a non-date member (`Union([DateTimeUtcFromString, Number])`, `isMixedDateUnion`), whose non-date values a date encoder would throw on. Otherwise the field is encoded through the same substituted, tolerant schema the read path decodes it with (`substituteSchemaDeep` + the aggregate's ref resolver), falling back to the field's own `encode`, then `decode → encode`. |
| **Which schema** | The schema the decomposed value actually has: the root model's fields for the root, a `one` edge's entity model (or the model field's own class when the edge has no entity), the array **element** for a `many` edge (`PlayerSheet`, not `Player`), a sub-aggregate's own schema for its root item. Encoders are keyed by the element's field names, so a custom `decompose` that **renames** fields escapes them: the renamed values are stored in domain form (a `DateTime` as a map). This is a known limitation; the read path still lifts those maps. |
| **Per attribute, not per aggregate** | The aggregate is never encoded as a whole before decomposition: key composition needs Type-side values (`numericTypeWithStringEncoding`). |
| **Union members** | `substituteSchemaDeep` walks `Union`, `Record`, `Tuple`, `TupleWithRest` and `StructWithRest` in every mode (`walkedContainer`), rebuilding each container kind around substituted children with the original node's annotations and `.check()` refinements (`withMetadataOf`). Under `tolerantTransforms` (aggregates) every transformed leaf is substituted; for entity derivation only self-date and `Redacted` leaves are. Union members follow the union rules in §8 (Self dates nested in containers): a self date accepts only its own wire kind, its domain or a legacy map; on a storage-kind collision only its exact canonical form, decoded first; an epoch date next to a numeric member is rejected (**EDD-9058**). The collision set propagates into nested unions. A transform date member keeps its own decode, so a stored `5` in `Union([DateTimeUtcFromString, Number])` stays a number. |
| **Keys unchanged** | A `many` edge's `sk.composite` and the root's list-index composites are read from a second encoder set (`buildKeyAttrEncoders`) that keeps the pre-#133 top-level-only behaviour. Composed keys are therefore byte-identical to earlier versions; only stored attribute values gained the deeper encoding. |

**Ref resolution.** `DynamoModel.ref` annotates with `Schema.annotate`, which
drops a `Schema.Class`'s `.fields`, so the schema walker cannot recurse into an
annotated ref. `collectRefTargets` registers each such field with the model it
should be read (and encoded) as: root `one`/`ref` edge fields, opaque ref fields
inside sub-aggregate edges and `many` elements (found by
`deriveEntityFieldName`), a `many` field whose element *is* an opaque ref
(`Schema.Array(X.pipe(DynamoModel.ref))`, re-pointed as a whole at
`Schema.Array(<entity model>)` because the walker re-points fields, not
elements), and refs nested in an edge entity's own model (#116). A
plain-class element field is walked directly and needs no registration. Targets
are keyed by **field schema identity**, not field name: the resolver is
consulted at every depth, and a name-keyed table re-pointed any same-named field
anywhere in the model.

**Legacy maps on read.** Versions ≤1.22.0 stored nested `DateTime`s as marshalled
maps. The tolerant date decoder (`liftToDomain`) rebuilds any plain object with a
finite numeric `epochMilliseconds` and `_tag: "Utc"`, or `_tag: "Zoned"` with a
recoverable named or offset `zone`, into a real `DateTime`. The type-id key is
deliberately not inspected (`~effect/time/DateTime` on the rc, `~effect/DateTime`
on 4.0.0). An object that duck-types as a `DateTime` but carries no recoverable
instant is rejected rather than passed to the domain. There is no backfill: a
legacy row is rewritten in wire form only when an `update` changes its
decomposed group, because the diff compares decomposed (re-encoded) groups and a
no-op update writes nothing.

**Stored-type change.** Because the gate now looks through `optional` /
`NullOr` and into refs, some attributes that ≤1.22.0 stored in domain form are
now encoded: a top-level `optional` / `NullOr` around a non-date transform, and
a `NumberFromString` inside a hydrated ref (`{N:"5"}` → `{S:"5"}`). Keys are
unaffected (they use `buildKeyAttrEncoders`), and both forms decode, but a
`list` `filter` / `filterBy` on such an attribute can match old and new rows
differently, and Streams consumers see the type change. An `optional` /
`NullOr` `BigIntFromString` stored as `{N}` by ≤1.22.0 was never readable,
because unmarshalling yields a `number`. The tolerant transform
(`buildTolerantTransform`) now lifts a safe-integer `number` to `bigint` for a
bigint domain, so those rows read. Domain objects with no enumerable state
(`Schema.Date`, `URL`, `Duration`, `BigDecimal`) were stored as maps holding no
value and cannot be recovered.

**Create input cloning.** `replaceRefIds` deep-copies the create input with
`cloneInput` rather than `structuredClone`, which reduced every Effect data type
to a bare object (a `DateTime` to `{ epochMilliseconds }`, a `Redacted` to `{}`,
an `Option` lost its variant). Values implementing `Equal` are immutable and are
kept by reference; built-ins `structuredClone` knows still go through it; plain
objects, arrays and other class instances are copied to plain objects; cycles
are preserved.

### Nested Sub-Aggregates (#133)

A `BoundSubAggregate` inside another sub-aggregate inherits the parent's
discriminator, as a `one` edge does: `resolveNode` merges
`{ ...parentDiscriminator, ...bound.discriminator }`, so the inner rows carry
both attributes and assembly, which matches on the merged set, can tell the
parent's bindings apart. On the write side the inner sort keys are prefixed with
the parent's `name#value` pairs, and each nested sub-aggregate is its own
transaction group, named by its path (`club2.squad`):

```
SK = $app#v1#leagueclub#clubno#1                        → club1 root
SK = $app#v1#leaguesquad#clubno#1#squadno#1             → club1.squad root
SK = $app#v1#leaguesquadplayer#clubno#1#squadno#1#p-1   → club1.squad.players[*]
```

(numeric values zero-padded in real keys). A sub-aggregate bound on the root has
no parent discriminator, so depth-1 keys are unchanged. Before #133 depth-2 rows
were written without the parent's values and could not be assembled
(`Missing key at ["club"]["squad"]`); they are not read under the new keys.

**EDD-9056.** `validateNestedDiscriminators` runs at `make()` and rejects a
nested binding that declares a discriminator attribute it already inherits: the
inner value would overwrite the outer one on the inner rows, so both bindings of
the parent would key and assemble the inner sub-aggregate identically.

### Aggregate System Timestamps

`Aggregate.make(schema, { timestamps })` takes the same `TimestampsConfig` as
`Entity.make`, and applies it to **every row the aggregate writes** — the root
item and all edge items, including those inside sub-aggregate transaction groups.

```typescript
const MatchAggregate = Aggregate.make(Match, {
  …,
  timestamps: {
    created: { field: "created", schema: DynamoModel.DateEpochMs },
    updated: { field: "updated", schema: DynamoModel.DateEpochMs },
  },
})
```

Aggregates compose their DynamoDB items directly (`buildDynamoItems`) rather than
routing through `Entity` write ops, so Entity's timestamp machinery never reached
these rows — in a single-table design where aggregates hold most of the data that
left the majority of the table with no modification timestamp, and no
configuration that could add one (#98).

**Four rules govern the implementation:**

1. **Injected after the diff, never during decomposition.** `update` diffs
   *decomposed* groups (`deepEqualGroups`) to decide which sub-aggregate
   transactions to rewrite. A timestamp added in `decomposeAggregate` would
   differ on every comparison and silently collapse the diff into a
   full-partition rewrite on every update. Timestamps are therefore stamped in
   `buildDynamoItems`, downstream of the diff.
2. **`created` is carried forward.** Aggregate writes are `Put`, not
   `UpdateItem`, so a freshly generated `created` would clobber the original on
   every update. `update` already fetches the whole partition to assemble current
   state, so the stored `created` is read back from those raw items, keyed by
   `sk`, and re-emitted verbatim. A row that is new in this write gets `created`
   = `updated` = now.
3. **Per-row semantics.** `updated` records when *that row* last changed. A
   diff-based update rewrites only the groups whose content changed, so rows the
   mutation did not touch keep their stored `updated` — which is what a
   downstream sync guarding writes with
   `attribute_exists(updated) and updated < :updated` needs. There is
   deliberately no forced rewrite of the root item to give the aggregate a
   single "modified" marker: it would cost an extra item in an extra transaction
   on every update, and the per-row stamp already answers the question the marker
   was standing in for.
4. **Generated values win over `context`.** Context fields are merged onto edge
   items during decomposition; timestamp injection runs later, so a root field
   propagated via `context` under the same name is overwritten by the generated
   value rather than racing it.

**Read path.** The configured timestamp attribute names are stripped from
assembled items alongside the other DynamoDB metadata (`__edd_e__`, keys, index
mirrors), so they never leak into the decoded domain object. A root model that
declares the field itself follows the same collision rule as Entity, and in that
case the value is kept and decoded normally.

The `schema` half of each field config obeys **EDD-9044** exactly as it does on
`Entity.make` — the aggregate shares the `resolveSystemFields` resolver.

### Optic-Powered Mutations

The aggregate exposes optics derived from its Schema.Class for immutable updates:

```typescript
const db = yield* DynamoClient.make(MainTable)

yield* db.Matches.update({ matchId: "match-123" }, ({ cursor }) =>
  cursor
    .key("team1").key("players").at(0)
    .modify((s) => ({ ...s, isCaptain: true }))
)
```

The `update` mutation context provides: `state` (plain object), `cursor` (pre-bound optic), `optic` (composable optic), `current` (Schema.Class instance).

### Cascade Updates

When a source entity changes, all items that embed it via `ref` must be updated:

```typescript
const { Players } = yield* DynamoClient.make(MainTable)
yield* Players.provide(
  PlayerEntity.update({ playerId: "player-smith" }).pipe(
    Entity.set({ displayName: "Steven Smith" }),
    Entity.cascade({ targets: [MatchPlayerEntity] }),
  ).asEffect()
)
```

**Explicit targets required.** No implicit discovery. Default mode is eventual consistency (batch writes). Transactional mode available for small datasets.

### Aggregate vs Collection

| Capability | Collection | Aggregate |
|-----------|-----------|-----------|
| Multi-entity query | Yes | Yes (uses Collection internally) |
| Domain shape assembly | No | Yes — returns Schema.Class instance |
| Decomposition (write) | No | Yes — walks edge entity graph |
| Write-time ref hydration | No | Yes — fetches + denormalizes ref entities |
| Context propagation | No | Yes |
| Optics | No | Yes |
| Diff-based updates | No | Yes — only changed edges written |
| Transaction boundaries | No | Yes — sub-aggregate = transaction unit |

### Implementation Notes

**Behavioral Notes:**
- `Aggregate.update` handles orphaned items when reducing a many-edge array via diff-based delete operations.
- Both `"eventual"` (default) and `"transactional"` cascade modes are supported.
- Edge entities inherit all entity features: versioning, timestamps, unique constraints, soft delete.

**Deferred Features:**
- Pre-built graph-edge optics (generic `.key()` chains cover the same use cases)
- `Aggregate.Input` type extractor (recursive ref→ID transformation)
- Computed discriminators (only static literal discriminators supported)

---

## 12. EventStore

### Overview

`EventStore` provides typed, Effect-native event sourcing on DynamoDB. It implements the Decider pattern (command → events → state) with stream-based event persistence.

### Stream Definition and Binding

A stream is bound to a table at definition time. Its operations require
`DynamoClient | TableConfig`; `EventStore.bind` resolves both and returns a
`BoundEventStream` with `R = never` for use inside `Context.Service` make effects:

```typescript
const MatchEvents = EventStore.makeStream({
  table: EventsTable,
  streamName: "Match",
  events: [MatchStarted, InningsCompleted, MatchEnded],
  streamId: { composite: ["matchId"] },
})

const program = Effect.gen(function* () {
  const stream = yield* EventStore.bind(MatchEvents)
  yield* stream.append({ matchId: "m-1" }, [new MatchStarted({ venue: "MCG" })], 0)
  const events = yield* stream.read({ matchId: "m-1" })
  const version = yield* stream.currentVersion({ matchId: "m-1" })
})
```

### Consistent Reads (#139)

`read`, `readFrom`, `currentVersion` and `readLatest` take an optional
`ReadOptions` (`{ consistentRead?: boolean }`, default `false`; `readLatest`'s
`ReadLatestOptions` adds `verifySnapshot`);
`consistentRead: true` sets `ConsistentRead` on every `Query` page for
read-your-writes. `query.events` composes with `Query.consistentRead` instead.
`commandHandler` loads state strongly consistently **by default**
(`CommandHandlerOptions.consistentRead: false` opts out): an eventually
consistent load can hand `decide` stale state, which then fails the append with
`VersionConflict`.

### Key Layout

```
pk           $<schema>#v<n>#<stream>#<streamId composites>
sk (event)   $<schema>#v<n>#<stream>.event_1#<10-digit version>
sk (command) $<schema>#v<n>#<stream>.command#<commandId>     (idempotency sentinel)
sk (snapshot)$<schema>#v<n>#<stream>.snapshot
__edd_e__    "<stream>.event" | "<stream>.command" | "<stream>.snapshot"
```

Stream-id composites carry no `name_` prefix (unlike entity keys). Without
`makeStream({ casing })`, `<stream>` is `streamName` lower-cased and the rest of
the key follows the schema's casing — the layout streams have always had. With
`casing`, the stream name is taken as written and that casing applies to the
whole key except the schema prefix, as an index's `casing` does. On a
`"lowercase"`/`"uppercase"` schema, `casing` equal to the schema's reproduces the
default layout. The `__edd_e__` values are always lower-cased. In the next major,
omitting `casing` will mean the schema's casing.

### Command Handler

The `commandHandler` combinator implements the read-decide-append cycle:

```typescript
const handler = EventStore.commandHandler(MatchDecider, MatchEvents)
const result = yield* handler({ matchId: "m-1" }, new StartMatch({ venue: "MCG" }))
// result: { state, version, events }
```

Full design of the command path: [`docs/designs/eventstore-command-path.md`](docs/designs/eventstore-command-path.md)
(#136–#140; #141 was not implemented — see [Large Commands](#large-commands--stepped-commands)).
Each invocation runs:

```
load → [expectedVersion check] → decide → fold new events → derive items → append → [snapshot]
```

- **Load (#139).** Strongly consistent by default;
  `CommandHandlerOptions.consistentRead: false` opts out. A snapshot-configured
  stream loads through `readLatest` (one `Query` for the snapshot and its
  delta), so the option covers the snapshot too. On a `mode: "inline"` stream
  without `every`, `CommandHandlerOptions.verifySnapshot: false` loads the
  snapshot item alone (one `GetItem`), with verified fallbacks — see
  [Unverified snapshot loads](#unverified-snapshot-loads--verifysnapshot-false).
- **Caller expected version — If-Match (#136).** Per call,
  `handle(streamId, command, { expectedVersion })`. A loaded version other than
  `expectedVersion` fails with `VersionConflict` carrying
  `actualVersion` (the loaded version) **before `decide` runs**; otherwise the
  append is conditioned on it, and a writer that slips in between fails the
  append with `VersionConflict` (no `actualVersion`). Neither is retried. With
  `idempotency`, a pre-decide mismatch first probes the command's sentinel (one
  consistent `GetItem`, on the mismatch path only): a redelivered command that
  already committed fails with `DuplicateCommand`, matching `append`'s
  precedence. A value that is not a non-negative integer is a
  `ValidationError`, raised before anything is read.
- **Fold before append (#137).** The new events are folded into state before
  the append, so the returned, snapshotted and projected state is always the
  `evolve` fold — never anything produced inside `decide`.
- **Decision-derived items (#137).** `CommandOptions.additionalItems` takes a
  static array or a function of the `Decision`
  (`{ events, state, previous, version }`) returning the ops or an `Effect` of
  them — an inline projection. See below.
- **One atomic append.** One command → one decision → one atomic append. A
  decision too large for one transaction fails with `AppendTooLarge` before
  anything is written — see
  [Large Commands — Stepped Commands](#large-commands--stepped-commands).

#### Decision-derived `additionalItems` — inline projections (#137)

```typescript
yield* handle({ matchId }, command, {
  additionalItems: ({ state, previous, events, version }) => [
    MatchStatus.put({ matchId, status: state.status }),
  ],
})
```

- `events` is what `decide` returned (never empty — the function is not called
  for a no-op decision); `state` is the `evolve` fold of `previous` with
  `events`; `previous` is the state `decide` was given; `version` is the version
  the events are appended after.
- The function may return the ops directly or an `Effect` of them. The effect's
  `E2` / `R2` join the handler's error channel and requirements (both default to
  `never`, so the static and pure forms add nothing).
- The items commit in the **same** `TransactWriteItems` as the events. They
  count towards `AppendTooLarge` and the 4 MB check, and a failing item
  condition is `AdditionalItemConditionFailed`, with `indices` into the
  returned array.
- The function runs after `decide` and the fold, and is re-run on every retry
  attempt against the fresh decision.

**The evolve-fold state contract.** The state the handler returns, writes as a
snapshot and passes to `additionalItems` is always the `evolve` fold of stored
and new events. `evolve` **may mutate state in place** — the library does not
require immutable state, and does not enforce `decide` purity (#142 is a design
decision left to the application; see the design doc §8). When `evolve` mutates
and returns the same object, `previous` and `state` are the same reference and
`previous` already reflects the new events; a projection that needs a pristine
`previous` needs an `evolve` that returns new state.

### Snapshots

Full design: `docs/designs/eventstore-snapshots.md` (#84). Opt-in via `snapshot` on
`makeStream`:

```typescript
const MatchEvents = EventStore.makeStream({
  table: EventsTable,
  streamName: "Match",
  events: [MatchStarted, InningsCompleted, MatchEnded],
  streamId: { composite: ["matchId"] },
  snapshot: { schema: MatchStateSchema, every: 100 },
})
```

**Storage.** One snapshot item per stream, in the stream partition, overwritten in place:

```
pk           <stream partition key>                     (same as events)
sk           $<schema>#v<n>#<stream>.snapshot           (never collides with event SKs)
__edd_e__    "<stream>.snapshot"                        (excluded by event queries' filter)
streamId     "<composites joined with #>"
asOfVersion  event version the state reflects
state        Schema.encodeEffect(snapshot.schema) output
timestamp    ISO (Clock-backed)
```

Event read paths (`read`, `readFrom`, `currentVersion`, `query.events`) are additionally
SK-range-hardened (`begins_with` on the event prefix / `BETWEEN` on versions) so the
snapshot is excluded at the key-condition level, not just by the `__edd_e__` filter.
Writes are monotonic: `attribute_not_exists(pk) OR asOfVersion < :new` — a stale
concurrent snapshot write is a successful no-op.

**Primitives** (also on `BoundEventStream` with `R = never`):

```typescript
stream.writeSnapshot(streamId, state, asOfVersion)  // Effect<void, ValidationError | DynamoClientError, ...>
stream.readSnapshot(streamId)                       // Effect<Option<Snapshot<State>>, ...>
// Snapshot<State> = { state, asOfVersion, timestamp }
```

State round-trips through the user-supplied `snapshot.schema` — `Schema.encodeEffect`
on write, `Schema.decodeUnknownEffect` on read — so transforming schemas work.

**Snapshot-aware commandHandler.** When the stream declares `snapshot`, each handler
invocation loads state with `readLatest` (the snapshot plus its delta in one `Query`,
below) instead of a full replay, then runs the command path above. In the default
`mode: "after-append"`, with `every: N`, the handler writes a fresh snapshot of the
post-fold state after a successful append once ≥ N events accumulated since the last
snapshot (best-effort — a snapshot-write failure is logged and never fails the command).

### Inline Snapshots and `readLatest` (#138)

```typescript
snapshot: { schema: StateSchema, mode: "inline" }             // every append
snapshot: { schema: StateSchema, mode: "inline", every: 10 }  // inline, at a cadence
snapshot: { schema: StateSchema, every: 100 }                 // "after-append" (default)
```

`SnapshotConfig.mode` (`SnapshotMode`: `"after-append" | "inline"`) defaults to
`"after-append"`; any other value throws `[EDD-9062]` at `makeStream`. In `"inline"`
mode the handler passes the post-fold state as `AppendOptions.snapshot`, and `append`
adds an unconditional snapshot `Put` (`asOfVersion = expectedVersion + events.length`)
to the **same transaction**, after the idempotency sentinel. It commits if and only if
the events do; the event puts already prove the writer owns `asOfVersion`, so it cannot
regress. It counts towards the item and size limits, requires at least one event
(`ValidationError` otherwise), and on a stream without `snapshot` dies with
`[EDD-9026]`.

```typescript
stream.readLatest(streamId, { consistentRead? })
// Effect<LatestState<State, Event>, DynamoClientError | ValidationError, …>
// LatestState = { snapshot: Option<Snapshot<State>>; events; version }
```

`readLatest` issues one reverse `Query` over `sk BETWEEN <event SK prefix> AND
<snapshot SK>` filtered to the stream's event and snapshot items. The snapshot SK sorts
after every event and command sentinels sort before events under every casing, so the
range holds exactly the events plus the snapshot. The first page's `Limit` is
`(every ?? 1) + 1`; it pages on (each further page sized to the events still missing)
until the event at `asOfVersion + 1` is in hand or the partition is exhausted, then
returns the events after `asOfVersion` ascending. A current inline snapshot loads in one
request; a snapshot lagging by up to `every` events also loads in one, otherwise two.
The first page is read whatever the lag, so an `"after-append"` stream with a large
`every` reads up to `every + 1` items per load — the `SnapshotConfig.every` JSDoc says
so. On a stream without `snapshot`, `readLatest` is `read` plus the head.

#### Unverified snapshot loads — `verifySnapshot: false`

```typescript
stream.readLatest(streamId, { verifySnapshot: false })          // ReadLatestOptions
EventStore.commandHandler(decider, stream, { verifySnapshot: false })
```

`verifySnapshot` (default `true`) is a `readLatest` option (`ReadLatestOptions`, which
extends `ReadOptions`; `read` / `readFrom` / `currentVersion` do not take it) and a
`CommandHandlerOptions` field. With an inline snapshot written in every append, the
snapshot is normally at the head, yet the verified `Query` still reads the snapshot
**and** the newest event (`Limit: 2`). A `Query`'s read capacity counts every item it
reads; a `GetItem` reads only the snapshot — with large events, roughly half the read
capacity per command. `false` therefore reads the snapshot item alone with one `GetItem`
(consistent per `consistentRead`, so strongly consistent in `commandHandler` by
default):

- **`readLatest`** returns `{ snapshot, events: [], version: snapshot.asOfVersion }` —
  `version` is **unverified** (events appended without a snapshot may follow it). With no
  snapshot item it falls back to the verified read and returns that.
- **Accepted only on `mode: "inline"` without `every`** — the only mode in which the
  snapshot is normally at the head. Any other snapshot config: `commandHandler` throws
  `[EDD-9068]` when the handler is created (data-first and data-last, `EventStream` and
  `BoundEventStream`), and `readLatest` dies with `[EDD-9068]` (a defect, like
  `[EDD-9026]`). On a stream without `snapshot` the option is ignored (the full verified
  read, as before).

A snapshot can still lag the head (events appended raw, or data predating
`mode: "inline"`), and then **every** answer made on it is stale — a domain error and a
no-op as much as an append. So `commandHandler` returns no answer made on an unverified
snapshot until the head confirms it. A successful append confirms it by itself (its
version condition held); anything else is checked with one more read, and a stale load
falls back to a verified one:

| Situation | Behaviour |
|---|---|
| No snapshot item | `readLatest` falls back to the verified read at once; `decide` runs once |
| `expectedVersion`, snapshot `asOfVersion` ≠ it | Verified `readLatest`, then the normal If-Match check: `VersionConflict.actualVersion` is the **verified** head; the `DuplicateCommand` probe keeps its precedence. `decide` does not run |
| `expectedVersion`, snapshot `asOfVersion` = it | The snapshot is exactly the state the caller saw: decide on it. An append confirms the head. A domain error, a no-op or an append `VersionConflict` is checked with one head `Query` above the If-Match (it reads no items while the stream is still there): a head at the If-Match returns the answer; a head past it discards the decision and answers as the verified load would — `DuplicateCommand` for a committed redelivery, else `VersionConflict` with the head as `actualVersion`. Never re-decided or retried (the If-Match rule) |
| No `expectedVersion`, a domain error or a no-op | One head `Query` above the snapshot's version. Head at the snapshot → the answer stands. Head past it → the decision was made on stale state: a verified `readLatest` and **decide again once** on that state (its outcome is final for the attempt) |
| No `expectedVersion`, the append fails with `VersionConflict` | One verified `readLatest`. Events past the current snapshot → the load was stale: **decide again once** on the verified state and append (outside, and in addition to, the `retry` policy; its own conflict follows the policy). Snapshot at the head → a genuine race: the `VersionConflict` goes to the `retry` policy as always, and a retried attempt loads unverified again |

`decide` can therefore run **twice for one call** (the last two rows), and so can a
function-form `additionalItems` when both decisions append. This is the price of skipping
the verification read — the same price a `VersionConflict` retry pays: a decision is only
as current as the state it was made on, and one made on stale state cannot be patched,
only made again. Without the re-decision a lagging snapshot would fail or mis-answer every
command on the stream, because only a successful inline append rewrites it; the
re-decision's append repairs it. With an If-Match, `decide` may run on a snapshot whose
version the stream has since moved past — on exactly the state the caller saw, as in a
verified load that loses a race — but that decision is never returned. A decision that
appends nothing adds one head `Query` bounded below by the snapshot's version (reverse,
`Limit: 1`). While the snapshot is current that range is empty, so the `Query` reads no
items and costs only DynamoDB's per-request minimum, however large the events are; only a
lagging snapshot makes it read an event (the head). The trade-off for such decisions is
one extra round trip, not extra item reads.

### Large Commands — Stepped Commands

An append is always one `TransactWriteItems`: it is never split. An append receives
**one decision** — many events and the one state after all of them — so split across
transactions it would have no valid intermediate states to put at the boundaries (no
snapshot, projection or sentinel that is true there). Only the application's decider can
produce a valid state for each boundary. An append over 100 items fails with
`AppendTooLarge` (or over 4 MB with `ValidationError`) before anything is written.

A large command — an import, a bulk correction, a compensating undo — is therefore run
as **stepped commands**:

1. The application plans the steps (newest first for an undo), each covering at most a
   configured number of entities. A fixed size is preferable to one computed from the
   events, because event content varies.
2. Each step is an ordinary `commandHandler` call — decide → fold → one atomic append
   with its own snapshot, projections and sentinel — with `expectedVersion` set to the
   version the previous step returned.
3. A failure partway leaves the stream at a real, consistent intermediate state: the
   last committed step.

With `idempotency`, each step needs its own `commandId` (e.g. `` `${commandId}#step-${n}` ``),
which is also what lets a **redelivered** large command — same `commandId`, same original
`expectedVersion` — resume after a partial failure. Its first step's If-Match is stale, so
the handler consults that step's sentinel before deciding: a committed step is a
`DuplicateCommand`. The application skips it **keeping the stale If-Match**, so each
following step is checked the same way, without `decide` running; the first step not
committed is a `VersionConflict` whose `actualVersion` is the head, and runs from there,
chaining as usual. Every step committed → every step skipped, nothing written.
(`DuplicateCommand` carries no version: chaining the next step from the head instead
would make `decide` run against the steps already applied — a compensating undo would
fail its own validation.) The tutorial's helper does exactly this. An
`AppendTooLarge` on a step carries `count` and `limit`: the signal that the configured
step size is too large. Rule of thumb: **one command → one decision → one atomic
append.** The tutorial's `stepped-command` example region runs a 150-event import and
its compensating undo in fixed 50-entity steps.

### Stream Indexes (#140)

```typescript
const Entries = EventStore.makeStream({
  …,
  indexes: {
    byEntry: { index: "lsi1", sk: "lsi1sk", key: (event, version) => … },          // LSI (default)
    byDay: { type: "gsi", index: "gsi1", pk: "gsi1pk", sk: "gsi1sk", key: … },     // GSI
  },
})

stream.readIndex("byEntry", streamId, { beginsWith?, between?, reverse?, limit?, consistentRead? })
stream.query.index("byEntry", streamId)            // Query<StreamEvent<…>>
EventStore.indexDefinitions(...streams)             // CreateTable fragments
```

A stream index is a sub-stream of the stream's events ordered by a key derived from
each event. `key(event, version)` returns the sort key, stored **raw** (no casing, no
prefix), or `undefined` to leave the event out (sparse); an empty or non-string key, a
key over 1024 bytes or a throwing `key` fails the append with `ValidationError` before
anything is written. Only event items carry index attributes — snapshots and sentinels
are never indexed. Index names are a type parameter (`TIndexName`) inferred from
`indexes`; an undeclared name at runtime is a defect (`[EDD-9066]`). Definition errors:
`[EDD-9063]` malformed entry, `[EDD-9064]` attribute owned by the stream, `[EDD-9065]`
indexes sharing a physical index or attribute.

- **LSI** (`type: "lsi"`, default) uses the table's `pk`, so it holds exactly the
  stream's indexed events and supports strongly consistent reads. An LSI **must be
  created with the table**, and a table with any LSI caps the item collection of
  **every** partition key value at **10 GB** — every stream's partitions and every
  entity partition sharing the table, not only the indexed stream's.
- **GSI** (`type: "gsi"`) names a `pk` attribute that `append` fills with the stream's
  partition key value, so it is the eventually consistent, uncapped equivalent scoped
  to the same stream. `consistentRead` on it is refused.
- **Projection `ALL`** — events are decoded from the index item.
  `EventStore.indexDefinitions(...streams)` returns `AttributeDefinitions`,
  `LocalSecondaryIndexes` and `GlobalSecondaryIndexes` (projection `ALL`) to merge
  into the caller's `CreateTable` input; event tables are not derived by
  `Table.definition`. Conflicting definitions of one physical index throw
  `[EDD-9067]`.
- Index attributes are written only by `append` and events are never rewritten, so an
  index added to a stream that already holds events (a GSI added by `UpdateTable`), or
  a changed `key`, covers only events appended from then on. The library does not
  backfill.

### Command Handler Retry

```typescript
const handler = EventStore.commandHandler(matchDecider, matchEvents, { retry: 3 })
// or an Effect Schedule:
EventStore.commandHandler(matchDecider, matchEvents, {
  retry: Schedule.exponential("50 millis").pipe(Schedule.compose(Schedule.recurs(5))),
})
```

On `VersionConflict` the **full read–decide–append cycle re-runs** — never a blind
re-append of stale events, and a function-form `additionalItems` is re-derived from
the fresh decision. Only `VersionConflict` is retried; domain errors,
`DuplicateCommand` and infrastructure errors fail immediately. A call that
supplies `expectedVersion` is never retried — its `VersionConflict` is the answer
to a conditional write. A number `n` is shorthand for
`Schedule.recurs(n)` (n retries after the initial attempt). Default: no retry.

`commandHandler` dispatches data-first vs data-last on the `EventStreamTypeId` brand of
its second argument, so all four call shapes work:

```typescript
EventStore.commandHandler(decider, stream)
EventStore.commandHandler(decider, stream, { retry: 3 })
stream.pipe(EventStore.commandHandler(decider))
stream.pipe(EventStore.commandHandler(decider, { retry: 3 }))
```

---

## 13. GeoIndex (effect-dynamodb-geo)

### Overview

`GeoIndex` provides geospatial indexing and radius-based proximity search using H3 hexagonal grid. It wraps an entity with automatic geo field enrichment on writes and multi-cell parallel query on reads.

### Client Gateway Pattern

GeoIndex definitions are registered on a table and accessed through the typed client:

```typescript
// Definition — binds geo config to entity definition
const VehicleGeo = GeoIndex.make({
  entity: VehiclesEntity,
  index: "byCell",
  coordinates: (item) => ({ latitude: item.latitude, longitude: item.longitude }),
  fields: {
    cell: { field: "cell", resolution: 15 },
    parentCell: { field: "parentCell", resolution: 3 },
    timePartition: { field: "timePartition", source: "timestamp", bucket: "hourly" },
  },
})

// Register on table
const MainTable = Table.make({
  schema: AppSchema,
  entities: { Vehicles: VehiclesEntity },
  geoIndexes: { VehicleGeo },
})

// Access through typed client
const program = Effect.gen(function* () {
  const db = yield* DynamoClient.make(MainTable)
  yield* db.VehicleGeo.put({ vehicleId: "v-1", latitude: 37.77, longitude: -122.42, timestamp: now })
  const results = yield* db.VehicleGeo.nearby({ center, radius: 2000, unit: "m" })
})
```

As a service:

```typescript
class VehicleSearch extends Context.Service<VehicleSearch>()("@fleet/VehicleSearch", {
  make: Effect.gen(function* () {
    const { VehicleGeo } = yield* DynamoClient.make(MainTable)
    return VehicleGeo
  }),
}) {}
```

### Layering

GeoIndex composes Entity operations (for writes) and Query (for reads). It adds geo field enrichment and multi-cell search orchestration on top:

```
db.VehicleGeo.put(input)
  → enrich(input)           // compute H3 cell, parent cell, time partition
  → Entity.put(enriched)    // delegate to entity

db.VehicleGeo.nearby(options)
  → compute search cells    // H3 ring + prune
  → build N queries          // one per (timePartition, cell chunk)
  → execute in parallel     // via Query module
  → post-process            // distance filter + sort
```

---

## 14. Vector Search

### Overview

DynamoDB native vector search (GA 2026-08-05) adds approximate-nearest-neighbour
retrieval to the table itself: a **vector index** is declared on `CreateTable` /
`UpdateTable`, DynamoDB maintains it from an ordinary list-of-number attribute on
each item, and `SearchVectors` returns the top-K most similar items with a score.

effect-dynamodb models this the same way it models GSIs — **declaratively on the
entity**, with library-managed attributes so the domain model stays pure:

```typescript
const Products = Entity.make({
  model: Product,
  entityType: "product",
  primaryKey: { pk: { field: "pk", composite: ["productId"] }, sk: { field: "sk", composite: [] } },
  vectorIndexes: {
    byDescription: {
      name: "vec1",                      // physical vector index on the table
      dimensions: 1024,                  // immutable after CreateTable
      distance: "cosine",                // cosine | euclidean | dotProduct — immutable
      source: { fields: ["name", "description"] },
      partition: ["tenantId"],           // optional extra HASH composites
      filters: ["category", "status"],   // INLINE_FILTER attributes (equality-only)
    },
  },
})
```

Two library-managed attributes back each entity/index pair:

| Attribute | Purpose |
|---|---|
| `__edd_v_<physicalName>__` | The stored embedding (DynamoDB `L` of `N`). |
| `__edd_vp_<physicalName>__` | The composed HASH partition value (always present). |

INLINE_FILTER attributes are ordinary model fields — no wrapper attribute. The
naming mirrors `__edd_e__`: deliberately ugly so it cannot collide with a user
field, and never surfaced in the domain model or the decoded record.

### Partition composition — entity scoping for free

The vector index HASH attribute is **always** declared and **always** composed by
`KeyComposer`, using exactly the same prefixing and casing rules as every other
key:

```
$schema#v1#<entityType>[#<partition composite values>]
```

Two consequences fall out of this, both deliberate:

1. **Entity scoping is automatic.** The entity type is baked into the partition
   value, so a `SearchVectors` call on a shared physical index only ever sees one
   entity type's items. This is the vector-search analogue of the `__edd_e__`
   FilterExpression that scopes every Query/Scan — but it costs nothing at read
   time because it is the partition key, not a filter.
2. **Multiple entities can share one physical vector index.** The 5-index-per-table
   quota is tight for single-table designs; sharing is the norm rather than the
   exception. `Table.make` validates that every entity sharing a physical index
   agrees on `dimensions` and `distance` (they are immutable at the DynamoDB level,
   so disagreement is unrepresentable). `filters`, by contrast, are per-entity
   access patterns rather than a shared physical property, so they are **unioned**
   across sharers when the `SearchSchema` is emitted — keeping only the first
   declarer's set would silently leave the other sharers' filter attributes
   un-indexed. The 18-filter limit is checked against the union.

With `partition: ["tenantId"]`, tenant scoping composes into the same attribute,
and DynamoDB's per-partition-value throughput ceilings (1 GBps search, 10 MBps
write) distribute per tenant rather than per table.

**Sparse semantics.** DynamoDB indexes an item only when it carries BOTH the
vector attribute and (when declared) the HASH attribute. Removing either removes
the index entry. The library relies on this for lifecycle stripping (below) —
there is no "delete from index" operation to call.

### Embedder service

Embedding generation is entirely the application's responsibility (DynamoDB never
computes or refreshes a vector). The library models the generator as a service:

```typescript
// @effect-dynamodb/schema — AWS-free
export interface EmbedderService {
  readonly embed: (text: string) => Effect.Effect<ReadonlyArray<number>, EmbeddingError>
  readonly dimensions: number
}
export class Embedder extends Context.Service<Embedder, EmbedderService>()(
  "@effect-dynamodb/Embedder",
) {}
```

`Embedder.layerTest({ dimensions })` ships in-library: a deterministic
hash-based embedder used by the test suite, the connected suite, and the runnable
examples. A Bedrock-backed implementation is documented as an **example**, not
shipped — that would drag `@aws-sdk/client-bedrock-runtime` into the runtime graph
for every consumer.

`DynamoClient.make({ ..., embedder })` accepts an explicit service; otherwise the
`Embedder` service is resolved from context if present. Resolution mirrors the
`Crypto` bundling pattern exactly, so bound entity operations stay `R = never`.
Dimension agreement between the layer and every bound vector index is validated at
`DynamoClient.make` time — a mismatch is a construction-time failure, never a
runtime surprise on the thousandth write.

### Write path

| Operation | Embedder invoked? |
|---|---|
| `put` / `create` | Always (the full item is being written). |
| `upsert` | Always — emitted as plain `SET`, never `if_not_exists`, so an upsert that overwrites the source overwrites the vector derived from it. |
| `update` / `patch` | **Only** when the write touches a `source.fields` member. |
| `.withVector(name, vector)` | Never — the caller supplies a pre-computed embedding. |
| `.append()` (time-series) | Never on event items; vector attributes are stripped. |

"Touches" spans every channel that can change the source, not just the `set()`
payload: a `remove([...])` clearing a source field, a null/undefined payload
entry, and a path-based operation whose root segment names a source field all
fire the gate. This is the same discipline the policy-aware GSI composer applies
to `removedSet` (§7) — a clear IS a change, and an embedding that survives its
source is worse than no embedding at all.

Otherwise the gate mirrors §7 exactly: a writer that touches none of the source
must not pay for an embedding call, and must not clobber a vector another writer
owns. No source change ⇒ no Embedder call ⇒ stored vector untouched.

**Clearing the source removes the item from the index.** When the gate fires but
the post-update record has no source text left, the write emits `REMOVE` for the
vector AND partition attributes. Sparse semantics then drop the item out of the
index — which is the only way to delete a vector index entry. `.withVector()` on
the same write wins over this, since an explicit vector is not derived from the
source at all.

The partition attribute is composed on every write that composes keys, exactly
like a GSI half whose composites are all primary-key members.

**Out-of-band refresh.** `db.entities.Products.reembed({ concurrency })` streams
the entity's items (Scan scoped by `__edd_e__`), re-derives source text, embeds,
and writes the vector back. This is the migration path when the embedding model
changes — DynamoDB will never recompute a vector for you.

### Lifecycle integration

Version snapshots and soft-delete tombstones already strip GSI key attributes so
they cannot appear in index queries. Vector and partition attributes join the same
strip set, and sparse semantics do the rest:

- **Version snapshots** (`versioned: { retain: true }`) — vector + partition
  attributes stripped. A snapshot is never an ANN hit.
- **Soft delete** — vector + partition attributes stripped, but the vector is
  **stashed** under `__edd_vs_<physicalName>__` (a non-indexed attribute).
  `restore()` un-stashes it, so restoring never costs an Embedder call.
- **Time-series event items** — vector + partition attributes stripped, like GSI
  keys. Only the current item is searchable.
- **Purge / hard delete** — nothing to do; deleting the item deletes the entry.

### Query path — `BoundVectorQuery`

Each declared vector index becomes an accessor on the bound entity, in the same
family as index query accessors:

```typescript
const hits = yield* db.entities.Products
  .byDescription("waterproof hiking boots")   // string ⇒ Embedder; number[] accepted directly
  .partition({ tenantId })                     // required by the types iff partition composites declared
  .filter({ category: "footwear" })            // equality-only shorthand
  .topK(25)                                    // clamped to 1..100; default 10
  .select(["name", "price"])
  .collect()
// Array<{ item: Product; similarity: Similarity; rawScore: number }>
```

Deliberate shape decisions:

- **`.collect()` is the only terminal.** `SearchVectors` has no pagination and no
  cursor, so `.fetch()`, `.paginate()`, `.startFrom()`, `.maxPages()`, and
  `.reverse()` are *structurally absent* from the type rather than present and
  failing. The builder cannot express an operation the API does not have.
- **`.filter()` is equality-only, over declared attributes only.** The API
  Reference restricts `SearchConditionExpression` to `=` for both HASH and
  INLINE_FILTER attributes; that restriction lives in exactly one type alias
  (`VectorFilterInput`) so that when AWS relaxes it — the SDK JSDoc already
  advertises range operators for INLINE_FILTER — one edit widens the surface.
  Separately, only attributes listed in the index's `filters: [...]` are
  filterable: the accessor types the filter keys as that declared tuple, the
  terminal re-checks at runtime with a `ValidationError`, and the emulation layer
  rejects an undeclared attribute the way real DynamoDB does. All three exist so
  an undeclared filter cannot pass locally and fail in production.
- **`.partition()` is required iff partition composites are declared.** Mirrors
  the "PK composites required" rule on index query accessors, enforced at the type
  level via a conditional on the declared `partition` tuple.
- **`Similarity` is branded and normalized higher-is-more-similar**, regardless of
  distance function. `rawScore` preserves the wire value.

| Distance function | Wire score | Direction | `Similarity` |
|---|---|---|---|
| `cosine` | 0 … 2 | lower is closer | `1 - raw / 2` → 0 … 1 |
| `euclidean` | 0 … ∞ | lower is closer | `1 / (1 + raw)` → 0 … 1 |
| `dotProduct` | −∞ … ∞ | higher is closer | `raw` (already correct) |

Reference ranking for query `[1, 0, 0, 0]` (from the DynamoDB developer guide) —
reproduced verbatim by the emulation layer's unit tests:

| Stored vector | cosine | euclidean | dotProduct |
|---|---|---|---|
| `[1, 0, 0, 0]` | 0.0 | 0.0 | 1.0 |
| `[10, 0, 0, 0]` | 0.0 | 9.0 | 10.0 |
| `[0.7071, 0.7071, 0, 0]` | 0.29 | 0.77 | 0.71 |
| `[-1, 0, 0, 0]` | 2.0 | 2.0 | −1.0 |

**Backfill.** `SearchVectors` errors while a newly added index is backfilling.
That failure is surfaced as `VectorIndexBackfilling`, whose message points at
`db.tables.MainTable.waitForVectorIndex(name)`.

### Table operations

- `db.tables.MainTable.create()` emits merged `VectorIndexes` derived from every
  registered entity's vector index declarations (deduplicated by physical name).
- `db.tables.MainTable.addVectorIndex(name)` / `.removeVectorIndex(name)` emit
  `UpdateTable` with `VectorIndexUpdates`.
- `db.tables.MainTable.waitForVectorIndex(name, options?)` polls `DescribeTable`
  until the index reports `ACTIVE` and `Backfilling: false`.

Vector search is **on-demand capacity only** and the raw operation needs the new
`dynamodb:SearchVectors` IAM action — it is NOT covered by existing read policies.
Endpoint routing to `search-dynamodb.{region}.amazonaws.com` happens automatically
inside the standard `DynamoDBClient` (an explicit `endpoint` override wins), so
there is no second client to configure.

**AttributeDefinitions carry the SearchSchema** (verified against the live
service, 2026-08-17): `CreateTable`/`UpdateTable` reject a vector index whose
SearchSchema elements are not declared in `AttributeDefinitions` ("One element in
SearchSchema is not defined in attribute definitions"). `Table.definition` and
`addVectorIndex` therefore emit an `S` definition for the composed HASH partition
attribute and a per-field definition for every INLINE_FILTER attribute. Since
`AttributeDefinitions` admits only scalar types, a filter field must encode to a
string (`S`) or number (`N`) — derived from the model schema at `Entity.make`
(EDD-9039; EDD-9040 when entities sharing an index disagree on a stored filter
attribute's type). DynamoDB Local inverts the requirement — it discards
`VectorIndexes` and rejects the now-"unreferenced" definitions — so
`VectorSearchEmulation` strips vector-only `AttributeDefinitions` and
`VectorIndexes`/`VectorIndexUpdates` before forwarding table operations.

### Local emulation

DynamoDB Local does **not** support vector search: `CreateTable` silently accepts
and discards `VectorIndexes`, and `SearchVectors` fails with
`UnknownOperationException`. LocalStack wraps DynamoDB Local and inherits the gap.

`VectorSearchEmulation.layer` wraps a `DynamoClient` layer and replaces
`searchVectors` with a Scan + brute-force implementation: all three distance
functions with faithful score directions, HASH/INLINE_FILTER equality predicates,
projection, and TopK. Connected tests and the runnable example execute through it.
It is the only way to exercise the full write→search round trip without an AWS
account, and its ranking output is unit-tested against the developer-guide table
above.

```typescript
const DdbLocal = DynamoClient.layer({ region: "us-east-1", endpoint: "http://localhost:8000" })
const Emulated = VectorSearchEmulation.layer(DdbLocal)
```

### Limits enforced

| Limit | Value | Enforced at |
|---|---|---|
| Dimensions | 1 … 4096 | `Entity.make` |
| Vector indexes per table | 5 | `Table.definition` |
| INLINE_FILTER attributes per index | 18 | `Entity.make` |
| HASH attributes per index | 1 (library-composed) | by construction |
| `TopK` | 1 … 100 (default 10) | `.topK()` clamp |
| Capacity mode | on-demand only | documented |

---

## 15. Error Types

### Complete Error Taxonomy

| Error | Cause |
|-------|-------|
| `DynamoError` | AWS SDK error wrapper |
| `ItemNotFound` | No item: `get`, `update` of a missing item (unless a plain `.set()` of a complete item, which is created), `restore` without a tombstone |
| `ConditionalCheckFailed` | A user `.condition()` failed, or an op's own guard did: `create()` of an existing item (or aggregate), `patch()` or `deleteIfExists()` of a missing item |
| `ValidationError` | Schema decode/encode failure, or a refused operation: an item with an incarnation token but no version, a write that would overwrite a different `v#N` snapshot, a replacing put in `Batch.write`, a condition or filter with an empty part under `or` / `not` (or an `or()` with no parts, or an `isIn` with no values), `consistentRead` on a GSI, `expectedVersion` on an unversioned entity |
| `TransactionCancelled` | Transaction failed with cancellation reasons |
| `UniqueConstraintViolation` | Sentinel item already exists for unique field (from the entity's write, `transactWrite`, or an `append`'s `additionalItems`) |
| `OptimisticLockError` | A versioned write lost a version race (`expectedVersion` mismatch, a concurrent write between a read-then-write update's read and write, or a guarded put / upsert / transaction put that lost the race on every attempt); carries the real `actualVersion` |
| `ConcurrentModification` | An unversioned read-then-write write found an attribute it read changed before its write landed (or lost a guarded put's race on every attempt), or a unique sentinel it was releasing changed hands; nothing written (`attributes`, `current`) |
| `TransactionOverflow` | A write's own transaction (item, sentinels, snapshot) would exceed 100 items |
| `UpdateAppliedButUnreadable` | A retain update was applied at `version` but its result could not be read back provably; do not retry (`version`, `reason`) |
| `ItemNotDeleted` | `restore` found a live item under the key alongside the tombstone |
| `RefNotFound` | Referenced entity does not exist during hydration |
| `AggregateAssemblyError` | Collection query returned unexpected/incomplete data |
| `AggregateDecompositionError` | Decomposition produced items that fail schema validation |
| `AggregateTransactionOverflow` | Sub-aggregate exceeds 100-item transaction limit |
| `CascadePartialFailure` | Cascade update partially failed (eventual mode) |
| `EmbeddingError` | `Embedder.embed` failed, or no `Embedder` was provided for an entity with vector indexes |
| `VectorIndexBackfilling` | `SearchVectors` called while the vector index is still backfilling |
| `VersionConflict` | An `EventStore` append's version guard failed — the stream moved past `expectedVersion` (or is behind it) — or a `commandHandler` call's caller-supplied `expectedVersion` (If-Match, #136) did not match the loaded version. `actualVersion?` is set by the pre-decide check (the loaded version — after an unverified snapshot load, `verifySnapshot: false`, the verified head re-read before the check) and by an unverified-snapshot If-Match whose decision is confirmed against the head (the head read), and unset on any other append-time conflict, where the actual version is unknown without another read |
| `DuplicateCommand` | An `EventStore` `commandId` was already applied to the stream |
| `AdditionalItemConditionFailed` | A condition the caller set on an `EventStore` `additionalItems` op failed (`indices` into the caller's array) — not a version conflict |
| `AppendTooLarge` | An `EventStore` append exceeds 100 transact items; nothing is written and an append is never split. For a stepped command, `count` against `limit` means the step size is too large |

### Declared Errors per Operation

Each operation declares a fixed error union, whatever the entity's
configuration: a `put` declares `UniqueConstraintViolation` even on an entity
without unique constraints. Every union also includes `DynamoClientError`,
plus `RefErrors` / `VectorErrors` where the entity has refs or vector indexes.

| Operation | Declared errors (besides `DynamoClientError`) |
|-----------|-----------------------------------------------|
| `put` | `ValidationError`, `UniqueConstraintViolation`, `OptimisticLockError`, `ConcurrentModification`, `TransactionOverflow` |
| `create` | as `put`, plus `ConditionalCheckFailed` |
| `upsert` | as `put`, plus `ItemNotFound`, `ConditionalCheckFailed` |
| `update` | `ItemNotFound`, `OptimisticLockError`, `ConcurrentModification`, `UpdateAppliedButUnreadable`, `UniqueConstraintViolation`, `ValidationError`, `TransactionOverflow` |
| `patch` | as `update`, plus `ConditionalCheckFailed` |
| `delete` | `ItemNotFound`, `OptimisticLockError`, `ConcurrentModification`, `ValidationError`, `TransactionOverflow`, `DeleteAppliedButUnreadable` |
| `deleteIfExists` | as `delete`, plus `ConditionalCheckFailed` |
| `restore` | `ItemNotFound`, `ItemNotDeleted`, `ValidationError`, `UniqueConstraintViolation`, `TransactionOverflow` |
| `purge` | `ValidationError` |
| `Transaction.transactWrite` | `ValidationError`, `TransactionCancelled`, `UniqueConstraintViolation`, `OptimisticLockError`, `ConcurrentModification` |
| Aggregate `create` | `AggregateWriteError` plus `ConditionalCheckFailed` |
| `EventStore` `append` / `commandHandler` (plus the decider's errors, and a function-form `additionalItems` effect's `E2`) | `AppendError`: `VersionConflict`, `DuplicateCommand`, `AdditionalItemConditionFailed`, `AppendTooLarge`, `ValidationError`, `TransactionCancelled`, `UniqueConstraintViolation`, `OptimisticLockError`, `ConcurrentModification` |
| `EventStore` `read` / `readFrom` / `currentVersion` / `readLatest` / `readIndex` | `ValidationError` |

`.condition()` adds `ConditionalCheckFailed` to an operation that lacks it.

---

### Key encoding: one rule for every path

Both paths compose keys through the same rule:

> Compose from the **Encoded** form, EXCEPT when the domain type is numeric
> (`number`/`bigint`) and the encoded form is a **string** — then compose from the
> numeric **Type** form so `serializeValue` pads it.

`serializeValue` zero-pads numbers to 16 digits and bigints to 38 so they sort
correctly; a numeric composite stored via its string wire form would skip that padding
and sort lexicographically (`100 < 42 < 5`). `DynamoModel.DateEpochMs` composites use
the encoded epoch, which pads as a number.

Every composition site routes through this one function. `test/KeyFormInvariant.test.ts`
reads each module as source text and fails if a `KeyComposer` call receives a record
that did not — the guard exists because eleven modules each deciding independently is
what produced the divergence it replaced.

**This changed stored keys in 1.16.0, and rows written before it are orphaned.**

- Entity keys and GSIs with a `Schema.BigIntFromString` or `Schema.NumberFromString`
  composite: previously written unpadded (`…#seq_420`), now padded
  (`…#seq_000…0420`). `put` succeeded and wrote those rows on 1.15.0 — only `get` was
  broken — so **this data exists and must be rewritten** (read by scan, re-`put`).
- Aggregate partition and collection keys with a `DateEpochMs` / `DateEpochSeconds`
  composite: ISO form → padded epoch. Same migration.

An earlier draft of this section argued the entity change needed no migration because
"no data existed in either format". That was wrong: on 1.15.0 `put({seq: 420n})`
succeeds and writes `seq_420`. Only reads were broken, so callers do have rows in the
old format.

Composites of every other shape — plain numbers, bigints, strings, `Schema.Date`,
`DateTimeUtc`, literals — are byte-identical and need no migration.
### Diagnostic Code Registry (`EDD-xxxx`)

Configuration and usage mistakes carry a stable `EDD-xxxx` code in the message, so an
error can be searched for without matching on prose that may be reworded. Codes are
allocated in bands by subject area; **the number is permanent once released** — retire a
code rather than reusing it for a different condition.

**Before allocating a new code, add the row here first.** The registry is the allocation
record, not a summary written afterwards. Codes had been allocated by grepping the source,
which does not see other branches in flight: two parallel branches both took `EDD-9046`
for unrelated errors and the collision was caught only at review.

| Band | Subject |
|------|---------|
| 9001–9008 | Entity definition: keys, indexes, `configure()` |
| 9010–9016 | Time-series (`timeSeries`) |
| 9020–9027 | Sparse maps, composite nullability, EventStore snapshots |
| 9030–9040 | Vector search |
| 9041–9044 | Aggregates, timestamps |
| 9045–9050 | Query sort-key conditions, `purge`, multi-item write paths, key encoding |
| 9051–9058 | Aggregate cursors, read-path descriptors, sort-key pins, client predicates, collection casing, nested aggregates, self dates |
| 9059–9061 | Reserved — PR #129 (`transactWrite` updates) |
| 9062–9067 | EventStore command path: snapshot mode, stream indexes |

| Code | Raised in | Condition |
|------|-----------|-----------|
| `EDD-9001` | `Entity.ts` | Primary key declares no composite attribute in either `pk` or `sk` |
| `EDD-9002` | `Entity.ts`, `DynamoClient.ts` | A required partition-key composite is missing for an index |
| `EDD-9003` | `KeyComposer.ts` | Malformed `GsiConfig` — missing `name`/`pk`/`sk`, or the pre-v3 `index` property |
| `EDD-9004` | `DynamoClient.ts` | Sort-key composite supplied without its prior composites (prefix ordering) |
| `EDD-9005` | `Entity.ts` | TTL string is not a valid duration, or resolves to a non-finite duration |
| `EDD-9006` | `DynamoClient.ts` | Unknown composite attribute named for an index |
| `EDD-9007` | `DynamoModel.ts` | `configure()` field rename collides with an existing model field |
| `EDD-9008` | `Entity.ts` | `generatedId.field` does not name a model field, or does not participate in the key |
| `EDD-9010` | `Entity.ts` | `.history()` called on an entity with no `timeSeries` config |
| `EDD-9011` | `Entity.ts` | `timeSeries.orderBy` also appears as a primary-key composite |
| `EDD-9012` | `Entity.ts` | `timeSeries` and `versioned` are mutually exclusive |
| `EDD-9013` | `Entity.ts` | `timeSeries.appendInput` omits `orderBy` |
| `EDD-9014` | `Entity.ts` | `timeSeries.orderBy` names a ref field |
| `EDD-9015` | `Entity.ts` | `timeSeries` and `softDelete` are mutually exclusive |
| `EDD-9016` | `Entity.ts` | `timeSeries.appendInput` is required and was not supplied |
| `EDD-9020` | `Entity.ts` | Sparse field does not exist on the model, or is not a `Schema.Record` |
| `EDD-9021` | `Entity.ts` | Sparse field has a Record-typed value schema |
| `EDD-9022` | `Entity.ts` | Sparse field is also a primary-key or GSI composite |
| `EDD-9023` | `Entity.ts` | Sparse-field prefixes collide with each other or with a non-sparse field |
| `EDD-9024` | — | **Retired** in v1.7.1. Do not reuse |
| `EDD-9025` | `Errors.ts` | Composite attribute schema admits `null` |
| `EDD-9026` | `EventStore.ts` | Snapshot operation on a stream with no snapshot config |
| `EDD-9027` | `EventStore.ts` | `snapshot.every` is not a positive integer |
| `EDD-9030` | `VectorIndex.ts` | Vector index `dimensions` invalid |
| `EDD-9031` | `VectorIndex.ts` | Vector index declares no `source.fields`, or references an unknown field |
| `EDD-9032` | `VectorIndex.ts`, `Table.ts` | Vector index resolves to more INLINE_FILTER entries than allowed |
| `EDD-9033` | `VectorIndex.ts` | Two vector indexes on one entity resolve to the same physical index |
| `EDD-9034` | `VectorIndex.ts`, `Table.ts` | Too many vector indexes declared on an entity or table |
| `EDD-9035` | `Table.ts` | Entities sharing a vector index disagree on `dimensions` or `distance` |
| `EDD-9036` | `DynamoClient.ts` | No registered entity declares the requested vector index |
| `EDD-9037` | `DynamoClient.ts` | Vector index declaration mismatch at bind time |
| `EDD-9038` | `DynamoClient.ts` | Vector index name collides with an existing index |
| `EDD-9039` | `VectorIndex.ts` | Vector filter field's encoded type is not derivable from the model |
| `EDD-9040` | `Table.ts` | Vector index filter attribute is not a stored attribute |
| `EDD-9041` | `Aggregate.ts` | Aggregate collection PK field conflicts with the table's |
| `EDD-9042` | `Aggregate.ts` | `consistentRead` requested against a GSI-backed aggregate collection |
| `EDD-9043` | `Aggregate.ts` | `collection.index` and `collection.sk` must be supplied together |
| `EDD-9044` | `internal/EntitySchemas.ts` | Timestamp `schema` carries no `DynamoEncoding` annotation |
| `EDD-9045` | `DynamoClient.ts` | `.where()` used on an index whose sort key has no composites — unreachable from well-typed code since the `ResolveSkFields` repair (#121); retained for untyped and cast call sites |
| `EDD-9046` | `DynamoClient.ts` | Strict `lt` on the last sort-key composite with an earlier composite pinned — inexpressible in one DynamoDB sort-key condition |
| `EDD-9047` | `Entity.ts` | `.condition()` applied to `purge()`, which spans batched writes and cannot be guarded atomically |
| `EDD-9048` | `internal/TransactableOps.ts` | A multi-item write path cannot compile a **delete** for an entity with `unique`, `versioned: { retain: true }` or `softDelete` — those side items derive from the *stored* row, which these paths never read |
| `EDD-9049` | `Batch.ts` | `Batch.write` cannot compile a write for those same configs — `BatchWriteItem` has no `ConditionExpression` (the whole basis of a uniqueness sentinel), no `UpdateRequest`, and no atomicity across chunks |
| `EDD-9050` | `internal/CompositeCodec.ts` | A key composite's value cannot be encoded to its wire form, so it cannot be placed in a key — raised rather than composing a string that silently matches nothing |
| `EDD-9051` | `Aggregate.ts` | `list({ cursor })` on a **sharded** aggregate (`list.cardinality`) — a fan-out over N partitions has no resumable position, so the cursor is rejected rather than silently ignored |
| `EDD-9052` | `Batch.ts`, `Transaction.ts` | A read path (`Batch.get`, `Transaction.transactGet`, `Transaction.check`) was handed something that is not a get descriptor — pass `Entity.get(key)` or the bound `db.entities.X.get(key)` |
| `EDD-9053` | `DynamoClient.ts` | `.where()` targets a sort-key composite the accessor already pinned — `Query.where` REPLACES the accessor's `begins_with`, so the condition would discard the pin and return rows outside it rather than narrowing within it |
| `EDD-9054` | `Query.ts` | A client-side predicate (`.filterBy()`) and a projection (`.select()`) are both active — the predicate is an opaque closure, so its attribute reads cannot be borrowed into the `ProjectionExpression` the way key attributes are, and it would be handed items missing the fields it tests |
| `EDD-9055` | `KeyComposer.ts` (via `DynamoClient.ts`, `Collection.ts`) | A collection's members compose its keys with different casings (index `casing` vs schema `casing`) — they share one physical index, so their keys would never meet |
| `EDD-9056` | `Aggregate.ts` | A nested sub-aggregate binding declares a discriminator attribute it already inherits from an enclosing binding — the inner value would overwrite the outer one on the inner rows, so the parent's bindings could no longer be told apart. Use a distinct attribute name (e.g. `{ squadNo: 1 }` inside `{ clubNo: 1 }`) |
| `EDD-9057` | `internal/EntitySchemas.ts` | A `DynamoModel.configure` `storedAs` override on a union field with more than one self-date member — the override cannot say which member it applies to. Annotate the intended member with `.pipe(DynamoModel.storedAs(...))` instead |
| `EDD-9058` | `internal/EntitySchemas.ts` | A union's self-date member is stored as an epoch number next to a member also stored as a number (`Number`, a number literal, `BigInt`, another epoch date) — a stored number could belong to either, so it cannot be read back reliably. On aggregates a member whose DOMAIN is numeric (`NumberFromString`, `BigIntFromString`) is rejected too, since `update` re-decodes domain values. Store the date as a string, or remove the numeric member |
| `EDD-9059` | `internal/TransactableOps.ts` | An `update` with `.cascade(...)` in a transaction — a cascade is a follow-up write to other entities after the update commits, so it cannot share the transaction |
| `EDD-9060` | `internal/TransactableOps.ts` | `.returnValues(...)` returning an item on an `update` or `delete` in a transaction — a transaction returns no item attributes, so the mode would be dropped |
| `EDD-9061` | `internal/TransactableOps.ts` | An `update` of an entity with `vectorIndexes` in a transaction — recomputing the embedding needs the `Embedder` service, which the transact path does not provide |
| `EDD-9062` | `EventStore.ts` | `snapshot.mode` is neither `"after-append"` nor `"inline"` |
| `EDD-9063` | `EventStore.ts` | Malformed stream index: a `type` other than `"lsi"` / `"gsi"`, a `gsi` without `pk`, an `lsi` with `pk`, an empty `index` / `sk` / `pk`, or a `key` that is not a function |
| `EDD-9064` | `EventStore.ts` | A stream index attribute collides with an attribute the stream writes itself (`pk`, `sk`, `__edd_e__`, `streamId`, `version`, `eventType`, `data`, `metadata`, `timestamp`, `asOfVersion`, `state`, `commandId`, `_ttl`) |
| `EDD-9065` | `EventStore.ts` | Two indexes of one stream share a physical index name or an attribute, or a GSI's `pk` and `sk` are the same attribute |
| `EDD-9066` | `EventStore.ts` | `readIndex` / `query.index` called with a name the stream does not declare (a defect; unreachable from typed code) |
| `EDD-9067` | `EventStore.ts` | `indexDefinitions` given two streams that define the same physical index differently |
| `EDD-9068` | `EventStore.ts` | `verifySnapshot: false` on a stream whose snapshot config is not `mode: "inline"` without `every` — thrown by `commandHandler` at handler construction, a defect from `readLatest` |

Next free code: **`EDD-9069`** (or `9009`, `9017`–`9019`, `9028`–`9029` within their bands).

## Appendix A: Migration Guide (v1 → v2 → v3)

### v2 → v3: Client Gateway Migration

| v2 (bind pattern) | v3 (client gateway) |
|--------------------|---------------------|
| `Entity.make({ model, table: MainTable, ... })` | `Entity.make({ model, ... })` — no `table` |
| `Table.make({ schema })` | `Table.make({ schema, entities: { Users }, aggregates: { Matches } })` |
| `yield* Entity.bind(Users)` | `const { Users } = yield* DynamoClient.make(MainTable)` |
| `yield* Aggregate.bind(MatchAggregate)` | `const { Matches } = yield* DynamoClient.make(MainTable)` |
| `yield* Table.bind(MainTable)` → `table.create([Users])` | `db.createTable()` |
| `yield* EventStore.bind(MatchEvents)` | `const { MatchEvents } = yield* DynamoClient.make(EventsTable)` |
| `yield* GeoIndex.bind(VehicleGeo)` | `const { VehicleGeo } = yield* DynamoClient.make(MainTable)` |

### v1 → v2: Module-by-Module Mapping

| v1 Module | v2 Module | Changes |
|-----------|-----------|---------|
| `DynamoModel.ts` | `DynamoModel.ts` | Annotations (Hidden, identifier, ref) and `configure()` for per-field overrides (immutable, field rename, storedAs). Models use `Schema.Class`. |
| `Table.ts` | `Table.ts` | Stripped to `schema` ref only. Physical name via `Table.layer()`. Key structure derived from entities. |
| `Entity.ts` | `Entity.ts` | Major redesign: ElectroDB-style indexes, system fields, unique constraints, collections. |
| `KeyComposer.ts` | `KeyComposer.ts` | Rewritten: attribute-list composition, convention-based format, casing rules. |
| `EntityRepository.ts` | Merged into `Entity.ts` | Operations are now methods on the Entity object. No separate repository. |
| `Collection.ts` | `Collection.ts` | Typed entity selectors, pipeable queries, isolated/clustered modes. |
| `Transaction.ts` | Absorbed into Entity | Transactions are now internal to Entity operations. |
| `DynamoClient.ts` | `DynamoClient.ts` | Adds `updateItem` operation. |
| — | `DynamoSchema.ts` | **New**: Application namespace and versioning. |
| — | `Query.ts` | **New**: Pipeable query data type with combinators. |

## Appendix B: Full Walkthrough — Multi-Tenant SaaS

See `walkthrough.md` for a complete walkthrough demonstrating a multi-tenant project management system with three entities: Tenant, Employee, and Task, exercising all major features.
