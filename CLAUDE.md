# CLAUDE.md — Effect DynamoDB ORM

> **Version:** This project targets **Effect v4** (`effect@^4.0.0`, stable since 2026-10-01, published from the main [Effect-TS/effect](https://github.com/Effect-TS/effect) repo). The former `effect/unstable/*` modules now live at top-level paths (`effect/http`, `effect/http-api`, …); modules still tagged `@stability unstable` may change in minor releases. Toolchain: TypeScript 6, pnpm 12 (pinned via `packageManager`), vitest 5. The v3 codebase is preserved at git tag `effect-dynamodb-v3`.

## Project Overview

Effect TS ORM for DynamoDB providing Schema-driven entity modeling, single-table design as a first-class pattern, composite key composition from entity attributes, type-safe index-aware queries with Stream-based pagination, and DynamoClient as an Effect Service with Layer-based dependency injection.

**Status:** All modules implemented. 1902 core tests, 368 schema tests, 56 geo tests, 90 language-service tests, 48 doctest tests, 35 examples, 528 connected tests against DynamoDB Local.
**Design:** `DESIGN.md` — API specification (source of truth for implementation)

## Architecture

### Client Gateway Pattern

`DynamoClient.make({ entities, aggregates?, tables? })` is the **typed execution gateway** — the central pattern of this library. **There is only one form** — the table-shortcut overload was removed; the entity-centric form is canonical.

1. `Entity.make({ model, primaryKey, indexes })` — entity defines domain model + primary key + GSI indexes (with optional `collection` property)
2. `Table.make({ schema, entities: { Users, Tasks } })` — registers entities on a physical table
3. `yield* DynamoClient.make({ entities: { Users, Tasks }, aggregates: { OrderAggregate }, tables: { MainTable } })` — binds the listed entities/aggregates, auto-discovers collections from entity indexes, and returns a typed client with `R = never`
4. Access via `db.entities.*` (CRUD + query accessors), `db.aggregates.*` (bound aggregates), `db.collections.*` (auto-discovered cross-entity queries), `db.tables.*` (table management)
5. Use the typed client inside `Context.Service` make effects for DI and layer-based testing

```
┌─────────────────────────────────────┐
│  DynamoClient.make({ entities, ... })│ ← typed gateway (binds all members, R = never)
├─────────────────────────────────────┤
│  Aggregate / GeoIndex / EventStore  │  ← orchestration (decompose, assemble, diff)
├─────────────────────────────────────┤
│  Collection / Transaction / Batch   │  ← multi-entity coordination
├─────────────────────────────────────┤
│  Entity                             │  ← domain model + primary key + indexes + CRUD
├─────────────────────────────────────┤
│  Table.make()                       │  ← physical table definition (schema + entities)
├─────────────────────────────────────┤
│  DynamoClient (raw service)         │  ← raw AWS SDK operations
└─────────────────────────────────────┘
```

### Adding New Entities

1. Define model with `Schema.Class` (or `Schema.Struct`) — pure domain fields only. Use `DynamoModel.configure(model, { field: { immutable: true } })` for fields that shouldn't change after creation.
2. Create entity definition with `Entity.make({ model, entityType, primaryKey, indexes?, timestamps?, versioned?, softDelete?, unique? })` — primary key + GSI indexes. Use `collection` property on indexes for cross-entity queries.
3. Register entity on a table: `Table.make({ schema, entities: { ..., MyEntity } })`.
4. Access via typed client: `const db = yield* DynamoClient.make({ entities: { MyEntity }, tables: { MainTable } })` → `db.entities.MyEntity.get(...)`, `db.entities.MyEntity.put(...)`, `db.entities.MyEntity.byIndex({...}).collect()`. Collections auto-discovered: `db.collections.myCollection({...}).collect()`.
5. For services: build the typed client inside `Context.Service` make effects for DI and layer-based testing.
6. Add unit tests in `test/` and update integration test if needed.

### Module Structure

> **Package split (issue #62):** the pure, AWS-free derivation layer lives in
> `@effect-dynamodb/schema` (`packages/schema/src/`): `DynamoModel`, `DynamoSchema`,
> `KeyComposer`, `Errors`, `Projection`, the entity/aggregate derivation internals
> (`EntityConfig`, `EntityTypes`, `EntitySchemas`, `Aggregate*`, `SchemaAccessors`,
> `DefaultCrypto`), and a **pure `Entity.make` / `Aggregate.make`** that return a
> definition carrying the derived `inputSchema` / `updateSchema` / `createSchema`
> (no CRUD ops). It has ZERO `@aws-sdk` in both its runtime graph and `.d.ts`
> (guarded by `packages/schema/test/aws-free-guard.test.ts`). `effect-dynamodb`
> depends on and re-exports the schema package, then adds the AWS runtime below.
> The operational `Entity`/`Aggregate` types in `effect-dynamodb` carry the CRUD
> ops; their `make` shares derivation via the schema package's pure helpers.

```
packages/effect-dynamodb/src/
├── DynamoModel.ts      # Schema annotations (Hidden, identifier, ref) and configure() for field overrides (immutable, field rename, storedAs)
├── DynamoSchema.ts     # Application namespace (name + version) for key prefixing
├── Table.ts            # Table definition: { schema, entities } — registers entities on a physical table
├── Entity.ts           # Entity definition (model + primaryKey + indexes) + typed operations
├── Collections.ts      # Collection auto-discovery from entity indexes + explicit Collections.make() for advanced use
├── KeyComposer.ts      # Composite key composition from index definitions
├── Query.ts            # Query descriptor with combinators (limit, reverse, filter, startFrom, consistentRead)
├── Collection.ts       # Multi-entity query execution with per-entity Schema decode
├── Expression.ts       # Expression types (ConditionInput / UpdateInput / ExpressionResult) + legacy shorthand builders
├── Transaction.ts      # TransactGetItems + TransactWriteItems (atomic multi-item ops)
├── Batch.ts            # BatchGet + BatchWrite with auto-chunking and retry
├── Projection.ts       # ProjectionExpression builder for selecting specific attributes
├── Aggregate.ts        # Graph-based composite domain model (decompose/assemble/diff)
├── EventStore.ts       # Event sourcing with ordered event streams per aggregate
├── VectorSearchEmulation.ts # Scan + brute-force stand-in for SearchVectors (DDB Local has none)
├── DynamoClient.ts     # Context.Service wrapping AWS SDK + DynamoClient.make({ entities, aggregates?, tables? }) typed gateway
├── Marshaller.ts       # Thin wrapper around @aws-sdk/util-dynamodb
├── Errors.ts           # Tagged errors (DynamoError, ItemNotFound, ConditionalCheckFailed, ValidationError, TransactionCancelled, UniqueConstraintViolation)
├── internal/           # Decomposed internals
│   ├── Expr.ts         # Expr ADT — 16 expression node types, ConditionOps, compileExpr, parseShorthand
│   ├── PathBuilder.ts  # PathBuilder — recursive Proxy for type-safe attribute path access
│   ├── BoundQuery.ts   # BoundQuery fluent builder — wraps Query<A> with pre-resolved services
│   ├── BoundCrud.ts    # Bound CRUD builders (BoundPut/Update/Delete/Append) + BoundGet (an Effect), all carrying BoundOpTypeId for the one unwrap protocol
│   ├── BoundVectorQuery.ts # BoundVectorQuery — SearchVectors builder (.collect() is the only terminal)
│   ├── EntityOps.ts    # Entity operation intermediates + UpdateState (record + path-based)
│   ├── EntityCombinators.ts # Terminal functions, update combinators (record + path-based)
│   ├── EntityTypes.ts  # Type-level computations for Entity derived types
│   ├── EntitySchemas.ts # Schema derivation (7 derived schemas)
│   ├── CompositeCodec.ts # (schema pkg) THE composite key-form rule — every path uses it; see DESIGN §7
│   ├── TransactableOps.ts # Shared Batch/Transaction helpers (table name resolution, key composition, put-item building)
│   ├── TransactWriteOps.ts # Shared TransactWriteItems builder + ConditionCheckOp (Transaction.transactWrite AND EventStore.append additionalItems)
│   └── ...             # AggregateCursor, AggregateEdges, etc.
└── index.ts            # Public API barrel export
```

### Module Dependencies

```
Aggregate → Entity, Collection, Transaction, Errors (never DynamoClient directly)
Entity → DynamoClient, DynamoSchema, Table, KeyComposer, Marshaller, Expr, Errors
Collections → Entity (type-level for member validation), KeyComposer
Collection → DynamoClient, Entity, Table, Marshaller, Errors
Transaction → DynamoClient, Entity, TransactableOps, Marshaller, Expression, Errors
Batch → DynamoClient, Entity, TransactableOps, Marshaller, Errors
EventStore → DynamoClient, DynamoSchema, Table, KeyComposer, Marshaller, Query, TransactWriteOps, Errors
GeoIndex → Entity, Query (in effect-dynamodb-geo package)
DynamoClient → effect (Context, Layer), @aws-sdk/client-dynamodb, Entity, Collections, Aggregate (for make() binding + collection auto-discovery)
Table → DynamoSchema, Entity (type-level for member registration)
BoundQuery → Query, PathBuilder, Expr, KeyComposer (thin typed wrapper over Query<A>; `.where()` operands stay unserialised until `composeSkCondition` encodes + composes them)
Expression → Marshaller (types only — shorthand compilation routes through Expr)
TransactableOps → Entity, KeyComposer, Marshaller, Errors (shared Batch/Transaction helpers)
DynamoModel → effect (Schema)
DynamoSchema → (standalone)
KeyComposer → (standalone)
Marshaller → @aws-sdk/util-dynamodb
Projection → (standalone)
Errors → effect (Data)
```

### Data Flow

```
User code → yield* DynamoClient.make({ entities: { Users, Tasks }, aggregates: { OrderAggregate }, tables: { MainTable } })
  → resolves DynamoClient service + TableConfig for each unique table
  → binds the listed entities (CRUD + query accessors from index definitions)
  → binds the listed aggregates (CRUD + list operations)
  → auto-discovers collections from entity index `collection` properties
  → builds table operations for each registered table
  → returns typed client: { entities: { Users, Tasks }, aggregates: { OrderAggregate }, collections: { assignments }, tables: { MainTable } }

db.entities.Users.put(inputData)
  → Schema.decode(Entity.Input) — validate input
  → compose keys (KeyComposer) for primary key + all entity indexes
  → add __edd_e__ + timestamps + version
  → marshall to DynamoDB format (Marshaller)
  → DynamoClient.putItem (or transactWriteItems for unique constraints)
  → Schema.decode(Entity.Record) — decode full item for return

db.entities.Tasks.byProject({ project: "alpha" }).filter(...).limit(10).collect()
  → BoundQuery builder composes Query<A> with combinators (immutable, each returns new BoundQuery)
  → terminal (.collect()) triggers execution:
    → compose PK/SK from composite attributes (KeyComposer)
    → build KeyConditionExpression + __edd_e__ FilterExpression
    → Stream.paginate (automatic DynamoDB pagination)
    → unmarshall → Schema.decode(Entity.Record) per item

db.collections.assignments({ employee: "dfinlay" }).collect()
  → queries each member entity's table (parallel for cross-table collections)
  → groups results by member name: { Employees: [...], Tasks: [...] }
```

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Native build (not wrapping ElectroDB) | Full control over Effect integration, no impedance mismatch |
| Raw AWS SDK (not @effect-aws) | Avoid extra dependency; thin wrapper is simple enough |
| Effect Schema as sole schema system | Native Effect integration, bidirectional transforms, branded types |
| Schema.Class/Struct for models | Pure domain schemas — no DynamoDB concepts in models. Entity derives DynamoDB types |
| DynamoClient.make({ entities, aggregates?, tables? }) as typed gateway | Entity-centric client with namespaced access: `db.entities.*`, `db.aggregates.*`, `db.collections.*`, `db.tables.*`. The table-shortcut overload was removed — entity-centric is the only form. |
| Entities define primary key + GSI indexes | `Entity.make({ primaryKey, indexes })` — entity is self-contained. Collections auto-discovered from `collection` property on indexes |
| Collections auto-discovered from entity indexes | No explicit `Collections.make()` needed. Entities sharing the same `collection` name on the same physical GSI are grouped automatically |
| BoundQuery fluent builder | `.filter().limit().collect()` — reads naturally, type-safe through method chaining, no `asEffect()` needed |
| Aggregates own their write path | Read: partition query + assembly. Write: decompose the domain object, compose the rows in `buildDynamoItems`, and issue one `transactWriteItems` per sub-aggregate group — they do NOT route writes through Entity ops, which is why entity-level config (e.g. `timestamps`) has to be re-declared on the aggregate |
| ElectroDB-style composite indexes | `{ index: { name, pk, sk }, composite: [...], sk: [...] }` — GsiConfig with shared PK composites + entity SK composites |
| `__edd_e__` entity type attribute | Ugly name convention avoids collisions with user model fields |
| Vector indexes declared on the entity | `Entity.make({ vectorIndexes })` — the embedding + composed HASH partition are library-managed attributes (`__edd_v_*`, `__edd_vp_*`), so domain models stay pure and entity/tenant scoping is automatic. See `DESIGN.md §14` |
| @aws-sdk/util-dynamodb for marshalling | Proven, maintained; Effect Schema handles validation layer above |

## Repository Structure

pnpm workspace monorepo:

```
├── packages/
│   ├── schema/                   # @effect-dynamodb/schema — pure, AWS-free derivation layer
│   │   ├── src/                  # DynamoModel, DynamoSchema, KeyComposer, Errors, Projection,
│   │   │                         #   pure Entity.make/Aggregate.make + derivation internals
│   │   └── test/                 # incl. aws-free guard test (no @aws-sdk in dist JS or .d.ts)
│   ├── effect-dynamodb/          # Full library — depends on + re-exports @effect-dynamodb/schema,
│   │   │                         #   adds the AWS runtime (DynamoClient, ops, Batch/Transaction)
│   │   ├── src/
│   │   ├── test/
│   │   └── examples/             # Runnable examples — source of truth for doc code snippets
│   ├── effect-dynamodb-geo/      # Geospatial index and search using H3 (depends on effect-dynamodb)
│   │   ├── src/
│   │   └── test/
│   ├── docs/                     # Documentation site (Astro + Starlight)
│   │   ├── src/content/docs/
│   │   └── e2e/
│   ├── doctest/                  # Doc snippet sync verification (examples ↔ MDX)
│   │   ├── src/                  # MDX extractor, region parser, sync logic
│   │   └── test/                 # Sync, typecheck, and runtime tests
│   └── language-service/         # TS Language Service Plugin (hover tooltips)
│       ├── src/
│       └── test/
├── pnpm-workspace.yaml
├── biome.json
└── package.json                  # Workspace root
```

## Commands

```bash
pnpm build        # Build all packages (tsc)
pnpm test         # Run all tests across workspace
pnpm check        # Type check all packages (tsc --noEmit)
pnpm lint         # Lint + format check (biome check)
pnpm lint:fix     # Auto-fix lint + format issues (biome check --write)
```

All commands run from the repo root.

## Agent Workflow

**All agent work MUST be performed on a Git worktree** — never directly on the user's main checkout.

Use `git worktree add ../effect-dynamodb-<branch-name> -b <branch-name>` (or reuse an existing worktree) so the user's working tree, current branch, and any uncommitted in-flight work remain untouched. This isolation matters because:

- The user routinely has uncommitted WIP on the main checkout for parallel tracks (e.g. `fix/sparse-unique-constraints` alongside an unrelated fix). Agent branch switches on the shared checkout drag that WIP across branches, force repeated `git stash push/pop` juggling, and risk losing changes via stash conflicts or `.DS_Store`-style untracked collisions.
- Worktrees give the agent an independent filesystem path for `pnpm install`, `pnpm test`, and editor state — no contention with whatever the user is running in the main checkout.
- Cleanup is explicit: `git worktree remove <path>` when the PR is merged or abandoned, leaving no orphaned branches on the main checkout.

If the `Agent` tool's `isolation: "worktree"` parameter is available for the task, prefer it. Otherwise, create the worktree explicitly as the first step of any code-modifying task.

## Coding Conventions

### Effect TS Patterns

- **Effect<A, E, R>** — Success, Error, Requirements. Effects are lazy and immutable.
- **Generator style** (`Effect.gen`) for sequential logic; **pipe style** for short transformations.
- **Tagged errors** via `Data.TaggedError` — all errors must be tagged for discrimination. Tagged errors are **yieldable**: prefer `yield* new ErrorClass(...)` over `yield* Effect.fail(new ErrorClass(...))` in generators.
- **Service pattern** — `Context.Service` for all service definitions. Service methods have `R = never`. Tag identifiers use `@package/ServiceName` format.
- **Schema** — `Schema.Class` or `Schema.Struct` for domain models (pure, no DynamoDB concepts). Entity derives DynamoDB-specific types. Use `Schema.decodeUnknownEffect` (not `Sync`) inside effectful code. `Schema.Literals([...])` for literal unions (not `Schema.Literal(...spread)`). `Schema.Union([...])` takes an array. Validation via `.check()` with named check factories.
- **Custom annotations** — Use `Symbol.for()` for identifiers, never string keys. This project uses custom annotations for `DynamoModel.Hidden`, `DynamoModel.identifier`, `DynamoModel.ref`.
- **Dual APIs** — Public library functions transforming a data type must use `Function.dual` for data-first and data-last (pipeable) support.
- **TypeId + Pipeable** — Every data type carries a `TypeId` (unique symbol) and implements `Pipeable` + `Inspectable`. Custom types follow the same triad.
- **Yieldable trait** — `EntityOp`, `EntityDelete`, `BoundPut`, `BoundUpdate`, `BoundDelete` all implement `Pipeable.Pipeable` + `[Symbol.iterator]` (via `Utils.SingleShotGen`) — NOT `Effect.Effect`. They are yieldable in `Effect.gen` (`yield*` works) but cannot be passed to Effect combinators (`Effect.map`, `Effect.flip`, etc.) directly. Use `.asEffect()` to convert to `Effect` when piping to Effect combinators. **`BoundGet` is the one exception and must stay one**: `db.entities.X.get(key)` has always returned a real `Effect`, so it is built on `Effectable.Prototype` and IS an `Effect<A, E, never>` while also carrying the bound-op marker (#108). Making it merely yieldable would break `.pipe(Effect.catchTag("ItemNotFound", …))` at every call site. BoundEntity is the **composition point** — all CRUD methods AND query accessors return fluent builders. CRUD: `db.entities.Users.update(key).set({...}).expectedVersion(3)`, `db.entities.Users.put(input).condition({...})`, `db.entities.Users.delete(key).condition({...})` — every combinator is a method on the builder, no variadic rest-args. Query accessors are injected from collection memberships: `db.entities.Tasks.byProject({...})` returns a `BoundQuery`. `BoundQuery` is a fluent builder: `.filter().select().limit().collect()` / `.fetch()` / `.paginate()` / `.count()`. `scan()` also returns `BoundQuery`.
- **Option over nullable** — `Option<A>` in services and domain logic. Convert at boundaries with `Option.fromNullable`.
- **No tacit style** — always explicit lambdas: `Effect.map((x) => fn(x))`.
- **`run*` at the edge only** — never `runPromise`/`runSync` inside an Effect.
- **Testing** — `@effect/vitest` with `it.effect` / `it.scoped`. Mock `DynamoClient` via `mockDynamoClientLayer({ putItem: mockPutItem, ... })` from `test/helpers/MockDynamoClient.ts` — it supplies a dying default for every operation, so a test declares only what it exercises and a new operation on `DynamoClientService` needs a default added in exactly one place. Never hand-roll a full `Layer.succeed(DynamoClient, { ... })` literal. Use `Effect.provide(layer, { local: true })` for test isolation.
- **Packages** — There is no `effect/unstable/*` namespace in 4.0.0; import former unstable modules from their top-level paths (`effect/http`, `effect/http-api`). `Config` constructors are PascalCase (`Config.String`, `Config.Int`).

### Critical Anti-Patterns

Do NOT:
- Use `Effect.promise` for fallible async — use `Effect.tryPromise`
- Use `Effect.sync` for throwable code — use `Effect.try`
- Use string/generic Error types — use `Data.TaggedError`
- Use `Effect.fail(new TaggedError(...))` in generators — use `yield* new TaggedError(...)` directly
- Have service methods with `R != never` — resolve deps in Layer
- Use `Schema.decodeUnknownSync` in effectful code — use `Schema.decodeUnknownEffect`
- Use `yield* ref` / `yield* deferred` / `yield* fiber` — use `Ref.get`/`Deferred.await`/`Fiber.join` (not Yieldable)
- Use v3 service APIs (`Context.Tag`, `Effect.Service`) — use `Context.Service`
- Use v3 Schema APIs (`Schema.filter`, `Schema.fromKey`, `Schema.Literal`) — use `.check()`, `.withKey()`, `Schema.Literals`
- Use pre-GA names (`SchemaGetter.transformOrFail`, `Config.string`, `effect/unstable/*`) — use `SchemaGetter.transformEffect`, `Config.String`, top-level module paths
- Put DynamoDB concepts in domain models — keep models pure, Entity handles DynamoDB binding
- Extract `A`/`E`/`R` from entity ops via `Effect.Effect<infer A>` — match against `EntityOp<infer A, ...>` instead

### TypeScript Conventions

- `strict: true`, `exactOptionalPropertyTypes`, `noUncheckedIndexedAccess`, `verbatimModuleSyntax`
- ESM only (`"type": "module"`, NodeNext module resolution, ES2022 target)
- `type` over `interface` (default). `interface` only for declaration merging.
- Union types over enums. `unknown` over `any`. Named exports only.
- `satisfies` for validation + literal preservation.
- **Branded types** — Effect Schema branded types for domain IDs (e.g., `TeamId`, `PlayerId`).
- **Barrel exports** — `src/index.ts` is the sole public entry point. Namespace exports for modules, direct exports for errors and `DynamoClient`.

### DynamoDB Conventions

**Single-Table Design:**
- All entities share one physical table using structured key values: `$schema#v1#entity_type#attr1_value#attr2_value`
- `$` sentinel prefix, `#` delimiter for ORM/non-ORM coexistence
- Casing (default: `"lowercase"`) applied to the **entire key** including composite attribute values — `"Male"` and `"male"` produce identical keys. Stored attribute values retain original casing.
- `__edd_e__` entity type discriminator on every item, enforced via FilterExpression on all queries
- Index overloading — generic GSI names (gsi1, gsi2) serve different patterns per entity type. Logical names (`byTenant`) map to physical GSI names.
- Item collections group multiple entity types under the same index for cross-entity queries.

**Operations:**
- Always use `ExpressionAttributeNames` + `ExpressionAttributeValues` (many common names are reserved words)
- Pagination handled via `Stream.paginate`. Batch retry handled in `batchGet`/`batchWrite`.
- `ConditionalCheckFailedException` is expected control flow, not exceptional.
- Transactions: up to 100 items, 2x WCU for writes.

**Key Limits:** Max item 400 KB | BatchWrite 25 | BatchGet 100 | Transaction 100 | GSIs 20 | Query/Scan response 1 MB | Per-partition 3,000 RCU / 1,000 WCU

## Documentation Code Examples

**Examples are the source of truth for all documentation code snippets.** Every tutorial and guide MDX page is backed by a runnable example file in `packages/effect-dynamodb/examples/`.

### How it works

1. **Example files** (`examples/*.ts`) contain complete, runnable programs with `// #region name` / `// #endregion` markers around sections that appear in docs.
2. **MDX code blocks** reference their backing example via `region="name" example="filename.ts"` attributes on the code fence.
3. **Sync tests** (`packages/doctest/`) verify that MDX snippet content matches the corresponding example region.

### Adding or updating documentation code

1. Write or update the **example file first** — it must type-check (`tsconfig.examples.json`) and run against DynamoDB Local.
2. Add `// #region name` / `// #endregion` markers around the code section.
3. In the MDX file, add `region="name" example="filename.ts"` to the code fence and paste the region content (minus imports and `Console.log` lines).
4. Run `pnpm --filter @effect-dynamodb/doctest test` to verify sync.

### Example ↔ MDX mapping

| Tutorial/Guide MDX | Example file |
|---|---|
| `tutorials/starter.mdx` | `examples/starter.ts` |
| `tutorials/crud.mdx` | `examples/crud.ts` |
| `tutorials/gamemanager.mdx` | `examples/cricket.ts` |
| `tutorials/human-resources.mdx` | `examples/hr.ts` |
| `guides/modeling.mdx` | `examples/guide-modeling.ts` |
| `guides/indexes.mdx` | `examples/guide-indexes.ts` |
| `guides/index-policy.mdx` | `examples/guide-index-policy.ts` |
| `guides/queries.mdx` | `examples/guide-queries.ts` |
| `guides/expressions.mdx` | `examples/guide-expressions.ts` |
| `guides/timeseries.mdx` | `examples/guide-timeseries.ts` |
| `guides/vector-search.mdx` | `examples/guide-vector-search.ts` |
| ... (all other tutorials/guides follow the same pattern) | |

### Sync normalization

The sync comparison normalizes content by stripping: `Console.log` lines, blank lines, `assertEq`/`assert` lines, `#region`/`#endregion` markers, leading whitespace, inline output comments (`// →`, `// State:`, etc.), and rewriting import paths. Code blocks without `region`/`example` attributes are illustrative-only and not sync-checked.

### Commands

```bash
pnpm --filter @effect-dynamodb/doctest test           # Sync + typecheck verification
pnpm --filter @effect-dynamodb/doctest test:connected  # Runtime execution (needs DynamoDB Local)
```

## Quality Gates

Before committing:
1. `pnpm lint` — zero lint/format errors (Biome)
2. `pnpm check` — zero type errors
3. `pnpm test` — all unit tests pass (uses `vitest.config.ts`; does NOT run the connected suite). Note that Vitest transpiles without type-checking, so a green suite says nothing about types — gate 2 is what compiles `test/` (every package's `tsconfig.test.json` includes the whole `test` directory; issue #106). Never narrow one of those `include` arrays back to a file list.
4. `pnpm --filter effect-dynamodb test:connected` — **all integration tests pass against DynamoDB Local** (uses `vitest.connected.ts`; requires `docker run -d --rm -p 8000:8000 amazon/dynamodb-local`). **Mandatory** for any PR that touches `KeyComposer`, `Entity`, `Query`, `Collection`, `Transaction`, `Aggregate`, `EventStore`, `Errors`, the `internal/` Entity helpers, or any test fixture. Do NOT skip on the assumption that unit tests cover the same ground — the connected suite catches end-to-end fixture drift, schema-level type widening that only fails on real DDB writes, and policy-aware GSI behavior that mocks can't reproduce.
5. `pnpm --filter @effect-dynamodb/doctest test` — doc snippet sync verification passes
6. `npx tsx examples/<name>.ts` — examples run against DynamoDB Local (`docker run -p 8000:8000 amazon/dynamodb-local`). Run after changes to Entity, Query, Table, DynamoSchema, KeyComposer, Collection, Transaction, or Errors. **Note: running an example successfully is NOT a substitute for gate 4** — examples exercise one or two scenarios; the connected suite covers the cross-product of behaviors. An agent that runs examples and skips `test:connected` has not satisfied this gate.
7. New modules must have corresponding test files in `test/`
8. New errors must use `Data.TaggedError`
8a. New `EDD-xxxx` diagnostic codes must be **allocated in `DESIGN.md` § Diagnostic Code Registry first**, then used in code — never allocated by grepping the source for the highest number. A grep cannot see branches in flight: two parallel branches both took `EDD-9046` for unrelated errors and the collision surfaced only at review. Codes are permanent once released; retire one rather than reusing it.
9. New services must follow `Context.Service` pattern
10. New or updated doc pages must have a backing example file with region markers

> **For agents:** all of the above gates must run successfully before reporting a task complete. The single most common gap is skipping gate 4 — `pnpm test:connected` requires DDB Local Docker running and is genuinely time-consuming, but it is mandatory for the modules listed there. Reporting "examples ran end-to-end against DDB Local ✓" does NOT satisfy gate 4. CI will catch the gap, but it costs a CI cycle, a stale PR review, and the agent's credibility. Run it before pushing.

### Canonical GSI-composite test-fixture shapes

When changing **`KeyComposer.composeGsiKeysForUpdatePolicyAware`** or any policy-aware composition path (`Entity.update`, `Entity.append`, retain path, BoundUpdate combinators), test coverage MUST span the canonical GSI-composite shapes below. Missing one shape leads directly to consumer-facing regressions — the v1.7.1 fixture matrix missed shape #2 (PK-composites-only) and shipped a regression that left items invisible to channel-scoped queries (#43); the v1.7.2 fixture matrix missed shape #6 (empty-composite half) and shipped a regression that left items invisible to lookup GSIs whose SK is just the entity prefix (#46). Verify each shape in the unit suite (`KeyComposer.test.ts`), at the entity wiring layer (`IndexPolicyV3.test.ts`), AND in the connected suite (`connected.test.ts`).

1. **Multi-writer GSI** — composites split across writers (e.g. enrichment-owned PK composite + telemetry-owned SK composites). The per-half gate must skip halves the current writer doesn't touch. Anchor scenario for the `'preserve'` contract.
2. **PK-composites-only GSI** — composites entirely subset of the entity primary key (e.g. `byChannel: { pk: [channel], sk: [deviceId] }` on `primaryKey: [channel, deviceId]`). The per-half gate must fire via `keyRecord` membership and SET on every write (idempotent — values are immutable). **This is the #43 regression scenario** — must be present in unit, entity-level, AND connected suites for any composer change.
3. **Hierarchical GSI** — composites form a parent → child hierarchy (e.g. `[region, country, city, site]`). The structural rule must truncate via `set({ parents }).remove(["leaf"])`.
4. **Hole pattern GSI** — optional leading composite + present trailing composite. Must collapse into the unified can't-compose rule (drop under `'sparse'`, noop or cascade-override under `'preserve'`).
5. **All composites mutable** — every composite is a non-PK model field, and `appendInput` / update payloads carry them all. The standard case; the gate fires through the payload.
6. **Empty-composite half** — at least one half is `composite: []` (e.g. `byDeviceBinding: { pk: [deviceBinding], sk: { composite: [] } }`, common in single-table designs where the SK is just the entity prefix). The per-half gate must always evaluate the empty half — the value is a constant prefix, multi-writer protection does not apply. **This is the #46 regression scenario** — must be present in unit, entity-level, AND connected suites for any composer change.

`DESIGN.md §7` documents the same canonical shapes alongside the policy semantics. Cross-reference both whenever editing the composer.

> **Meta-instruction for future gate changes.** When changing per-half gate logic, audit existing fixtures for degenerate cases AND add at least one new degenerate fixture per change. v1.7.3 reframed the gate from a touched-predicate to a skip-predicate to close this class of bugs structurally — but if the gate is touched again (or a new structural rule is introduced), this discipline still applies. Each tactical fix to the touched-predicate (#41 in v1.7.1, #43 in v1.7.2) closed one concrete shape but missed the next; the skip-predicate's `length > 0 && !hasRemoved && every(absent in payload AND keyRecord)` framing was designed to make the gate's purpose (multi-writer protection) the only thing the predicate checks. New shapes that fit that framing (constant prefix, immutable PK composites, explicitly invalidated composites, payload-asserted ownership) all fall out automatically.

## PR Conventions

Every PR that resolves a tracked issue **MUST** reference the issue(s) it closes using a GitHub closing keyword (`Closes #N`, `Fixes #N`, or `Resolves #N`) in the PR body. This is non-negotiable — without a closing keyword, the issue stays open after merge and drifts out of sync with reality. A bare `#N` mention (e.g. "relates to #N") does **not** auto-close; use one of the keywords above, one per issue. If a PR only partially addresses an issue, reference it without a closing keyword and say so explicitly in the PR body.

## Release Workflow

This repo uses [Changesets](https://github.com/changesets/changesets) with **fixed lockstep versioning** across the four publishable packages (`effect-dynamodb`, `@effect-dynamodb/schema`, `@effect-dynamodb/geo`, `@effect-dynamodb/language-service`). Publishing is automated: every push to `main` runs `.github/workflows/release.yml`, which detects packages whose `package.json` version is ahead of npm and publishes them via Trusted Publishing (OIDC — no `NPM_TOKEN`).

**There is no "Version Packages" bot PR.** The required process is:

### Bump PRs (Option A — the required workflow)

Any PR that is intended to trigger a release **must run `pnpm changeset version` as part of that PR**. This means the PR includes, in the same commit set:

1. **The feature/fix code change itself.**
2. **A changeset file** (`.changeset/<name>.md`) created with `pnpm changeset` — declares which packages bump and at what semver level.
3. **The result of `pnpm changeset version`**:
   - The changeset file is **deleted** (consumed).
   - Each affected `package.json` has its `version` bumped (lockstep → all four move together).
   - Each affected package's `CHANGELOG.md` is regenerated with the consumed entry.

The typical authoring loop:

```bash
# 1. Make code changes, tests, docs
# 2. Declare the bump
pnpm changeset                          # interactive — pick packages + semver level
git add .changeset/<generated-name>.md
git commit -m "..."

# 3. Apply the bump in this same PR
pnpm changeset version                  # consumes changeset, bumps versions, writes CHANGELOG
git add -A
git commit -m "Version Packages 0.X.0"
git push
```

When the PR merges, `release.yml` detects the version bump, builds, tests, and publishes. No second PR.

### Chore PRs (no release)

CI-only, test-hygiene, or doc-only changes that should **not** trigger a release still need to satisfy CI's "Require changeset or version bump" gate. Add an **empty changeset** (empty frontmatter, no packages listed) as the explicit "no-release" signal:

```markdown
---
---

Chore: <one-line summary>. No version change.
```

### CI enforcement

`ci.yml` scans `.changeset/` on every PR **targeting `main`**. If any **unconsumed release-declaring changeset** remains (a file with packages listed in its frontmatter), CI fails with a message telling the author to run `pnpm changeset version` and commit the result. Empty chore changesets pass through.

The gate is scoped to `main` so a **stacked release train** can work: several PRs chained onto a `release/**` integration branch, each carrying its own unconsumed changeset, with a single `pnpm changeset version` at the tip producing one version and one CHANGELOG entry for the whole batch. The guarantee is unchanged — nothing reaches `main` with a changeset left unconsumed. CI itself has no `branches` filter and runs on every PR whatever its base, because a stack chains onto `docs/**` / `fix/**` / `feat/**` branches rather than onto `release/**` directly, and a base-branch allowlist would silently leave the intermediate PRs with no CI at all.

### Release ordering

`release.yml` fires on **every** push to `main` and publishes whenever a `package.json`
version differs from npm. That makes the version-bumping PR order-sensitive: merge it
while other release work is still open and you publish a release missing those PRs — and
every later merge then publishes **nothing**, because the version already matches npm.
The batch sits unpublished until someone hand-cuts the next version.

**The version bump merges last.** `ci.yml`'s "Version bump must merge last" gate enforces
it: a PR that bumps a publishable package version fails if any other open PR into `main`
carries a changeset file or touches publishable `src/`. Empty chore changesets count —
in a batched release they are exactly how the queued PRs are marked. Override with the
`release-order-override` label when the ordering is deliberate.

### Trusted Publishing setup

Each of the four publishable packages must be configured on npmjs.com with this repo + `release.yml` as a trusted publisher. No `NPM_TOKEN` is required. The workflow uses `npm publish --provenance --access public` to emit a signed provenance attestation on each publish — verifiable by consumers. **`@effect-dynamodb/schema` is new and must be registered as a Trusted Publisher on npmjs.com before its first release** (this cannot be automated from the repo).

## Behavioral Notes

### Entity Operations
- **BoundEntity CRUD methods return fluent builders.** `db.entities.Users.put(input)` → `BoundPut`; `db.entities.Users.update(key)` → `BoundUpdate`; `db.entities.Users.delete(key)` → `BoundDelete`; `db.entities.Users.get(key)` → `BoundGet` (an `Effect`, see the Yieldable-trait note). All are Yieldable (yield* to execute) and Pipeable (chain methods). Every combinator is a method: `update(key).set({...}).expectedVersion(3).condition({...})`, `put(input).condition({...})`, `delete(key).condition({...}).returnValues("ALL_OLD")`. Use `.asEffect()` to convert to `Effect` when piping to Effect combinators (`Effect.catchTag`, `Effect.map`, etc.). Queries use the same fluent-builder shape via `BoundQuery`.
- **BoundQuery is a fluent builder.** `db.entities.Tasks.byProject({ project: "alpha" }).filter(...).limit(10).collect()`. Terminals: `.collect()` → `Effect<Array<A>>`, `.fetch()` → `Effect<Page<A>>` (single page + cursor), `.paginate()` → `Stream<A>`, `.count()` → `Effect<number>`. Combinators: `.where()` (SK condition), `.filter()`, `.select()`, `.limit()`, `.pageSize()`, `.maxPages()`, `.reverse()`, `.startFrom()`, `.consistentRead()`, `.ignoreOwnership()`. `.limit(n)` bounds the RESULT (at most n items, accumulating across requests — it is not DynamoDB's `Limit`, which cannot express this under a `FilterExpression`); `.pageSize(n)` sets DynamoDB's `Limit` (rows examined per request); `.maxPages(n)` is the hard stop on round trips. Once a request over-reads and the surplus is discarded, `.fetch()`'s cursor is rebuilt from the last item returned, so it resumes after what the caller saw.
- **Query accessors from entity indexes.** Each GSI index on an entity becomes a query accessor: `db.entities.Tasks.byProject({...})` returns a `BoundQuery`. PK composites required, SK composites optional (narrows via auto `begins_with`). `.where()` provides type-safe access to remaining SK composites not already provided.
- **Collection accessors auto-discovered.** Entities sharing the same `collection` name on the same GSI are grouped: `db.collections.assignments({ employee: "dfinlay" }).collect()` → `Effect<{ Employees: Employee[], Tasks: Task[] }>`. Collection queries support the same BoundQuery combinators (`.filter()`, `.limit()`, etc.) but `.where()` is not available.
- **put/get/query return model type from BoundEntity.** Entity definitions return intermediates (`EntityOp`, `EntityDelete`) with `asRecord`/`asNative` terminals for advanced decode modes.
- **`Entity.create()` = put + attribute_not_exists.** Returns `ConditionalCheckFailed` on duplicate.
- **Conditional writes via `.condition()`.** Works on `BoundPut`, `BoundUpdate`, `BoundDelete` (and on unbound `EntityPut`/`EntityUpdate`/`EntityDelete` via `Entity.condition()` pipeable). User condition ANDed with optimistic lock condition on updates. Two APIs: callback `.condition((t, { eq }) => eq(t.status, "active"))` and equality shorthand `.condition({ status: "active" })` (each key ANDed as `=`; the operator form `{ eq: {...} }` is `Expression.condition` / unbound `Entity.condition`). A later `.condition()` replaces an earlier one; the op's own guard (`create`'s not-exists, `patch`'s and `deleteIfExists`'s exists, version/incarnation) is structural — derived from the op's kind (`putKind: "create"`, `patch: true`, `_mustExist`), never held as its condition — and always ANDed with the caller's condition (`withGuard`, compiled as one `and`), in the entity's own write and in `extractTransactable` (transactions, EventStore additional items, Batch refusal). The `condition` combinator is the one place an empty condition (`{}`, `and()`) becomes none (`nonEmptyCondition`); an empty part directly under `and` is left out, but one under `or` / `not`, an `or()` with no parts, or an `isIn` with no values, is refused with a `ValidationError` before sending (`emptyPartProblem`) — for conditions, entity/collection filters, aggregate `list` filters and `Transaction.check` alike. `compileExpr` itself throws on anything `emptyPartProblem` refuses (an assertion: every entry point must refuse first). `.consistentRead()` on a GSI (entity index or collection) is refused with a `ValidationError` before sending; entity indexes are always treated as GSIs. `.expectedVersion()` on an unversioned entity is refused with a `ValidationError`. `patch()` of a missing item is `ConditionalCheckFailed` on every path, read-first included.
- **Filter expressions on BoundQuery.** Callback `.filter((t, { gt }) => gt(t.price, 30))` or shorthand `.filter({ status: "active" })`.
- **Projections on BoundQuery.** Callback `.select((t) => [t.name, t.price])` or shorthand `.select(["name", "price"])`. Returns partial records.
- **PathBuilder + Expr ADT.** `PathBuilder<Model>` is a recursive Proxy for type-safe attribute path access (nested: `t.address.city`, array: `t.roster.at(0).name`, size: `t.tags.size()`). `Expr` is a 16-node discriminated union compiled to DynamoDB expression strings via `compileExpr()`. `ConditionOps<Model>` provides typed comparison/logical operators for callbacks.
- **Rich update operations on `BoundUpdate`.** Record-based methods: `.set(updates)`, `.remove(fields)` (REMOVE), `.add(values)` (ADD), `.subtract(values)` (SET subtraction), `.append(values)` (list_append), `.deleteFromSet(values)` (DELETE from set). Path-based: `.pathSet(op)`, `.pathRemove(segments)`, `.pathAdd(op)`, `.pathSubtract(op)`, `.pathAppend(op)`, `.pathPrepend(op)`, `.pathDelete(op)`. All compose with `.expectedVersion()`, `.condition()`, `.cascade()`, `.returnValues()`. Unbound `Entity.update(key).pipe(Entity.set(...), Entity.pathAdd(...))` uses the same combinator names as pipeable functions.
- **GSI composites defined on Entity.** Each entity defines its own GSI indexes via `Entity.make({ indexes })`. Entity writes compose keys for primary key + all entity indexes automatically. `DynamoClient.make()` auto-discovers collections from the `collection` property on indexes.
- **Consistent reads via combinator.** `Entity.consistentRead()` on `EntityGet`, `.consistentRead()` on `BoundQuery`.
- **Scan via `db.entities.Tasks.scan()`.** Returns `BoundQuery` in scan mode (no `.where()` available).
- **Batch operations auto-chunk.** `batchGet` at 100, `batchWrite` at 25. Both retry unprocessed items.
- **Multi-item ops accept bound builders, reads and writes alike, through ONE protocol.** `Batch.write`, `Transaction.transactWrite`, `EventStore.append({ additionalItems })`, `Batch.get`, `Transaction.transactGet` and `Transaction.check` all route through `Entity.extractTransactable`, which unwraps anything carrying `BoundOpTypeId` + `_op` to the `EntityOp` / `EntityDelete` it wraps — the write builders (`db.entities.X.put(...)` / `.create(...)` / `.delete(...)` / `.deleteIfExists(...)`, #100) and `BoundGet` (`db.entities.X.get(...)`, #108). This is what lets entities authored with the pure `@effect-dynamodb/schema` `Entity.make` take part at all — a pure definition carries no ops, so the bound op is the only descriptor its author can hold. **Do not add a second unwrap mechanism**: a new bound op joins by carrying the marker.
- **`BoundGet` composes the SAME key as `db.entities.X.get`, by construction.** It wraps the entity's own `EntityGet`, so `TransactableOps.composePrimaryKey` sees the identical key record and routes it through `entity._keyForm` — the divergence that made a `BigIntFromString` composite invisible to `Batch.get` (#111) cannot reappear through this path. A non-descriptor handed to a read path fails with a `ValidationError` carrying **EDD-9051** on the error channel, not a thrown defect.
- **An op these paths cannot reproduce is REJECTED, never silently reinterpreted.** They emit exactly one `Put`/`Delete` per op, so `internal/TransactableOps.rejectUnsupportedOp` fails with a `ValidationError` for: **`upsert`** (an `UpdateItem` with `if_not_exists` on `createdAt`/immutable fields/version — a `Put` resets all three), `refs` (hydration needs a read), `generatedId` (needs `Crypto`), and vector indexes (needs `Embedder`). `update`/`patch` are planned by `transactWrite` only (see the next bullet); `EventStore.append` / `Batch.write` do not accept them. A planned op cannot carry an update `.cascade()` (**EDD-9059**), `.returnValues()` returning an item (**EDD-9060**), or an update of a vector-indexed entity (**EDD-9061**) — refused in the same `rejectUnsupportedOp` (`opType: "update"`, `details`). `EntityPutImpl` carries `_putKind: "put" | "create" | "upsert"` to tell the three apart — **every combinator that reconstructs an `EntityPutImpl` MUST carry `_putKind` through**, or an upsert silently becomes a plain `Put` again (guarded by the `.condition()` preservation test).
- **Multi-item lifecycle features split by DIRECTION (#113).** `unique`, `versioned: { retain: true }` and `softDelete` need more than one item per write. The line is whether the extra items derive from the caller's payload or from stored state. **put** → `transactWrite` / `additionalItems` EXPAND via `Entity._buildPutSideItems`: the row, one `attribute_not_exists`-guarded sentinel per satisfiable constraint (sparse — an unset constraint emits none), and the v1 snapshot. **delete** (and every **update**) → `transactWrite` PLANS them in a pre-pass (`planTransactWriteOps`, run every attempt): `Entity._planUpdate` / `_planDelete` run the entity's OWN op with its write recorded instead of sent (`internal/TransactPlan.planWrite` — a `DynamoClient` whose reads pass through and whose first write is recorded; a second write is a defect). The recorded items join as a `GuardedWrite`, read by the same verdict contract as a guarded put (reserve rejected → `UniqueConstraintViolation`; retain snapshot rejected → `historyConflict`; main rejected with the stored item (`ALL_OLD`) unchanged from the read — version and incarnation, or the attributes the main item's condition names — under the caller's own `.condition()` → `TransactionCancelled`; otherwise a lost race → re-plan from fresh reads, which is how a now-missing row or a stale `expectedVersion` gets the standalone outcome). A plain update — which standalone finds a missing row through its write's own condition — is checked with one consistent read, and a missing row is answered to the op as its write would be (`planWrite`'s `rowMissing`), so it takes its own create / `ItemNotFound` / `patch` path. One code path, so the transactional and standalone writes cannot drift. `EventStore.append({ additionalItems })` plans none and still rejects these deletes (**EDD-9048**) — the sentinel to release is keyed by the STORED row's unique values, a retain snapshot copies it, and a soft-delete tombstone IS that row relocated to a new SK. `Batch.write` rejects both directions (**EDD-9049**) — no `ConditionExpression`, no `UpdateRequest`, no atomicity across 25-item chunks — EXCEPT a put of a `softDelete`-only entity, which needs no side items.
- **`buildTransactWriteItems` returns `{ items, provenance }`.** One caller op may emit several items, so the old `itemIndex === opIndex` assumption is gone. `provenance[i]` carries `{ opIndex, kind, constraintName?, fields?, entityType }`. **Any consumer mapping cancellation reasons positionally MUST go through it.** `transactWrite` uses it to raise `UniqueConstraintViolation` (not a bare `TransactionCancelled`) when a sentinel is what failed; `EventStore.append` uses it so `AdditionalItemConditionFailed.indices` stay indices into the CALLER's `additionalItems` array, deduped when several items of one op fail. Precedence is unchanged.
- **Item caps count the EXPANDED set.** `transactWrite` checks `TRANSACT_WRITE_ITEMS_LIMIT` after compiling and names both numbers ("34 operation(s) expanded to 102 items"). `EventStore.append` keeps a cheap pre-flight lower bound (expansion only ever adds) and re-checks authoritatively after compiling; `AppendTooLarge.count` is the expanded total. Still exactly one constant.
- **The composite key form is not optional on ANY path (#111).** `Entity._keyForm` is the exposed handle on the rule (`internal/CompositeCodec.ts`), and the modules that compose keys OUTSIDE `makeImpl` — `internal/TransactableOps.ts`, `internal/TransactWriteOps.ts`, `Batch.ts`, `Aggregate.ts`, geo's `GeoSearch.ts` — must use it. Skipping it means two APIs spell the same logical key differently: a transact `put` wrote `txn_420` where `Entity.put` wrote `txn_000…0420`, so each write produced an orphan row no accessor could read; `composePrimaryKey` took the caller's RAW key, so `Batch.get`/`transactGet` returned null and `Batch.write(delete)` reported success against a row it never touched. `test/KeyFormInvariant.test.ts` scans all seven source files and fails on a `KeyComposer` call whose record did not go through the form — **add every new file that composes keys to its `SOURCES` list**; it caught six live bypasses.
- **The transact/batch put path mirrors `Entity.put` exactly.** Same key form, same `_renameToDynamo` (a `field:` rename otherwise produced a differently-shaped item depending on which API wrote it — and the v1 retain snapshot inherited the wrong shape). Anything `Entity.put` does to the item before storing, this path does too, in the same order.
- **A uniqueness sentinel's guard names the CONFIGURED pk attribute.** One `sentinelGuard()` in `Entity.ts` emits `attribute_not_exists(#sentinel_pk)` + `ExpressionAttributeNames`. A literal `attribute_not_exists(pk)` is vacuously true on an entity declaring `pk: { field: "PK" }`, which silently unenforces the constraint.
- **Conditions on transact items are honoured, never dropped.** `extractTransactable` surfaces the op condition (`.condition(...)`, `Entity.condition(...)`, `create()`'s implicit `attribute_not_exists`, `deleteIfExists()`'s implicit `attribute_exists`); `buildTransactWriteItems` compiles it onto the `Put` / `Delete`. `ExpressionAttributeValues` is omitted when the condition is value-free — DynamoDB rejects an empty map. `Batch.write` CANNOT express a condition (BatchWriteItem has no `ConditionExpression`), so it rejects a conditioned op with a `ValidationError` rather than writing unconditionally.
- **Table operations via `db.tables.*`.** `create()`, `delete()`, `describe()`, backup/restore, PITR, TTL, tags, export.
- **`.history(key)` for time-series entities.** Returns a `BoundQuery` auto-scoped to event items via `begins_with("<currentSk>#e#")`. `.where()` restricted to the configured `orderBy` attribute; `.filter()` works on any model attribute.

### Vector Search
- **Declared on the entity.** `Entity.make({ vectorIndexes: { byDescription: { name, dimensions, distance, source: { fields }, partition?, filters? } } })`. Models stay pure — the embedding lives under `__edd_v_<name>__` and the composed HASH partition under `__edd_vp_<name>__`.
- **Partition composition scopes for free.** `KeyComposer` composes `$schema#v1#<entityType>[#composites]` into the HASH attribute, so a search on a shared physical vector index only ever sees one entity type (and one tenant, with `partition: ["tenantId"]`). `.partition()` is required by the types iff partition composites are declared.
- **`Embedder` service** (`@effect-dynamodb/Embedder`) supplies embeddings. Bundled into the captured context like `Crypto`, so bound ops stay `R = never`. `Embedder.layerTest({ dimensions })` for tests/examples; dimension agreement validated at `DynamoClient.make` (EDD-9037).
- **Write gate.** put/create/upsert always embed; update/patch embed **only** when the write touches a `source.fields` member — by `set()`, `remove()`, a null clear, or a path op (mirrors the §7 GSI `removedSet` discipline) — and read the current item once when the payload is a partial source. Clearing every source field REMOVEs the vector + partition attributes so the item leaves the index. `.withVector(name, vector)` on `BoundPut`/`BoundUpdate` skips the Embedder (name typed + validated). `reembed({ concurrency })` rewrites stale vectors under `attribute_exists(pk)`.
- **Filters are declared, not free-form.** Only attributes in `filters: [...]` are filterable — enforced by the accessor type, a runtime `ValidationError`, and the emulation layer. Entities sharing a physical index union their filters into one `SearchSchema`.
- **Lifecycle.** Vector + partition attributes join the GSI strip sets for snapshots, tombstones and time-series event items; the tombstone stashes the embedding under `__edd_vs_<name>__` so `restore()` never re-embeds.
- **`BoundVectorQuery`.** `.partition().filter().topK().select().collect()`. `.collect()` is the ONLY terminal — `SearchVectors` has no cursor, so pagination combinators are structurally absent. `.filter()` is equality-only (one relaxable alias, `VectorFilterInput`). Hits are `{ item, similarity, rawScore }` with `Similarity` branded and normalized higher-is-more-similar.
- **DynamoDB Local has no vector search.** `CreateTable` discards `VectorIndexes`, `SearchVectors` throws `UnknownOperationException`. Connected tests and examples run through `VectorSearchEmulation.layer(inner, { tables })`, which emulates `searchVectors` via Scan + brute force. Its ranking is unit-tested against the developer-guide reference table.

### Lifecycle Operations
- **Opt-in.** `versioned: { retain: true }` for version snapshots. `softDelete: true` (or `{ ttl, preserveUnique }`) for soft-delete.
- **Version snapshots strip GSI keys.** SK becomes `$schema#v1#entity#v#0000001`.
- **Soft-deleted items strip GSI keys.** SK becomes `$schema#v1#entity#deleted#<timestamp>`. `deletedAt` added, optional `_ttl`.
- **With primary SK composites, history keys carry the item (#133).** Several items share a partition, so snapshot/tombstone SKs insert the composite part of the live SK after the marker (`#v#line_a#0000001`, `#deleted#line_a#<ts>`; `KeyComposer.composeHistoryItemSegment`, `DynamoSchema.HistoryKeyOptions.item`). Without SK composites there is no segment — keys must stay byte-identical. Readers (`versions`, `getVersion`, `deleted.get`, `restore`, `highestRetainedVersion`) key by the item AND read unsegmented rows earlier releases wrote whose stored composites compose the item's key (`legacyHistory` / `isItemsRow`; segmented wins on the same version, the later tombstone wins); `deleted.list` stays partition-wide.
- **History rows are not items.** Snapshots, tombstones and time-series events keep `__edd_e__`, so primary-key queries, scans and primary-key collections drop them via `Query`'s `liveRows` (`Entity._liveRows`): a row is dropped only if its SK isn't the one its own composites compose (or they don't compose) AND the SK is history-shaped (`#v#…digits`, `#deleted#…ISO`, `#e#` under the live key). A row the composer can't reproduce (1.15 unpadded number composite) is kept. Client-side before decode for queries AND scans; when rows can be dropped, the request `Limit` starts at `limit` and doubles per page (never the remainder — that cost one request per history row); `count()` reads only `reads`.
- **Renamed fields in bound queries go through naming hooks.** `BoundQueryConfig.renameExpr` / `selectAs` (`entityNaming`, `collectionNaming` in `internal/BoundQuery.ts`): filters name stored attributes; a select of a renamed field uses numbered placeholders (`selectProjected`) and maps back; a select of no renamed field stays the plain projection (byte-identical requests). Collections resolve each member's domain fields: a field a member lacks reads as absent (or `__edd_absent__` when it stores another field under that name); a filter compiles as written when every member reads the same attributes (byte-identical for plain collections) and as an OR over `__edd_e__` only when they differ; and keep their grouping through every combinator (`groupCollected`). Chained filters compile as ONE expression (placeholders would collide).
- **A retain hard delete snapshots the final state.** Read + `TransactWriteItems` (guarded `Delete` + `snapshotPut` at `v#N`), so a re-created item continues at N+1 and a stale `expectedVersion(N)` writer fails. Deleting a missing retain item writes nothing. Every read-first delete (retain, unique, soft) with no `.condition()` — or only `deleteIfExists`'s — retries a lost race (changed, deleted, sentinel moved) from a fresh read (`GUARDED_PUT_ATTEMPTS`); with another condition it fails. `delete().returnValues("allOld")` returns the deleted item on every path.
- **Transactions refuse repeated items and > 4 MB before sending.** `refuseRepeatedItems` / `refuseOversizedTransaction` (`internal/TransactWriteOps.ts`, sizes from `internal/ItemSize.ts`) run in `transactWrite` and `EventStore.append`, over every compiled item including derived sentinels and snapshots. The refusal uses a LOWER size bound (numbers: ⌈significant digits / 2⌉ + 1, no list/map overhead) so it never refuses what DynamoDB accepts; `Batch.write` chunking uses the `"upper"` bound.
- **Restore recomposes all keys.** Re-establishes unique constraint sentinels atomically. The sentinel guard depends on `preserveUnique`: OFF → `attribute_not_exists(#pk)` (the delete released it, so anyone may have taken the value); ON → `attribute_not_exists(#pk) OR (#epk = :epk AND #esk = :esk)` against the sentinel's `_entity_pk`/`_entity_sk` back-pointer, because the delete KEPT the reservation and a plain absence test can never hold — which previously made `restore` impossible on any `preserveUnique` entity with a constraint (a bug, not a design rule: every such restore was cancelled and reported as a violation against its own reservation). The relaxed guard re-claims only a reservation this row still owns.
- **The restore-time retain snapshot is built from a CLEANED copy of the tombstone.** `deletedAt` and the soft-delete TTL attribute come off first — `buildSnapshotItem` only overrides the TTL when `versioned.ttl` is set, and this Put lands on the same `#v#<version>` SK the delete-time snapshot used, so anything dirty here REPLACES a clean snapshot with one that looks deleted and expires on the `softDelete.ttl` clock.
- **Purge deletes the item's rows — only rows of its own `__edd_e__`.** Without SK composites, every row of this entity in the partition; with them, only the named item's live row, nested rows, own history and unsegmented legacy history whose composites match. Resolves unique sentinels, batch-deletes in chunks of 25.
- **Retain-aware operations use transactWriteItems.** put/update/delete with retain create snapshots atomically.
- **A row read BACK from DynamoDB is attribute-keyed; anything that reads it by field name goes through `toDomainView` first (#127).** Key composition, vector-partition recomposition and unique-sentinel composition all name DOMAIN fields, so under a `field:` rename they read `undefined` off a raw row — key composition throws a defect and the sentinel's sparse rule silently skips the constraint, orphaning it. `toDomainView` returns a COPY, so the row written back (tombstone, restored item, snapshot) stays attribute-keyed. The read paths that decode and discard (`decodeRecord`, `deleted.get`, `deleted.list`) rename in place instead — `deserializeSparseFields` THEN `renameFromDynamo`, in that order. `Entity._buildPutSideItems` is the trap on the multi-item write path: `validateAndBuildPutItem` has already renamed the item it receives.
- **Time-series via `timeSeries: { orderBy, ttl?, appendInput }`.** Current-item SK unchanged; event items SK is `<currentSk>#e#<orderBy-value>`, GSI keys stripped, `_ttl` set. `.append(input)` is a `TransactWriteItems` (UpdateItem current + Put event) with CAS `attribute_not_exists(pk) OR #orderBy < :newOb`. Returns `{ applied: true | false, current }` — stale is a value, not an error. Mutually exclusive with `versioned` (EDD-9012) and `softDelete` (EDD-9015).
- **Time-series enrichment preservation.** `.append()` SET clause enumerates only fields in `appendInput` (required at `make()` time — EDD-9016). Fields outside `appendInput` are never touched on the current item. `appendInput` must include `orderBy` plus all PK/SK composites (EDD-9013).

### EventStore Operations
- **`append(streamId, events, expectedVersion, { additionalItems })`.** Caller-owned transact items (`EntityPut`, `EntityDelete`, the bound `db.entities.*` builders, `Transaction.check`) commit atomically with the event puts — the same op union `Transaction.transactWrite` accepts, compiled by the shared `internal/TransactWriteOps.buildTransactWriteItems`. `EntityUpdate` is not supported (neither is it in `transactWrite`); it lands for both at once when the shared builder gains it.
- **Cancellation mapping is position-aware.** Item layout is `[contiguityCheck?, events…, additionalItems…, sentinel?, snapshot?]` — the version-contiguity `ConditionCheck` (#82) is prepended only when `expectedVersion > 0` AND events are being written (a zero-event side-write opens no version gap), and the sentinel and the inline snapshot (#138) come AFTER `additionalItems` so caller-visible `additionalItems` indices never shift (a reason at the snapshot's position is `TransactionCancelled`). Reasons are matched by index, offset by the check. Precedence: `DuplicateCommand` > `VersionConflict` (contiguity check or any event put) > `AdditionalItemConditionFailed` > `TransactionCancelled` — ordered by how terminal the caller's response should be. NEVER reintroduce an any-reason `VersionConflict` mapping: it misattributes caller-condition failures and sends callers into an unresolvable retry loop.
- **Command idempotency.** `commandHandler(decider, stream, { idempotency: { ttl? } })` + a per-call `commandId` writes a dedup sentinel (`__edd_e__ = "<stream>.command"`, `attribute_not_exists(pk)`) into the same transaction, co-located in the stream partition and invisible to `read`/`readFrom`/`currentVersion`. Replay → `DuplicateCommand` (rejected, not replayed). Without `idempotency` the default is documented **at-least-once**. Configuring it makes `commandId` required at the type level.
- **100-item cap** counted across `events + additionalItems + sentinel + snapshot + contiguityCheck` → `AppendTooLarge`, checked before any AWS call (4 MB → `ValidationError`, via the shared `refuseOversizedTransaction` lower bound). The limit is the single named constant `TRANSACT_WRITE_ITEMS_LIMIT` (exported from `@effect-dynamodb/schema/Errors.js`, also used by `Transaction.transactWrite`) — there is exactly one, never a second local copy.
- **`commandHandler` is a hand-rolled dual, dispatching on the `EventStreamTypeId` brand** of its second argument — NOT `Function.dual`'s numeric-arity form, which silently drops the trailing options argument in the data-last position. Dropping it degrades `{ retry }` to no retry and `{ idempotency }` to at-least-once, both of which look like success until they matter. `CommandHandlerOptions` is ONE type carrying `retry` + `idempotency` + `consistentRead` + `verifySnapshot`; per-call `CommandOptions` carries `metadata` + `commandId` + `expectedVersion` + `additionalItems`. Guarded by the `commandHandler dual dispatch` tests, which assert both option kinds survive both data-last forms.
- **Command path order (#136–#140, `docs/designs/eventstore-command-path.md`).** `load → [expectedVersion check] → decide → fold new events → derive items → append → [snapshot]`. The load is strongly consistent by default (#139) and goes through `readLatest` on every snapshot-configured stream (#138). A caller `expectedVersion` (If-Match, #136) that differs from the loaded version fails with `VersionConflict { actualVersion }` BEFORE `decide`; with `idempotency` the mismatch first probes the sentinel so a redelivery is `DuplicateCommand`. A call with `expectedVersion` is never retried, nor is `DuplicateCommand`.
- **The evolve fold is the only source of state (#137).** Returned, snapshotted and projected state is the `evolve` fold of stored + new events. A function-form `additionalItems` receives `Decision { events, state, previous, version }`, is skipped for a no-op decision and re-run per retry; its `E2` / `R2` join the handler's channels. `evolve` MAY mutate in place (then `previous === state`). Read-only `decide` state (#142) is deliberately NOT enforced — no `DeepReadonly`, no runtime freeze; it is the application's design decision.
- **Inline snapshots + `readLatest` (#138).** `snapshot.mode: "inline"` passes the post-fold state as `AppendOptions.snapshot` — an UNCONDITIONAL `Put` in the append transaction (the event puts prove ownership of `asOfVersion`); needs ≥ 1 event. `readLatest` is ONE reverse `Query` over `sk BETWEEN <event prefix> AND <snapshot SK>` (sentinels sort before events, the snapshot after, under every casing), first-page `Limit` `(every ?? 1) + 1`, paging only for the events still missing.
- **Unverified snapshot loads — `verifySnapshot: false` (`CommandHandlerOptions` / `ReadLatestOptions`).** Accepted ONLY on `snapshot.mode: "inline"` without `every` (the one mode whose snapshot is normally at the head); anything else is `[EDD-9068]` — thrown by `commandHandler` at construction, a defect from `readLatest`; ignored without a snapshot config. Loads the snapshot item alone (one `GetItem`); with no snapshot item it falls back to the verified `readLatest`. Contract: **no answer decided on an unverified snapshot is returned until the head confirms it** — a successful append confirms it by itself; a domain error, a no-op or an append `VersionConflict` costs one more read. If-Match ≠ snapshot `asOfVersion` → verified re-read BEFORE `decide` (the #136 check, verified `actualVersion`). If-Match = `asOfVersion` → `decide` on the snapshot (exactly the state the caller saw); a non-append answer is checked with a head `Query` bounded below by the snapshot's version (reverse, `Limit: 1` over `[floor+1 … max]` — reads ZERO items while the snapshot is current; never a `currentVersion`-style read of the newest event, which would pay the event size the option exists to skip), and a head past the If-Match answers as the verified handler (`DuplicateCommand` probe, else `VersionConflict { actualVersion: head }`) — never re-decided. No If-Match: domain error / no-op → head check, stale → verified load + re-decide once; append conflict → verified load, events past the snapshot → re-decide once, snapshot at head → genuine race → `retry` policy. **Why `decide` runs twice:** a decision made on a lagging snapshot is stale and cannot be patched, only made again (the same price a `VersionConflict` retry pays); surfacing it instead would fail or mis-answer every command on the stream, because only a successful inline append rewrites the snapshot. At most once per attempt; a function-form `additionalItems` re-runs only when both decisions append. Do NOT reintroduce "staleness always surfaces at the append" reasoning — it does not hold for decisions that append nothing.
- **An append is never split — large commands are stepped commands (#141 not implemented).** One command → one decision → one atomic append: an append carries one decision's events and the ONE state after all of them, so split across transactions it would have no valid intermediate state (snapshot, projection, sentinel) at the boundaries. Over the limits → `AppendTooLarge` / `ValidationError` before anything is written. The application splits a large command into fixed-size steps, each an ordinary `commandHandler` call chained by `expectedVersion` (with idempotency, a `commandId` per step, e.g. `${commandId}#step-${n}`); a failure partway leaves a real intermediate state. A redelivered large command (same `commandId`, original `expectedVersion`) resumes by skipping each `DuplicateCommand` step while KEEPING the stale If-Match (so the next step is sentinel-probed before `decide`), then running the first `VersionConflict` step at its `actualVersion` — never chain from the head after a duplicate (`decide` would run against already-applied steps). Do not re-add a library-side chunked append.
- **Stream indexes (#140).** `makeStream({ indexes })` — LSI (default) or GSI (`pk` attribute filled with the stream's partition key); `key(event, version)` stored RAW, `undefined` = sparse; only event items carry index attributes. `readIndex` / `query.index` with type-checked names; `indexDefinitions` emits projection-`ALL` CreateTable fragments. An LSI must be created with the table and caps EVERY partition key value's item collection at 10 GB; index attributes are written only by `append`, never backfilled. EDD-9062–9067.

### Aggregate Operations
- **Edge entities are first-class.** Own models, keys, indexes, and configuration. Composed via `Aggregate.one()`, `Aggregate.many()`, `BoundSubAggregate`.
- **Write-time ref hydration.** Framework fetches referenced entity at create/update time, denormalizes into edge entity. Read path is cheap.
- **Aggregates compose their own rows.** Read: partition query + assembly. Write: `decomposeAggregate` → `buildDynamoItems` (keys, `__edd_e__`, context, system timestamps) → one `transactWriteItems` per sub-aggregate group. Entity write ops are NOT in the path — anything Entity does at write time (timestamps, versioning) must be implemented for aggregates separately. Diff-based updates only write changed groups. **`create` is guarded on the root (#134):** the root item is ordered first in the first group and its Put carries `attribute_not_exists(#pk)`; its cancellation reason maps to `ConditionalCheckFailed` (`AggregateCreateError`), so an existing aggregate fails `create` with nothing written. Edge / `many` / nested sub-aggregate rows have no guard of their own. Groups are still separate transactions — a later group's failure leaves earlier groups written, as it always did. `update` / `delete` don't pass a guard. Orphan edge rows (no root) don't stop `create` — documented, not checked (it would cost a read per create). Aggregate `delete` (`deleteAllItems`) retries `UnprocessedItems` with backoff, 5 retries, then `DynamoError`.
- **Aggregate attributes are ENCODED before marshalling (#116).** Decomposition works from the schema-DECODED domain object, so `buildAttrEncoders` gives every node (root, sub-aggregate root, `one` edge, `many` element) one encoder per field: a date encoding (`storedAs` override, else the inferred default) wins; otherwise the field's own `encode` with `decode -> encode` as fallback; an untransformed field gets none, so its bytes are unchanged. Exactly one encoder per field — nothing is encoded twice. Without this a `BigIntFromString` stored `{N:"5"}` and assembly's decode rejected it, so the aggregate could not round-trip at all. **Context values go through the same encoders**, or the same logical field is stored two ways on two rows of one partition.
- **`fieldsOf` unwraps `DynamoModel.configure`.** `configure()` returns a `{ model, attributes }` WRAPPER, not a schema, so reading `.fields` off it yields nothing and the node got NO encoders — dates included. Any `identifier: true` / `field:` rename produces a configured model, so this covered most real edges.
- **Encoding attributes must never move a composed KEY.** Keys come from the assembled object through the key form, which normalises from either side, so the Type→Encoded shift is absorbed. Pinned literally by a connected test on an untransformed model.
- **`aggregate.update` re-decodes the mutated state TOLERANTLY (#116).** After a mutation a field may hold either the domain value the caller set or the wire value that came back from storage, so `tolerantTransforms` substitutes a tolerant schema for EVERY leaf transform, not just dates (`buildTolerantTransform`). **Tolerant is not lax**: decode tries the transform (wire form), then validates against the transform's own Type side (domain form), and still rejects a value that is neither — pinned by a connected test that asserts nonsense is refused on the update path. **The seam:** `tolerantTransforms` is set at exactly ONE production call site (the aggregate's `decodeSchema`); every entity-derived schema calls `substituteSchemaDeep` with no options, so entity behaviour is unchanged by construction. `resolveRef` now propagates through the recursion too — a `DynamoModel.ref` field nested inside an edge model is annotated, and `Schema.annotate` drops a `Schema.Class`'s `.fields`, so without it the target's own transformed fields kept strict schemas.
- **Diff narrowing survives the encode.** `deepEqualGroups` compares DECOMPOSED groups and decomposition is where encoding happens, so both sides are encoded identically and a no-op update still writes nothing. Asserted at the UNIT level (`test/Aggregate.test.ts`, "skips write when nothing changed, on a transformed model") where the `transactWrite` call count is observable — a connected test cannot see a skipped write.
- **Discriminator SK format is `name#value`.** `{ teamNumber: 1 }` → `#teamNumber#1`.
- **Domain models are pure.** Entity association declared at edge level in `Aggregate.make()`, not in Schema.Class model.
- **Aggregate.update mutation context.** Receives `UpdateContext` with: `state` (plain object), `cursor` (pre-bound optic), `optic` (composable optic), `current` (Schema.Class instance).
- **System timestamps via `Aggregate.make({ timestamps })`.** Same `TimestampsConfig` as `Entity.make`, applied to every row the aggregate writes (root + edges + sub-aggregate groups). Stamped in `buildDynamoItems` — AFTER the update diff, so timestamps never defeat diff narrowing. `created` is carried forward from the stored row (aggregate writes are `Put`); `updated` is per row, so rows a diff leaves alone keep their value. Timestamp attributes are stripped on the read path unless the root model declares the field itself.
- **`aggregate.list(key, options)` pages on RESULTS, not rows (#104).** `limit` is at most _n_ aggregates and accumulates across requests; `pageSize` is DynamoDB's `Limit` (rows examined); `filter` compiles a `FilterExpression` onto the ROOT-ITEM query through the same `Expr` compiler `BoundQuery.filter()` uses (callback + shorthand); `reverse` sets `ScanIndexForward: false`. Filtering server-side is a performance fix as much as an ergonomic one — `list` assembles each surviving root item with its own partition read, so an in-memory predicate pays a full assembly per discarded aggregate. Once a filtered request over-reads, the cursor is rebuilt from the last item RETURNED (same rule as `Query.execute`), so `cursor: null` still means genuinely exhausted.
- **A sharded (`cardinality`) list has no cursor.** The fan-out spans N partitions with no defined order across them, so `limit` truncates the merged result and `cursor` is always `null`; a passed cursor is REJECTED with `EDD-9051` rather than ignored.
- **Optional sub-aggregates supported.** `Schema.optionalKey` → decomposition skips null/undefined, assembly omits the key entirely.

## MCP Servers

- **effect-docs** — Effect TS documentation search. Use `effect_docs_search` to search and `get_effect_doc` to retrieve specific docs. **Note:** May serve v3 documentation until v4 is stable — cross-reference with migration guides.
- v4 source: https://github.com/Effect-TS/effect — ground truth for v4 APIs (effect-smol was the beta-era home)
- v4 migration: https://github.com/Effect-TS/effect/blob/main/MIGRATION.md
