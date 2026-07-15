# tangleflick

A **Bun + TypeScript template framework** for building event-driven services.

Clone it, add your **event schemas** and **event handlers**, and the framework wires up
the rest: config loading, an **event bus**, and a **shared data store** — each selectable
by config and hidden behind stable **ports & adapters** so your handlers never depend on a
specific transport or database.

| Concern    | Default (embedded)      | External options            |
| ---------- | ----------------------- | --------------------------- |
| Event bus  | in-process `EventEmitter` | Kafka, NATS/JetStream       |
| Data store | SQLite (`bun:sqlite`)   | PostgreSQL (Bun native `SQL`) |

Handlers depend only on the `EventBus` and `DataStore` interfaces — switching Kafka↔NATS or
SQLite↔Postgres is a one-line config change, never a code change.

---

## Quick start

```bash
# 1. Install Bun (https://bun.sh) if you don't have it
curl -fsSL https://bun.sh/install | bash

# 2. Install dependencies
bun install

# 3. Create your config
cp config/config.example.json config/config.json

# 4. Run (defaults: emitter bus + embedded SQLite)
bun start
```

The bundled example handler subscribes to `example.created`, persists the payload, and
publishes `example.processed`.

> **New here?** Follow [`WALKTHROUGH.md`](./WALKTHROUGH.md) to build your first event flow
> step by step — write a schema and handler, emit an event back onto the bus, and run it.

```bash
bun test          # run the test suite
bun run typecheck # tsc --noEmit
bun run migrate   # apply pending migrations only
bun run dev       # run with --watch
```

---

## Architecture

```
config.json ─► Application ─► builds ─┬─► EventBus port  ◄─ emitter | kafka | nats
                                      └─► DataStore port ◄─ sqlite  | postgres
                                            ▲
              events/*.event.ts ─ schemas ──┤
              handlers/*.handler.ts ────────┘ (auto-discovered, subscribed)
```

- **Ports** (`src/core/ports/`) — the stable interfaces `EventBus` and `DataStore`.
- **Adapters** (`src/adapters/`) — concrete backends. The *only* places that name a backend
  are `src/core/bus/factory.ts` and `src/core/store/factory.ts`.
- **Envelope** — every message is wrapped in a normalized `EventEnvelope` (id, type, time,
  source, correlationId, headers, payload). All adapters share one JSON codec, so
  serialization and payload validation behave identically everywhere.

---

## Adding an event

Create a file under `events/` ending in `.event.ts`. Use `defineEvent` to bind an event
type to a Zod schema — this is your single source of truth for both the TypeScript type and
runtime validation.

```ts
// events/order.event.ts
import { z } from "zod";
import { defineEvent } from "../src/core/events/define.ts";

export const OrderPlaced = defineEvent(
  "order.placed",
  z.object({ orderId: z.string(), total: z.number().positive() }),
);
export type OrderPlacedPayload = z.infer<typeof OrderPlaced.schema>;
```

Event definitions are auto-registered at startup, so their types can be **published** and
validated even if no local handler consumes them (e.g. events for other services).

## Adding a handler

Create a file under `handlers/` ending in `.handler.ts` with a **default export** built by
`defineHandler`. It's auto-discovered and subscribed at startup.

```ts
// handlers/order.handler.ts
import { defineHandler } from "../src/core/handler/types.ts";
import { OrderPlaced, type OrderPlacedPayload } from "../events/order.event.ts";

export default defineHandler<OrderPlacedPayload>({
  eventType: OrderPlaced.type,
  schema: OrderPlaced.schema,
  // group: "billing",   // optional: consumer group / durable for load-balancing

  async handle(event, ctx) {
    // 1. process
    ctx.logger.info("order placed", { orderId: event.payload.orderId });

    // 2. persist state (same API on SQLite and Postgres)
    const orders = ctx.store.repository<OrderPlacedPayload>("orders");
    await orders.put(event.payload.orderId, event.payload);

    // 3. publish a follow-up event back onto the bus
    await ctx.publish("order.confirmed", { orderId: event.payload.orderId });
  },
});
```

Each handler receives a `HandlerContext`:

- `ctx.publish(type, payload, opts?)` — publish back onto the bus
- `ctx.store` — the shared `DataStore`
- `ctx.logger` — structured logger, pre-scoped to the handler

## Using the data store

`ctx.store.repository<T>(collection, schema?)` returns a collection-scoped key/value
`Repository<T>`:

```ts
const repo = ctx.store.repository<T>("things", ThingSchema); // schema optional
await repo.put(id, value);           // upsert
await repo.get(id);                  // T | null
await repo.has(id);                  // boolean
await repo.delete(id);               // boolean
await repo.list({ prefix, limit });  // Entry<T>[]
await repo.where("field", value);    // equality on a stored JSON field (string/number)
await ctx.store.transaction(async (tx) => { /* ... */ });
```

Values are stored as JSON in a generic `kv_store` table, so any collection works without
bespoke DDL. For richer relational queries, add your own tables via a migration.

---

## Configuration (`config/config.json`)

```jsonc
{
  "app": { "name": "tangleflick", "logLevel": "info" },
  "bus":  { "type": "emitter" },
  "store": { "type": "sqlite", "path": "./data/tangleflick.db" }
}
```

**Bus options**

```jsonc
{ "type": "emitter" }
{ "type": "kafka", "brokers": ["localhost:9092"], "clientId": "app", "groupId": "app", "ssl": false }
{ "type": "nats",  "servers": ["nats://localhost:4222"], "stream": "EVENTS", "durablePrefix": "app" }
```

**Store options**

```jsonc
{ "type": "sqlite", "path": "./data/tangleflick.db" }
{ "type": "postgres", "url": "postgres://user:pass@localhost:5432/tangleflick" }
```

Config is validated by a Zod schema (`src/config/schema.ts`) at startup; invalid config
fails fast with a readable error. Override the path with `TANGLEFLICK_CONFIG=/path bun start`.

---

## Migrations

Ordered `.sql` files in `migrations/` are applied at startup (and via `bun run migrate`),
tracked in a `schema_migrations` table. Backend-specific variants are resolved by suffix:

```
migrations/0002_add_orders.sqlite.sql     # used when store.type = sqlite
migrations/0002_add_orders.postgres.sql   # used when store.type = postgres
migrations/0003_generic.sql               # used for any backend (fallback)
```

The framework ships `0001_init_kv.*`, which creates the `kv_store` table the repositories
use. Add your own numbered files for custom tables.

---

## Project layout

```
src/
  index.ts            entrypoint
  application.ts      bootstrap + lifecycle
  config/             schema.ts (Zod) + load.ts
  core/
    ports/            event-bus.ts, data-store.ts   ← the stable interfaces
    events/           envelope.ts, registry.ts, define.ts, discover.ts
    serialization/    json-codec.ts
    bus/factory.ts    store/factory.ts, store/migrator.ts
    handler/          types.ts, discover.ts, register.ts
  adapters/           emitter/ kafka/ nats/ sqlite/ postgres/
  cli/migrate.ts
events/               ← YOUR event schemas (*.event.ts)
handlers/             ← YOUR handlers (*.handler.ts, auto-discovered)
migrations/           ← ordered .sql
tests/                ← bun test
```

## Switching backends locally

Bring up brokers/DB (e.g. via Docker), then flip `config.json`:

```bash
# Kafka:    docker run -p 9092:9092 apache/kafka
# NATS:     docker run -p 4222:4222 nats -js
# Postgres: docker run -p 5432:5432 -e POSTGRES_PASSWORD=pass postgres
```

The same handlers run unchanged — that's the point of the ports/adapters split.
