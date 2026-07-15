import { z } from "zod";

/**
 * Configuration schema. This is the single source of truth for both the shape
 * of config/config.json AND its runtime validation. `bus` and `store` are
 * discriminated unions on `type`, so only the block relevant to the selected
 * backend is required/validated.
 */

const AppSchema = z.object({
  name: z.string().min(1).default("tangleflick"),
  logLevel: z.enum(["debug", "info", "warn", "error"]).default("info"),
});

/* ------------------------------- Event bus -------------------------------- */

const EmitterBusSchema = z.object({
  type: z.literal("emitter"),
});

const KafkaBusSchema = z.object({
  type: z.literal("kafka"),
  brokers: z.array(z.string().min(1)).min(1),
  clientId: z.string().min(1).default("tangleflick"),
  /** Default consumer group; a handler may override via its `group`. */
  groupId: z.string().min(1).default("tangleflick"),
  ssl: z.boolean().default(false),
});

const NatsBusSchema = z.object({
  type: z.literal("nats"),
  servers: z.array(z.string().min(1)).min(1),
  /** JetStream stream that subjects are published to / consumed from. */
  stream: z.string().min(1).default("EVENTS"),
  /** Prefix for durable consumer names (durable = `${prefix}_${group}`). */
  durablePrefix: z.string().min(1).default("tangleflick"),
});

const BusSchema = z.discriminatedUnion("type", [
  EmitterBusSchema,
  KafkaBusSchema,
  NatsBusSchema,
]);

/* ------------------------------- Data store ------------------------------- */

const SqliteStoreSchema = z.object({
  type: z.literal("sqlite"),
  path: z.string().min(1).default("./data/tangleflick.db"),
});

const PostgresStoreSchema = z.object({
  type: z.literal("postgres"),
  url: z.string().min(1),
});

const StoreSchema = z.discriminatedUnion("type", [
  SqliteStoreSchema,
  PostgresStoreSchema,
]);

/* --------------------------------- Root ----------------------------------- */

export const ConfigSchema = z.object({
  app: AppSchema.default({}),
  bus: BusSchema,
  store: StoreSchema,
});

export type Config = z.infer<typeof ConfigSchema>;
export type BusConfig = z.infer<typeof BusSchema>;
export type StoreConfig = z.infer<typeof StoreSchema>;
export type AppConfig = z.infer<typeof AppSchema>;
