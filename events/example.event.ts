import { z } from "zod";
import { defineEvent } from "../src/core/events/define.ts";

/**
 * Example event definitions. Each `defineEvent` binds an event type to its Zod
 * payload schema. Import these in handlers, and the framework auto-registers
 * them (so they can be published/validated even without a local consumer).
 *
 * Replace these with your own `*.event.ts` files.
 */

export const ExampleCreated = defineEvent(
  "example.created",
  z.object({
    message: z.string().min(1),
    count: z.number().int().nonnegative().default(0),
  }),
);
export type ExampleCreatedPayload = z.infer<typeof ExampleCreated.schema>;

export const ExampleProcessed = defineEvent(
  "example.processed",
  z.object({
    originalMessage: z.string(),
    processedAt: z.string(),
  }),
);
export type ExampleProcessedPayload = z.infer<typeof ExampleProcessed.schema>;
