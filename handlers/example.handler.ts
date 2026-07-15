import { defineHandler } from "../src/core/handler/types.ts";
import {
  ExampleCreated,
  ExampleProcessed,
  type ExampleCreatedPayload,
} from "../events/example.event.ts";

/**
 * Example handler demonstrating the full loop:
 *   subscribe → handle (process) → persist state → publish a follow-up event.
 *
 * Drop your own `*.handler.ts` files into this directory; they are
 * auto-discovered and registered at startup.
 */
export default defineHandler<ExampleCreatedPayload>({
  eventType: ExampleCreated.type,
  schema: ExampleCreated.schema,

  async handle(event, ctx) {
    ctx.logger.info("handling event", {
      id: event.id,
      message: event.payload.message,
    });

    // Persist state to the shared data store (SQLite or Postgres — same API).
    const examples = ctx.store.repository<ExampleCreatedPayload>("examples");
    await examples.put(event.id, event.payload);

    // Publish a follow-up event back onto the bus.
    await ctx.publish(ExampleProcessed.type, {
      originalMessage: event.payload.message,
      processedAt: new Date().toISOString(),
    });
  },
});
