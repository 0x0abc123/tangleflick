import { defineHandler } from "../src/core/handler/types.ts";
import { ExampleCreated, type ExampleCreatedPayload } from "../events/example.event.ts";
import { auditEvent } from "./_shared.ts";

// Fan-out: this handler ALSO subscribes to `example.created` (alongside
// example.handler.ts). Both run for each event. Here we reuse the shared
// `auditEvent` logic instead of duplicating it.
export default defineHandler<ExampleCreatedPayload>({
  eventType: ExampleCreated.type,
  schema: ExampleCreated.schema,
  handle: auditEvent,
});
