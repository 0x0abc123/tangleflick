import { defineHandler } from "../src/core/handler/types.ts";
import { ExampleProcessed, type ExampleProcessedPayload } from "../events/example.event.ts";
import { auditEvent } from "./_shared.ts";

// A different event type (`example.processed`) reusing the SAME shared function.
export default defineHandler<ExampleProcessedPayload>({
  eventType: ExampleProcessed.type,
  schema: ExampleProcessed.schema,
  handle: auditEvent,
});
