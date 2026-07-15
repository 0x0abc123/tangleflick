import type { EventEnvelope } from "../src/core/events/envelope.ts";
import type { HandlerContext } from "../src/core/handler/types.ts";

/**
 * Shared handler logic reusable across event types. Because it accepts a
 * generic `EventEnvelope` (payload typed as `unknown`), any handler can use it
 * as its `handle` regardless of the event's payload shape.
 *
 * Files that don't end in `.handler.ts` (like this one) are NOT auto-discovered,
 * so this is a plain helper, not a handler itself.
 */
export async function auditEvent(
  event: EventEnvelope,
  ctx: HandlerContext,
): Promise<void> {
  ctx.logger.info("audit", { type: event.type, id: event.id });

  const audit = ctx.store.repository<{
    type: string;
    source: string;
    time: string;
    correlationId: string | null;
  }>("audit_log");

  await audit.put(event.id, {
    type: event.type,
    source: event.source,
    time: event.time,
    correlationId: event.correlationId ?? null,
  });
}
