import { Glob } from "bun";
import { resolve } from "node:path";
import { isEventHandler, type EventHandler } from "./types.ts";
import type { Logger } from "../../logger.ts";

export const DEFAULT_HANDLERS_DIR = "handlers";

export interface DiscoveredHandler {
  handler: EventHandler;
  path: string;
}

/**
 * Auto-discovery: import every `*.handler.ts` under the handlers dir and collect
 * its default export if it satisfies the EventHandler contract. This is the
 * "drop a file and it's wired up" convention.
 */
export async function discoverHandlers(
  logger: Logger,
  dir: string = DEFAULT_HANDLERS_DIR,
): Promise<DiscoveredHandler[]> {
  const glob = new Glob("**/*.handler.ts");
  const discovered: DiscoveredHandler[] = [];

  for await (const rel of glob.scan({ cwd: dir, onlyFiles: true })) {
    const path = resolve(dir, rel);
    const mod = (await import(path)) as { default?: unknown };
    const candidate = mod.default;

    if (!isEventHandler(candidate)) {
      logger.warn("skipping file without a valid default-exported handler", { path });
      continue;
    }

    discovered.push({ handler: candidate, path });
    logger.debug("discovered handler", {
      path,
      eventType: candidate.eventType,
    });
  }

  return discovered;
}
