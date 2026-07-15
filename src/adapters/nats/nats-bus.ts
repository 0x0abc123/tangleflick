import {
  connect,
  consumerOpts,
  createInbox,
  type JetStreamClient,
  type JetStreamSubscription,
  type NatsConnection,
} from "nats";
import type {
  EventBus,
  EventListener,
  PublishOptions,
  SubscribeOptions,
  Subscription,
} from "../../core/ports/event-bus.ts";
import { createEnvelope, type EventEnvelope } from "../../core/events/envelope.ts";
import type { JsonCodec } from "../../core/serialization/json-codec.ts";
import type { Logger } from "../../logger.ts";

interface NatsBusConfig {
  type: "nats";
  servers: string[];
  stream: string;
  durablePrefix: string;
}

export interface NatsBusDeps {
  config: NatsBusConfig;
  codec: JsonCodec;
  source: string;
  logger: Logger;
}

/**
 * NATS/JetStream-backed event bus. Event type maps to the subject
 * `${stream}.${eventType}`, and the stream is provisioned to capture
 * `${stream}.>`. Subscriptions are durable push consumers; a handler `group`
 * becomes a queue group for load-balanced delivery.
 */
export class NatsBus implements EventBus {
  private nc!: NatsConnection;
  private js!: JetStreamClient;
  private readonly subs: JetStreamSubscription[] = [];

  constructor(private readonly deps: NatsBusDeps) {}

  private subjectFor(eventType: string): string {
    return `${this.deps.config.stream}.${eventType}`;
  }

  async connect(): Promise<void> {
    this.nc = await connect({ servers: this.deps.config.servers });
    const jsm = await this.nc.jetstreamManager();
    const subjects = [`${this.deps.config.stream}.>`];
    try {
      await jsm.streams.add({ name: this.deps.config.stream, subjects });
    } catch {
      // Stream already exists — make sure it covers our subject space.
      await jsm.streams.update(this.deps.config.stream, { subjects });
    }
    this.js = this.nc.jetstream();
    this.deps.logger.info("nats bus connected", {
      servers: this.deps.config.servers,
      stream: this.deps.config.stream,
    });
  }

  async disconnect(): Promise<void> {
    for (const sub of this.subs) sub.unsubscribe();
    await this.nc?.drain();
    this.deps.logger.info("nats bus disconnected");
  }

  async publish<T>(
    eventType: string,
    payload: T,
    opts?: PublishOptions,
  ): Promise<void> {
    const envelope = createEnvelope({
      type: eventType,
      payload,
      source: this.deps.source,
      correlationId: opts?.correlationId,
      headers: opts?.headers,
    });
    await this.js.publish(this.subjectFor(eventType), this.deps.codec.encode(envelope));
  }

  async subscribe<T>(
    eventType: string,
    listener: EventListener<T>,
    opts?: SubscribeOptions,
  ): Promise<Subscription> {
    const subject = this.subjectFor(eventType);
    const durable = this.durableName(eventType, opts?.group);

    const settings = consumerOpts();
    settings.durable(durable);
    settings.deliverTo(createInbox());
    settings.manualAck();
    settings.ackExplicit();
    if (opts?.group) settings.queue(opts.group);

    const sub = await this.js.subscribe(subject, settings);
    this.subs.push(sub);
    this.deps.logger.info("nats subscribed", { subject, durable });

    // Drive the async iterator in the background.
    void (async () => {
      for await (const msg of sub) {
        let envelope: EventEnvelope<T>;
        try {
          envelope = this.deps.codec.decode(msg.data) as EventEnvelope<T>;
        } catch (err) {
          this.deps.logger.error("failed to decode nats message", {
            subject,
            error: (err as Error).message,
          });
          msg.term(); // poison message — don't redeliver
          continue;
        }
        try {
          await listener(envelope);
          msg.ack();
        } catch {
          msg.nak(); // negative-ack → JetStream redelivers
        }
      }
    })();

    return {
      unsubscribe: async () => {
        sub.unsubscribe();
        const idx = this.subs.indexOf(sub);
        if (idx >= 0) this.subs.splice(idx, 1);
      },
    };
  }

  private durableName(eventType: string, group?: string): string {
    const raw = `${this.deps.config.durablePrefix}_${group ?? "default"}_${eventType}`;
    return raw.replace(/[^A-Za-z0-9_]/g, "_");
  }
}
