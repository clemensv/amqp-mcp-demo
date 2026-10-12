// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import {
  Container,
  ConnectionEvents,
  Sender,
  SenderEvents,
  Session,
  SessionEvents,
  type Connection,
  type ConnectionOptions,
  type EventContext,
  type SenderOptions,
} from 'rhea-promise';
import { requiredEnv, type Config } from './config.js';
import { enableTypedIdCapture } from './wire.js';

interface SaslReceiveState {
  transport: { read_complete: boolean };
  next: { read(buffer: Buffer): number };
}

const adaptedSaslReaders = new WeakSet<object>();

/** Drains AMQP bytes received in the same buffer as the final SASL outcome. */
function enableSaslTransportAdapter(container: Container): void {
  const prototype = (
    container.sasl.Client as unknown as {
      prototype: {
        read(this: SaslReceiveState, buffer: Buffer): number;
      };
    }
  ).prototype;
  if (adaptedSaslReaders.has(prototype)) {
    return;
  }
  const read = prototype.read;
  prototype.read = function (buffer: Buffer): number {
    const wasSasl = !this.transport.read_complete;
    const consumed = read.call(this, buffer);
    if (wasSasl && this.transport.read_complete && consumed < buffer.length) {
      return consumed + this.next.read(buffer.subarray(consumed));
    }
    return consumed;
  };
  adaptedSaslReaders.add(prototype);
}

/**
 * Builds broker options with certificate verification and exactly the configured SASL mechanism.
 * @throws Error for inaccessible TLS files or missing configured credential variables.
 */
export async function connectionOptions(
  config: Config,
  container: Container,
  environment: NodeJS.ProcessEnv = process.env,
): Promise<ConnectionOptions> {
  const endpoint = new URL(config.endpoint);
  const host = endpoint.hostname.replace(/^\[|\]$/g, '');
  const common = {
    host,
    hostname: config.sasl.hostname ?? host,
    sasl_init_hostname: config.sasl.hostname ?? host,
    port: Number(endpoint.port || (endpoint.protocol === 'amqps:' ? 5671 : 5672)),
    // Reconnecting would invalidate sender-owned queues and in-flight correlations.
    reconnect: false,
    container_id: `amqp-mcp-${randomUUID()}`,
    idle_time_out: 60000,
    operationTimeoutInSeconds: config.operationTimeoutMs / 1000,
    max_frame_size: 65536,
    properties: config.broker === 'servicebus' ? { 'com.microsoft:is-client-provider': true } : {},
  };
  const options: ConnectionOptions =
    endpoint.protocol === 'amqps:'
      ? {
          ...common,
          transport: 'tls',
          minVersion: 'TLSv1.2',
          rejectUnauthorized: true,
          servername: config.tls.serverName ?? host,
          ...(config.tls.caFile ? { ca: await readFile(config.tls.caFile) } : {}),
          ...(config.tls.certFile ? { cert: await readFile(config.tls.certFile) } : {}),
          ...(config.tls.keyFile ? { key: await readFile(config.tls.keyFile) } : {}),
        }
      : { ...common, transport: 'tcp' };
  if (config.sasl.mode !== 'none') {
    const mechanisms = container.sasl.client_mechanisms();
    if (config.sasl.mode === 'plain') {
      mechanisms.enable_plain(
        config.sasl.username!,
        requiredEnv(config.sasl.passwordEnv!, environment),
      );
    } else if (config.sasl.mode === 'external') {
      mechanisms.enable_external();
    } else {
      mechanisms.enable_anonymous('amqp-mcp');
    }
    Object.assign(options, { sasl_mechanisms: mechanisms });
  }
  return options;
}

/** Owns one non-reconnecting AMQP connection authenticated with the configured SASL mode. */
export class AmqpConnection {
  private closed = false;
  private raw: EventContext['_context']['connection'] | undefined;
  private keepalive: NodeJS.Timeout | undefined;

  private constructor(readonly connection: Connection) {}

  /** Opens and authenticates a connection; cleans up partially initialized resources on failure. */
  static async open(config: Config, onFailure: (error: Error) => void): Promise<AmqpConnection> {
    enableTypedIdCapture();
    const container = new Container();
    enableSaslTransportAdapter(container);
    const connection = container.createConnection(await connectionOptions(config, container));
    const owned = new AmqpConnection(connection);
    connection.once(ConnectionEvents.connectionOpen, (context: EventContext) => {
      owned.raw = context._context.connection;
      if (config.broker === 'ibmmq') {
        owned.keepalive = setInterval(() => {
          if (!owned.closed && owned.raw?.is_open()) {
            (
              owned.raw as EventContext['_context']['connection'] & {
                _write_frame(): void;
              }
            )._write_frame();
          }
        }, 10000);
        owned.keepalive.unref();
      }
    });
    const failure = () => {
      if (!owned.closed) {
        onFailure(new Error('AMQP connection was lost.'));
      }
    };
    connection.on(ConnectionEvents.connectionError, failure);
    connection.on(ConnectionEvents.connectionClose, failure);
    connection.on(ConnectionEvents.disconnected, failure);
    try {
      await connection.open();
      return owned;
    } catch (error) {
      await owned.close();
      throw error;
    }
  }

  /**
   * Attaches a lifetime-owning sender on a dedicated session without requiring send credit.
   * The caller must retain the returned link until the corresponding reply receiver closes.
   * @throws Error when ownership cannot be established within the attachment deadline.
   */
  async createOwnerSender(options: SenderOptions, timeoutMs = 10000): Promise<Sender> {
    if (!this.raw || this.closed) {
      throw new Error('AMQP ownership connection is not open.');
    }
    const raw = this.raw;
    const rawSession = raw.create_session();
    // rhea-promise's normal sender factory waits for credit, which an ownership link may never receive.
    const session = new Session(this.connection, rawSession);
    session.begin();
    const rawSender = rawSession.attach_sender(options);
    const sender = new Sender(session, rawSender, options);
    // Manual wrappers forward events but do not register the factory's option callbacks.
    if (options.onError) {
      sender.on(SenderEvents.senderError, options.onError);
    }
    if (options.onClose) {
      sender.on(SenderEvents.senderClose, options.onClose);
    }
    if (options.onSessionError) {
      session.on(SessionEvents.sessionError, options.onSessionError);
    }
    if (options.onSessionClose) {
      session.on(SessionEvents.sessionClose, options.onSessionClose);
    }
    try {
      await new Promise<void>((resolve, reject) => {
        const cleanup = () => {
          clearTimeout(timer);
          rawSender.removeListener(SenderEvents.senderOpen, opened);
          rawSender.removeListener(SenderEvents.senderError, failed);
          rawSender.removeListener(SenderEvents.senderClose, failed);
          rawSession.removeListener(SessionEvents.sessionError, failed);
          rawSession.removeListener(SessionEvents.sessionClose, failed);
          raw.removeListener(ConnectionEvents.disconnected, failed);
        };
        const opened = () => {
          if (rawSender.target?.address) {
            cleanup();
            resolve();
          }
        };
        const failed = () => {
          cleanup();
          reject(new Error('Temporary queue owner attach failed.'));
        };
        const timer = setTimeout(() => {
          cleanup();
          reject(new Error('Temporary queue owner attach timed out.'));
        }, timeoutMs);
        rawSender.once(SenderEvents.senderOpen, opened);
        rawSender.once(SenderEvents.senderError, failed);
        rawSender.once(SenderEvents.senderClose, failed);
        rawSession.once(SessionEvents.sessionError, failed);
        rawSession.once(SessionEvents.sessionClose, failed);
        raw.once(ConnectionEvents.disconnected, failed);
      });
      return sender;
    } catch (error) {
      rawSender.close();
      rawSession.close();
      session.remove();
      throw error;
    }
  }

  /** Idempotently closes the connection and removes its event listeners. */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    clearInterval(this.keepalive);
    this.keepalive = undefined;
    try {
      await this.connection.close();
    } finally {
      this.connection.removeAllListeners();
    }
  }
}
