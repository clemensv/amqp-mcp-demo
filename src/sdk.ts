// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import {
  Server,
  type Implementation,
  type JSONRPCMessage,
  type ServerOptions,
  type Transport,
} from '@modelcontextprotocol/server';
import type { Config } from './config.js';

/** Fixed stateless MCP revision implemented by the AMQP binding. */
export const MCP_PROTOCOL_VERSION = '2026-07-28';

/** Creates a fresh SDK server for each exchange, with application-owned handlers. */
export type McpServerFactory = () => Server;

/** Selects the stateless SDK codec without an initialize exchange. */
export class StatelessMcpServer extends Server {
  constructor(
    serverInfo: Implementation,
    options: Omit<ServerOptions, 'supportedProtocolVersions'> = {},
  ) {
    super(serverInfo, { ...options, supportedProtocolVersions: [MCP_PROTOCOL_VERSION] });
    this._negotiatedProtocolVersion = MCP_PROTOCOL_VERSION;
  }
}

/** One SDK exchange whose emitter retains the original AMQP reply route. */
class RequestScopedTransport implements Transport {
  onmessage: Transport['onmessage'];
  onerror: Transport['onerror'];
  onclose: Transport['onclose'];
  private closed = false;
  private tail: Promise<void> = Promise.resolve();
  private resolve: () => void = () => {};
  private reject: (error: Error) => void = () => {};
  readonly completed = new Promise<void>((resolve, reject) => {
    this.resolve = resolve;
    this.reject = reject;
  });

  constructor(private readonly emit: (message: JSONRPCMessage) => Promise<void>) {}

  async start(): Promise<void> {}

  async send(message: JSONRPCMessage): Promise<void> {
    if (this.closed) {
      return;
    }
    const operation = this.tail.then(() => this.emit(message));
    this.tail = operation.catch(() => this.reject(new Error('MCP reply delivery failed.')));
    await operation;
    if ('id' in message) {
      this.resolve();
    }
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.onclose?.();
    await this.tail;
    this.resolve();
  }
}

/** Dispatches isolated SDK exchanges independently of application tool definitions. */
export class McpEngine {
  private readonly active = new Set<Server>();
  private closed = false;

  constructor(
    private readonly config: Pick<Config, 'maxConcurrentRequests'>,
    private readonly createServer: McpServerFactory,
    private readonly onError: (error: Error) => void = () => {},
  ) {}

  /** Dispatches one exchange; abort closes only that call and emission awaits AMQP delivery. */
  async dispatch(
    message: JSONRPCMessage,
    signal: AbortSignal,
    emit: (message: JSONRPCMessage) => Promise<void>,
  ): Promise<void> {
    if (this.closed) {
      throw new Error('MCP engine is closed.');
    }
    if (signal.aborted) {
      return;
    }
    if (this.active.size >= this.config.maxConcurrentRequests) {
      throw new Error('MCP concurrency limit reached.');
    }
    const server = this.createServer();
    const transport = new RequestScopedTransport(emit);
    server.onerror = () => this.onError(new Error('MCP rejected a request or a handler failed.'));
    this.active.add(server);
    const cancelled = () => {
      void server.close().catch(() => this.onError(new Error('MCP call cleanup failed.')));
    };
    try {
      await server.connect(transport);
      signal.addEventListener('abort', cancelled, { once: true });
      if (signal.aborted) {
        await server.close();
        return;
      }
      transport.onmessage?.(message, {
        classification: { era: 'modern', revision: MCP_PROTOCOL_VERSION },
      });
      if (!('id' in message)) {
        return;
      }
      await transport.completed;
    } finally {
      signal.removeEventListener('abort', cancelled);
      await server.close();
      this.active.delete(server);
    }
  }

  /** Closes all in-flight SDK exchanges, including open subscriptions. */
  async close(): Promise<void> {
    this.closed = true;
    await Promise.all([...this.active].map((server) => server.close()));
  }
}
