// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import type { Readable, Writable } from 'node:stream';
import { JSONRPCMessageSchema } from '@modelcontextprotocol/core';
import type { JSONValue } from '@modelcontextprotocol/server';
import type { RpcEnvelope } from './wire.js';
import { MCP_PROTOCOL_VERSION } from './sdk.js';
import * as z from 'zod/v4';

const initializationSchema = z.object({
  protocolVersion: z.enum(['2024-11-05', '2025-03-26', '2025-06-18', '2025-11-25']),
  clientInfo: z.object({ name: z.string(), version: z.string() }),
  capabilities: z.record(z.string(), z.json()),
});

/** Adapts a legacy stdio handshake while preserving stateless AMQP discovery and request IDs. */
export class StdioProtocolAdapter {
  private initialization: { id: string | number; version: string } | undefined;
  private metadata: Record<string, JSONValue> | undefined;
  private initialized = false;

  constructor(
    private readonly forward: (envelope: RpcEnvelope) => Promise<void>,
    private readonly emit: (envelope: RpcEnvelope) => Promise<void>,
  ) {}

  /** Maps legacy initialization and stamps subsequent requests with binding metadata. */
  async receive(envelope: RpcEnvelope): Promise<void> {
    if (Array.isArray(envelope)) {
      if (this.metadata) {
        const mapped = envelope
          .filter(
            (message) => !('method' in message && message.method === 'notifications/initialized'),
          )
          .map((message) =>
            'method' in message
              ? {
                  ...message,
                  params: {
                    ...message.params,
                    _meta: { ...message.params?._meta, ...this.metadata },
                  },
                }
              : message,
          );
        if (mapped.length) {
          await this.forward(mapped);
        }
      } else {
        await this.forward(envelope);
      }
      return;
    }
    if (!('method' in envelope)) {
      await this.forward(envelope);
      return;
    }
    if (envelope.method === 'initialize' && 'id' in envelope) {
      const parsed = initializationSchema.safeParse(envelope.params);
      if (!parsed.success || this.metadata) {
        await this.emit({
          jsonrpc: '2.0',
          id: envelope.id,
          error: { code: -32602, message: 'Invalid or repeated MCP initialization.' },
        });
        return;
      }
      this.initialization = { id: envelope.id, version: parsed.data.protocolVersion };
      this.metadata = {
        'io.modelcontextprotocol/protocolVersion': MCP_PROTOCOL_VERSION,
        'io.modelcontextprotocol/clientInfo': parsed.data.clientInfo,
        'io.modelcontextprotocol/clientCapabilities': {},
      };
      await this.forward({
        jsonrpc: '2.0',
        id: envelope.id,
        method: 'server/discover',
        params: { _meta: this.metadata },
      });
      return;
    }
    if (this.metadata) {
      if (envelope.method === 'notifications/initialized') {
        return;
      }
      if (!this.initialized && 'id' in envelope) {
        await this.emit({
          jsonrpc: '2.0',
          id: envelope.id,
          error: { code: -32000, message: 'MCP discovery has not completed.' },
        });
        return;
      }
      const params = envelope.params ?? {};
      await this.forward({
        ...envelope,
        params: { ...params, _meta: { ...params._meta, ...this.metadata } },
      });
      return;
    }
    await this.forward(envelope);
  }

  /** Translates discovery only; errors and ordinary responses retain their original IDs. */
  async send(envelope: RpcEnvelope): Promise<void> {
    if (Array.isArray(envelope)) {
      await this.emit(envelope);
      return;
    }
    if (this.initialization && 'id' in envelope && envelope.id === this.initialization.id) {
      const pending = this.initialization;
      this.initialization = undefined;
      if ('result' in envelope) {
        const discovery = z
          .object({
            supportedVersions: z.array(z.string()),
            _meta: z.object({
              'io.modelcontextprotocol/serverInfo': z
                .object({ name: z.string(), version: z.string() })
                .passthrough(),
            }),
            capabilities: z.record(z.string(), z.json()),
            instructions: z.string().optional(),
          })
          .safeParse(envelope.result);
        if (
          !discovery.success ||
          !discovery.data.supportedVersions.includes(MCP_PROTOCOL_VERSION)
        ) {
          await this.emit({
            jsonrpc: '2.0',
            id: pending.id,
            error: { code: -32603, message: 'Incompatible MCP discovery response.' },
          });
          return;
        }
        this.initialized = true;
        await this.emit({
          jsonrpc: '2.0',
          id: pending.id,
          result: {
            protocolVersion: pending.version,
            serverInfo: discovery.data._meta['io.modelcontextprotocol/serverInfo'],
            capabilities: discovery.data.capabilities,
            ...(discovery.data.instructions === undefined
              ? {}
              : { instructions: discovery.data.instructions }),
          },
        });
        return;
      }
    }
    await this.emit(envelope);
  }
}

/** Bounded UTF-8 newline JSON-RPC relay; input forwarding and whole-line output are serialized separately. */
export class StdioRelay {
  private buffer = Buffer.alloc(0);
  private inbound: Promise<void> = Promise.resolve();
  private outbound: Promise<void> = Promise.resolve();
  private closed = false;

  constructor(
    private readonly input: Readable,
    private readonly output: Writable,
    private readonly maxMessageBytes: number,
    private readonly forward: (envelope: RpcEnvelope) => Promise<void>,
    private readonly onFailure: (error: Error) => void,
    private readonly onEnd: () => void,
  ) {}

  /** Attaches input/output handlers; call once after the AMQP transport is ready. */
  start(): void {
    this.input.on('data', this.receive);
    this.input.once('end', this.ended);
    this.input.on('error', this.failed);
    this.output.on('error', this.failed);
  }

  private readonly failed = () => {
    this.onFailure(new Error('MCP stdio stream failed.'));
  };
  private readonly ended = () => {
    void this.inbound.then(() => {
      if (this.buffer.length) {
        this.onFailure(new Error('MCP stdin ended with an incomplete message.'));
      } else {
        this.onEnd();
      }
    });
  };

  private readonly receive = (chunk: Buffer | string) => {
    if (this.closed) {
      return;
    }
    // Pause until forwarding settles so broker backpressure reaches the producer on stdin.
    this.input.pause();
    this.buffer = Buffer.concat([this.buffer, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);
    try {
      let newline = this.buffer.indexOf(10);
      while (newline >= 0) {
        if (newline > this.maxMessageBytes) {
          throw new Error('MCP stdin message exceeds the size limit.');
        }
        const line = this.buffer.subarray(0, newline);
        this.buffer = this.buffer.subarray(newline + 1);
        if (line.length) {
          const json: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line));
          const envelope = Array.isArray(json)
            ? json.map((entry: unknown) => JSONRPCMessageSchema.parse(entry))
            : JSONRPCMessageSchema.parse(json);
          this.inbound = this.inbound.then(() => this.forward(envelope));
        }
        newline = this.buffer.indexOf(10);
      }
      if (this.buffer.length > this.maxMessageBytes) {
        throw new Error('MCP stdin message exceeds the size limit.');
      }
      this.inbound = this.inbound.catch(() =>
        this.onFailure(new Error('Could not forward an MCP stdin message.')),
      );
      void this.inbound.then(() => {
        if (!this.closed) {
          this.input.resume();
        }
      });
    } catch {
      this.onFailure(new Error('MCP stdin contains invalid or oversized JSON-RPC.'));
    }
  };

  /** Writes one complete line atomically; rejects on a closed stream, excess size, or write failure. */
  send(envelope: RpcEnvelope): Promise<void> {
    if (this.closed) {
      return Promise.reject(new Error('MCP stdio relay is closed.'));
    }
    const line = `${JSON.stringify(envelope)}\n`;
    if (Buffer.byteLength(line) > this.maxMessageBytes + 1) {
      return Promise.reject(new Error('MCP stdout message exceeds the size limit.'));
    }
    const operation = this.outbound.then(
      () =>
        new Promise<void>((resolve, reject) => {
          this.output.write(line, 'utf8', (error) => {
            if (error) {
              reject(error);
            } else {
              resolve();
            }
          });
        }),
    );
    this.outbound = operation.catch(() => {});
    return operation;
  }

  /** Stops reading, drains accepted work and output, and leaves stream ownership to the caller. */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.input.pause();
    this.input.removeListener('data', this.receive);
    this.input.removeListener('end', this.ended);
    this.input.removeListener('error', this.failed);
    await this.inbound;
    await this.outbound;
    this.output.removeListener('error', this.failed);
  }
}
