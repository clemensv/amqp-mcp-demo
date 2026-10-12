// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { JSONRPCMessageSchema } from '@modelcontextprotocol/core';
import type { JSONRPCMessage, JSONValue, RequestId } from '@modelcontextprotocol/server';
import { randomUUID } from 'node:crypto';
import type { EventContext, Receiver, Session } from 'rhea-promise';
import { SessionEvents } from 'rhea-promise';
import { AmqpConnection } from './auth.js';
import { authorizeReplyAddress, namedQueueTerminus, type Config } from './config.js';
import { McpEngine, type McpServerFactory } from './sdk.js';
import { OrderedSender } from './transport.js';
import {
  decodeJsonBody,
  encodeRpc,
  isRequest,
  readMessageId,
  type ReplyRoute,
  type RpcEnvelope,
  type RpcMessage,
} from './wire.js';

interface ActiveCall {
  controller: AbortController;
  timedOut: boolean;
}

interface ReplyClient {
  address: string;
  sender: Promise<OrderedSender>;
  users: number;
  timer: NodeJS.Timeout | undefined;
  retired: boolean;
  closing: boolean;
}

function rpcError(id: RequestId | null, code: number, message: string): RpcMessage {
  return id === null
    ? { jsonrpc: '2.0', id: null, error: { code, message } }
    : { jsonrpc: '2.0', id, error: { code, message } };
}

/**
 * Consumes bounded AMQP envelopes and dispatches application-owned SDK servers with scoped replies.
 * @remarks Manual AMQP settlement does not provide exactly-once tool execution. A connection loss
 * after execution but before settlement can cause broker redelivery; non-idempotent tools need
 * an application-specific deduplication strategy before being used with this listener.
 * @see https://github.com/oasis-tcs/amqp-specs/blob/master/mcp/mcp-amqp-binding-spec.md
 */
export class AmqpServer {
  onerror: ((error: Error) => void) | undefined;
  onclose: (() => void) | undefined;
  private owned: AmqpConnection | undefined;
  private session: Session | undefined;
  private receiver: Receiver | undefined;
  private readonly engine: McpEngine;
  private readonly calls = new Map<RequestId, Set<ActiveCall>>();
  private readonly jobs = new Set<Promise<void>>();
  private readonly replyClients = new Map<string, ReplyClient>();
  private readonly replyClosures = new Set<Promise<void>>();
  private activeCount = 0;
  private started = false;
  private closed = false;
  private closing: Promise<void> | undefined;

  constructor(
    private readonly config: Config,
    createServer: McpServerFactory,
  ) {
    this.engine = new McpEngine(config, createServer, (error) => this.onerror?.(error));
  }

  /** Opens the authenticated listener once with manual settlement and bounded receiver credit. */
  async start(): Promise<void> {
    if (this.started || this.closed) {
      throw new Error('AMQP server can only be started once.');
    }
    this.started = true;
    try {
      this.owned = await AmqpConnection.open(this.config, (error) => this.fail(error));
      this.session = await this.owned.connection.createSession();
      this.session.setMaxListeners(Math.max(10, this.config.maxConcurrentRequests * 2 + 3));
      this.receiver = await this.session.createReceiver({
        name: `mcp-request-listener-${randomUUID()}`,
        source: namedQueueTerminus(this.config, this.config.node),
        credit_window: 0,
        autoaccept: false,
        onMessage: (context) => {
          const job = this.process(context)
            .catch(() => {
              context.delivery?.reject({
                condition: 'amqp:internal-error',
                description: 'MCP request processing or reply delivery failed.',
              });
              this.onerror?.(new Error('MCP request processing or reply delivery failed.'));
            })
            .finally(() => {
              this.jobs.delete(job);
              if (!this.closed) {
                this.receiver?.addCredit(1);
              }
            });
          this.jobs.add(job);
        },
        onError: () => this.fail(new Error('AMQP request listener failed.')),
        onClose: () => this.fail(new Error('AMQP request listener closed.')),
        onSessionError: () => this.fail(new Error('AMQP responder session failed.')),
        onSessionClose: () => this.fail(new Error('AMQP responder session closed.')),
      });
      if (this.closed) {
        throw new Error('AMQP server closed during startup.');
      }
      this.receiver.addCredit(this.config.maxConcurrentRequests + 1);
    } catch (error) {
      await this.close();
      throw error;
    }
  }

  private async process(context: EventContext): Promise<void> {
    const encoded = context.message;
    if (!encoded || this.closed) {
      throw new Error('AMQP request listener is closed.');
    }
    const messageId = readMessageId(encoded);
    if (messageId === undefined) {
      throw new Error('JSON-RPC AMQP messages require message-id.');
    }
    let route: ReplyRoute | undefined;
    if (encoded.reply_to !== undefined) {
      if (typeof encoded.reply_to !== 'string') {
        throw new Error('Invalid reply address.');
      }
      authorizeReplyAddress(encoded.reply_to, this.config);
      route = { address: encoded.reply_to, messageId };
    }
    let body: JSONValue;
    try {
      body = decodeJsonBody(encoded, this.config.maxMessageBytes);
    } catch (error) {
      if (!route) {
        throw error;
      }
      await this.reply(
        route,
        rpcError(
          null,
          error instanceof SyntaxError ? -32700 : -32600,
          error instanceof SyntaxError ? 'Parse error' : 'Invalid Request',
        ),
      );
      context.delivery?.accept();
      return;
    }
    const batched = Array.isArray(body);
    // Validate members separately so invalid batch entries receive errors without hiding valid calls.
    const items: JSONValue[] = Array.isArray(body) ? body : [body];
    if (items.length === 0 || items.length > this.config.maxConcurrentRequests) {
      if (!route) {
        throw new Error('Invalid batch without a reply address.');
      }
      await this.reply(route, rpcError(null, -32600, 'Invalid Request'));
      context.delivery?.accept();
      return;
    }
    const parsed = items.map((item) => JSONRPCMessageSchema.safeParse(item));
    const requests = parsed.flatMap((item) =>
      item.success && isRequest(item.data) ? [item.data] : [],
    );
    const invalid = parsed.some((item) => !item.success || !('method' in item.data));
    if (requests.length && !route) {
      throw new Error('JSON-RPC requests require reply-to.');
    }
    if (!requests.length && !invalid && route) {
      throw new Error('Requester notifications must not contain reply-to.');
    }
    if (
      encoded.subject !== undefined &&
      (batched ||
        parsed.some(
          (item) =>
            !item.success || !('method' in item.data) || item.data.method !== encoded.subject,
        ))
    ) {
      if (!route) {
        throw new Error('AMQP subject does not match the request body.');
      }
      await this.reply(route, rpcError(null, -32600, 'Invalid Request'));
      context.delivery?.accept();
      return;
    }
    if (!requests.length && !invalid) {
      const mqNoCorrelation =
        this.config.broker === 'ibmmq' &&
        Buffer.isBuffer(encoded.correlation_id) &&
        encoded.correlation_id.length === 24 &&
        encoded.correlation_id.every((byte) => byte === 0);
      if (encoded.correlation_id !== undefined && !mqNoCorrelation) {
        throw new Error('Requester notifications must not contain correlation-id.');
      }
      for (const item of parsed) {
        if (item.success) {
          this.notification(item.data);
        }
      }
      context.delivery?.accept();
      return;
    }
    if (!route || !this.session) {
      throw new Error('Invalid request cannot be answered.');
    }
    if (this.activeCount + requests.length > this.config.maxConcurrentRequests) {
      const errors = requests.map((request) =>
        rpcError('id' in request ? request.id : null, -32000, 'Server concurrency limit reached.'),
      );
      await this.reply(route, batched ? errors : errors[0]!);
      context.delivery?.accept();
      return;
    }
    // Reserve capacity before asynchronous reply-link attachment can admit another envelope.
    this.activeCount += requests.length;
    let client: ReplyClient | undefined;
    const controllers: AbortController[] = [];
    let replyFailed = false;
    try {
      client = await this.acquireReplyClient(route);
      const replySender = await client.sender;
      const results = await Promise.all(
        parsed.map(async (item): Promise<RpcMessage | undefined> => {
          if (!item.success || !('method' in item.data)) {
            return rpcError(null, -32600, 'Invalid Request');
          }
          if (!isRequest(item.data)) {
            this.notification(item.data);
            return undefined;
          }
          const request = item.data;
          const id = request.id;
          const active: ActiveCall = { controller: new AbortController(), timedOut: false };
          controllers.push(active.controller);
          const sameId = this.calls.get(id) ?? new Set<ActiveCall>();
          sameId.add(active);
          this.calls.set(id, sameId);
          const listen = request.method === 'subscriptions/listen';
          const timer = setTimeout(
            () => {
              active.timedOut = true;
              active.controller.abort();
            },
            listen ? this.config.subscriptionTimeoutMs : this.config.requestTimeoutMs,
          );
          let terminal: JSONRPCMessage | undefined;
          try {
            try {
              await this.engine.dispatch(request, active.controller.signal, async (message) => {
                if ('id' in message) {
                  terminal = message;
                } else {
                  try {
                    await replySender.send(encodeRpc(message, { route }));
                  } catch (error) {
                    replyFailed = true;
                    for (const controller of controllers) {
                      controller.abort();
                    }
                    throw error;
                  }
                }
              });
            } catch (error) {
              if (!active.controller.signal.aborted || replyFailed) {
                throw error;
              }
            }
            if (terminal) {
              return terminal;
            }
            if (listen && active.timedOut) {
              return {
                jsonrpc: '2.0',
                id,
                result: {
                  resultType: 'complete',
                  _meta: { 'io.modelcontextprotocol/subscriptionId': id },
                },
              };
            }
            return rpcError(
              id,
              -32000,
              active.timedOut ? 'Server request deadline exceeded.' : 'Request cancelled.',
            );
          } finally {
            clearTimeout(timer);
            sameId.delete(active);
            if (!sameId.size) {
              this.calls.delete(id);
            }
          }
        }),
      );
      const responses = results.filter((result): result is RpcMessage => result !== undefined);
      if (!this.closed && responses.length) {
        await replySender.send(encodeRpc(batched ? responses : responses[0]!, { route }));
      }
      // Settle only after reply acceptance; failed delivery must not acknowledge request success.
      context.delivery?.accept();
    } catch (error) {
      if (client) {
        this.retireReplyClient(client);
      }
      throw error;
    } finally {
      for (const controller of controllers) {
        controller.abort();
      }
      this.activeCount -= requests.length;
      if (client) {
        this.releaseReplyClient(client);
      }
    }
  }

  private notification(message: JSONRPCMessage): void {
    if (!('method' in message) || message.method !== 'notifications/cancelled') {
      return;
    }
    const id = message.params?.requestId;
    if (typeof id !== 'number' && typeof id !== 'string') {
      return;
    }
    const candidates = this.calls.get(id);
    // Body IDs are local to a client, not globally unique on a shared queue. Never cancel two clients.
    if (candidates?.size === 1) {
      for (const active of candidates) {
        active.controller.abort();
      }
    } else if (candidates && candidates.size > 1) {
      this.onerror?.(new Error('Ambiguous shared-queue cancellation was ignored.'));
    }
  }

  private async reply(route: ReplyRoute, envelope: RpcEnvelope): Promise<void> {
    const client = await this.acquireReplyClient(route);
    try {
      const sender = await client.sender;
      await sender.send(encodeRpc(envelope, { route }));
    } catch (error) {
      this.retireReplyClient(client);
      throw error;
    } finally {
      this.releaseReplyClient(client);
    }
  }

  private async acquireReplyClient(route: ReplyRoute): Promise<ReplyClient> {
    if (this.closed) {
      throw new Error('AMQP server is closed.');
    }
    let client = this.replyClients.get(route.address);
    if (!client) {
      const created: ReplyClient = {
        address: route.address,
        sender: Promise.resolve().then(() =>
          this.createReplySender(route, () => this.retireReplyClient(created)),
        ),
        users: 0,
        timer: undefined,
        retired: false,
        closing: false,
      };
      this.replyClients.set(route.address, created);
      client = created;
    }
    clearTimeout(client.timer);
    client.timer = undefined;
    client.users++;
    try {
      await client.sender;
      if (client.retired || this.closed) {
        throw new Error('AMQP reply client closed during attachment.');
      }
      return client;
    } catch (error) {
      this.retireReplyClient(client);
      this.releaseReplyClient(client);
      throw error;
    }
  }

  private releaseReplyClient(client: ReplyClient): void {
    client.users--;
    if (client.users) {
      return;
    }
    if (client.retired || this.closed) {
      this.retireReplyClient(client);
      return;
    }
    client.timer = setTimeout(
      () => this.retireReplyClient(client),
      this.config.replyClientIdleTimeoutMs,
    );
    client.timer.unref();
    for (const cached of this.replyClients.values()) {
      if (this.replyClients.size <= this.config.maxConcurrentRequests) {
        break;
      }
      if (!cached.users) {
        this.retireReplyClient(cached);
      }
    }
  }

  private retireReplyClient(client: ReplyClient): void {
    client.retired = true;
    clearTimeout(client.timer);
    client.timer = undefined;
    if (this.replyClients.get(client.address) === client) {
      this.replyClients.delete(client.address);
    }
    if (client.users || client.closing) {
      return;
    }
    client.closing = true;
    const job = client.sender
      .catch(() => undefined)
      .then((sender) => sender?.close())
      .catch(() => this.onerror?.(new Error('AMQP reply client cleanup failed.')))
      .finally(() => this.replyClosures.delete(job));
    this.replyClosures.add(job);
  }

  private async createReplySender(
    route: ReplyRoute,
    onInvalid: () => void,
  ): Promise<OrderedSender> {
    const session = this.session;
    if (!session) {
      throw new Error('AMQP responder session is not open.');
    }
    const failed = () => this.fail(new Error('AMQP reply session failed.'));
    const cleanup = () => session.removeListener(SessionEvents.sessionError, failed);
    try {
      const sender = await session.createAwaitableSender({
        name: `mcp-reply-${randomUUID()}`,
        target: namedQueueTerminus(this.config, route.address),
        snd_settle_mode: 0,
        onError: onInvalid,
        onClose: onInvalid,
        onSessionError: failed,
      });
      return new OrderedSender(sender, this.config.operationTimeoutMs, cleanup);
    } catch (error) {
      cleanup();
      throw error;
    }
  }

  /** Waits for a snapshot of tracked envelopes; later arrivals are outside this drain operation. */
  async drain(): Promise<void> {
    await Promise.all([...this.jobs]);
  }

  private fail(error: Error): void {
    if (this.closed) {
      return;
    }
    this.onerror?.(error);
    void this.close();
  }

  /** Idempotently stops reception, aborts calls, drains jobs, and closes links and authentication. */
  close(): Promise<void> {
    if (this.closing) {
      return this.closing;
    }
    this.closed = true;
    for (const candidates of this.calls.values()) {
      for (const active of candidates) {
        active.controller.abort();
      }
    }
    this.closing = (async () => {
      const operations = [
        () => this.receiver?.close({ closeSession: false }),
        () => this.engine.close(),
        () => this.drain(),
        async () => {
          for (const client of this.replyClients.values()) {
            this.retireReplyClient(client);
          }
          await Promise.all([...this.replyClosures]);
        },
        () => this.session?.close(),
        () => this.owned?.close(),
      ];
      for (const operation of operations) {
        try {
          await operation();
        } catch {
          this.onerror?.(new Error('AMQP responder cleanup failed.'));
        }
      }
      this.onclose?.();
    })();
    return this.closing;
  }
}
