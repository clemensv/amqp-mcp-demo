// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { randomUUID } from 'node:crypto';
import type { JSONRPCMessage, RequestId, Transport } from '@modelcontextprotocol/server';
import {
  SenderEvents,
  type AwaitableSender,
  type EventContext,
  type Message,
  type Receiver,
  type Sender,
  type Session,
} from 'rhea-promise';
import { AmqpConnection } from './auth.js';
import { namedQueueTerminus, type Config } from './config.js';
import {
  decodeRpc,
  encodeRpc,
  hasRequests,
  isRequest,
  messageIdKey,
  readMessageId,
  temporaryQueueOptions,
  type RpcEnvelope,
  type RpcMessage,
} from './wire.js';

function errorResponse(id: RequestId | null, code: number, message: string): RpcMessage {
  return id === null
    ? { jsonrpc: '2.0', id: null, error: { code, message } }
    : { jsonrpc: '2.0', id, error: { code, message } };
}

/** Serializes sends through credit and remote disposition deadlines on one shared-session link. */
export class OrderedSender {
  private tail: Promise<void> = Promise.resolve();
  private closed = false;

  constructor(
    private readonly sender: AwaitableSender,
    private readonly timeoutMs: number,
    private readonly onClosed?: () => void,
  ) {}

  /** Rejects on credit timeout, rejected delivery, or a closed link; never retries execution. */
  send(message: Message): Promise<void> {
    if (this.closed) {
      return Promise.reject(new Error('AMQP sender is closed.'));
    }
    const operation = this.tail.then(async () => {
      await this.waitForCredit();
      await this.sender.send(message, { timeoutInSeconds: this.timeoutMs / 1000 });
    });
    // Keep the queue usable after failure; the caller still observes the original rejection.
    this.tail = operation.catch(() => {});
    return operation;
  }

  private async waitForCredit(): Promise<void> {
    if (this.sender.sendable()) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer);
        this.sender.removeListener(SenderEvents.sendable, ready);
        this.sender.removeListener(SenderEvents.senderClose, failed);
        this.sender.removeListener(SenderEvents.senderError, failed);
      };
      const ready = () => {
        if (this.sender.sendable()) {
          cleanup();
          resolve();
        }
      };
      const failed = () => {
        cleanup();
        reject(new Error('AMQP sender closed while waiting for credit.'));
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error('AMQP send credit timed out.'));
      }, this.timeoutMs);
      this.sender.on(SenderEvents.sendable, ready);
      this.sender.on(SenderEvents.senderClose, failed);
      this.sender.on(SenderEvents.senderError, failed);
      ready();
    });
  }

  /** Waits for queued sends, closes only this link, and always releases registered callbacks. */
  async close(): Promise<void> {
    this.closed = true;
    try {
      await this.tail;
      await this.sender.close({ closeSession: false });
    } finally {
      this.onClosed?.();
    }
  }
}

interface Pending {
  ids: (RequestId | null)[];
  timer: ReturnType<typeof setTimeout>;
  batched: boolean;
}

/**
 * Bridges MCP requests to AMQP using a temporary or preconfigured reply queue.
 * @remarks Independent requests may complete out of order. Connection loss is terminal.
 * The temporary target sender owns an automatic queue; preconfigured queues remain externally owned.
 * AMQP correlation identifies an envelope, while JSON-RPC IDs identify calls inside it.
 * @see https://github.com/oasis-tcs/amqp-specs/blob/master/mcp/mcp-amqp-binding-spec.md
 */
export class AmqpClientTransport implements Transport {
  onclose: Transport['onclose'];
  onerror: Transport['onerror'];
  onmessage: Transport['onmessage'];
  /** Whole-envelope callback; when set, takes precedence over SDK single-message callbacks. */
  onEnvelope: ((message: RpcEnvelope) => void) | undefined;
  private owned: AmqpConnection | undefined;
  private session: Session | undefined;
  private owner: Sender | undefined;
  private receiver: Receiver | undefined;
  private sender: OrderedSender | undefined;
  private replyAddress: string | undefined;
  private closed = false;
  private started = false;
  private closing: Promise<void> | undefined;
  private readonly pending = new Map<string, Pending>();
  private readonly rpcIds = new Map<RequestId | null, string>();

  constructor(private readonly config: Config) {}

  /** Assigned or configured reply address, available after startup; no credentials are included. */
  get replyTo(): string | undefined {
    return this.replyAddress;
  }

  /** Opens authenticated links once; releases partially opened resources if attachment fails. */
  async start(): Promise<void> {
    if (this.started || this.closed) {
      throw new Error('AMQP transport can only be started once.');
    }
    this.started = true;
    try {
      this.owned = await AmqpConnection.open(this.config, (error) => this.fail(error));
      this.session = await this.owned.connection.createSession();
      if (this.config.replyAddress !== undefined) {
        this.replyAddress = this.config.replyAddress;
      } else {
        // The dynamic-target sender owns the broker-created reply queue until it closes.
        this.owner = await this.owned.createOwnerSender(
          {
            ...temporaryQueueOptions(),
            onError: () => this.fail(new Error('Temporary reply queue owner failed.')),
            onClose: () => this.fail(new Error('Temporary reply queue ownership was lost.')),
            onSessionError: () => this.fail(new Error('AMQP session failed.')),
            onSessionClose: () => this.fail(new Error('AMQP session closed.')),
          },
          this.config.operationTimeoutMs,
        );
        this.replyAddress =
          this.config.broker === 'ibmmq'
            ? this.owner.target.address?.trimEnd()
            : this.owner.target.address;
        if (!this.replyAddress) {
          throw new Error('The broker did not assign a temporary reply address.');
        }
      }
      this.receiver = await this.session.createReceiver({
        name: `mcp-replies-${randomUUID()}`,
        source: namedQueueTerminus(this.config, this.replyAddress),
        credit_window: 0,
        autoaccept: false,
        onMessage: (context) => this.receive(context),
        onError: () => this.fail(new Error('AMQP reply receiver failed.')),
        onClose: () => this.fail(new Error('AMQP reply receiver closed.')),
        onSessionError: () => this.fail(new Error('AMQP session failed.')),
        onSessionClose: () => this.fail(new Error('AMQP session closed.')),
      });
      this.sender = new OrderedSender(
        await this.session.createAwaitableSender({
          name: `mcp-requests-${randomUUID()}`,
          target: namedQueueTerminus(this.config, this.config.node),
          snd_settle_mode: 0,
        }),
        this.config.operationTimeoutMs,
      );
      if (this.closed) {
        throw new Error('AMQP transport closed during startup.');
      }
      // Link credit is the allowance for incoming deliveries; receive() replenishes it as replies finish.
      this.receiver.addCredit(this.config.maxConcurrentRequests);
    } catch (error) {
      await this.close();
      throw error;
    }
  }

  /** Sends one SDK message; completion acknowledges AMQP delivery, not the JSON-RPC result. */
  async send(message: JSONRPCMessage): Promise<void> {
    await this.sendEnvelope(message);
  }

  /**
   * Sends a request, notification, or intact batch and reserves its body IDs until completion.
   * @throws Error for duplicate IDs, excess concurrency or size, or unavailable transport.
   * @remarks Response deadlines emit errors and attempt cancellation without replaying requests.
   */
  async sendEnvelope(envelope: RpcEnvelope): Promise<void> {
    if (this.closed || !this.sender || !this.replyAddress) {
      throw new Error('AMQP transport is not open.');
    }
    const entries = Array.isArray(envelope) ? envelope : [envelope];
    if (entries.some((entry) => !('method' in entry))) {
      throw new Error('MCP 2026-07-28 clients send only requests and notifications.');
    }
    const ids: (RequestId | null)[] = entries.filter(isRequest).map((entry) => entry.id);
    if (Array.isArray(envelope) && envelope.length === 0) {
      ids.push(null);
    }
    if (new Set(ids).size !== ids.length || ids.some((id) => this.rpcIds.has(id))) {
      throw new Error('A JSON-RPC request ID is already in flight.');
    }
    if (this.rpcIds.size + ids.length > this.config.maxConcurrentRequests) {
      throw new Error('Too many in-flight JSON-RPC requests.');
    }
    const wire = encodeRpc(envelope, { replyTo: this.replyAddress });
    wire.to = this.config.node;
    if (Buffer.byteLength(JSON.stringify(envelope)) > this.config.maxMessageBytes) {
      throw new Error('JSON-RPC message exceeds the size limit.');
    }
    const key = messageIdKey(wire.message_id!);
    // Register correlation before sending: a fast responder can reply before send acceptance arrives.
    if (hasRequests(envelope) || ids.length) {
      const timer = setTimeout(() => {
        void this.expire(key);
      }, this.config.requestTimeoutMs);
      this.pending.set(key, {
        ids,
        timer,
        batched: Array.isArray(envelope) && envelope.length > 0,
      });
      for (const id of ids) {
        this.rpcIds.set(id, key);
      }
    }
    try {
      await this.sender.send(wire);
    } catch (error) {
      this.removePending(key);
      throw error;
    }
  }

  private receive(context: EventContext): void {
    try {
      if (!context.message || readMessageId(context.message) === undefined) {
        throw new Error('AMQP reply is missing message-id.');
      }
      const correlation = readMessageId(context.message, 'correlation_id');
      if (correlation === undefined) {
        throw new Error('AMQP reply is missing correlation-id.');
      }
      const key = messageIdKey(correlation);
      const pending = this.pending.get(key);
      // Deadlines remove reservations. Accept and discard late replies rather than redelivering them.
      if (!pending) {
        context.delivery?.accept();
        return;
      }
      const envelope = decodeRpc(context.message, this.config.maxMessageBytes);
      const entries = Array.isArray(envelope) ? envelope : [envelope];
      if (entries.some(isRequest) || context.message.reply_to !== undefined) {
        throw new Error('Unexpected server-initiated MCP request.');
      }
      const responses = entries.filter((entry) => 'id' in entry);
      if (responses.length) {
        const receivedIds = responses.map((entry) => ('id' in entry ? (entry.id ?? null) : null));
        if (
          responses.length !== entries.length ||
          receivedIds.length !== pending.ids.length ||
          new Set(receivedIds).size !== receivedIds.length ||
          receivedIds.some((id) => !pending.ids.includes(id))
        ) {
          throw new Error('Reply JSON-RPC IDs do not match the correlated request.');
        }
      }
      for (const entry of entries) {
        if (
          context.message.subject !== undefined &&
          (!('method' in entry) ||
            context.message.subject !== entry.method ||
            Array.isArray(envelope))
        ) {
          throw new Error('AMQP subject does not match the reply body.');
        }
      }
      // Malformed responses must not consume the reservation needed by a later valid reply.
      if (responses.length) {
        this.removePending(key);
      }
      this.emit(envelope);
      context.delivery?.accept();
    } catch {
      context.delivery?.reject({
        condition: 'amqp:decode-error',
        description: 'Invalid JSON-RPC reply.',
      });
      this.onerror?.(new Error('Rejected an invalid or mismatched AMQP reply.'));
    } finally {
      if (!this.closed) {
        this.receiver?.addCredit(1);
      }
    }
  }

  private removePending(key: string): Pending | undefined {
    const pending = this.pending.get(key);
    if (pending) {
      clearTimeout(pending.timer);
      this.pending.delete(key);
      for (const id of pending.ids) {
        this.rpcIds.delete(id);
      }
    }
    return pending;
  }

  private expire(key: string): void {
    const pending = this.removePending(key);
    if (!pending || this.closed) {
      return;
    }
    for (const id of pending.ids) {
      if (id !== null) {
        // Cancellation is advisory. It cannot undo a completed tool operation or guarantee delivery.
        void this.sender
          ?.send({
            ...encodeRpc({
              jsonrpc: '2.0',
              method: 'notifications/cancelled',
              params: { requestId: id, reason: 'AMQP response deadline exceeded.' },
            }),
            to: this.config.node,
          })
          .catch(() =>
            this.onerror?.(new Error('Could not send best-effort request cancellation.')),
          );
      }
    }
    const responses = pending.ids.map((id) =>
      errorResponse(id, -32000, 'AMQP response deadline exceeded.'),
    );
    this.emit(pending.batched ? responses : responses[0]!);
  }

  private emit(envelope: RpcEnvelope): void {
    if (this.onEnvelope) {
      this.onEnvelope(envelope);
    } else {
      for (const message of Array.isArray(envelope) ? envelope : [envelope]) {
        if ('id' in message && message.id === null) {
          this.onerror?.(new Error('Received a JSON-RPC error without a body request ID.'));
        } else {
          this.onmessage?.(message);
        }
      }
    }
  }

  private fail(error: Error): void {
    if (this.closed) {
      return;
    }
    this.onerror?.(error);
    void this.close();
  }

  /** Resolves pending calls with errors and closes links; only automatic queues have an owner link. */
  close(): Promise<void> {
    if (this.closing) {
      return this.closing;
    }
    this.closed = true;
    for (const key of [...this.pending.keys()]) {
      const pending = this.removePending(key)!;
      const errors = pending.ids.map((id) =>
        errorResponse(id, -32000, 'AMQP connection closed before a reply arrived.'),
      );
      this.emit(pending.batched ? errors : errors[0]!);
    }
    this.closing = (async () => {
      const operations = [
        // Stop consuming before detaching the sender that owns the temporary queue.
        () => this.receiver?.close({ closeSession: false }),
        () => this.sender?.close(),
        () => this.owner?.close({ closeSession: true }),
        () => this.session?.close(),
        () => this.owned?.close(),
      ];
      for (const operation of operations) {
        try {
          await operation();
        } catch {
          this.onerror?.(new Error('AMQP resource cleanup failed.'));
        }
      }
      this.onclose?.();
    })();
    return this.closing;
  }
}
