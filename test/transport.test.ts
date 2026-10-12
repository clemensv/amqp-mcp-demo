// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { EventEmitter } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Client } from '@modelcontextprotocol/client';
import type { EventContext, ReceiverOptions, SenderOptions } from 'rhea-promise';
import { SenderEvents } from 'rhea-promise';
import { AmqpConnection } from '../src/auth.js';
import { parseConfig } from '../src/config.js';
import { AmqpClientTransport, OrderedSender } from '../src/transport.js';
import { AmqpServer } from '../src/responder.js';
import { MCP_PROTOCOL_VERSION, McpEngine, StatelessMcpServer } from '../src/sdk.js';
import { createToolServer } from '../example/src/tools.js';
import { decodeRpc, encodeRpc } from '../src/wire.js';

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

function fixture(overrides: object = {}) {
  const order: string[] = [];
  let onMessage: (context: EventContext) => void = () => {};
  let ownerOptions: SenderOptions = {};
  let receiverOptions: ReceiverOptions = {};
  const sender = Object.assign(new EventEmitter(), {
    sendable: vi.fn(() => true),
    send: vi.fn().mockResolvedValue({ settled: true }),
    close: vi.fn(async () => {
      order.push('request-sender');
    }),
  });
  const receiver = {
    addCredit: vi.fn(),
    close: vi.fn(async () => {
      order.push('receiver');
    }),
  };
  const owner = {
    target: { address: 'opaque-reply' },
    close: vi.fn(async () => {
      order.push('owner');
    }),
  };
  const session = Object.assign(new EventEmitter(), {
    createSender: vi.fn(async (options: SenderOptions) => {
      ownerOptions = options;
      return owner;
    }),
    createReceiver: vi.fn(async (options: ReceiverOptions) => {
      receiverOptions = options;
      onMessage = options.onMessage!;
      return receiver;
    }),
    createAwaitableSender: vi.fn().mockResolvedValue(sender),
    close: vi.fn(async () => {
      order.push('session');
    }),
  });
  const owned = {
    connection: { createSession: vi.fn().mockResolvedValue(session) },
    createOwnerSender: session.createSender,
    close: vi.fn(async () => {
      order.push('connection');
    }),
  };
  vi.spyOn(AmqpConnection, 'open').mockResolvedValue(owned as unknown as AmqpConnection);
  const config = parseConfig(
    JSON.stringify({
      endpoint: 'amqp://localhost',
      node: 'requests',
      requestTimeoutMs: 1000,
      ...overrides,
    }),
  );
  const transport = new AmqpClientTransport(config);
  const received = vi.fn();
  const errors = vi.fn();
  transport.onmessage = received;
  transport.onerror = errors;
  const deliver = (message: object) => {
    const delivery = { accept: vi.fn(), reject: vi.fn() };
    onMessage({ message, delivery } as unknown as EventContext);
    return delivery;
  };
  return {
    transport,
    config,
    sender,
    receiver,
    owner,
    session,
    owned,
    received,
    errors,
    deliver,
    order,
    ownerOptions: () => ownerOptions,
    receiverOptions: () => receiverOptions,
  };
}

describe('reply queue client transport', () => {
  it.each(['ibmmq', 'generic'])(
    'handles fixed-width assigned queue names for %s',
    async (broker) => {
      const test = fixture({ broker });
      test.owner.target.address = 'AMQP.REPLY                           ';
      await test.transport.start();
      const expected = broker === 'ibmmq' ? 'AMQP.REPLY' : test.owner.target.address;
      expect(test.transport.replyTo).toBe(expected);
      expect(test.receiverOptions().source).toMatchObject({ address: expected });
      await test.transport.send({ jsonrpc: '2.0', id: 1, method: 'tools/list' });
      expect(test.sender.send.mock.calls[0]![0].reply_to).toBe(expected);
      expect(test.owner.target.address).toBe('AMQP.REPLY                           ');
      await test.transport.close();
    },
  );

  it.each(['generic', 'ibmmq', 'servicebus'])(
    'uses a preconfigured reply queue for %s',
    async (broker) => {
      const replyAddress = broker === 'ibmmq' ? 'REPLIES   ' : 'client-replies';
      const test = fixture({
        broker,
        replyAddress,
        endpoint: 'amqps://broker.example',
        sasl: { mode: 'plain', username: 'policy', passwordEnv: 'KEY' },
      });
      await test.transport.start();
      expect(test.owned.createOwnerSender).not.toHaveBeenCalled();
      expect(test.transport.replyTo).toBe(replyAddress);
      expect(test.receiverOptions().source).toEqual(
        broker === 'ibmmq'
          ? { address: replyAddress, capabilities: ['queue'] }
          : { address: replyAddress },
      );
      expect(test.receiverOptions().source).not.toHaveProperty('dynamic');
      await test.transport.send({ jsonrpc: '2.0', id: 1, method: 'tools/list' });
      const request = test.sender.send.mock.calls[0]![0];
      expect(request.reply_to).toBe(replyAddress);
      const reply = encodeRpc(
        { jsonrpc: '2.0', id: 1, result: {} },
        { route: { address: replyAddress, messageId: request.message_id } },
      );
      expect(test.deliver(reply).accept).toHaveBeenCalledOnce();
      expect(test.received).toHaveBeenCalledWith({ jsonrpc: '2.0', id: 1, result: {} });
      await test.transport.close();
      await test.transport.close();
      expect(test.owner.close).not.toHaveBeenCalled();
      expect(test.order).toEqual(['receiver', 'request-sender', 'session', 'connection']);
    },
  );

  it('cleans up when attaching a preconfigured reply queue fails', async () => {
    const test = fixture({ replyAddress: 'client-replies' });
    test.session.createReceiver.mockRejectedValueOnce(new Error('Reply queue attach refused.'));
    await expect(test.transport.start()).rejects.toThrow('Reply queue attach refused.');
    expect(test.owned.createOwnerSender).not.toHaveBeenCalled();
    expect(test.owner.close).not.toHaveBeenCalled();
    expect(test.order).toEqual(['session', 'connection']);
  });

  it.each(['onError', 'onClose', 'onSessionError', 'onSessionClose'] as const)(
    'closes a preconfigured reply transport on %s',
    async (callback) => {
      const test = fixture({ replyAddress: 'client-replies' });
      await test.transport.start();
      await test.transport.send({ jsonrpc: '2.0', id: 1, method: 'tools/list' });
      test.receiverOptions()[callback]!({} as EventContext);
      await test.transport.close();
      expect(test.errors).toHaveBeenCalledOnce();
      expect(test.received).toHaveBeenCalledWith(
        expect.objectContaining({ id: 1, error: expect.any(Object) }),
      );
      expect(test.owner.close).not.toHaveBeenCalled();
      expect(test.order).toEqual(['receiver', 'request-sender', 'session', 'connection']);
    },
  );

  it('connects directly to the SDK client without the proxy or example handlers', async () => {
    const test = fixture();
    const engine = new McpEngine(test.config, () => {
      const server = new StatelessMcpServer(
        { name: 'application-tools', version: '1' },
        { capabilities: { tools: {} } },
      );
      server.setRequestHandler('tools/list', async () => ({ tools: [] }));
      return server;
    });
    test.sender.send.mockImplementation(async (message) => {
      const envelope = decodeRpc(message);
      const messageId: string = message.message_id;
      if (Array.isArray(envelope) || !('method' in envelope)) {
        throw new Error('Expected an SDK request.');
      }
      await engine.dispatch(envelope, new AbortController().signal, async (response) => {
        test.deliver(encodeRpc(response, { route: { address: 'opaque-reply', messageId } }));
      });
      return { settled: true };
    });
    const client = new Client(
      { name: 'direct-client', version: '1' },
      { versionNegotiation: { mode: { pin: MCP_PROTOCOL_VERSION } } },
    );
    try {
      await client.connect(test.transport);
      expect(client.getServerVersion()).toMatchObject({ name: 'application-tools' });
      expect(await client.listTools()).toMatchObject({ tools: [] });
    } finally {
      await client.close();
      await engine.close();
    }
  });

  it('uses explicit queue termini and message destinations for MQ', async () => {
    const test = fixture({ broker: 'ibmmq' });
    await test.transport.start();
    expect(test.receiverOptions().source).toEqual({
      address: 'opaque-reply',
      capabilities: ['queue'],
    });
    expect(test.session.createAwaitableSender).toHaveBeenCalledWith(
      expect.objectContaining({ target: { address: 'requests', capabilities: ['queue'] } }),
    );
    await test.transport.send({ jsonrpc: '2.0', id: 1, method: 'tools/list' });
    expect(test.sender.send.mock.calls[0]![0].to).toBe('requests');
    await test.transport.close();
  });

  it('creates a dynamic target and holds the owner until the named receiver closes', async () => {
    const test = fixture();
    await test.transport.start();
    expect(test.ownerOptions().target).toMatchObject({
      dynamic: true,
      capabilities: ['temporary-queue'],
    });
    expect(test.session.createReceiver).toHaveBeenCalledWith(
      expect.objectContaining({
        source: { address: 'opaque-reply' },
        credit_window: 0,
        autoaccept: false,
      }),
    );
    expect(test.owner.close).not.toHaveBeenCalled();
    await test.transport.send({ jsonrpc: '2.0', id: 1, method: 'tools/list' });
    const request = test.sender.send.mock.calls[0]![0];
    const reply = encodeRpc(
      { jsonrpc: '2.0', id: 1, result: {} },
      { route: { address: 'opaque-reply', messageId: request.message_id } },
    );
    expect(test.deliver(reply).accept).toHaveBeenCalledOnce();
    expect(test.received).toHaveBeenCalledWith({ jsonrpc: '2.0', id: 1, result: {} });
    expect(test.owner.close).not.toHaveBeenCalled();
    await test.transport.close();
    await test.transport.close();
    expect(test.order).toEqual(['receiver', 'request-sender', 'owner', 'session', 'connection']);
  });

  it('sends cancellation with the original body ID at the deadline and drops late replies', async () => {
    vi.useFakeTimers();
    const test = fixture();
    await test.transport.start();
    await test.transport.send({ jsonrpc: '2.0', id: 'body-id', method: 'tools/call' });
    const request = test.sender.send.mock.calls[0]![0];
    await vi.advanceTimersByTimeAsync(1000);
    expect(test.sender.send.mock.calls[1]![0]).toMatchObject({
      subject: 'notifications/cancelled',
    });
    expect(test.sender.send.mock.calls[1]![0].reply_to).toBeUndefined();
    expect(test.received).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'body-id',
        error: { code: -32000, message: 'AMQP response deadline exceeded.' },
      }),
    );
    test.deliver(
      encodeRpc(
        { jsonrpc: '2.0', id: 'body-id', result: {} },
        { route: { address: 'opaque-reply', messageId: request.message_id } },
      ),
    );
    expect(test.received).toHaveBeenCalledOnce();
    await test.transport.close();
  });

  it('isolates correlation, allows scoped notifications, and rejects mismatched response IDs', async () => {
    const test = fixture();
    await test.transport.start();
    await test.transport.send({ jsonrpc: '2.0', id: 7, method: 'tools/list' });
    const route = {
      address: 'opaque-reply',
      messageId: test.sender.send.mock.calls[0]![0].message_id,
    };
    test.deliver(
      encodeRpc(
        { jsonrpc: '2.0', id: 7, result: {} },
        { route: { ...route, messageId: 'unrelated' } },
      ),
    );
    expect(test.received).not.toHaveBeenCalled();
    const wrong = test.deliver(encodeRpc({ jsonrpc: '2.0', id: '7', result: {} }, { route }));
    expect(wrong.reject).toHaveBeenCalledOnce();
    test.deliver(
      encodeRpc(
        {
          jsonrpc: '2.0',
          method: 'notifications/progress',
          params: { progressToken: 1, progress: 1 },
        },
        { route },
      ),
    );
    expect(test.received).toHaveBeenCalledWith(
      expect.objectContaining({ method: 'notifications/progress' }),
    );
    test.deliver(encodeRpc({ jsonrpc: '2.0', id: 7, result: {} }, { route }));
    expect(test.received).toHaveBeenCalledTimes(2);
    await test.transport.close();
  });

  it('accepts zero message IDs and drops unrelated zero correlation IDs without rejection', async () => {
    const test = fixture();
    await test.transport.start();
    await test.transport.send({ jsonrpc: '2.0', id: 0, method: 'tools/list' });
    const route = {
      address: 'opaque-reply',
      messageId: test.sender.send.mock.calls[0]![0].message_id,
    };
    const unrelated = encodeRpc(
      { jsonrpc: '2.0', id: 0, result: {} },
      { route: { ...route, messageId: 0 } },
    );
    expect(test.deliver(unrelated).accept).toHaveBeenCalledOnce();
    expect(test.received).not.toHaveBeenCalled();
    const reply = { ...encodeRpc({ jsonrpc: '2.0', id: 0, result: {} }, { route }), message_id: 0 };
    expect(test.deliver(reply).accept).toHaveBeenCalledOnce();
    expect(test.received).toHaveBeenCalledWith({ jsonrpc: '2.0', id: 0, result: {} });
    expect(test.errors).not.toHaveBeenCalled();
    await test.transport.close();
  });

  it('bounds concurrency and refuses duplicate request IDs and server-directed responses', async () => {
    const test = fixture({ maxConcurrentRequests: 1 });
    await expect(test.transport.send({ jsonrpc: '2.0', id: 1, method: 'ping' })).rejects.toThrow(
      'not open',
    );
    await test.transport.start();
    await expect(test.transport.start()).rejects.toThrow('once');
    await test.transport.send({ jsonrpc: '2.0', id: 1, method: 'ping' });
    await expect(test.transport.send({ jsonrpc: '2.0', id: 1, method: 'ping' })).rejects.toThrow(
      'already',
    );
    await expect(test.transport.send({ jsonrpc: '2.0', id: 2, method: 'ping' })).rejects.toThrow(
      'Too many',
    );
    await expect(test.transport.send({ jsonrpc: '2.0', id: 3, result: {} })).rejects.toThrow(
      'only requests',
    );
    await test.transport.close();
    expect(test.received).toHaveBeenCalledWith(
      expect.objectContaining({ id: 1, error: expect.any(Object) }),
    );
  });

  it('fails on ownership loss and cleans up partial attaches', async () => {
    const test = fixture();
    await test.transport.start();
    test.ownerOptions().onClose!({} as EventContext);
    await test.transport.close();
    expect(test.errors).toHaveBeenCalledWith(
      new Error('Temporary reply queue ownership was lost.'),
    );
    expect(test.owned.close).toHaveBeenCalledOnce();
    vi.restoreAllMocks();
    const refused = fixture();
    refused.owner.target.address = '';
    await expect(refused.transport.start()).rejects.toThrow('assign');
    expect(refused.owned.close).toHaveBeenCalledOnce();
  });

  it('serializes send dispositions and waits for bounded link credit', async () => {
    vi.useFakeTimers();
    const test = fixture();
    const ordered = new OrderedSender(
      test.sender as unknown as ConstructorParameters<typeof OrderedSender>[0],
      100,
    );
    test.sender.sendable.mockReturnValue(false);
    const pending = ordered.send({ body: 'first' });
    await Promise.resolve();
    test.sender.sendable.mockReturnValue(true);
    test.sender.emit(SenderEvents.sendable, {});
    await pending;
    expect(test.sender.send).toHaveBeenCalledWith({ body: 'first' }, { timeoutInSeconds: 0.1 });
    test.sender.sendable.mockReturnValue(false);
    const timedOut = expect(ordered.send({ body: 'second' })).rejects.toThrow('credit timed out');
    await vi.advanceTimersByTimeAsync(100);
    await timedOut;
    await ordered.close();
    await expect(ordered.send({ body: Buffer.alloc(0) })).rejects.toThrow('closed');
  });

  it('closes on owner, receiver, and session failures and reports cleanup errors', async () => {
    for (const callback of ['onError', 'onClose', 'onSessionError', 'onSessionClose'] as const) {
      const test = fixture();
      await test.transport.start();
      test.ownerOptions()[callback]?.({} as EventContext);
      await test.transport.close();
      expect(test.errors).toHaveBeenCalled();
    }
    for (const callback of ['onError', 'onClose'] as const) {
      const test = fixture();
      await test.transport.start();
      test.receiverOptions()[callback]?.({} as EventContext);
      await test.transport.close();
    }
    const broken = fixture();
    await broken.transport.start();
    broken.receiver.close.mockRejectedValue(new Error('close'));
    await broken.transport.close();
    expect(broken.errors).toHaveBeenCalledWith(new Error('AMQP resource cleanup failed.'));
    const waiting = fixture();
    waiting.sender.sendable.mockReturnValue(false);
    const ordered = new OrderedSender(
      waiting.sender as unknown as ConstructorParameters<typeof OrderedSender>[0],
      100,
    );
    const send = expect(ordered.send({ body: '' })).rejects.toThrow('closed while waiting');
    await Promise.resolve();
    waiting.sender.emit(SenderEvents.senderClose);
    await send;
    await ordered.close();
  });
});

describe('AMQP console tool responder', () => {
  const meta = {
    'io.modelcontextprotocol/protocolVersion': '2026-07-28',
    'io.modelcontextprotocol/clientInfo': { name: 'test', version: '1' },
    'io.modelcontextprotocol/clientCapabilities': {},
  };

  it('removes reply session handlers after repeated calls and failed attaches', async () => {
    vi.useFakeTimers();
    const test = fixture();
    test.session.createAwaitableSender.mockImplementation(async (options: SenderOptions) => {
      test.session.on('session_error', options.onSessionError!);
      return test.sender;
    });
    const server = new AmqpServer(test.config, createToolServer);
    server.onerror = test.errors;
    await server.start();
    expect(test.session.getMaxListeners()).toBe(test.config.maxConcurrentRequests * 2 + 3);
    for (let index = 0; index < 32; index++) {
      test.deliver(
        encodeRpc(
          { jsonrpc: '2.0', id: index, method: 'tools/list', params: { _meta: meta } },
          { replyTo: 'reply' },
        ),
      );
      await server.drain();
      expect(test.session.listenerCount('session_error')).toBe(1);
      await vi.advanceTimersByTimeAsync(60000);
      expect(test.session.listenerCount('session_error')).toBe(0);
    }
    test.session.createAwaitableSender.mockImplementationOnce(async (options: SenderOptions) => {
      test.session.on('session_error', options.onSessionError!);
      throw new Error('Attach refused.');
    });
    test.deliver(
      encodeRpc(
        { jsonrpc: '2.0', id: 99, method: 'tools/list', params: { _meta: meta } },
        { replyTo: 'reply' },
      ),
    );
    await server.drain();
    expect(test.session.listenerCount('session_error')).toBe(0);
    const options = test.session.createAwaitableSender.mock.calls[0]![0];
    options.onSessionError!({} as EventContext);
    expect(test.errors).toHaveBeenCalledWith(new Error('AMQP reply session failed.'));
    await server.close();
  });

  it('reuses reply clients by address and expires them after the default idle timeout', async () => {
    vi.useFakeTimers();
    const test = fixture();
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    for (const id of [1, 2]) {
      test.deliver(
        encodeRpc(
          { jsonrpc: '2.0', id, method: 'tools/list', params: { _meta: meta } },
          { replyTo: 'reply', messageId: `message-${id}` },
        ),
      );
      await server.drain();
      await vi.advanceTimersByTimeAsync(59999);
      expect(test.sender.close).not.toHaveBeenCalled();
    }
    expect(test.session.createAwaitableSender).toHaveBeenCalledOnce();
    expect(test.sender.send.mock.calls.map((call) => call[0].correlation_id)).toEqual([
      'message-1',
      'message-2',
    ]);
    await vi.advanceTimersByTimeAsync(1);
    expect(test.sender.close).toHaveBeenCalledOnce();
    test.deliver(
      encodeRpc(
        { jsonrpc: '2.0', id: 3, method: 'tools/list', params: { _meta: meta } },
        { replyTo: 'reply' },
      ),
    );
    await server.drain();
    expect(test.session.createAwaitableSender).toHaveBeenCalledTimes(2);
    await server.close();
    expect(test.sender.close).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('suspends idle expiry until the last overlapping request for the address completes', async () => {
    vi.useFakeTimers();
    const test = fixture({ replyClientIdleTimeoutMs: 100 });
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    test.deliver(
      encodeRpc(
        { jsonrpc: '2.0', id: 0, method: 'tools/list', params: { _meta: meta } },
        { replyTo: 'reply' },
      ),
    );
    await server.drain();
    await vi.advanceTimersByTimeAsync(99);
    const finish: (() => void)[] = [];
    const dispatch = vi
      .spyOn(McpEngine.prototype, 'dispatch')
      .mockImplementation(async (message, _signal, emit) => {
        await new Promise<void>((resolve) => finish.push(resolve));
        if ('id' in message && message.id !== undefined) {
          await emit({ jsonrpc: '2.0', id: message.id, result: {} });
        }
      });
    const deliveries = [1, 2].map((id) =>
      test.deliver(encodeRpc({ jsonrpc: '2.0', id, method: 'tools/list' }, { replyTo: 'reply' })),
    );
    await vi.advanceTimersByTimeAsync(200);
    expect(dispatch).toHaveBeenCalledTimes(2);
    expect(test.session.createAwaitableSender).toHaveBeenCalledOnce();
    expect(test.sender.close).not.toHaveBeenCalled();
    finish[0]!();
    await vi.advanceTimersByTimeAsync(200);
    expect(deliveries[0]!.accept).toHaveBeenCalledOnce();
    expect(deliveries[1]!.accept).not.toHaveBeenCalled();
    expect(test.sender.close).not.toHaveBeenCalled();
    finish[1]!();
    await server.drain();
    await vi.advanceTimersByTimeAsync(99);
    expect(test.sender.close).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(test.sender.close).toHaveBeenCalledOnce();
    await server.close();
  });

  it('shares pending reply attachments and does not reuse failed ones', async () => {
    vi.useFakeTimers();
    const test = fixture();
    let finish: (() => void) | undefined;
    test.session.createAwaitableSender.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = () => resolve(test.sender);
        }),
    );
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    for (const id of [1, 2]) {
      test.deliver(
        encodeRpc(
          { jsonrpc: '2.0', id, method: 'tools/list', params: { _meta: meta } },
          { replyTo: 'reply' },
        ),
      );
    }
    await vi.advanceTimersByTimeAsync(0);
    expect(test.session.createAwaitableSender).toHaveBeenCalledOnce();
    expect(test.sender.close).not.toHaveBeenCalled();
    finish!();
    await server.drain();
    const options = test.session.createAwaitableSender.mock.calls[0]![0];
    options.onClose!({} as EventContext);
    await vi.advanceTimersByTimeAsync(0);
    expect(test.sender.close).toHaveBeenCalledOnce();
    test.session.createAwaitableSender.mockRejectedValueOnce(new Error('Attach refused.'));
    const request = encodeRpc(
      { jsonrpc: '2.0', id: 3, method: 'tools/list', params: { _meta: meta } },
      { replyTo: 'reply' },
    );
    const refused = test.deliver(request);
    await server.drain();
    expect(refused.reject).toHaveBeenCalledOnce();
    test.deliver(request);
    await server.drain();
    expect(test.session.createAwaitableSender).toHaveBeenCalledTimes(3);
    await server.close();
  });

  it('evicts failed sends and bounds idle reply clients without closing the shared session', async () => {
    vi.useFakeTimers();
    const test = fixture({ maxConcurrentRequests: 1 });
    const server = new AmqpServer(test.config, createToolServer);
    server.onerror = test.errors;
    await server.start();
    const request = encodeRpc(
      { jsonrpc: '2.0', id: 1, method: 'tools/list', params: { _meta: meta } },
      { replyTo: 'one' },
    );
    test.sender.send.mockRejectedValueOnce(new Error('Delivery failed.'));
    expect(test.deliver(request).accept).not.toHaveBeenCalled();
    await server.drain();
    await vi.advanceTimersByTimeAsync(0);
    expect(test.sender.close).toHaveBeenCalledOnce();
    test.deliver(request);
    await server.drain();
    expect(test.session.createAwaitableSender).toHaveBeenCalledTimes(2);
    test.deliver({ ...request, reply_to: 'two' });
    await server.drain();
    await vi.advanceTimersByTimeAsync(0);
    expect(test.sender.close).toHaveBeenCalledTimes(2);
    expect(test.session.close).not.toHaveBeenCalled();
    expect(test.sender.close).toHaveBeenCalledWith({ closeSession: false });
    test.sender.close.mockRejectedValueOnce(new Error('Close failed.'));
    await server.close();
    expect(test.errors).toHaveBeenCalledWith(new Error('AMQP reply client cleanup failed.'));
    expect(test.session.close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(['generic', 'ibmmq'])(
    'serves a real MCP tool request with exact correlation on %s',
    async (broker) => {
      const test = fixture({ broker });
      const server = new AmqpServer(test.config, createToolServer);
      server.onerror = test.errors;
      await server.start();
      const request = encodeRpc(
        {
          jsonrpc: '2.0',
          id: 'body-id',
          method: 'tools/call',
          params: { name: 'add', arguments: { left: 20, right: 22 }, _meta: meta },
        },
        { replyTo: 'temp-queue://client-one', messageId: 'amqp-id' },
      );
      const delivery = test.deliver(request);
      await server.drain();
      expect(test.session.createAwaitableSender).toHaveBeenCalledWith(
        expect.objectContaining({
          target: {
            address: 'temp-queue://client-one',
            ...(broker === 'ibmmq' ? { capabilities: ['queue'] } : {}),
          },
        }),
      );
      const response = test.sender.send.mock.calls[0]![0];
      expect(response.correlation_id).toBe('amqp-id');
      expect(response.reply_to).toBeUndefined();
      expect(decodeRpc(response)).toMatchObject({
        id: 'body-id',
        result: { structuredContent: { sum: 42 } },
      });
      expect(delivery.accept).toHaveBeenCalledOnce();
      await server.close();
    },
  );

  it('answers a mixed batch once and never replies to an all-notification batch', async () => {
    const test = fixture();
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    const batch = [
      { jsonrpc: '2.0' as const, id: 1, method: 'tools/list', params: { _meta: meta } },
      { jsonrpc: '2.0' as const, method: 'notifications/cancelled', params: { requestId: 'none' } },
      {
        jsonrpc: '2.0' as const,
        id: 2,
        method: 'tools/call',
        params: { name: 'echo', arguments: { text: 'hello' }, _meta: meta },
      },
    ];
    test.deliver(encodeRpc(batch, { replyTo: 'reply' }));
    await server.drain();
    expect(test.sender.send).toHaveBeenCalledOnce();
    expect(decodeRpc(test.sender.send.mock.calls[0]![0])).toMatchObject([{ id: 1 }, { id: 2 }]);
    test.deliver(encodeRpc([batch[1]!]));
    await server.drain();
    expect(test.sender.send).toHaveBeenCalledOnce();
    await server.close();
  });

  it('returns parse/invalid-request errors only when usable reply routing exists', async () => {
    const test = fixture();
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    const request = encodeRpc({ jsonrpc: '2.0', id: 1, method: 'ping' }, { replyTo: 'reply' });
    request.body.content = Buffer.from('{');
    test.deliver(request);
    await server.drain();
    expect(decodeRpc(test.sender.send.mock.calls[0]![0])).toMatchObject({
      id: null,
      error: { code: -32700 },
    });
    test.deliver(encodeRpc([], { replyTo: 'reply' }));
    await server.drain();
    expect(decodeRpc(test.sender.send.mock.calls[1]![0])).toMatchObject({
      id: null,
      error: { code: -32600 },
    });
    const missing = test.deliver({ ...request, reply_to: undefined });
    await server.drain();
    expect(missing.reject).toHaveBeenCalledOnce();
    await server.close();
  });

  it('reserves concurrency before a reply link finishes attaching', async () => {
    const test = fixture({ maxConcurrentRequests: 1 });
    let release: (() => void) | undefined;
    let enter: (() => void) | undefined;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    test.session.createAwaitableSender.mockImplementationOnce(() => {
      enter?.();
      return new Promise((resolve) => {
        release = () => resolve(test.sender);
      });
    });
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    test.deliver(
      encodeRpc(
        { jsonrpc: '2.0', id: 1, method: 'tools/list', params: { _meta: meta } },
        { replyTo: 'first' },
      ),
    );
    await entered;
    const second = test.deliver(
      encodeRpc(
        { jsonrpc: '2.0', id: 2, method: 'tools/list', params: { _meta: meta } },
        { replyTo: 'second' },
      ),
    );
    await new Promise<void>((resolve) => second.accept.mockImplementation(resolve));
    expect(decodeRpc(test.sender.send.mock.calls[0]![0])).toMatchObject({
      id: 2,
      error: { code: -32000 },
    });
    release?.();
    await server.drain();
    expect(test.sender.send).toHaveBeenCalledTimes(2);
    await server.close();
  });

  it('keeps same-ID clients isolated and completes bounded subscriptions on their original reply links', async () => {
    vi.useFakeTimers();
    const test = fixture({ subscriptionTimeoutMs: 100, maxConcurrentRequests: 2 });
    const server = new AmqpServer(test.config, createToolServer);
    server.onerror = test.errors;
    await server.start();
    let acknowledge: (() => void) | undefined;
    const acknowledged = new Promise<void>((resolve) => {
      acknowledge = resolve;
    });
    let count = 0;
    test.sender.send.mockImplementation(async (message) => {
      const envelope = decodeRpc(message);
      if (
        !Array.isArray(envelope) &&
        'method' in envelope &&
        envelope.method === 'notifications/subscriptions/acknowledged' &&
        ++count === 2
      ) {
        acknowledge?.();
      }
      return { settled: true };
    });
    for (const client of ['one', 'two']) {
      test.deliver(
        encodeRpc(
          {
            jsonrpc: '2.0',
            id: 1,
            method: 'subscriptions/listen',
            params: { notifications: { toolsListChanged: true }, _meta: meta },
          },
          { replyTo: client, messageId: client },
        ),
      );
    }
    await acknowledged;
    test.deliver(
      encodeRpc({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 1 } }),
    );
    expect(test.errors).toHaveBeenCalledWith(
      new Error('Ambiguous shared-queue cancellation was ignored.'),
    );
    await vi.advanceTimersByTimeAsync(100);
    await server.drain();
    expect(test.session.createAwaitableSender).toHaveBeenCalledTimes(2);
    expect(test.sender.send).toHaveBeenCalledTimes(4);
    for (const client of ['one', 'two']) {
      const replies = test.sender.send.mock.calls
        .map((call) => call[0])
        .filter((message) => message.to === client);
      expect(replies).toHaveLength(2);
      expect(replies.every((message) => message.correlation_id === client)).toBe(true);
      expect(decodeRpc(replies[1])).toMatchObject({ id: 1, result: { resultType: 'complete' } });
    }
    await server.close();
  });

  it('cancels a unique active request and stops processing after failed notification delivery', async () => {
    const test = fixture();
    let entered: (() => void) | undefined;
    const active = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const dispatch = vi
      .spyOn(McpEngine.prototype, 'dispatch')
      .mockImplementation(async (_message, signal) => {
        entered?.();
        await new Promise<void>((_resolve, reject) =>
          signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true }),
        );
      });
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    test.deliver(
      encodeRpc({ jsonrpc: '2.0', id: 'cancel', method: 'tools/list' }, { replyTo: 'reply' }),
    );
    await active;
    test.deliver(
      encodeRpc({
        jsonrpc: '2.0',
        method: 'notifications/cancelled',
        params: { requestId: 'cancel' },
      }),
    );
    await server.drain();
    expect(decodeRpc(test.sender.send.mock.calls[0]![0])).toMatchObject({
      id: 'cancel',
      error: { message: 'Request cancelled.' },
    });
    test.sender.send.mockRejectedValue(new Error('rejected'));
    let observed: AbortSignal | undefined;
    dispatch.mockImplementation(async (_message, signal, emit) => {
      observed = signal;
      await emit({
        jsonrpc: '2.0',
        method: 'notifications/progress',
        params: { progressToken: 1, progress: 1 },
      });
    });
    const rejected = test.deliver(
      encodeRpc({ jsonrpc: '2.0', id: 2, method: 'tools/list' }, { replyTo: 'reply' }),
    );
    await server.drain();
    expect(observed?.aborted).toBe(true);
    expect(rejected.reject).toHaveBeenCalledOnce();
    await server.close();
  });

  it('rejects unauthorized routing and answers invalid mixed batches without processing responses as requests', async () => {
    const test = fixture();
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    const invalid = encodeRpc(
      { jsonrpc: '2.0', id: 1, method: 'tools/list', params: { _meta: meta } },
      { replyTo: 'reply' },
    );
    delete invalid.subject;
    invalid.body.content = Buffer.from(
      JSON.stringify([
        { jsonrpc: '2.0', id: 1, method: 'tools/list', params: { _meta: meta } },
        42,
        { jsonrpc: '2.0', id: 2, result: {} },
      ]),
    );
    test.deliver(invalid);
    await server.drain();
    expect(decodeRpc(test.sender.send.mock.calls[0]![0])).toMatchObject([
      { id: 1 },
      { id: null, error: { code: -32600 } },
      { id: null, error: { code: -32600 } },
    ]);
    const refused = test.deliver(
      encodeRpc({ jsonrpc: '2.0', id: 1, method: 'ping' }, { replyTo: '$cbs' }),
    );
    const missing = test.deliver({ ...invalid, message_id: undefined });
    const notification = test.deliver({
      ...encodeRpc({
        jsonrpc: '2.0',
        method: 'notifications/cancelled',
        params: { requestId: [] },
      }),
      correlation_id: 'illegal',
    });
    await server.drain();
    for (const delivery of [refused, missing, notification]) {
      expect(delivery.reject).toHaveBeenCalledOnce();
    }
    await server.close();
    await server.close();
    await expect(server.start()).rejects.toThrow('once');
  });

  it.each([
    { broker: 'ibmmq', correlation: Buffer.alloc(24), accepted: true },
    { broker: 'generic', correlation: Buffer.alloc(24), accepted: false },
    { broker: 'ibmmq', correlation: Buffer.alloc(23), accepted: false },
    { broker: 'ibmmq', correlation: Buffer.alloc(24, 1), accepted: false },
    { broker: 'ibmmq', correlation: 'illegal', accepted: false },
  ])('recognizes only MQCI_NONE for MQ notifications: $broker / $accepted', async (scenario) => {
    const test = fixture({ broker: scenario.broker });
    const server = new AmqpServer(test.config, createToolServer);
    await server.start();
    const delivery = test.deliver({
      ...encodeRpc({
        jsonrpc: '2.0',
        method: 'notifications/cancelled',
        params: { requestId: 'absent' },
      }),
      correlation_id: scenario.correlation,
    });
    await server.drain();
    expect(delivery.accept).toHaveBeenCalledTimes(scenario.accepted ? 1 : 0);
    expect(delivery.reject).toHaveBeenCalledTimes(scenario.accepted ? 0 : 1);
    await server.close();
  });

  it('handles listener failures, failed startup, and failed cleanup', async () => {
    for (const callback of ['onError', 'onClose', 'onSessionError', 'onSessionClose'] as const) {
      const test = fixture();
      const server = new AmqpServer(test.config, createToolServer);
      server.onerror = test.errors;
      await server.start();
      test.receiverOptions()[callback]?.({} as EventContext);
      await server.close();
      expect(test.errors).toHaveBeenCalled();
    }
    const broken = fixture();
    broken.session.createReceiver.mockRejectedValue(new Error('attach'));
    const server = new AmqpServer(broken.config, createToolServer);
    server.onerror = broken.errors;
    broken.session.close.mockRejectedValue(new Error('close'));
    await expect(server.start()).rejects.toThrow('attach');
    expect(broken.errors).toHaveBeenCalledWith(new Error('AMQP responder cleanup failed.'));
  });
});
