// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { EventEmitter } from 'node:events';
import { readFile } from 'node:fs/promises';
import {
  Container,
  ConnectionEvents,
  SenderEvents,
  SessionEvents,
  type Connection,
} from 'rhea-promise';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { AmqpConnection, connectionOptions } from '../src/auth.js';
import { parseConfig } from '../src/config.js';

vi.mock('node:fs/promises', () => ({ readFile: vi.fn() }));
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function saslMechanisms(options: Awaited<ReturnType<typeof connectionOptions>>): object {
  if (
    !('sasl_mechanisms' in options) ||
    typeof options.sasl_mechanisms !== 'object' ||
    options.sasl_mechanisms === null
  ) {
    throw new Error('Expected configured SASL mechanisms.');
  }
  return options.sasl_mechanisms;
}

describe('connection and SASL authentication', () => {
  it.each(['ibmmq', 'generic'])(
    'sends explicit keepalive only for %s and clears it on close',
    async (broker) => {
      vi.useFakeTimers();
      const raw = { is_open: vi.fn(() => true), _write_frame: vi.fn() };
      const connection = Object.assign(new EventEmitter(), {
        open: vi.fn(async () => {
          connection.emit(ConnectionEvents.connectionOpen, { _context: { connection: raw } });
        }),
        close: vi.fn().mockResolvedValue(undefined),
      });
      vi.spyOn(Container.prototype, 'createConnection').mockReturnValue(
        connection as unknown as Connection,
      );
      const owned = await AmqpConnection.open(
        parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'queue', broker })),
        vi.fn(),
      );
      await vi.advanceTimersByTimeAsync(30000);
      expect(raw._write_frame).toHaveBeenCalledTimes(broker === 'ibmmq' ? 3 : 0);
      raw.is_open.mockReturnValue(false);
      await vi.advanceTimersByTimeAsync(10000);
      expect(raw._write_frame).toHaveBeenCalledTimes(broker === 'ibmmq' ? 3 : 0);
      await owned.close();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each([
    {
      name: 'coalesced Open',
      initial: false,
      complete: true,
      bytes: 24,
      nextBytes: 8,
      expected: 24,
      nextCalls: 1,
    },
    {
      name: 'partial AMQP header',
      initial: false,
      complete: true,
      bytes: 20,
      nextBytes: 0,
      expected: 16,
      nextCalls: 1,
    },
    {
      name: 'outcome without trailing bytes',
      initial: false,
      complete: true,
      bytes: 16,
      nextBytes: 0,
      expected: 16,
      nextCalls: 0,
    },
    {
      name: 'incomplete or rejected SASL',
      initial: false,
      complete: false,
      bytes: 24,
      nextBytes: 8,
      expected: 16,
      nextCalls: 0,
    },
    {
      name: 'already in AMQP',
      initial: true,
      complete: true,
      bytes: 24,
      nextBytes: 8,
      expected: 8,
      nextCalls: 1,
    },
  ])('drains the SASL receive transition: $name', async (scenario) => {
    const connection = Object.assign(new EventEmitter(), {
      open: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(Container.prototype, 'createConnection').mockReturnValue(
      connection as unknown as Connection,
    );
    const config = parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'queue' }));
    const owned = await AmqpConnection.open(config, vi.fn());
    type Reader = {
      transport: { read_complete: boolean; read(buffer: Buffer): number };
      next: { read(buffer: Buffer): number };
    };
    const prototype = (
      new Container().sasl.Client as unknown as {
        prototype: { read(this: Reader, buffer: Buffer): number };
      }
    ).prototype;
    const transport = {
      read_complete: scenario.initial,
      read: vi.fn(() => {
        transport.read_complete = scenario.complete;
        return 16;
      }),
    };
    const next = { read: vi.fn(() => scenario.nextBytes) };
    const buffer = Buffer.alloc(scenario.bytes);
    expect(prototype.read.call({ transport, next }, buffer)).toBe(scenario.expected);
    expect(next.read).toHaveBeenCalledTimes(scenario.nextCalls);
    if (scenario.nextCalls) {
      expect(next.read).toHaveBeenCalledWith(scenario.initial ? buffer : buffer.subarray(16));
    }
    await owned.close();
  });

  it('selects TLS validation, Premium capability, and exactly SASL PLAIN', async () => {
    const config = parseConfig(
      JSON.stringify({
        endpoint: 'amqps://broker.example',
        node: 'queue',
        broker: 'servicebus',
        sasl: { mode: 'plain', username: 'policy', passwordEnv: 'KEY' },
      }),
    );
    const options = await connectionOptions(config, new Container(), { KEY: 'secret' });
    expect(options).toMatchObject({
      transport: 'tls',
      port: 5671,
      rejectUnauthorized: true,
      minVersion: 'TLSv1.2',
      reconnect: false,
      properties: { 'com.microsoft:is-client-provider': true },
    });
    expect(Object.keys(saslMechanisms(options))).toEqual(['PLAIN']);
    await expect(connectionOptions(config, new Container(), {})).rejects.toThrow('KEY');
  });

  it('can disable SASL or select only ANONYMOUS', async () => {
    const none = parseConfig(
      JSON.stringify({ endpoint: 'amqp://[::1]', node: 'queue', sasl: { mode: 'none' } }),
    );
    expect(await connectionOptions(none, new Container())).toMatchObject({
      transport: 'tcp',
      host: '::1',
      port: 5672,
    });
    expect(await connectionOptions(none, new Container())).not.toHaveProperty('sasl_mechanisms');
    const anonymous = parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'queue' }));
    expect(
      Object.keys(saslMechanisms(await connectionOptions(anonymous, new Container()))),
    ).toEqual(['ANONYMOUS']);
  });

  it('loads the configured trust and client certificates and selects EXTERNAL only', async () => {
    vi.mocked(readFile).mockResolvedValue(Buffer.from('test-certificate'));
    const config = parseConfig(
      JSON.stringify({
        endpoint: 'amqps://broker.example:5675',
        node: 'queue',
        tls: {
          caFile: 'ca.pem',
          certFile: 'client.pem',
          keyFile: 'client-key.pem',
          serverName: 'sni.example',
        },
        sasl: { mode: 'external', hostname: 'amqp.example' },
      }),
    );
    const options = await connectionOptions(config, new Container());
    expect(options).toMatchObject({
      port: 5675,
      hostname: 'amqp.example',
      sasl_init_hostname: 'amqp.example',
      servername: 'sni.example',
      rejectUnauthorized: true,
    });
    for (const field of ['ca', 'cert', 'key']) {
      expect(options).toHaveProperty(field, Buffer.from('test-certificate'));
    }
    expect(Object.keys(saslMechanisms(options))).toEqual(['EXTERNAL']);
  });

  it('reports connection loss, suppresses expected closure, and cleans failed opens', async () => {
    const connection = Object.assign(new EventEmitter(), {
      id: 'unit-connection',
      open: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(Container.prototype, 'createConnection').mockReturnValue(
      connection as unknown as Connection,
    );
    const config = parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'queue' }));
    const failed = vi.fn();
    const owned = await AmqpConnection.open(config, failed);
    connection.emit(ConnectionEvents.disconnected);
    expect(failed).toHaveBeenCalledWith(new Error('AMQP connection was lost.'));
    connection.close.mockImplementation(async () => {
      connection.emit(ConnectionEvents.connectionClose);
    });
    await owned.close();
    await owned.close();
    expect(failed).toHaveBeenCalledOnce();
    expect(connection.eventNames()).toHaveLength(0);
    connection.open.mockRejectedValueOnce(new Error('open failed'));
    await expect(AmqpConnection.open(config, failed)).rejects.toThrow('open failed');
    expect(connection.close).toHaveBeenCalledTimes(2);
  });

  it('opens ownership on peer Attach without credit and releases failed or timed-out sessions', async () => {
    vi.useFakeTimers();
    const raw = Object.assign(new EventEmitter(), { id: 'raw', create_session: vi.fn() });
    const rawSession = Object.assign(new EventEmitter(), {
      connection: raw,
      begin: vi.fn(),
      close: vi.fn(),
      remove: vi.fn(),
      attach_sender: vi.fn(),
    });
    const rawSender = Object.assign(new EventEmitter(), {
      name: 'owner',
      session: rawSession,
      connection: raw,
      target: { address: 'temporary' },
      close: vi.fn(),
      sendable: vi.fn(() => false),
    });
    raw.create_session.mockReturnValue(rawSession);
    rawSession.attach_sender.mockReturnValue(rawSender);
    const connection = Object.assign(new EventEmitter(), {
      id: 'unit-owner',
      open: vi.fn(async () => {
        connection.emit(ConnectionEvents.connectionOpen, { _context: { connection: raw } });
      }),
      close: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(Container.prototype, 'createConnection').mockReturnValue(
      connection as unknown as Connection,
    );
    const owned = await AmqpConnection.open(
      parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'queue' })),
      vi.fn(),
    );
    const context = { connection: raw, session: rawSession, sender: rawSender };
    const callbacks = {
      onError: vi.fn(),
      onClose: vi.fn(),
      onSessionError: vi.fn(),
      onSessionClose: vi.fn(),
    };
    const opening = owned.createOwnerSender({ target: { dynamic: true }, ...callbacks }, 100);
    rawSender.emit(SenderEvents.senderOpen, context);
    expect((await opening).target.address).toBe('temporary');
    expect(rawSender.sendable).not.toHaveBeenCalled();
    rawSender.emit(SenderEvents.senderError, context);
    rawSender.emit(SenderEvents.senderClose, context);
    rawSession.emit(SessionEvents.sessionError, context);
    rawSession.emit(SessionEvents.sessionClose, context);
    for (const callback of Object.values(callbacks)) {
      expect(callback).toHaveBeenCalledOnce();
    }
    for (const target of [undefined, null, { address: '' }]) {
      Object.assign(rawSender, { target });
      const settlement = vi.fn();
      const rejected = owned.createOwnerSender({ target: { dynamic: true } }, 100);
      void rejected.then(settlement, settlement);
      rawSender.emit(SenderEvents.senderOpen, context);
      await Promise.resolve();
      expect(settlement).not.toHaveBeenCalled();
      const rejection = expect(rejected).rejects.toThrow('attach failed');
      rawSender.emit(SenderEvents.senderClose, context);
      await rejection;
    }
    const refused = expect(owned.createOwnerSender({}, 100)).rejects.toThrow('attach failed');
    rawSender.emit(SenderEvents.senderError, context);
    await refused;
    const timedOut = expect(owned.createOwnerSender({}, 100)).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(100);
    await timedOut;
    expect(rawSession.remove).toHaveBeenCalledTimes(5);
    await owned.close();
    await expect(owned.createOwnerSender({})).rejects.toThrow('not open');
  });
});
