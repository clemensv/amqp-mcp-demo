// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { PassThrough } from 'node:stream';
import { readFile } from 'node:fs/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { StdioProtocolAdapter, StdioRelay } from '../src/stdio.js';
import { McpEngine } from '../src/sdk.js';
import { createToolServer } from '../example/src/tools.js';
import { parseConfig } from '../src/config.js';
import type { RpcEnvelope } from '../src/wire.js';
import { Client } from '@modelcontextprotocol/client';
import type { Transport } from '@modelcontextprotocol/server';
import { JSONRPCMessageSchema } from '@modelcontextprotocol/core';
import { runProxy } from '../src/runtime.js';
import { runServer } from '../example/src/runtime.js';
import { AmqpClientTransport } from '../src/transport.js';
import { AmqpServer } from '../src/responder.js';
import { loadConfig } from '../src/config.js';

vi.mock('node:fs/promises', () => ({ readFile: vi.fn() }));
const originalArgv = process.argv;
const originalExitCode = process.exitCode;
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  process.argv = originalArgv;
  process.exitCode = originalExitCode;
});

function relay(max = 1024) {
  const input = new PassThrough();
  const output = new PassThrough();
  const forwarded = vi.fn().mockResolvedValue(undefined);
  const failed = vi.fn();
  const ended = vi.fn();
  const channel = new StdioRelay(input, output, max, forwarded, failed, ended);
  channel.start();
  return { channel, input, output, forwarded, failed, ended };
}

describe('bounded MCP stdio relay', () => {
  it('assembles partial UTF-8 lines and forwards single and batch messages', async () => {
    const test = relay();
    let count = 0;
    const forwarded = new Promise<void>((resolve) =>
      test.forwarded.mockImplementation(async () => {
        if (++count === 2) {
          resolve();
        }
      }),
    );
    test.input.write('{"jsonrpc":"2.0","id":1,');
    test.input.write(
      '"method":"ping"}\n[{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}]\n',
    );
    await forwarded;
    await test.channel.close();
    expect(test.forwarded.mock.calls).toEqual([
      [{ jsonrpc: '2.0', id: 1, method: 'ping' }],
      [[{ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 2 } }]],
    ]);
    expect(test.failed).not.toHaveBeenCalled();
  });

  it('writes only ordered newline-separated JSON-RPC to stdout', async () => {
    const test = relay();
    await Promise.all([
      test.channel.send({ jsonrpc: '2.0', id: 1, result: {} }),
      test.channel.send({ jsonrpc: '2.0', id: 2, result: {} }),
    ]);
    expect(test.output.read().toString()).toBe(
      '{"jsonrpc":"2.0","id":1,"result":{}}\n{"jsonrpc":"2.0","id":2,"result":{}}\n',
    );
    await test.channel.close();
  });

  it('rejects malformed, oversized, and incomplete input', async () => {
    const invalid = relay();
    invalid.input.write('not-json\n');
    expect(invalid.failed).toHaveBeenCalledOnce();
    await invalid.channel.close();
    const oversized = relay(8);
    oversized.input.write('0123456789');
    expect(oversized.failed).toHaveBeenCalledOnce();
    await expect(oversized.channel.send({ jsonrpc: '2.0', id: 1, result: {} })).rejects.toThrow(
      'size limit',
    );
    await oversized.channel.close();
    const incomplete = relay();
    const done = new Promise<void>((resolve) => incomplete.ended.mockImplementation(resolve));
    incomplete.input.end();
    await done;
    await incomplete.channel.close();
  });

  it('handles EOF fragments, input failures, output failures, and forwarding rejection', async () => {
    const fragment = relay();
    fragment.input.write('{"jsonrpc"');
    const ended = new Promise<void>((resolve) => fragment.failed.mockImplementation(resolve));
    fragment.input.end();
    await ended;
    expect(fragment.failed).toHaveBeenCalledOnce();
    await fragment.channel.close();
    const broken = relay();
    broken.input.emit('error', new Error('input'));
    broken.output.emit('error', new Error('output'));
    expect(broken.failed).toHaveBeenCalledTimes(2);
    await broken.channel.close();
    const rejected = relay();
    rejected.forwarded.mockRejectedValue(new Error('forward'));
    const failed = new Promise<void>((resolve) => rejected.failed.mockImplementation(resolve));
    rejected.input.write('{"jsonrpc":"2.0","id":1,"method":"ping"}\n');
    await failed;
    await rejected.channel.close();
    await expect(rejected.channel.send({ jsonrpc: '2.0', id: 1, result: {} })).rejects.toThrow(
      'closed',
    );
  });
});

function runtime() {
  process.argv = ['node', 'application.js', '--config', 'test-config.json'];
  vi.mocked(readFile).mockResolvedValue(
    JSON.stringify({ endpoint: 'amqp://localhost', node: 'requests' }),
  );
  const signals = new Map<string, () => void>();
  const originalOnce = process.once.bind(process);
  vi.spyOn(process, 'once').mockImplementation((event, listener) => {
    if (event === 'SIGINT' || event === 'SIGTERM') {
      signals.set(event, listener);
    } else {
      originalOnce(event, listener);
    }
    return process;
  });
  const stderr = vi.spyOn(process.stderr, 'write').mockReturnValue(true);
  const input = new PassThrough();
  const output = new PassThrough();
  vi.spyOn(process, 'stdin', 'get').mockReturnValue(input as unknown as typeof process.stdin);
  vi.spyOn(process, 'stdout', 'get').mockReturnValue(output as unknown as typeof process.stdout);
  vi.spyOn(process.stdin, 'destroy').mockReturnValue(process.stdin);
  return { signals, stderr, input, output };
}

describe('legacy stdio protocol mapping', () => {
  it('connects a real legacy SDK client and executes both example tools', async () => {
    const engine = new McpEngine(
      parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'requests' })),
      createToolServer,
    );
    const transport: Transport = {
      async start() {},
      async close() {},
      async send(message) {
        await adapter.receive(message);
      },
    };
    const adapter: StdioProtocolAdapter = new StdioProtocolAdapter(
      async (message) => {
        if (Array.isArray(message)) {
          throw new Error('Unexpected test envelope');
        }
        await engine.dispatch(
          JSONRPCMessageSchema.parse(message),
          new AbortController().signal,
          (reply) => adapter.send(reply),
        );
      },
      async (message) => {
        if (Array.isArray(message)) {
          throw new Error('Unexpected test envelope');
        }
        transport.onmessage?.(JSONRPCMessageSchema.parse(message));
      },
    );
    const client = new Client(
      { name: 'legacy-client', version: '1' },
      {
        supportedProtocolVersions: ['2025-11-25'],
      },
    );
    try {
      await client.connect(transport);
      expect(client.getServerVersion()).toMatchObject({ name: 'amqp-example-tools' });
      expect(await client.listTools()).toMatchObject({
        tools: [{ name: 'echo' }, { name: 'add' }],
      });
      expect(
        await client.callTool({ name: 'echo', arguments: { text: 'Hello through proxy' } }),
      ).toMatchObject({
        content: [{ type: 'text', text: 'Hello through proxy' }],
      });
      expect(
        await client.callTool({ name: 'add', arguments: { left: 19, right: 23 } }),
      ).toMatchObject({
        content: [{ type: 'text', text: '42' }],
        structuredContent: { sum: 42 },
      });
    } finally {
      await client.close();
      await engine.close();
    }
  });

  it('preserves native stateless envelopes and rejects invalid legacy initialization', async () => {
    const forward = vi.fn().mockResolvedValue(undefined);
    const emit = vi.fn().mockResolvedValue(undefined);
    const adapter = new StdioProtocolAdapter(forward, emit);
    const request = {
      jsonrpc: '2.0' as const,
      id: 7,
      method: 'server/discover',
      params: { _meta: {} },
    };
    await adapter.receive(request);
    await adapter.receive([request]);
    await adapter.receive({ jsonrpc: '2.0', id: 8, result: {} });
    expect(forward.mock.calls).toEqual([
      [request],
      [[request]],
      [{ jsonrpc: '2.0', id: 8, result: {} }],
    ]);
    await adapter.receive({ jsonrpc: '2.0', id: 9, method: 'initialize', params: {} });
    expect(emit).toHaveBeenCalledWith({
      jsonrpc: '2.0',
      id: 9,
      error: { code: -32602, message: 'Invalid or repeated MCP initialization.' },
    });
  });

  it('retains batches, protects protocol metadata, and preserves discovery errors', async () => {
    const forward = vi.fn().mockResolvedValue(undefined);
    const emit = vi.fn().mockResolvedValue(undefined);
    const adapter = new StdioProtocolAdapter(forward, emit);
    const initialize = {
      jsonrpc: '2.0' as const,
      id: 'init',
      method: 'initialize',
      params: {
        protocolVersion: '2025-06-18',
        clientInfo: { name: 'VS Code', version: '1' },
        capabilities: {},
      },
    };
    await adapter.receive(initialize);
    await adapter.receive(initialize);
    await adapter.receive({ jsonrpc: '2.0', id: 2, method: 'tools/list' });
    expect(emit.mock.calls[0]).toMatchObject([{ id: 'init', error: { code: -32602 } }]);
    expect(emit.mock.calls[1]).toMatchObject([{ id: 2, error: { code: -32000 } }]);
    const error = {
      jsonrpc: '2.0' as const,
      id: 'init',
      error: { code: -32000, message: 'Unavailable' },
    };
    await adapter.send(error);
    expect(emit).toHaveBeenLastCalledWith(error);
    const invalid = new StdioProtocolAdapter(forward, emit);
    await invalid.receive(initialize);
    await invalid.send({ jsonrpc: '2.0', id: 'init', result: {} });
    expect(emit).toHaveBeenLastCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ code: -32603 }) }),
    );
    const ready = new StdioProtocolAdapter(forward, emit);
    await ready.receive(initialize);
    await ready.send({
      jsonrpc: '2.0',
      id: 'init',
      result: {
        supportedVersions: ['2026-07-28'],
        capabilities: { tools: {} },
        instructions: 'Use tools',
        _meta: { 'io.modelcontextprotocol/serverInfo': { name: 'tools', version: '1' } },
      },
    });
    expect(emit).toHaveBeenLastCalledWith(
      expect.objectContaining({ result: expect.objectContaining({ instructions: 'Use tools' }) }),
    );
    await ready.receive([
      {
        jsonrpc: '2.0',
        id: 3,
        method: 'tools/list',
        params: {
          _meta: { 'io.modelcontextprotocol/protocolVersion': 'wrong' },
        },
      },
      { jsonrpc: '2.0', method: 'notifications/initialized' },
    ]);
    expect(forward).toHaveBeenLastCalledWith([
      {
        jsonrpc: '2.0',
        id: 3,
        method: 'tools/list',
        params: {
          _meta: {
            'io.modelcontextprotocol/protocolVersion': '2026-07-28',
            'io.modelcontextprotocol/clientInfo': { name: 'VS Code', version: '1' },
            'io.modelcontextprotocol/clientCapabilities': {},
          },
        },
      },
    ]);
    const reply = [{ jsonrpc: '2.0' as const, id: 3, result: {} }];
    await ready.send(reply);
    expect(emit).toHaveBeenLastCalledWith(reply);
    await ready.receive({
      jsonrpc: '2.0',
      method: 'notifications/cancelled',
      params: { requestId: 3 },
    });
    expect(forward).toHaveBeenLastCalledWith(
      expect.objectContaining({ method: 'notifications/cancelled' }),
    );
  });

  it('initializes against real stateless discovery and forwards tool calls with metadata', async () => {
    const output: RpcEnvelope[] = [];
    const engine = new McpEngine(
      parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'requests' })),
      createToolServer,
    );
    const forwarded: RpcEnvelope[] = [];
    const adapter = new StdioProtocolAdapter(
      async (message) => {
        forwarded.push(message);
        if (!Array.isArray(message)) {
          await engine.dispatch(
            JSONRPCMessageSchema.parse(message),
            new AbortController().signal,
            (reply) => adapter.send(reply),
          );
        }
      },
      async (message) => {
        output.push(message);
      },
    );
    try {
      await adapter.receive({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2025-11-25',
          clientInfo: { name: 'VS Code', version: '1.141.0' },
          capabilities: {},
        },
      });
      expect(output[0]).toMatchObject({
        id: 1,
        result: {
          protocolVersion: '2025-11-25',
          serverInfo: { name: 'amqp-example-tools' },
          capabilities: { tools: {} },
        },
      });
      await adapter.receive({ jsonrpc: '2.0', method: 'notifications/initialized' });
      await adapter.receive({ jsonrpc: '2.0', id: 'list', method: 'tools/list' });
      await adapter.receive({
        jsonrpc: '2.0',
        id: 3,
        method: 'tools/call',
        params: {
          name: 'add',
          arguments: { left: 19, right: 23 },
          _meta: { progressToken: 'progress' },
        },
      });
      expect(forwarded).toHaveLength(3);
      expect(forwarded[0]).toMatchObject({ method: 'server/discover' });
      expect(output[1]).toMatchObject({
        id: 'list',
        result: { tools: [{ name: 'echo' }, { name: 'add' }] },
      });
      expect(output[2]).toMatchObject({ id: 3, result: { structuredContent: { sum: 42 } } });
      expect(forwarded[2]).toMatchObject({
        params: {
          _meta: {
            progressToken: 'progress',
            'io.modelcontextprotocol/protocolVersion': '2026-07-28',
          },
        },
      });
    } finally {
      await engine.close();
    }
  });
});

describe('console application lifecycle', () => {
  it('starts the proxy from MCP environment settings without reading a file', async () => {
    const test = runtime();
    process.argv = ['node', 'application.js'];
    vi.stubEnv('AMQP_MCP_CONFIG', undefined);
    vi.stubEnv('AMQP_MCP_ENDPOINT', 'amqp://localhost');
    vi.stubEnv('AMQP_MCP_NODE', 'requests');
    vi.stubEnv('AMQP_MCP_SASL_MODE', 'plain');
    vi.stubEnv('AMQP_MCP_USERNAME', 'admin');
    vi.stubEnv('AMQP_MCP_PASSWORD', 'test-secret-value');
    vi.mocked(readFile).mockClear();
    const started = vi.spyOn(AmqpClientTransport.prototype, 'start').mockResolvedValue(undefined);
    const finished = new Promise<void>((resolve) => {
      vi.spyOn(AmqpClientTransport.prototype, 'close').mockImplementation(async () => {
        resolve();
      });
    });
    vi.spyOn(StdioRelay.prototype, 'close').mockResolvedValue(undefined);
    await runProxy();
    expect(started).toHaveBeenCalledOnce();
    expect(readFile).not.toHaveBeenCalled();
    expect(test.stderr.mock.calls.map((call) => String(call[0])).join('')).not.toContain(
      'test-secret-value',
    );
    test.signals.get('SIGINT')?.();
    await finished;
  });

  it('loads a selected file successfully without merging environment settings', async () => {
    runtime();
    const environment = {
      AMQP_MCP_ENDPOINT: 'amqp://remote.example',
      AMQP_MCP_CONFIG: 'fallback.json',
    };
    vi.mocked(readFile).mockClear();
    const explicit = await loadConfig('selected.json', environment);
    expect(readFile).toHaveBeenCalledWith('selected.json', 'utf8');
    expect(explicit.endpoint).toBe('amqp://localhost');
    const fallback = await loadConfig(undefined, environment);
    expect(readFile).toHaveBeenCalledWith('fallback.json', 'utf8');
    expect(fallback.node).toBe('requests');
  });

  it('wires proxy envelopes to stdio and closes after EOF', async () => {
    const test = runtime();
    const started = vi.spyOn(AmqpClientTransport.prototype, 'start').mockResolvedValue(undefined);
    const sent = vi.spyOn(StdioRelay.prototype, 'send').mockResolvedValue(undefined);
    const forwarded = vi
      .spyOn(AmqpClientTransport.prototype, 'sendEnvelope')
      .mockResolvedValue(undefined);
    const forwardedDone = new Promise<void>((resolve) =>
      forwarded.mockImplementation(async () => {
        resolve();
      }),
    );
    const closed = vi.spyOn(AmqpClientTransport.prototype, 'close').mockResolvedValue(undefined);
    const finished = new Promise<void>((resolve) =>
      vi.mocked(process.stdin.destroy).mockImplementation(() => {
        resolve();
        return process.stdin;
      }),
    );
    await runProxy();
    const client = started.mock.contexts[0] as AmqpClientTransport;
    const response = { jsonrpc: '2.0' as const, id: 1, result: {} };
    client?.onEnvelope?.(response);
    expect(sent).toHaveBeenCalledWith(response);
    test.input.write('{"jsonrpc":"2.0","id":1,"method":"ping"}\n');
    await forwardedDone;
    expect(forwarded).toHaveBeenCalledOnce();
    client?.onerror?.(new Error('delivery failed'));
    expect(test.stderr).toHaveBeenCalled();
    test.signals.get('SIGINT')?.();
    await finished;
    expect(closed).toHaveBeenCalledOnce();
  });

  it('waits for startup before shutdown and never starts stdio after a signal', async () => {
    const test = runtime();
    let release: (() => void) | undefined;
    let entered: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    vi.spyOn(AmqpClientTransport.prototype, 'start').mockImplementation(() => {
      entered?.();
      return new Promise<void>((resolve) => {
        release = resolve;
      });
    });
    const closed = vi.spyOn(AmqpClientTransport.prototype, 'close').mockResolvedValue(undefined);
    const stdioStarted = vi.spyOn(StdioRelay.prototype, 'start').mockImplementation(() => {});
    vi.spyOn(StdioRelay.prototype, 'close').mockResolvedValue(undefined);
    const running = runProxy();
    await started;
    test.signals.get('SIGTERM')?.();
    expect(closed).not.toHaveBeenCalled();
    release?.();
    await running;
    expect(closed).toHaveBeenCalledOnce();
    expect(stdioStarted).not.toHaveBeenCalled();
  });

  it('starts the console responder and performs idempotent close', async () => {
    const test = runtime();
    const started = vi.spyOn(AmqpServer.prototype, 'start').mockResolvedValue(undefined);
    let finish: (() => void) | undefined;
    const finished = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const closed = vi.spyOn(AmqpServer.prototype, 'close').mockImplementation(async () => {
      finish?.();
    });
    await runServer();
    const server = started.mock.contexts[0] as AmqpServer;
    expect(test.stderr).toHaveBeenCalledWith(expect.stringContaining('Listening'));
    server?.onerror?.(new Error('request failed'));
    server?.onclose?.();
    test.signals.get('SIGINT')?.();
    await finished;
    expect(closed).toHaveBeenCalledOnce();
  });

  it('sanitizes startup errors and avoids starting after an early signal', async () => {
    const test = runtime();
    const started = vi
      .spyOn(AmqpClientTransport.prototype, 'start')
      .mockRejectedValue(new Error('secret credential'));
    vi.spyOn(AmqpClientTransport.prototype, 'close').mockResolvedValue(undefined);
    vi.spyOn(StdioRelay.prototype, 'close').mockResolvedValue(undefined);
    await runProxy();
    expect(test.stderr.mock.calls.map((call) => String(call[0])).join('')).not.toContain(
      'secret credential',
    );
    expect(process.exitCode).toBe(1);
    const running = runProxy();
    test.signals.get('SIGINT')?.();
    await running;
    expect(started).toHaveBeenCalledOnce();
  });
});
