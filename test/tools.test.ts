// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { afterEach, describe, expect, it } from 'vitest';
import type { JSONRPCMessage } from '@modelcontextprotocol/server';
import { createToolServer } from '../example/src/tools.js';
import { McpEngine, StatelessMcpServer } from '../src/sdk.js';
import { parseConfig } from '../src/config.js';

const meta = {
  'io.modelcontextprotocol/protocolVersion': '2026-07-28',
  'io.modelcontextprotocol/clientInfo': { name: 'test-client', version: '1' },
  'io.modelcontextprotocol/clientCapabilities': {},
};
const engines: McpEngine[] = [];
afterEach(async () => {
  await Promise.all(engines.splice(0).map((engine) => engine.close()));
});

function engine(maxConcurrentRequests = 32): McpEngine {
  const engine = new McpEngine(
    parseConfig(
      JSON.stringify({ endpoint: 'amqp://localhost', node: 'requests', maxConcurrentRequests }),
    ),
    createToolServer,
  );
  engines.push(engine);
  return engine;
}

async function request(method: string, params: object = {}): Promise<JSONRPCMessage[]> {
  const responses: JSONRPCMessage[] = [];
  await engine().dispatch(
    { jsonrpc: '2.0', id: 1, method, params: { ...params, _meta: meta } },
    new AbortController().signal,
    async (message) => {
      responses.push(message);
    },
  );
  return responses;
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve = () => {};
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

describe('real stateless MCP SDK server', () => {
  it('observes handler execution without logging raw arguments or echoed text', async () => {
    const events: object[] = [];
    const observed = new McpEngine({ maxConcurrentRequests: 2 }, () =>
      createToolServer((event) => events.push(event), 'amqp-rabbitmq-tools'),
    );
    engines.push(observed);
    const responses: JSONRPCMessage[] = [];
    for (const [id, name, args] of [
      [41, 'echo', { text: 'private payload' }],
      [42, 'add', { left: 19, right: 23 }],
      [43, 'add', { left: 'invalid', right: 23 }],
    ] as const) {
      await observed.dispatch(
        {
          jsonrpc: '2.0',
          id,
          method: 'tools/call',
          params: { name, arguments: args, _meta: meta },
        },
        new AbortController().signal,
        async (message) => {
          responses.push(message);
        },
      );
    }
    expect(events).toEqual([
      { phase: 'started', tool: 'echo', requestId: 41 },
      { phase: 'completed', tool: 'echo', requestId: 41, isError: false, textLength: 15 },
      { phase: 'started', tool: 'add', requestId: 42 },
      { phase: 'completed', tool: 'add', requestId: 42, isError: false, sum: 42 },
      { phase: 'started', tool: 'add', requestId: 43 },
      { phase: 'completed', tool: 'add', requestId: 43, isError: true },
    ]);
    expect(JSON.stringify(events)).not.toContain('private payload');
    expect(responses).toHaveLength(3);
  });

  it('dispatches an application-defined catalog through an injected SDK server factory', async () => {
    let created = 0;
    const custom = new McpEngine(
      parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'requests' })),
      () => {
        created += 1;
        const server = new StatelessMcpServer(
          { name: 'custom-server', version: '1' },
          { capabilities: { tools: {} } },
        );
        server.setRequestHandler('tools/list', async () => ({
          tools: [{ name: 'custom', inputSchema: { type: 'object' } }],
        }));
        return server;
      },
    );
    engines.push(custom);
    const responses: JSONRPCMessage[] = [];
    await custom.dispatch(
      { jsonrpc: '2.0', id: 1, method: 'tools/list', params: { _meta: meta } },
      new AbortController().signal,
      async (message) => {
        responses.push(message);
      },
    );
    expect(created).toBe(1);
    expect(responses).toMatchObject([{ id: 1, result: { tools: [{ name: 'custom' }] } }]);
  });

  it('discovers MCP 2026-07-28 without initialize or session state', async () => {
    const responses = await request('server/discover');
    expect(responses).toHaveLength(1);
    expect(responses[0]).toMatchObject({ id: 1, result: { supportedVersions: ['2026-07-28'] } });
  });

  it('lists the echo and add tools and validates arguments', async () => {
    expect(await request('tools/list')).toMatchObject([
      { result: { tools: [{ name: 'echo' }, { name: 'add' }] } },
    ]);
    expect(
      await request('tools/call', { name: 'echo', arguments: { text: 'hello' } }),
    ).toMatchObject([
      {
        result: {
          content: [{ type: 'text', text: 'hello' }],
          structuredContent: { text: 'hello' },
        },
      },
    ]);
    expect(
      await request('tools/call', { name: 'add', arguments: { left: 17, right: 25 } }),
    ).toMatchObject([{ result: { structuredContent: { sum: 42 } } }]);
    expect(
      await request('tools/call', { name: 'add', arguments: { left: 'invalid', right: 1 } }),
    ).toMatchObject([{ result: { isError: true } }]);
  });

  it('reports unknown methods and arithmetic overflow', async () => {
    expect(await request('unknown')).toMatchObject([{ error: { code: -32601 } }]);
    expect(await request('tools/call', { name: 'missing', arguments: {} })).toMatchObject([
      { result: { isError: true } },
    ]);
    expect(await request('tools/call', { name: 'echo', arguments: { text: 42 } })).toMatchObject([
      { result: { isError: true } },
    ]);
    expect(
      await request('tools/call', {
        name: 'add',
        arguments: { left: Number.MAX_VALUE, right: Number.MAX_VALUE },
      }),
    ).toMatchObject([{ result: { isError: true } }]);
  });

  it('rejects requests without protocol metadata', async () => {
    const responses: JSONRPCMessage[] = [];
    await engine().dispatch(
      { jsonrpc: '2.0', id: 3, method: 'tools/list' },
      new AbortController().signal,
      async (message) => {
        responses.push(message);
      },
    );
    expect(responses).toMatchObject([{ error: { code: -32602 } }]);
  });

  it('streams the real subscription acknowledgement and stops when aborted', async () => {
    const server = engine();
    const controller = new AbortController();
    const responses: JSONRPCMessage[] = [];
    const acknowledged = deferred();
    const listening = server.dispatch(
      {
        jsonrpc: '2.0',
        id: 'listen',
        method: 'subscriptions/listen',
        params: {
          notifications: {
            toolsListChanged: true,
            promptsListChanged: true,
            resourceSubscriptions: ['unavailable'],
          },
          _meta: meta,
        },
      },
      controller.signal,
      async (message) => {
        responses.push(message);
        acknowledged.resolve();
      },
    );
    await acknowledged.promise;
    expect(responses[0]).toMatchObject({
      method: 'notifications/subscriptions/acknowledged',
      params: {
        notifications: { toolsListChanged: true },
        _meta: { 'io.modelcontextprotocol/subscriptionId': 'listen' },
      },
    });
    if (responses[0] && 'params' in responses[0]) {
      expect(responses[0].params?.notifications).toEqual({ toolsListChanged: true });
    }
    controller.abort();
    await listening;
  });

  it('rejects invalid subscription filters and unsupported protocol versions', async () => {
    expect(await request('subscriptions/listen')).toMatchObject([{ error: { code: -32602 } }]);
    const responses: JSONRPCMessage[] = [];
    await engine().dispatch(
      {
        jsonrpc: '2.0',
        id: 1,
        method: 'ping',
        params: { _meta: { ...meta, 'io.modelcontextprotocol/protocolVersion': 'unsupported' } },
      },
      new AbortController().signal,
      async (message) => {
        responses.push(message);
      },
    );
    expect(responses).toMatchObject([{ error: { code: -32601 } }]);
  });

  it('isolates concurrent same-ID calls and awaits delivery without blocking another call', async () => {
    const server = engine();
    const started = deferred();
    const release = deferred();
    const responses: JSONRPCMessage[][] = [[], []];
    let firstCompleted = false;
    const first = server
      .dispatch(
        {
          jsonrpc: '2.0',
          id: 1,
          method: 'tools/call',
          params: { name: 'echo', arguments: { text: 'first' }, _meta: meta },
        },
        new AbortController().signal,
        async (message) => {
          responses[0]?.push(message);
          started.resolve();
          await release.promise;
        },
      )
      .then(() => {
        firstCompleted = true;
      });
    try {
      await started.promise;
      await server.dispatch(
        {
          jsonrpc: '2.0',
          id: 1,
          method: 'tools/call',
          params: { name: 'echo', arguments: { text: 'second' }, _meta: meta },
        },
        new AbortController().signal,
        async (message) => {
          responses[1]?.push(message);
        },
      );
      expect(firstCompleted).toBe(false);
      expect(responses).toMatchObject([
        [{ id: 1, result: { structuredContent: { text: 'first' } } }],
        [{ id: 1, result: { structuredContent: { text: 'second' } } }],
      ]);
    } finally {
      release.resolve();
      await first;
    }
  });

  it('bounds in-flight exchanges and closes open subscriptions on shutdown', async () => {
    const server = engine(1);
    const acknowledged = deferred();
    const listening = server.dispatch(
      {
        jsonrpc: '2.0',
        id: 1,
        method: 'subscriptions/listen',
        params: { notifications: {}, _meta: meta },
      },
      new AbortController().signal,
      async (message) => {
        expect(message).toMatchObject({ params: { notifications: {} } });
        acknowledged.resolve();
      },
    );
    await acknowledged.promise;
    await expect(
      server.dispatch(
        { jsonrpc: '2.0', id: 1, method: 'ping', params: { _meta: meta } },
        new AbortController().signal,
        async () => {},
      ),
    ).rejects.toThrow('concurrency limit');
    await server.close();
    await listening;
    await expect(
      server.dispatch(
        { jsonrpc: '2.0', id: 1, method: 'ping', params: { _meta: meta } },
        new AbortController().signal,
        async () => {},
      ),
    ).rejects.toThrow('closed');
  });

  it('propagates reply delivery failure and does not dispatch already-aborted calls', async () => {
    const server = engine();
    await expect(
      server.dispatch(
        { jsonrpc: '2.0', id: 1, method: 'ping', params: { _meta: meta } },
        new AbortController().signal,
        async () => {
          throw new Error('Delivery failed.');
        },
      ),
    ).rejects.toThrow();
    const controller = new AbortController();
    controller.abort();
    await server.dispatch(
      { jsonrpc: '2.0', id: 1, method: 'ping' },
      controller.signal,
      async () => {
        throw new Error('Must not emit after abort.');
      },
    );
  });
});
