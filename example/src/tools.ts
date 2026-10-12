// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE in the parent package.

import {
  SubscriptionsListenRequestParamsSchema,
  SubscriptionsListenResultSchema,
  ToolSchema,
} from '@modelcontextprotocol/core';
import type { Server } from '@modelcontextprotocol/server';
import { StatelessMcpServer } from 'amqp-mcp-transport';
import * as z from 'zod/v4';

const serverInfo = { name: 'amqp-example-tools', version: '0.1.0' };
const echoInput = z.object({ text: z.string().max(65536) });
const addInput = z.object({ left: z.number(), right: z.number() });

export interface ToolExecution {
  phase: 'started' | 'completed';
  tool: 'echo' | 'add' | 'unknown';
  requestId: string | number;
  isError?: boolean;
  sum?: number;
  textLength?: number;
}

/** Creates an SDK server with example-specific echo/add handlers and a fixed catalog. */
export function createToolServer(
  onExecution: (event: ToolExecution) => void = () => {},
  name = serverInfo.name,
): Server {
  const server = new StatelessMcpServer(
    { ...serverInfo, name },
    {
      capabilities: { tools: { listChanged: true } },
    },
  );
  server.setRequestHandler('tools/list', async () => ({
    tools: [
      ToolSchema.parse({
        name: 'echo',
        description: 'Return the supplied text.',
        inputSchema: z.toJSONSchema(echoInput),
        outputSchema: z.toJSONSchema(z.object({ text: z.string() })),
        annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
      }),
      ToolSchema.parse({
        name: 'add',
        description: 'Add two finite numbers.',
        inputSchema: z.toJSONSchema(addInput),
        outputSchema: z.toJSONSchema(z.object({ sum: z.number() })),
        annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
      }),
    ],
  }));
  server.setRequestHandler('tools/call', async (request, context) => {
    const tool =
      request.params.name === 'echo' || request.params.name === 'add'
        ? request.params.name
        : 'unknown';
    const requestId = context.mcpReq.id;
    onExecution({ phase: 'started', tool, requestId });
    const execute = (): {
      isError?: boolean;
      content: { type: 'text'; text: string }[];
      structuredContent?: { text: string } | { sum: number };
    } => {
      if (request.params.name === 'echo') {
        const input = echoInput.safeParse(request.params.arguments);
        if (!input.success) {
          return { isError: true, content: [{ type: 'text', text: 'Invalid echo arguments.' }] };
        }
        return {
          content: [{ type: 'text', text: input.data.text }],
          structuredContent: input.data,
        };
      }
      if (request.params.name === 'add') {
        const input = addInput.safeParse(request.params.arguments);
        if (!input.success) {
          return { isError: true, content: [{ type: 'text', text: 'Invalid add arguments.' }] };
        }
        const sum = input.data.left + input.data.right;
        if (!Number.isFinite(sum)) {
          return {
            isError: true,
            content: [{ type: 'text', text: 'The sum is outside the finite number range.' }],
          };
        }
        return { content: [{ type: 'text', text: String(sum) }], structuredContent: { sum } };
      }
      return { isError: true, content: [{ type: 'text', text: 'Unknown tool.' }] };
    };
    const result = execute();
    onExecution({
      phase: 'completed',
      tool,
      requestId,
      isError: 'isError' in result && result.isError === true,
      ...('structuredContent' in result &&
      result.structuredContent &&
      'sum' in result.structuredContent
        ? { sum: result.structuredContent.sum }
        : {}),
      ...('structuredContent' in result &&
      result.structuredContent &&
      'text' in result.structuredContent
        ? { textLength: result.structuredContent.text.length }
        : {}),
    });
    return result;
  });
  server.setRequestHandler(
    'subscriptions/listen',
    {
      params: SubscriptionsListenRequestParamsSchema,
      result: SubscriptionsListenResultSchema,
    },
    async (params, context) => {
      const metadata = { 'io.modelcontextprotocol/subscriptionId': context.mcpReq.id };
      const notifications = params.notifications.toolsListChanged ? { toolsListChanged: true } : {};
      await context.mcpReq.notify({
        method: 'notifications/subscriptions/acknowledged',
        params: { notifications, _meta: metadata },
      });
      await new Promise<void>((resolve) => {
        if (context.mcpReq.signal.aborted) {
          resolve();
          return;
        }
        context.mcpReq.signal.addEventListener('abort', () => resolve(), { once: true });
      });
      return { _meta: metadata };
    },
  );
  return server;
}
