// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE in the parent package.

import { AmqpServer } from 'amqp-mcp-transport';
import { runApplication } from 'amqp-mcp-transport/cli';
import { createToolServer } from './tools.js';

/** Runs the example server with application-owned handlers and the shared CLI lifecycle. */
export function runServer(): Promise<void> {
  return runApplication('server', (config, onEnd) => {
    const server = new AmqpServer(config, () =>
      createToolServer(
        (event) => {
          process.stderr.write(
            `${JSON.stringify({
              timestamp: new Date().toISOString(),
              level: 'info',
              application: 'server',
              broker: config.broker,
              message: `Tool ${event.phase}.`,
              ...event,
            })}\n`,
          );
        },
        `amqp-${config.broker ?? 'generic'}-tools`,
      ),
    );
    return {
      service: server,
      readyMessage: 'Listening for MCP requests.',
      ready() {
        process.stdin.once('end', onEnd);
        process.stdin.resume();
        return {};
      },
      async close() {
        await server.close();
        process.stdin.off('end', onEnd);
        process.stdin.pause();
      },
    };
  });
}
