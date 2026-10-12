// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';

export default defineConfig({
  resolve: {
    alias: [
      {
        find: 'amqp-mcp-transport/cli',
        replacement: fileURLToPath(new URL('./src/runtime.ts', import.meta.url)),
      },
      {
        find: 'amqp-mcp-transport',
        replacement: fileURLToPath(new URL('./src/index.ts', import.meta.url)),
      },
    ],
  },
  test: {
    include: ['test/**/*.test.ts'],
    testTimeout: 15000,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts', 'example/src/**/*.ts'],
      exclude: ['src/proxy.ts', 'example/src/server.ts'],
      thresholds: { statements: 90, lines: 90, functions: 90, branches: 80 },
    },
  },
});
