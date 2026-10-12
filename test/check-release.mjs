// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import console from 'node:console';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import process from 'node:process';
import { fileURLToPath, URL } from 'node:url';
import ts from 'typescript';

const root = fileURLToPath(new URL('../', import.meta.url));
const npmCli = join(dirname(process.execPath), 'node_modules', 'npm', 'bin', 'npm-cli.js');
const read = (path) => readFileSync(join(root, path), 'utf8');
const typeConfig = ts.readConfigFile(join(root, 'tsconfig.json'), ts.sys.readFile);
assert(!typeConfig.error, 'Could not read the TypeScript configuration.');
const testConfig = ts.parseJsonConfigFileContent(
  {
    ...typeConfig.config,
    compilerOptions: { ...typeConfig.config.compilerOptions, rootDir: '.', noEmit: true },
    include: ['src/**/*.ts', 'example/src/**/*.ts', 'test/**/*.ts'],
  },
  ts.sys,
  root,
);
const program = ts.createProgram(testConfig.fileNames, testConfig.options);
const diagnostics = [...testConfig.errors, ...ts.getPreEmitDiagnostics(program)];
assert.equal(
  diagnostics.length,
  0,
  ts.formatDiagnosticsWithColorAndContext(diagnostics, {
    getCanonicalFileName: (path) => path,
    getCurrentDirectory: () => root,
    getNewLine: () => '\n',
  }),
);
console.log('Strict source, example, and test typecheck passed.');
const manifest = JSON.parse(read('package.json'));
const lock = JSON.parse(read('package-lock.json'));
assert.equal(manifest.name, 'amqp-mcp-transport');
assert.equal(manifest.license, 'Apache-2.0');
assert.equal(lock.packages[''].license, manifest.license);
assert.equal(manifest.private, false);
assert.equal(manifest.publishConfig.access, 'public');
assert.equal(manifest.scripts.prepublishOnly, 'npm run verify');
assert.deepEqual(Object.keys(manifest.dependencies).sort(), ['rhea-promise', 'zod']);
const example = JSON.parse(read('example/package.json'));
for (const dependency of ['@modelcontextprotocol/core', '@modelcontextprotocol/server']) {
  assert.equal(manifest.peerDependencies[dependency], manifest.devDependencies[dependency]);
  assert.equal(example.dependencies[dependency], manifest.peerDependencies[dependency]);
}
assert.equal(
  manifest.devDependencies['@modelcontextprotocol/client'],
  manifest.peerDependencies['@modelcontextprotocol/core'],
);
assert.deepEqual(Object.keys(manifest.bin), ['amqp-mcp-proxy']);

const [packed] = JSON.parse(
  execFileSync(process.execPath, [npmCli, 'pack', '--dry-run', '--json'], {
    cwd: root,
    encoding: 'utf8',
  }),
);
const paths = new Set(packed.files.map((file) => file.path));
for (const required of [
  'README.md',
  'LICENSE',
  'NOTICE',
  'package.json',
  ...Object.values(manifest.bin),
]) {
  assert(paths.has(required), `Release is missing ${required}.`);
}
for (const entry of Object.values(manifest.exports)) {
  for (const target of typeof entry === 'string' ? [entry] : Object.values(entry)) {
    assert(paths.has(target.slice(2)), `Release is missing exported entry ${target}.`);
  }
}
for (const path of paths) {
  assert(
    /^(dist\/|src\/|README\.md$|LICENSE$|NOTICE$|package\.json$)/.test(path),
    `Unexpected release file: ${path}.`,
  );
  if (path.endsWith('.js') || (path.startsWith('src/') && path.endsWith('.ts'))) {
    const lines = read(path).split(/\r?\n/);
    const headerLine = lines[0].startsWith('#!') ? 1 : 0;
    assert.equal(
      lines[headerLine],
      '// SPDX-License-Identifier: Apache-2.0',
      `Missing header: ${path}.`,
    );
  }
  if (path.endsWith('.js.map')) {
    const map = JSON.parse(read(path));
    assert(map.sourcesContent?.length, `Missing embedded source: ${path}.`);
    assert(
      map.sourcesContent.every((source) => source.includes('SPDX-License-Identifier: Apache-2.0')),
    );
  }
  if (path.endsWith('.d.ts.map')) {
    const map = JSON.parse(read(path));
    assert(map.sources.length > 0, `Missing declaration source: ${path}.`);
  }
}
for (const bin of Object.values(manifest.bin))
  assert(read(bin).startsWith('#!/usr/bin/env node\n'));
assert(read('dist/transport.d.ts').includes('Connection loss is terminal.'));
assert(read('dist/wire.d.ts').includes('without type coercion'));
console.log(
  `Release checks passed: ${paths.size} allowlisted files, Apache metadata, headers, shebangs, API comments, and maps.`,
);

const temporary = mkdtempSync(join(tmpdir(), 'amqp-mcp-package-'));
try {
  const [archive] = JSON.parse(
    execFileSync(process.execPath, [npmCli, 'pack', '--json', '--pack-destination', temporary], {
      cwd: root,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    }),
  );
  const consumer = join(temporary, 'consumer');
  mkdirSync(consumer);
  writeFileSync(
    join(consumer, 'package.json'),
    JSON.stringify({
      name: 'amqp-mcp-release-verification',
      version: '0.0.0',
      private: true,
      type: 'module',
    }),
  );
  const nodeTypes = JSON.parse(read('node_modules/@types/node/package.json')).version;
  const installFlags = ['--ignore-scripts', '--no-audit', '--no-fund'];
  execFileSync(
    process.execPath,
    [npmCli, 'install', join(temporary, archive.filename), ...installFlags],
    { cwd: consumer, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  );
  const inventory = JSON.parse(
    execFileSync(
      process.execPath,
      [npmCli, 'sbom', '--sbom-format', 'cyclonedx', '--omit', 'dev'],
      {
        cwd: consumer,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    ),
  );
  assert.equal(inventory.bomFormat, 'CycloneDX');
  assert(
    inventory.components.some(
      (component) => component.name === manifest.name && component.version === manifest.version,
    ),
  );
  for (const component of inventory.components) {
    if (component.externalReferences) {
      component.externalReferences = component.externalReferences.filter(
        (reference) => !reference.url.startsWith('file:'),
      );
    }
  }
  assert(
    !/\b[A-Z]:[\\/]/i.test(JSON.stringify(inventory)),
    'Inventory contains a local drive path.',
  );
  mkdirSync(join(root, 'tmp'), { recursive: true });
  writeFileSync(join(root, 'tmp/sbom.cdx.json'), `${JSON.stringify(inventory, null, 2)}\n`);
  execFileSync(
    process.execPath,
    [
      npmCli,
      'install',
      '--save-dev',
      `@modelcontextprotocol/client@${manifest.devDependencies['@modelcontextprotocol/client']}`,
      `@types/node@${nodeTypes}`,
      ...installFlags,
    ],
    { cwd: consumer, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  );
  writeFileSync(
    join(consumer, 'consumer.mts'),
    `import assert from 'node:assert/strict';
import { Client } from '@modelcontextprotocol/client';
import { Server, type Transport } from '@modelcontextprotocol/server';
import {
  AmqpClientTransport, AmqpServer, StatelessMcpServer,
  MCP_PROTOCOL_VERSION, parseConfig, type McpServerFactory,
} from 'amqp-mcp-transport';
import { runApplication, runProxy } from 'amqp-mcp-transport/cli';

const config = parseConfig(JSON.stringify({ endpoint: 'amqp://localhost', node: 'requests' }));
const createServer: McpServerFactory = () => new StatelessMcpServer(
  { name: 'consumer', version: '1' }, { capabilities: { tools: {} } },
);
const server = createServer();
const transport: Transport = new AmqpClientTransport(config);
const listener = new AmqpServer(config, createServer);
const client = new Client(
  { name: 'consumer', version: '1' },
  { versionNegotiation: { mode: { pin: MCP_PROTOCOL_VERSION } } },
);
assert(server instanceof Server);
assert.equal(MCP_PROTOCOL_VERSION, '2026-07-28');
assert.equal(typeof runApplication, 'function');
assert.equal(typeof runProxy, 'function');
assert.equal(typeof client.connect, 'function');
await transport.close();
await listener.close();
await server.close();
`,
  );
  execFileSync(
    process.execPath,
    [
      join(root, 'node_modules/typescript/bin/tsc'),
      'consumer.mts',
      '--strict',
      '--exactOptionalPropertyTypes',
      '--noImplicitOverride',
      '--module',
      'NodeNext',
      '--target',
      'ES2022',
      '--skipLibCheck',
      '--outDir',
      'build',
    ],
    { cwd: consumer, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  );
  execFileSync(process.execPath, ['build/consumer.mjs'], {
    cwd: consumer,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  console.log(
    'Standalone archive installed, typechecked, and imported without the example package.',
  );
  console.log('CycloneDX inventory generated from the isolated archive installation.');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
