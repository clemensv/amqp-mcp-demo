// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { resolve } from 'node:path';
import process from 'node:process';
import { setTimeout, clearTimeout } from 'node:timers';
import { fileURLToPath, pathToFileURL, URL } from 'node:url';
import { parseArgs } from 'node:util';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport } from '@modelcontextprotocol/client/stdio';
import { AmqpConnection } from '../dist/auth.js';
import { parseConfig } from '../dist/config.js';
import { temporaryQueueOptions } from '../dist/wire.js';

const project = fileURLToPath(new URL('../', import.meta.url));
const { values } = parseArgs({
  options: {
    bootstrap: { type: 'boolean' },
    'require-all': { type: 'boolean' },
    broker: { type: 'string' },
    manifest: { type: 'string' },
    out: { type: 'string' },
  },
});
const output = resolve(values.out ?? resolve(project, 'tmp/mcp-amqp-demo'));
const version = '2026-07-28';
const meta = {
  'io.modelcontextprotocol/protocolVersion': version,
  'io.modelcontextprotocol/clientInfo': { name: 'amqp-demo', version: '1.0.0' },
  'io.modelcontextprotocol/clientCapabilities': {},
};
const report = {
  runId: randomUUID(),
  timestamp: new Date().toISOString(),
  protocolVersion: version,
  results: [],
};

async function bounded(operation, ms, label) {
  let timer;
  try {
    return await Promise.race([
      operation,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} exceeded ${ms} ms.`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function credentials(target) {
  const environment = { ...process.env };
  if (target.id === 'servicebus') {
    target.config.endpoint = environment.AMQP_DEMO_SERVICEBUS_ENDPOINT ?? target.config.endpoint;
    target.config.sasl.username =
      environment.AMQP_DEMO_SERVICEBUS_POLICY ?? target.config.sasl.username;
  }
  if (values.bootstrap && target.container) {
    const container = JSON.parse(
      execFileSync('docker', ['inspect', target.container], { encoding: 'utf8' }),
    )[0];
    target.image = container.Config.Image;
    const variables = Object.fromEntries(
      container.Config.Env.map((entry) => {
        const separator = entry.indexOf('=');
        return [entry.slice(0, separator), entry.slice(separator + 1)];
      }),
    );
    const names =
      target.id === 'artemis'
        ? ['ARTEMIS_USER', 'ARTEMIS_PASSWORD']
        : ['RABBITMQ_DEFAULT_USER', 'RABBITMQ_DEFAULT_PASS'];
    if (target.id === 'classic')
      environment[target.config.sasl.passwordEnv] ??=
        variables[target.config.sasl.passwordEnv] ?? 'admin';
    else if (target.id === 'ibmmq') {
      environment[target.config.sasl.passwordEnv] ??= execFileSync(
        'docker',
        ['exec', target.container, 'cat', '/run/secrets/mqAppPassword'],
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
      ).trim();
    } else {
      target.config.sasl.username = variables[names[0]];
      environment[target.config.sasl.passwordEnv] ??= variables[names[1]];
    }
  }
  if (!environment[target.config.sasl.passwordEnv])
    throw new Error(
      `Set ${target.config.sasl.passwordEnv}; --bootstrap can retrieve local Docker credentials.`,
    );
  return environment;
}

function diagnostic(stream, result, ready) {
  const lines = createInterface({ input: stream });
  lines.on('line', (line) => {
    try {
      const entry = JSON.parse(line);
      result.diagnostics.push(entry);
      if (entry.replyAddress) result.replyAddresses.push(entry.replyAddress);
      if (entry.level === 'info') ready?.();
    } catch {
      let text = line.slice(0, 2000);
      for (const [name, secret] of Object.entries(process.env)) {
        if (name.startsWith('AMQP_DEMO_') && secret) text = text.replaceAll(secret, '[redacted]');
      }
      result.diagnostics.push({
        level: /Warning:|^\(Use /u.test(text) ? 'warning' : 'error',
        message: text,
      });
    }
  });
  return lines;
}

async function requestQueue(target, cleanup, config) {
  const name = `amqp-mcp-demo-${report.runId}`;
  if (target.id === 'classic') {
    return config.node;
  }
  if (['artemis', 'rabbitmq'].includes(target.id)) {
    const execute = (script) =>
      execFileSync('docker', ['exec', target.container, 'sh', '-c', script, 'sh', name], {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
      });
    if (target.id === 'artemis') {
      const command = '/var/lib/artemis-instance/bin/artemis queue';
      const auth =
        '--silent --user="$ARTEMIS_USER" --password="$ARTEMIS_PASSWORD" --url=tcp://localhost:61616 --name="$1"';
      execute(`${command} create ${auth} --anycast --auto-create-address --no-durable`);
      cleanup.push(() => execute(`${command} delete ${auth}`));
    } else {
      const command =
        'rabbitmqadmin --username "$RABBITMQ_DEFAULT_USER" --password "$RABBITMQ_DEFAULT_PASS" queues';
      execute(
        `${command} declare --non-interactive --name "$1" --durable true --auto-delete false`,
      );
      cleanup.push(() => execute(`${command} delete --non-interactive --name "$1"`));
    }
    return target.id === 'rabbitmq' ? `/queues/${name}` : name;
  }
  const connection = await AmqpConnection.open(config, () => {});
  cleanup.push(() => connection.close());
  const owner = await connection.createOwnerSender(
    temporaryQueueOptions(),
    config.operationTimeoutMs,
  );
  assert.ok(owner.target?.address, 'Request queue address must be assigned.');
  return owner.target.address;
}

function child(application, configFile, environment, result) {
  const entry = application === 'server' ? 'example/dist/server.js' : 'dist/proxy.js';
  const processChild = spawn(process.execPath, [entry, '--config', configFile], {
    cwd: project,
    env: environment,
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  const exited = new Promise((resolveExit) => {
    processChild.once('exit', (code) => resolveExit(code));
    processChild.once('error', () => resolveExit(-1));
  });
  const ready = new Promise((resolveReady, reject) => {
    diagnostic(processChild.stderr, result, resolveReady);
    processChild.once('error', reject);
    processChild.once('exit', (code) =>
      reject(new Error(`${application} exited before ready (code ${code}).`)),
    );
  });
  return {
    process: processChild,
    ready,
    async close() {
      processChild.stdin.end();
      const code = await bounded(exited, 60000, `${application} graceful shutdown`);
      assert.equal(code, 0, `${application} exit code`);
    },
  };
}

async function sdkClient(configFile, environment, result) {
  const stdio = new StdioClientTransport({
    command: process.execPath,
    args: ['dist/proxy.js', '--config', configFile],
    cwd: project,
    env: environment,
    stderr: 'pipe',
  });
  diagnostic(stdio.stderr, result);
  const transport = {
    start: () => stdio.start(),
    close: () => stdio.close(),
    send: async (message) => {
      result.trace.push({ direction: 'request', message });
      await stdio.send(message);
    },
    set onmessage(handler) {
      stdio.onmessage = (message) => {
        result.trace.push({ direction: 'response', message });
        handler?.(message);
      };
    },
    set onerror(handler) {
      stdio.onerror = handler;
    },
    set onclose(handler) {
      stdio.onclose = handler;
    },
  };
  const client = new Client(
    { name: 'amqp-demo-sdk', version: '1.0.0' },
    { versionNegotiation: { mode: { pin: version } } },
  );
  try {
    await bounded(client.connect(transport), 15000, 'MCP SDK discovery');
  } catch (error) {
    await stdio.close();
    throw error;
  }
  return client;
}

function rawClient(configFile, environment, result) {
  const proxy = child('proxy', configFile, environment, result);
  const pending = new Set();
  const lines = createInterface({ input: proxy.process.stdout });
  lines.on('line', (line) => {
    const message = JSON.parse(line);
    result.trace.push({ direction: 'response', message });
    for (const waiter of [...pending])
      if (waiter.match(message)) {
        pending.delete(waiter);
        waiter.resolve(message);
      }
  });
  return {
    ...proxy,
    wait(match) {
      let waiter;
      const operation = new Promise((resolveMessage) => {
        waiter = { match, resolve: resolveMessage };
        pending.add(waiter);
      });
      return bounded(operation, 15000, 'Raw JSON-RPC response').finally(() =>
        pending.delete(waiter),
      );
    },
    async send(message) {
      result.trace.push({ direction: 'request', message });
      await new Promise((resolveWrite, reject) =>
        proxy.process.stdin.write(`${JSON.stringify(message)}\n`, (error) =>
          error ? reject(error) : resolveWrite(),
        ),
      );
    },
  };
}

async function exercise(target) {
  const result = {
    broker: target.name,
    id: target.id,
    status: 'FAIL',
    checks: [],
    diagnostics: [],
    trace: [],
    replyAddresses: [],
  };
  report.results.push(result);
  const cleanup = [];
  const started = Date.now();
  const check = (name) => {
    result.checks.push(name);
    process.stdout.write(`  PASS ${name}\n`);
  };
  let environment;
  try {
    environment = credentials(target);
    process.stdout.write(`RUN ${target.name} ${target.config.endpoint}\n`);
    if (target.config.sasl.passwordEnv) {
      process.env[target.config.sasl.passwordEnv] = environment[target.config.sasl.passwordEnv];
    }
    const config = parseConfig(
      JSON.stringify({
        ...target.config,
        requestTimeoutMs: 10000,
        operationTimeoutMs: 8000,
        subscriptionTimeoutMs: 1000,
      }),
    );
    result.authentication = { mechanism: `SASL ${config.sasl.mode}` };
    result.requestAddress = await requestQueue(target, cleanup, config);
    result.requestQueueLifetime =
      target.id === 'classic'
        ? 'Dedicated ordinary queue retained; management policy denies deletion.'
        : 'Demo-owned queue removed on shutdown.';
    result.endpoint = config.endpoint;
    result.image = target.image;
    check('Dedicated request queue assigned');
    const configFile = resolve(output, `${target.id}.json`);
    await writeFile(
      configFile,
      JSON.stringify({ ...config, node: result.requestAddress }, null, 2),
    );
    const server = child('server', configFile, environment, result);
    cleanup.push(() => server.close());
    await bounded(server.ready, 15000, 'Responder ready');
    const client = await sdkClient(configFile, environment, result);
    cleanup.push(() => client.close());
    assert.equal(
      result.trace.some((entry) => entry.message.method === 'initialize'),
      false,
    );
    assert.ok(result.trace.some((entry) => entry.message.method === 'server/discover'));
    check('Official MCP SDK discovery: 2026-07-28, no initialize');
    const tools = await client.listTools();
    assert.deepEqual(tools.tools.map((tool) => tool.name).sort(), ['add', 'echo']);
    const echo = await client.callTool({
      name: 'echo',
      arguments: { text: `${target.name} live stdio -> AMQP -> MCP` },
    });
    assert.equal(echo.structuredContent.text, `${target.name} live stdio -> AMQP -> MCP`);
    check('Real tools/list and echo through the stdio proxy');
    const calls = await Promise.all(
      Array.from({ length: 12 }, (_, index) =>
        client.callTool({ name: 'add', arguments: { left: index, right: 42 } }),
      ),
    );
    calls.forEach((answer, index) => assert.equal(answer.structuredContent.sum, index + 42));
    check('12 concurrent SDK tool calls with exact results');
    const raw = rawClient(configFile, environment, result);
    cleanup.push(() => raw.close());
    await bounded(raw.ready, 15000, 'Raw proxy ready');
    const acknowledgement = raw.wait(
      (message) => message.method === 'notifications/subscriptions/acknowledged',
    );
    const complete = raw.wait((message) => message.id === 'listen');
    await raw.send({
      jsonrpc: '2.0',
      id: 'listen',
      method: 'subscriptions/listen',
      params: { notifications: { toolsListChanged: true }, _meta: meta },
    });
    await acknowledgement;
    const batchReply = raw.wait(Array.isArray);
    await raw.send([
      {
        jsonrpc: '2.0',
        id: 'batch-add',
        method: 'tools/call',
        params: { name: 'add', arguments: { left: 20, right: 22 }, _meta: meta },
      },
      { jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 'absent' } },
      {
        jsonrpc: '2.0',
        id: 'batch-echo',
        method: 'tools/call',
        params: { name: 'echo', arguments: { text: 'batch' }, _meta: meta },
      },
    ]);
    const batch = await batchReply;
    assert.equal(batch.length, 2);
    assert.equal(
      batch.find((message) => message.id === 'batch-add').result.structuredContent.sum,
      42,
    );
    assert.equal(
      batch.find((message) => message.id === 'batch-echo').result.structuredContent.text,
      'batch',
    );
    check('Mixed batch: one array response, notification omitted');
    assert.equal((await complete).result.resultType, 'complete');
    const responses = result.trace.filter((entry) => entry.direction === 'response');
    assert.ok(
      responses.findIndex((entry) => Array.isArray(entry.message)) <
        responses.findIndex((entry) => entry.message.id === 'listen'),
    );
    check('Out-of-order completion without stdio write interleaving');
    check('Subscription acknowledgement and bounded complete result');
    const cancelAck = raw.wait(
      (message) => message.method === 'notifications/subscriptions/acknowledged',
    );
    const cancelled = raw.wait((message) => message.id === 'cancel-me');
    await raw.send({
      jsonrpc: '2.0',
      id: 'cancel-me',
      method: 'subscriptions/listen',
      params: { notifications: { toolsListChanged: true }, _meta: meta },
    });
    await cancelAck;
    await raw.send({
      jsonrpc: '2.0',
      method: 'notifications/cancelled',
      params: { requestId: 'cancel-me' },
    });
    assert.equal((await cancelled).error.message, 'Request cancelled.');
    check('Cancellation delivered through the request queue');
    assert.equal(new Set(result.replyAddresses).size, 2);
    check('Two distinct sender-owned reply queues retained across exchanges');
    assert.equal(result.diagnostics.filter((entry) => entry.level === 'error').length, 0);
    result.status = 'PASS';
  } catch (error) {
    let text = String(error?.message ?? error?.condition ?? 'AMQP demo failed.');
    const secret = environment?.[target.config.sasl.passwordEnv];
    if (secret) text = text.replaceAll(secret, '[redacted]');
    result.error = text;
    result.status = result.checks.length ? 'FAIL' : 'BLOCKED';
    process.stdout.write(`  ${result.status} ${text}\n`);
  } finally {
    for (const close of cleanup.reverse()) {
      try {
        await close();
      } catch (error) {
        result.status = 'FAIL';
        result.cleanupError = `Demo-owned resource cleanup failed: ${String(error.message)}`;
      }
    }
    if (result.status === 'PASS' && result.diagnostics.some((entry) => entry.level === 'error')) {
      result.status = 'FAIL';
      result.error = 'Application reported an error during shutdown.';
    }
    if (result.status === 'PASS') check('Graceful process shutdown and demo-owned queue cleanup');
    result.elapsedMs = Date.now() - started;
  }
}

function html() {
  const escape = (value) =>
    String(value).replace(
      /[&<>"']/g,
      (character) =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character],
    );
  const passed = report.results.filter((result) => result.status === 'PASS').length;
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AMQP MCP live interoperability</title><style>
  :root{font-family:Bahnschrift,'Segoe UI',sans-serif;color:#192b29;background:#f5f7f8;font-size:15px}*{box-sizing:border-box}body{margin:0}header{background:#123e35;color:white;padding:30px max(20px,calc((100% - 1100px)/2));border-bottom:5px solid #e8b440}h1{font-size:28px;line-height:1.2;margin:0 0 10px}h2{font-size:20px;margin:0}p{line-height:1.5}main{max-width:1140px;margin:auto;padding:24px 20px}.summary{font-size:23px;color:#166447;margin:0 0 20px}.table{overflow:auto}table{border-collapse:collapse;width:100%;background:white}th,td{text-align:left;padding:14px;border-bottom:1px solid #d4dedb}th{font-weight:600;background:#e9efed}.PASS{color:#176b45}.FAIL{color:#b22c35}.BLOCKED{color:#8b6100}.status{font-weight:700}section{padding:25px 0;border-bottom:1px solid #ccd8d3}section h2{display:flex;gap:15px;flex-wrap:wrap}li{padding:4px 0;line-height:1.35}code,pre{font-family:Consolas,monospace}code{overflow-wrap:anywhere}pre{font-size:12px;line-height:1.5;background:#172b28;color:#eaf4ef;padding:16px;overflow:auto;max-height:420px;max-width:100%}summary{cursor:pointer;padding:12px 0;font-weight:600}.meta{color:#576c65;font-size:13px}header .meta{color:#cfe0db}.route{overflow-wrap:anywhere}@media(max-width:600px){h1{font-size:24px}th,td{padding:10px}main{padding:20px 14px}header{padding:24px 14px}}</style><header><h1>AMQP MCP Interoperability</h1><div>Official MCP client &rarr; stdio proxy &rarr; AMQP broker &rarr; MCP tool server</div><p class="meta">${escape(report.timestamp)} &middot; protocol ${version}<br>Run ${escape(report.runId)}</p></header><main><p class="summary">${passed} / ${report.results.length} brokers passed real Node exchanges</p><div class="table"><table><thead><tr><th>Broker</th><th>Result</th><th>Checks</th><th>Elapsed</th></tr></thead><tbody>${report.results.map((result) => `<tr><td>${escape(result.broker)}</td><td class="status ${result.status}">${result.status}</td><td>${result.checks.length}</td><td>${(result.elapsedMs / 1000).toFixed(2)} s</td></tr>`).join('')}</tbody></table></div>${report.results.map((result) => `<section><h2>${escape(result.broker)} <span class="status ${result.status}">${result.status}</span></h2><p class="meta">${escape(result.endpoint ?? '')} ${escape(result.image ?? '')}</p>${result.error ? `<p>${escape(result.error)}</p>` : ''}${result.cleanupError ? `<p>${escape(result.cleanupError)}</p>` : ''}<ul>${result.checks.map((check) => `<li>${escape(check)}</li>`).join('')}</ul><p class="route">Request queue: <code>${escape(result.requestAddress ?? 'not assigned')}</code><br>${escape(result.requestQueueLifetime ?? '')}<br>Reply queues: ${result.replyAddresses.map((address) => `<code>${escape(address)}</code>`).join('<br>')}</p><details><summary>${result.trace.length} captured stdio messages</summary><pre>${escape(JSON.stringify(result.trace, null, 2))}</pre></details><details><summary>Process diagnostics</summary><pre>${escape(JSON.stringify(result.diagnostics, null, 2))}</pre></details></section>`).join('')}</main></html>`;
}

await mkdir(output, { recursive: true });
const manifest = values.manifest
  ? pathToFileURL(resolve(values.manifest))
  : new URL('./brokers.json', import.meta.url);
const entries = JSON.parse(await readFile(manifest, 'utf8'));
assert.ok(Array.isArray(entries), 'Broker manifest must be an array.');
const targets = await Promise.all(
  entries.map(async (entry) =>
    typeof entry === 'string'
      ? JSON.parse(await readFile(new URL(entry, manifest), 'utf8'))
      : entry,
  ),
);
const selected = targets.filter((target) => !values.broker || values.broker === target.id);
assert.ok(selected.length, 'No matching broker.');
for (const target of selected) await exercise(target);
await writeFile(resolve(output, 'results.json'), JSON.stringify(report, null, 2));
const rendered = html();
await writeFile(resolve(output, 'report.html'), rendered);
await writeFile(
  resolve(output, 'capture.js'),
  `async (page) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.setContent(${JSON.stringify(rendered)});
  await page.screenshot({ path: ${JSON.stringify(resolve(output, 'desktop.png'))} });
  const desktop = await page.evaluate(() => ({ width: innerWidth, content: document.documentElement.scrollWidth,
    rows: [...document.querySelectorAll('tbody tr')].map(row => row.innerText) }));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: ${JSON.stringify(resolve(output, 'mobile.png'))}, fullPage: true });
  const mobile = await page.evaluate(() => ({ width: innerWidth, content: document.documentElement.scrollWidth }));
  await page.setViewportSize({ width: 1100, height: 1200 });
  const section = page.locator('section').filter({ has: page.getByRole('heading', { name: /^Service Bus Premium / }) });
  if (await section.count()) {
    await section.locator('details').first().evaluate(element => { element.open = true; });
    await section.scrollIntoViewIfNeeded();
    await section.screenshot({ path: ${JSON.stringify(resolve(output, 'servicebus-live.png'))} });
  }
  return { desktop, mobile, title: await page.title() };
}`,
);
process.stdout.write(`REPORT ${resolve(output, 'report.html')}\n`);
process.exitCode = report.results.some(
  (result) => result.status === 'FAIL' || (values['require-all'] && result.status !== 'PASS'),
)
  ? 1
  : 0;
