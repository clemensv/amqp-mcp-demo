// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import console from 'node:console';
import { randomBytes } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import process from 'node:process';
import { fileURLToPath, URL } from 'node:url';
import { parseArgs } from 'node:util';

const { values } = parseArgs({
  options: { broker: { type: 'string' }, check: { type: 'boolean' } },
});
assert.ok(
  values.broker || values.check,
  'Select --broker <id>, or use --check to validate all setups.',
);
const entries = JSON.parse(await readFile(new URL('./brokers.json', import.meta.url), 'utf8'));
let selected = 0;
for (const entry of entries) {
  const descriptor = new URL(entry, new URL('./brokers.json', import.meta.url));
  const target = JSON.parse(await readFile(descriptor, 'utf8'));
  if (values.broker && target.id !== values.broker) continue;
  selected++;
  if (!target.setup) {
    console.log(
      `${target.id}: requires an existing external ${target.name} namespace; no Docker setup.`,
    );
    assert.ok(values.check, 'Configure the external broker and run test/interop.mjs instead.');
    continue;
  }
  const runtime = await mkdtemp(join(tmpdir(), `amqp-test-${target.id}-`));
  const password =
    process.env[target.config.sasl.passwordEnv] ?? randomBytes(24).toString('base64url');
  assert.ok(
    password && !/[\r\n\0]/u.test(password),
    'Test passwords must be nonempty single-line values.',
  );
  const passwordFile = join(runtime, 'password');
  await writeFile(passwordFile, password, { mode: 0o600 });
  const env = {
    ...process.env,
    [target.config.sasl.passwordEnv]: password,
    AMQP_TEST_PASSWORD_FILE: passwordFile,
  };
  const compose = fileURLToPath(new URL(target.setup.compose, descriptor));
  const args = [
    'compose',
    '--project-directory',
    dirname(compose),
    '-p',
    target.container,
    '-f',
    compose,
  ];
  try {
    execFileSync('docker', [...args, 'config', '--quiet'], {
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    if (!values.check) {
      execFileSync('docker', [...args, 'up', '--build', '-d', '--wait', '--wait-timeout', '180'], {
        env,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      if (target.setup.afterStart) {
        execFileSync('docker', target.setup.afterStart.args, {
          env,
          input: target.setup.afterStart.input,
          stdio: ['pipe', 'pipe', 'pipe'],
        });
      }
    }
    console.log(`${target.id}: ${values.check ? 'Compose configuration valid' : 'broker ready'}.`);
  } catch {
    throw new Error(
      `${target.id}: Docker setup failed; inspect this broker's Compose configuration and container logs locally.`,
    );
  } finally {
    if (values.check) await rm(runtime, { recursive: true, force: true });
  }
}
assert.ok(selected > 0, 'Unknown broker; choose classic, artemis, rabbitmq, ibmmq, or servicebus.');
