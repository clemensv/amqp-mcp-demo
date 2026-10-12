// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { access, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  authorizeReplyAddress,
  configPath,
  loadConfig,
  namedQueueTerminus,
  parseConfig,
  requiredEnv,
} from '../src/config.js';

const base = { endpoint: 'amqp://localhost:5672', node: 'mcp-requests' };
const parse = (overrides: object = {}) => parseConfig(JSON.stringify({ ...base, ...overrides }));

describe('configuration and routing policy', () => {
  it('loads MCP environment settings without storing the password', async () => {
    const config = await loadConfig(undefined, {
      AMQP_MCP_ENDPOINT: 'amqp://127.0.0.1:25674',
      AMQP_MCP_NODE: '/queues/mcp-requests',
      AMQP_MCP_BROKER: 'rabbitmq',
      AMQP_MCP_SASL_MODE: 'plain',
      AMQP_MCP_USERNAME: 'admin',
      AMQP_MCP_PASSWORD: 'test-secret-value',
    });
    expect(config.broker).toBe('rabbitmq');
    expect(config.node).toBe('/queues/mcp-requests');
    expect(config.sasl).toEqual({
      mode: 'plain',
      username: 'admin',
      passwordEnv: 'AMQP_MCP_PASSWORD',
    });
    expect(config.requestTimeoutMs).toBe(60000);
    expect(JSON.stringify(config)).not.toContain('test-secret-value');
  });

  it('accepts preconfigured reply addresses from JSON or environment settings', async () => {
    expect(parse().replyAddress).toBeUndefined();
    expect(parse({ replyAddress: 'client-replies' }).replyAddress).toBe('client-replies');
    expect(parse({ replyAddress: 'REPLIES   ', broker: 'ibmmq' }).replyAddress).toBe('REPLIES   ');
    const config = await loadConfig(undefined, {
      AMQP_MCP_ENDPOINT: base.endpoint,
      AMQP_MCP_NODE: base.node,
      AMQP_MCP_REPLY_ADDRESS: 'client-replies',
    });
    expect(config.replyAddress).toBe('client-replies');
    for (const replyAddress of [
      '',
      base.node,
      '$management',
      'reply-\n',
      'amqp://other.example/reply',
      'x'.repeat(4097),
    ]) {
      expect(() => parse({ replyAddress })).toThrow();
    }
    expect(() => parse({ replyAddress: 'other', replyAddressPrefixes: ['reply-'] })).toThrow();
    await expect(
      loadConfig(undefined, {
        AMQP_MCP_ENDPOINT: base.endpoint,
        AMQP_MCP_NODE: base.node,
        AMQP_MCP_REPLY_ADDRESS: 'reply-\n',
      }),
    ).rejects.toThrow('Invalid AMQP_MCP');
  });

  it('applies defaults and TLS and numeric settings from the MCP environment', async () => {
    const environment = {
      AMQP_MCP_ENDPOINT: 'amqps://broker.example',
      AMQP_MCP_NODE: 'requests',
      AMQP_MCP_TLS_CA_FILE: 'ca.pem',
      AMQP_MCP_TLS_CERT_FILE: 'cert.pem',
      AMQP_MCP_TLS_KEY_FILE: 'key.pem',
      AMQP_MCP_TLS_SERVER_NAME: 'broker.example',
      AMQP_MCP_SASL_MODE: 'external',
      AMQP_MCP_SASL_HOSTNAME: 'broker.example',
      AMQP_MCP_ALLOW_INSECURE_TRANSPORT: 'false',
      AMQP_MCP_REQUEST_TIMEOUT_MS: '2000',
      AMQP_MCP_OPERATION_TIMEOUT_MS: '1000',
      AMQP_MCP_SUBSCRIPTION_TIMEOUT_MS: '3000',
      AMQP_MCP_REPLY_CLIENT_IDLE_TIMEOUT_MS: '4000',
      AMQP_MCP_MAX_CONCURRENT_REQUESTS: '4',
      AMQP_MCP_MAX_MESSAGE_BYTES: '2048',
    };
    const config = await loadConfig(undefined, environment);
    expect(config.tls).toEqual({
      caFile: 'ca.pem',
      certFile: 'cert.pem',
      keyFile: 'key.pem',
      serverName: 'broker.example',
    });
    expect(config.sasl).toEqual({ mode: 'external', hostname: 'broker.example' });
    expect(config.broker).toBe('generic');
    expect(config.requestTimeoutMs).toBe(2000);
    expect(config.operationTimeoutMs).toBe(1000);
    expect(config.subscriptionTimeoutMs).toBe(3000);
    expect(config.replyClientIdleTimeoutMs).toBe(4000);
    expect(config.maxConcurrentRequests).toBe(4);
    expect(config.maxMessageBytes).toBe(2048);
    const defaults = await loadConfig(undefined, {
      AMQP_MCP_ENDPOINT: base.endpoint,
      AMQP_MCP_NODE: base.node,
    });
    expect(defaults.sasl.mode).toBe('anonymous');
    expect(defaults.tls).toEqual({});
  });

  it('rejects invalid or incomplete environment settings without echoing their values', async () => {
    const environment = { AMQP_MCP_ENDPOINT: base.endpoint, AMQP_MCP_NODE: base.node };
    for (const overrides of [
      { AMQP_MCP_ENDPOINT: 'amqp://remote.example' },
      { AMQP_MCP_ENDPOINT: 'amqp://user:secret-value@localhost' },
      { AMQP_MCP_BROKER: 'invalid-secret-value' },
      { AMQP_MCP_TLS_CERT_FILE: 'cert.pem' },
      { AMQP_MCP_MAX_CONCURRENT_REQUESTS: '0' },
      { AMQP_MCP_REQUEST_TIMEOUT_MS: '1e3' },
      { AMQP_MCP_REQUEST_TIMEOUT_MS: '' },
      { AMQP_MCP_REPLY_CLIENT_IDLE_TIMEOUT_MS: '0' },
      { AMQP_MCP_REPLY_CLIENT_IDLE_TIMEOUT_MS: '1e3' },
      { AMQP_MCP_NODE: '' },
    ]) {
      await expect(loadConfig(undefined, { ...environment, ...overrides })).rejects.toThrow(
        'Invalid AMQP_MCP',
      );
    }
    await expect(loadConfig(undefined, {})).rejects.toThrow('Invalid AMQP_MCP');
    await expect(
      loadConfig(undefined, { ...environment, AMQP_MCP_ALLOW_INSECURE_TRANSPORT: 'yes' }),
    ).rejects.toThrow('must be true or false');
    await expect(
      loadConfig(undefined, {
        ...environment,
        AMQP_MCP_SASL_MODE: 'plain',
        AMQP_MCP_USERNAME: 'admin',
      }),
    ).rejects.toThrow('AMQP_MCP_PASSWORD');
    await expect(
      loadConfig(undefined, {
        ...environment,
        AMQP_MCP_SASL_MODE: 'plain',
        AMQP_MCP_PASSWORD: 'secret-value',
      }),
    ).rejects.toThrow('Invalid AMQP_MCP');
    await expect(
      loadConfig(undefined, { ...environment, AMQP_MCP_SASL_MODE: 'invalid-secret-value' }),
    ).rejects.not.toThrow('invalid-secret-value');
    const insecure = await loadConfig(undefined, {
      ...environment,
      AMQP_MCP_ENDPOINT: 'amqp://remote.example',
      AMQP_MCP_ALLOW_INSECURE_TRANSPORT: 'true',
    });
    expect(insecure.allowInsecureTransport).toBe(true);
  });

  it('keeps file configuration precedence and never merges MCP environment overrides', async () => {
    const filePath = fileURLToPath(new URL('../package.json', import.meta.url));
    const environment = { AMQP_MCP_ENDPOINT: base.endpoint, AMQP_MCP_NODE: base.node };
    await expect(loadConfig(filePath, environment)).rejects.toThrow();
    await expect(
      loadConfig(undefined, { ...environment, AMQP_MCP_CONFIG: filePath }),
    ).rejects.toThrow();
    await expect(
      loadConfig(filePath, { ...environment, AMQP_MCP_CONFIG: 'missing-file.json' }),
    ).rejects.not.toThrow('ENOENT');
    await expect(loadConfig('missing-file.json', environment)).rejects.toThrow('ENOENT');
  });

  it('keeps each tested broker configuration and local setup in its own folder', async () => {
    const manifest = new URL('./brokers.json', import.meta.url);
    const entries: string[] = JSON.parse(await readFile(manifest, 'utf8'));
    const identifiers: string[] = [];
    for (const entry of entries) {
      const descriptor = new URL(entry, manifest);
      const target: { id: string; config: object; setup: { compose: string } | null } = JSON.parse(
        await readFile(descriptor, 'utf8'),
      );
      identifiers.push(target.id);
      expect(descriptor.pathname.endsWith(`/test/${target.id}/broker.json`)).toBe(true);
      const config = parseConfig(JSON.stringify(target.config));
      expect(config.broker).toBe(target.id);
      if (target.setup) {
        const compose: {
          services: Record<string, { ports: string[]; build?: string }>;
        } = JSON.parse(await readFile(new URL(target.setup.compose, descriptor), 'utf8'));
        expect(Object.keys(compose.services)).toHaveLength(1);
        for (const service of Object.values(compose.services)) {
          expect(service.ports).toHaveLength(1);
          expect(service.ports[0]).toMatch(/^127\.0\.0\.1:/u);
          if (service.build) {
            await access(new URL('Dockerfile', descriptor));
          }
        }
      } else {
        expect(target.id).toBe('servicebus');
        expect(config.endpoint).toMatch(/^amqps:/u);
      }
    }
    expect(identifiers.sort()).toEqual(['artemis', 'classic', 'ibmmq', 'rabbitmq', 'servicebus']);
  });

  it('declares explicit MQ queue semantics without changing other broker termini', () => {
    const address = 'AMQP.1234                           ';
    expect(namedQueueTerminus(parse({ broker: 'ibmmq' }), address)).toEqual({
      address,
      capabilities: ['queue'],
    });
    for (const broker of ['generic', 'servicebus', 'classic', 'artemis', 'rabbitmq']) {
      expect(
        namedQueueTerminus(
          parse({
            broker,
            endpoint: 'amqps://broker.example',
            sasl: { mode: 'plain', username: 'policy', passwordEnv: 'KEY' },
          }),
          address,
        ),
      ).toEqual({ address });
    }
  });

  it('uses bounded defaults and rejects unknown fields', () => {
    expect(parse().requestTimeoutMs).toBe(60000);
    expect(parse().replyClientIdleTimeoutMs).toBe(60000);
    expect(() => parse({ replyClientIdleTimeoutMs: 0 })).toThrow();
    expect(() => parse({ replyClientIdleTimeoutMs: 3600001 })).toThrow();
    expect(parse().sasl.mode).toBe('anonymous');
    expect(() => parse({ password: 'not-a-configuration-field' })).toThrow();
    expect(() => parse({ requestTimeoutMs: 0 })).toThrow();
  });

  it('allows loopback but requires an explicit remote plaintext opt-in', () => {
    expect(parse({ endpoint: 'amqp://[::1]' }).endpoint).toContain('[::1]');
    expect(() => parse({ endpoint: 'amqp://broker.example' })).toThrow('Plaintext');
    expect(
      parse({ endpoint: 'amqp://broker.example', allowInsecureTransport: true }),
    ).toBeDefined();
    expect(parse({ endpoint: 'amqps://broker.example' })).toBeDefined();
  });

  it('refuses credentials embedded in endpoints and inconsistent TLS/SASL settings', () => {
    for (const endpoint of [
      'https://broker.example',
      'amqp://user:secret@localhost',
      'amqp://localhost/queue',
      'amqp://localhost?x=1',
    ]) {
      expect(() => parse({ endpoint })).toThrow();
    }
    expect(() => parse({ sasl: { mode: 'plain' } })).toThrow('PLAIN');
    expect(() => parse({ tls: { certFile: 'cert.pem' } })).toThrow('together');
    expect(() => parse({ tls: { caFile: 'ca.pem' } })).toThrow('amqps');
    expect(() => parse({ sasl: { mode: 'external' } })).toThrow('EXTERNAL');
    expect(
      parse({
        endpoint: 'amqps://broker.example',
        tls: { certFile: 'cert.pem', keyFile: 'key.pem' },
        sasl: { mode: 'external' },
      }),
    ).toBeDefined();
  });

  it('requires SASL PLAIN and TLS for Service Bus', () => {
    for (const mode of ['none', 'anonymous', 'external']) {
      expect(() => parse({ broker: 'servicebus', sasl: { mode } })).toThrow('SASL PLAIN');
    }
    expect(
      parse({
        broker: 'servicebus',
        endpoint: 'amqps://broker.example',
        sasl: { mode: 'plain', username: 'policy', passwordEnv: 'KEY' },
      }).sasl.mode,
    ).toBe('plain');
    expect(() =>
      parse({
        broker: 'servicebus',
        sasl: { mode: 'plain', username: 'policy', passwordEnv: 'KEY' },
      }),
    ).toThrow('amqps://');
  });

  it('resolves config paths and secrets without returning secret values in errors', () => {
    expect(configPath(['--config', 'config.json'], {})).toBe('config.json');
    expect(configPath([], { AMQP_MCP_CONFIG: 'config.json' })).toBe('config.json');
    expect(() => configPath([], {})).toThrow('Usage');
    expect(requiredEnv('PASSWORD', { PASSWORD: 'value' })).toBe('value');
    expect(() => requiredEnv('PASSWORD', {})).toThrow('PASSWORD');
  });

  it('limits reply addresses to the configured broker and optional prefixes', () => {
    const config = parse({ replyAddressPrefixes: ['temp-queue://', 'reply-'] });
    expect(() => authorizeReplyAddress('temp-queue://ID:opaque', config)).not.toThrow();
    expect(() => authorizeReplyAddress('reply-123', config)).not.toThrow();
    for (const address of [
      '',
      'mcp-requests',
      '$cbs',
      'x/$management',
      'reply-\n',
      'other',
      'amqp://other.example/queue',
    ]) {
      expect(() => authorizeReplyAddress(address, config)).toThrow();
    }
    expect(() => authorizeReplyAddress('sb://localhost/reply', parse())).not.toThrow();
  });
});
