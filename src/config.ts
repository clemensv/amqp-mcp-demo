// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { readFile } from 'node:fs/promises';
import * as z from 'zod/v4';

const envName = z.string().regex(/^[A-Za-z_][A-Za-z0-9_]*$/);

const configSchema = z.strictObject({
  endpoint: z.url(),
  node: z.string().min(1).max(4096),
  replyAddress: z.string().min(1).max(4096).optional(),
  broker: z
    .enum(['generic', 'servicebus', 'classic', 'artemis', 'rabbitmq', 'ibmmq'])
    .default('generic'),
  allowInsecureTransport: z.boolean().default(false),
  tls: z
    .strictObject({
      caFile: z.string().min(1).optional(),
      certFile: z.string().min(1).optional(),
      keyFile: z.string().min(1).optional(),
      serverName: z.string().min(1).optional(),
    })
    .default({}),
  sasl: z
    .strictObject({
      mode: z.enum(['none', 'anonymous', 'plain', 'external']).default('anonymous'),
      username: z.string().min(1).optional(),
      passwordEnv: envName.optional(),
      hostname: z.string().min(1).optional(),
    })
    .default({ mode: 'anonymous' }),
  requestTimeoutMs: z.number().int().min(100).max(3600000).default(60000),
  operationTimeoutMs: z.number().int().min(100).max(60000).default(10000),
  subscriptionTimeoutMs: z.number().int().min(100).max(3600000).default(300000),
  replyClientIdleTimeoutMs: z.number().int().min(100).max(3600000).default(60000),
  maxConcurrentRequests: z.number().int().min(1).max(256).default(16),
  maxMessageBytes: z
    .number()
    .int()
    .min(1024)
    .max(16 * 1024 * 1024)
    .default(1024 * 1024),
  replyAddressPrefixes: z.array(z.string().min(1)).default([]),
});

/** Validated broker settings with defaults applied; secrets are named, never stored here. */
export type Config = z.infer<typeof configSchema>;

/** Creates a named terminus with explicit queue semantics when the broker requires them. */
export function namedQueueTerminus(
  config: Pick<Config, 'broker'>,
  address: string,
): { address: string; capabilities?: string[] } {
  return config.broker === 'ibmmq' ? { address, capabilities: ['queue'] } : { address };
}

/**
 * Parses strict JSON settings and validates transport and authentication combinations.
 * @throws SyntaxError for malformed JSON, or a validation error for invalid settings.
 * @remarks Plaintext outside loopback requires explicit opt-in.
 */
export function parseConfig(json: string): Config {
  const config = configSchema.parse(JSON.parse(json));
  const endpoint = new URL(config.endpoint);
  if (!['amqp:', 'amqps:'].includes(endpoint.protocol)) {
    throw new Error('Endpoint must use amqp:// or amqps://.');
  }
  if (
    endpoint.username ||
    endpoint.password ||
    !['', '/'].includes(endpoint.pathname) ||
    endpoint.search ||
    endpoint.hash
  ) {
    throw new Error(
      'Endpoint must contain only the AMQP host and port; configure the node and SASL separately.',
    );
  }
  const encrypted = endpoint.protocol === 'amqps:';
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname);
  if (!encrypted && !loopback && !config.allowInsecureTransport) {
    throw new Error('Plaintext AMQP outside loopback requires allowInsecureTransport=true.');
  }
  if (config.sasl.mode === 'plain' && (!config.sasl.username || !config.sasl.passwordEnv)) {
    throw new Error('SASL PLAIN requires username and passwordEnv.');
  }
  if (config.broker === 'servicebus' && config.sasl.mode !== 'plain') {
    throw new Error('Service Bus requires SASL PLAIN.');
  }
  if (config.broker === 'servicebus' && !encrypted) {
    throw new Error('Service Bus requires an amqps:// endpoint.');
  }
  if (Boolean(config.tls.certFile) !== Boolean(config.tls.keyFile)) {
    throw new Error('TLS certFile and keyFile must be configured together.');
  }
  if (!encrypted && Object.keys(config.tls).length) {
    throw new Error('TLS options require an amqps:// endpoint.');
  }
  if (config.sasl.mode === 'external' && (!encrypted || !config.tls.certFile)) {
    throw new Error('SASL EXTERNAL requires TLS with a client certificate and key.');
  }
  if (config.replyAddress !== undefined) {
    authorizeReplyAddress(config.replyAddress, config);
  }
  return config;
}

/**
 * Loads a settings file or constructs validated settings from AMQP_MCP environment variables.
 * @remarks An explicit path takes precedence over AMQP_MCP_CONFIG, then environment-only settings.
 * Sources are not merged. Password values remain outside the returned configuration.
 */
export async function loadConfig(
  filePath?: string,
  environment: NodeJS.ProcessEnv = process.env,
): Promise<Config> {
  const selectedPath = filePath ?? environment.AMQP_MCP_CONFIG;
  if (selectedPath) {
    return parseConfig(await readFile(selectedPath, 'utf8'));
  }
  const numeric = (name: string): number | undefined => {
    const value = environment[name];
    if (value === undefined) {
      return undefined;
    }
    if (!/^[0-9]+$/u.test(value)) {
      throw new Error(`${name} must be an unsigned integer.`);
    }
    return Number(value);
  };
  const insecure = environment.AMQP_MCP_ALLOW_INSECURE_TRANSPORT;
  if (insecure !== undefined && !['true', 'false'].includes(insecure)) {
    throw new Error('AMQP_MCP_ALLOW_INSECURE_TRANSPORT must be true or false.');
  }
  if (environment.AMQP_MCP_SASL_MODE === 'plain') {
    requiredEnv('AMQP_MCP_PASSWORD', environment);
  }
  try {
    return parseConfig(
      JSON.stringify({
        endpoint: requiredEnv('AMQP_MCP_ENDPOINT', environment),
        node: requiredEnv('AMQP_MCP_NODE', environment),
        replyAddress: environment.AMQP_MCP_REPLY_ADDRESS,
        broker: environment.AMQP_MCP_BROKER,
        allowInsecureTransport: insecure === undefined ? undefined : insecure === 'true',
        sasl: {
          mode: environment.AMQP_MCP_SASL_MODE,
          username: environment.AMQP_MCP_USERNAME,
          passwordEnv: environment.AMQP_MCP_SASL_MODE === 'plain' ? 'AMQP_MCP_PASSWORD' : undefined,
          hostname: environment.AMQP_MCP_SASL_HOSTNAME,
        },
        tls: {
          caFile: environment.AMQP_MCP_TLS_CA_FILE,
          certFile: environment.AMQP_MCP_TLS_CERT_FILE,
          keyFile: environment.AMQP_MCP_TLS_KEY_FILE,
          serverName: environment.AMQP_MCP_TLS_SERVER_NAME,
        },
        requestTimeoutMs: numeric('AMQP_MCP_REQUEST_TIMEOUT_MS'),
        operationTimeoutMs: numeric('AMQP_MCP_OPERATION_TIMEOUT_MS'),
        subscriptionTimeoutMs: numeric('AMQP_MCP_SUBSCRIPTION_TIMEOUT_MS'),
        replyClientIdleTimeoutMs: numeric('AMQP_MCP_REPLY_CLIENT_IDLE_TIMEOUT_MS'),
        maxConcurrentRequests: numeric('AMQP_MCP_MAX_CONCURRENT_REQUESTS'),
        maxMessageBytes: numeric('AMQP_MCP_MAX_MESSAGE_BYTES'),
      }),
    );
  } catch {
    throw new Error(
      'Invalid AMQP_MCP environment configuration. Check connection settings and limits.',
    );
  }
}

/** Resolves an explicit --config argument or AMQP_MCP_CONFIG; rejects unexpected arguments. */
export function configPath(argv: string[], environment: NodeJS.ProcessEnv = process.env): string {
  if (argv.length === 2 && argv[0] === '--config' && argv[1]) {
    return argv[1];
  }
  if (argv.length === 0 && environment.AMQP_MCP_CONFIG) {
    return environment.AMQP_MCP_CONFIG;
  }
  throw new Error(
    'Usage: amqp-mcp-proxy|amqp-mcp-server --config <file.json>, or set AMQP_MCP_CONFIG.',
  );
}

/** Reads a required variable; a missing-value error names the variable without exposing its value. */
export function requiredEnv(name: string, environment: NodeJS.ProcessEnv = process.env): string {
  const value = environment[name];
  if (!value) {
    throw new Error(`Required environment variable ${name} is missing.`);
  }
  return value;
}

/**
 * Rejects unsafe reply routes before opening a sender to a requester-supplied address.
 * @throws Error for restricted nodes, cross-broker URLs, or prefix allowlist violations.
 * @remarks An empty prefix list permits otherwise valid routes on the configured broker.
 */
export function authorizeReplyAddress(address: string, config: Config): void {
  if (
    !address ||
    address.length > 4096 ||
    Array.from(address).some(
      (character) => character.charCodeAt(0) < 0x20 || character.charCodeAt(0) === 0x7f,
    ) ||
    address === config.node ||
    address.includes('$cbs') ||
    address.includes('$management')
  ) {
    throw new Error('Reply address is not permitted.');
  }
  if (/^(amqps?|sb):\/\//i.test(address)) {
    const destination = new URL(address);
    if (
      destination.hostname !== new URL(config.endpoint).hostname ||
      destination.username ||
      destination.password
    ) {
      throw new Error('Reply address must belong to the configured AMQP broker.');
    }
  }
  if (
    config.replyAddressPrefixes.length &&
    !config.replyAddressPrefixes.some((prefix) => address.startsWith(prefix))
  ) {
    throw new Error('Reply address does not match replyAddressPrefixes.');
  }
}
