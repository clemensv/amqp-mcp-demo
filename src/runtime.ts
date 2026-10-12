// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { AmqpClientTransport } from './transport.js';
import { configPath, loadConfig, type Config } from './config.js';
import { StdioProtocolAdapter, StdioRelay } from './stdio.js';

/** CLI-owned service and stream lifecycle, independent of application handlers. */
export interface CliApplication {
  service: {
    onerror: ((error: Error) => void) | undefined;
    onclose: (() => void) | undefined;
    start(): Promise<void>;
    close(): Promise<void>;
  };
  readyMessage: string;
  ready(): Record<string, string>;
  close(): Promise<void>;
}

/**
 * Runs a CLI application until stdin EOF, a termination signal, or a terminal transport failure.
 * @remarks Diagnostics use stderr exclusively. Startup failures set process.exitCode to 1.
 */
export async function runApplication(
  application: string,
  createApplication: (config: Config, onEnd: () => void) => CliApplication,
): Promise<void> {
  let close: () => Promise<void> = async () => {};
  let shutdown: Promise<void> | undefined;
  let opening: Promise<void> = Promise.resolve();
  let stopped = false;
  const stop = () => {
    stopped = true;
    // A signal may arrive during attachment; finish startup before releasing its owned resources.
    shutdown ??= opening
      .catch(() => {})
      .then(close)
      .finally(() => {
        process.off('SIGINT', signal);
        process.off('SIGTERM', signal);
      });
    return shutdown;
  };
  const failed = (error: Error) => {
    // stdout is reserved for JSON-RPC; never print broker exceptions or credential values here.
    process.stderr.write(
      `${JSON.stringify({ level: 'error', application, message: error.message })}\n`,
    );
    process.exitCode = 1;
  };
  const signal = () => {
    void stop().catch(failed);
  };
  process.once('SIGINT', signal);
  process.once('SIGTERM', signal);
  try {
    const argv = process.argv.slice(2);
    const config = await loadConfig(argv.length ? configPath(argv) : undefined);
    if (stopped) {
      await stop();
      return;
    }
    const instance = createApplication(config, signal);
    instance.service.onerror = failed;
    instance.service.onclose = signal;
    close = () => instance.close();
    opening = instance.service.start();
    await opening;
    if (stopped) {
      await stop();
      return;
    }
    const details = instance.ready();
    process.stderr.write(
      `${JSON.stringify({ level: 'info', application, message: instance.readyMessage, ...details })}\n`,
    );
  } catch {
    failed(
      new Error(
        'Startup failed. Check configuration, environment credentials, and the AMQP endpoint.',
      ),
    );
    await stop().catch(failed);
  }
}

/** Runs the stdio-to-AMQP proxy without loading application-specific server handlers. */
export function runProxy(): Promise<void> {
  return runApplication('proxy', (config, onEnd) => {
    const transport = new AmqpClientTransport(config);
    const fatal = (error: Error) => {
      transport.onerror?.(error);
      onEnd();
    };
    const stdio: StdioRelay = new StdioRelay(
      process.stdin,
      process.stdout,
      config.maxMessageBytes,
      (message) => adapter.receive(message),
      fatal,
      onEnd,
    );
    const adapter: StdioProtocolAdapter = new StdioProtocolAdapter(
      (message) => transport.sendEnvelope(message),
      (message) => stdio.send(message),
    );
    transport.onEnvelope = (message) => {
      void adapter.send(message).catch(fatal);
    };
    return {
      service: transport,
      readyMessage: 'AMQP proxy ready.',
      ready() {
        stdio.start();
        const details: Record<string, string> = { node: config.node };
        if (transport.replyTo !== undefined) {
          details.replyAddress = transport.replyTo;
        }
        return details;
      },
      async close() {
        await transport.close();
        await stdio.close();
        process.stdin.destroy();
      },
    };
  });
}
