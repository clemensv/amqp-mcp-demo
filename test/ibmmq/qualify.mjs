// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { execFile, execFileSync } from 'node:child_process';
import console from 'node:console';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { createConnection, createServer } from 'node:net';
import { dirname } from 'node:path';
import process from 'node:process';
import { setTimeout, clearTimeout } from 'node:timers';
import { fileURLToPath, URL } from 'node:url';
import { format, parseArgs, promisify } from 'node:util';
import rheaLog from '../../node_modules/rhea/lib/log.js';
import rheaFrames from '../../node_modules/rhea/lib/frames.js';
import rheaTypes from '../../node_modules/rhea/lib/types.js';
import { AmqpConnection } from '../../dist/auth.js';
import { types } from '../../node_modules/rhea-promise/dist/lib/index.js';
import { parseConfig } from '../../dist/config.js';
import { OrderedSender } from '../../dist/transport.js';
import {
  encodeRpc,
  decodeRpc,
  temporaryQueueOptions,
  readMessageId,
  messageIdKey,
} from '../../dist/wire.js';

const { values } = parseArgs({
  options: {
    sasl: { type: 'string', default: 'plain' },
    dotnet: { type: 'boolean' },
    frames: { type: 'boolean' },
    wire: { type: 'boolean' },
    node: { type: 'string', default: 'DEV.AMQP.QUALIFY' },
    only: { type: 'string' },
    sessions: { type: 'string' },
  },
});
const container = 'amqpguide-ibmmq';
const reportPath = fileURLToPath(
  new URL('../../tmp/ibmmq-qualification/results.json', import.meta.url),
);
const docker = (args, input) =>
  execFileSync('docker', args, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    maxBuffer: 8 * 1024 * 1024,
  });
const report = {
  timestamp: new Date().toISOString(),
  container,
  tests: [],
};
if (values.frames) {
  report.frames = [];
  rheaLog.frames = (pattern, ...args) => {
    if (!pattern.includes('%j')) return;
    const kind = String(args[2]).split('#')[0];
    if (kind === 'sasl_init') {
      const frame = args[3];
      const metadata = {
        mechanism: frame.mechanism,
        hostname: frame.hostname,
        responseBytes: frame.initial_response?.length,
        matchesExpectedPlain: frame.initial_response?.equals(
          Buffer.from(`\0app\0${process.env.AMQP_DEMO_IBMMQ_PASSWORD}`, 'utf8'),
        ),
      };
      report.saslInit = metadata;
      console.log(`SASL INIT ${JSON.stringify(metadata)}`);
      return;
    }
    if (kind === 'sasl_outcome') {
      report.saslOutcome = args[3].code;
      console.log(`SASL OUTCOME ${report.saslOutcome}`);
      return;
    }
    if (!['open', 'begin', 'close'].includes(kind)) return;
    const line = format(pattern, ...args);
    report.frames.push(line);
    console.log(line);
  };
}
const inspected = JSON.parse(docker(['inspect', container]))[0];
report.image = inspected.Config.Image;
report.imageId = inspected.Image;
report.locale = inspected.Config.Env.find((entry) => entry.startsWith('LANG='))?.slice(5);
report.version = docker(['exec', container, 'dspmqver']).trim();
report.service = docker(
  ['exec', '-i', container, 'runmqsc', 'QM1'],
  'DISPLAY SVSTATUS(SYSTEM.AMQP.SERVICE) ALL\n',
);
process.env.AMQP_DEMO_IBMMQ_PASSWORD = docker([
  'exec',
  container,
  'cat',
  '/run/secrets/mqAppPassword',
]).trim();
let port = 25675;
let observer;
const observedSockets = new Set();
if (values.wire) {
  report.wire = [];
  let connectionId = 0;
  observer = createServer((client) => {
    const id = ++connectionId;
    const broker = createConnection({ host: '127.0.0.1', port: 25675 });
    const inspect = (direction) => {
      let pending = Buffer.alloc(0);
      return (chunk) => {
        pending = Buffer.concat([pending, chunk]);
        while (pending.length >= 8) {
          if (pending.subarray(0, 4).equals(Buffer.from('AMQP'))) {
            report.wire.push({
              id,
              direction,
              header: pending.subarray(0, 8).toString('hex'),
            });
            pending = pending.subarray(8);
            continue;
          }
          const size = pending.readUInt32BE(0);
          assert.ok(size >= 8 && size <= 1024 * 1024, 'Invalid observed frame size.');
          if (pending.length < size) return;
          const encoded = pending.subarray(0, size);
          pending = pending.subarray(size);
          const frame = rheaFrames.read_frame(encoded);
          if (!frame.performative) continue;
          const kind = String(frame.performative.constructor).split('#')[0];
          if (
            !['sasl_init', 'sasl_outcome', 'open', 'begin', 'close', 'disposition'].includes(kind)
          )
            continue;
          const fields = new rheaTypes.Reader(encoded.subarray(encoded[4] * 4)).read().value;
          const metadata = {
            id,
            direction,
            kind,
            size,
            fieldTypes: fields.map((field) => field.type.typecode),
          };
          const performative = frame.performative;
          if (kind === 'sasl_init') {
            Object.assign(metadata, {
              mechanism: performative.mechanism,
              hostname: performative.hostname,
              responseBytes: performative.initial_response?.length,
              matchesExpectedPlain: performative.initial_response?.equals(
                Buffer.from(`\0app\0${process.env.AMQP_DEMO_IBMMQ_PASSWORD}`, 'utf8'),
              ),
            });
          } else if (kind === 'sasl_outcome') {
            metadata.code = performative.code;
          } else {
            for (const field of [
              'container_id',
              'hostname',
              'max_frame_size',
              'channel_max',
              'idle_time_out',
              'remote_channel',
              'next_outgoing_id',
              'incoming_window',
              'outgoing_window',
              'error',
              'state',
            ]) {
              if (performative[field] !== undefined) metadata[field] = performative[field];
            }
          }
          report.wire.push(metadata);
          console.log(`WIRE ${JSON.stringify(metadata)}`);
        }
      };
    };
    for (const socket of [client, broker]) {
      observedSockets.add(socket);
      socket.on('error', () => {
        client.destroy();
        broker.destroy();
      });
      socket.on('close', () => {
        observedSockets.delete(socket);
        client.destroy();
        broker.destroy();
      });
    }
    client.on('data', inspect('client-to-mq'));
    broker.on('data', inspect('mq-to-client'));
    client.pipe(broker);
    broker.pipe(client);
  });
  await new Promise((resolve, reject) => {
    observer.once('error', reject);
    observer.listen(0, '127.0.0.1', resolve);
  });
  port = observer.address().port;
}
const config = parseConfig(
  JSON.stringify({
    endpoint: `amqp://127.0.0.1:${port}`,
    node: values.node,
    broker: 'ibmmq',
    operationTimeoutMs: 10000,
    sasl:
      values.sasl === 'plain'
        ? {
            mode: 'plain',
            username: 'app',
            passwordEnv: 'AMQP_DEMO_IBMMQ_PASSWORD',
          }
        : { mode: values.sasl },
  }),
);
report.sasl = values.sasl;

report.saslOffer = await new Promise((resolve, reject) => {
  const socket = createConnection({ host: '127.0.0.1', port: 25675 });
  let data = Buffer.alloc(0);
  socket.setTimeout(5000, () => socket.destroy(new Error('SASL offer timed out.')));
  socket.once('error', reject);
  socket.once('connect', () => socket.write(Buffer.from('414d515003010000', 'hex')));
  socket.on('data', (chunk) => {
    data = Buffer.concat([data, chunk]);
    if (data.length >= 16 && data.length >= 8 + data.readUInt32BE(8)) {
      socket.end();
      resolve(data.toString('hex'));
    }
  });
});
console.log(`SASL OFFER ${report.saslOffer}`);

async function probe(name, run, required = true) {
  if (values.only && !name.toLowerCase().includes(values.only.toLowerCase())) return;
  const errors = [];
  let connection;
  try {
    connection = await AmqpConnection.open(config, (error) => errors.push(error.message));
    connection.connection.on('sender_error', (context) => errors.push(context.sender?.error));
    connection.connection.on('receiver_error', (context) => errors.push(context.receiver?.error));
    connection.connection.on('rejected', (context) => {
      const state = context.delivery?.remote_state;
      errors.push(state);
      console.log(`REJECTED ${JSON.stringify(state)}`);
    });
    const detail = await run(connection);
    report.tests.push({ name, status: 'PASS', required, detail });
    console.log(`PASS ${name}: ${JSON.stringify(detail)}`);
  } catch (error) {
    const status = required ? 'FAIL' : 'LIMITATION';
    report.tests.push({ name, status, required, error: error.message, errors });
    console.log(`${status} ${name}: ${error.message} ${JSON.stringify(errors)}`);
  } finally {
    await connection?.close();
  }
}

if (!values.dotnet) {
  const count = Number(values.sessions ?? 12);
  assert.ok(Number.isInteger(count) && count >= 1 && count <= 50);
  await probe('Consecutive authenticated sessions', async (connection) => {
    await connection.connection.createSession();
    for (let index = 1; index < count; index++) {
      const peer = await AmqpConnection.open(config, () => {});
      try {
        await peer.connection.createSession();
      } finally {
        await peer.close();
      }
    }
    return { count, retries: 0 };
  });
}
if (values.dotnet) {
  try {
    const output = await promisify(execFile)(
      'dotnet',
      [
        'run',
        '--no-build',
        '--project',
        fileURLToPath(
          new URL('../../../src/CS/RabbitMq/Requester/Requester.csproj', import.meta.url),
        ),
      ],
      {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
        env: {
          ...process.env,
          AMQP_CONNECTION_STRING: `Endpoint=amqp://127.0.0.1:${port};SharedAccessKeyName=app;SharedAccessKey=${process.env.AMQP_DEMO_IBMMQ_PASSWORD}`,
          AMQP_REQUEST_QUEUE: config.node,
        },
      },
    );
    report.dotnet = { status: 'PASS', output: output.stdout };
  } catch (error) {
    report.dotnet = {
      status: 'FAIL',
      output: String(error.stdout ?? '') + String(error.stderr ?? ''),
    };
  }
  report.dotnet.output = report.dotnet.output.replaceAll(
    process.env.AMQP_DEMO_IBMMQ_PASSWORD,
    '[redacted]',
  );
  console.log(JSON.stringify(report.dotnet));
} else if (!values.sessions) {
  await probe('Named queue send/receive', async (connection) => {
    const session = await connection.connection.createSession();
    const id = randomUUID();
    let timer;
    let received;
    let failed;
    const delivery = new Promise((resolve, reject) => {
      failed = () => reject(new Error('Named queue receiver failed.'));
      received = (context) => {
        try {
          assert.equal(context.message.message_id, id);
          assert.deepEqual(decodeRpc(context.message), {
            jsonrpc: '2.0',
            id: 1,
            result: {},
          });
          resolve();
        } catch (error) {
          reject(error);
        }
      };
      timer = setTimeout(() => reject(new Error('Named queue receive timed out.')), 10000);
    });
    void delivery.catch(() => {});
    try {
      const receiver = await session.createReceiver({
        source: { address: config.node, capabilities: ['queue'] },
        onMessage: received,
        onError: failed,
        credit_window: 0,
      });
      receiver.addCredit(1);
      const sender = await session.createAwaitableSender({
        target: { address: config.node, capabilities: ['queue'] },
      });
      await new OrderedSender(sender, 10000).send({
        ...encodeRpc({ jsonrpc: '2.0', id: 1, result: {} }, { messageId: id }),
        to: config.node,
      });
      await delivery;
      return { source: receiver.source, target: sender.target };
    } finally {
      clearTimeout(timer);
    }
  });

  await probe('Notification MQCI_NONE compatibility', async (connection) => {
    let pending;
    const delivery = new Promise((resolve, reject) => {
      pending = { resolve, reject };
    });
    void delivery.catch(() => {});
    const timer = setTimeout(
      () => pending.reject(new Error('Notification receive timed out.')),
      10000,
    );
    try {
      const receiver = await connection.connection.createReceiver({
        source: { address: config.node, capabilities: ['queue'] },
        credit_window: 0,
        onMessage: (context) => pending.resolve(context.message),
        onError: () => pending.reject(new Error('Notification receiver failed.')),
      });
      const sender = await connection.connection.createAwaitableSender({
        target: { address: config.node, capabilities: ['queue'] },
      });
      const envelope = {
        jsonrpc: '2.0',
        method: 'notifications/cancelled',
        params: { requestId: 'test' },
      };
      receiver.addCredit(1);
      await new OrderedSender(sender, 10000).send({
        ...encodeRpc(envelope),
        to: config.node,
      });
      const message = await delivery;
      report.notificationProperties = {
        message_id: message.message_id,
        correlation_id: message.correlation_id,
        reply_to: message.reply_to,
        subject: message.subject,
      };
      console.log(`NOTIFICATION PROPERTIES ${JSON.stringify(report.notificationProperties)}`);
      assert.deepEqual(decodeRpc(message), envelope);
      assert.equal(message.reply_to, undefined, 'Notifications must not acquire reply-to.');
      if (message.correlation_id !== undefined) {
        assert.ok(Buffer.isBuffer(message.correlation_id));
        assert.equal(message.correlation_id.length, 24);
        assert.ok(message.correlation_id.every((byte) => byte === 0));
        report.tests.push({
          name: 'Notification property preservation',
          status: 'LIMITATION',
          required: false,
          detail: 'MQ adds the 24-byte zero MQCI_NONE correlation identifier to notifications.',
        });
      }
      assert.equal(message.subject, envelope.method);
      return { correlationPreserved: message.correlation_id === undefined };
    } finally {
      clearTimeout(timer);
    }
  });

  await probe(
    'Dynamic source temporary queue',
    async (connection) => {
      const receiver = await connection.connection.createReceiver({
        source: temporaryQueueOptions().target,
        credit_window: 0,
      });
      assert.ok(receiver.source.address, 'Peer must assign a source address.');
      return receiver.source;
    },
    false,
  );

  await probe('Sender-owned dynamic target temporary queue', async (connection) => {
    const owner = await connection.createOwnerSender(temporaryQueueOptions(), 10000);
    assert.ok(owner.target.address, 'Peer must assign a target address.');
    const address = owner.target.address;
    const queueName = address.trim();
    assert.match(queueName, /^[A-Z0-9.]+$/);
    const queueExists = () => {
      try {
        return docker(
          ['exec', '-i', container, 'runmqsc', 'QM1'],
          `DISPLAY QLOCAL('${queueName}')\n`,
        ).includes('AMQ8409I');
      } catch (error) {
        if (String(error.stdout).includes('AMQ8147E')) return false;
        throw error;
      }
    };
    assert.ok(queueExists(), 'The owner must create an MQ queue.');
    const peer = await AmqpConnection.open(config, () => {});
    let receiver;
    let sender;
    let pending;
    try {
      receiver = await connection.connection.createReceiver({
        source: { address, capabilities: ['queue'] },
        credit_window: 0,
        onMessage: (context) => pending?.resolve(context.message),
        onError: () => pending?.reject(new Error('Temporary queue receiver failed.')),
      });
      assert.equal(receiver.source.address, address);
      sender = await peer.connection.createAwaitableSender({
        target: { address, capabilities: ['queue'] },
      });
      for (const identifier of [randomUUID(), types.wrap_binary(randomBytes(24))]) {
        const delivery = new Promise((resolve, reject) => {
          pending = { resolve, reject };
        });
        void delivery.catch(() => {});
        const timer = setTimeout(
          () => pending.reject(new Error('Temporary queue receive timed out.')),
          10000,
        );
        try {
          receiver.addCredit(1);
          await new OrderedSender(sender, 10000).send(
            encodeRpc(
              { jsonrpc: '2.0', id: 1, result: { echoed: true } },
              { route: { address, messageId: identifier } },
            ),
          );
          const message = await delivery;
          assert.equal(
            messageIdKey(readMessageId(message, 'correlation_id')),
            messageIdKey(identifier),
          );
          assert.deepEqual(decodeRpc(message), {
            jsonrpc: '2.0',
            id: 1,
            result: { echoed: true },
          });
        } finally {
          clearTimeout(timer);
          pending = undefined;
        }
      }
      await sender.close();
      await receiver.close();
      assert.ok(queueExists(), 'Closing the receiver must not delete the owner-held queue.');
      await owner.close();
      assert.ok(!queueExists(), 'Closing the owner must delete the temporary queue.');
      return {
        address,
        exchanges: 2,
        correlationTypes: ['string', 'binary'],
        retainedAfterReceiverClose: true,
        deletedAfterOwnerClose: true,
      };
    } finally {
      await peer.close();
    }
  });

  await probe(
    'UUID correlation identifier support',
    async (connection) => {
      const sender = await connection.connection.createAwaitableSender({
        target: { address: config.node, capabilities: ['queue'] },
      });
      await new OrderedSender(sender, 10000).send(
        encodeRpc(
          { jsonrpc: '2.0', id: 1, result: {} },
          {
            route: {
              address: config.node,
              messageId: types.wrap_uuid(randomBytes(16)),
            },
          },
        ),
      );
      return { accepted: true };
    },
    false,
  );
}

for (const socket of observedSockets) socket.destroy();
if (observer) await new Promise((resolve) => observer.close(resolve));
await mkdir(dirname(reportPath), { recursive: true });
await writeFile(reportPath, JSON.stringify(report, null, 2));
console.log(`REPORT ${reportPath}`);
if (
  report.dotnet?.status === 'FAIL' ||
  (!report.dotnet && report.tests.length === 0) ||
  report.tests.some((test) => test.required !== false && test.status !== 'PASS')
)
  process.exitCode = 1;
