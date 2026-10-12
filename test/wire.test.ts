// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { describe, expect, it } from 'vitest';
import { message as amqpMessage, types } from 'rhea-promise';
import {
  decodeRpc,
  encodeRpc,
  enableTypedIdCapture,
  messageIdKey,
  readMessageId,
  temporaryQueueOptions,
} from '../src/wire.js';

describe('OASIS JSON-RPC AMQP framing', () => {
  it('round-trips a UTF-8 request as one data section', () => {
    const request = { jsonrpc: '2.0' as const, id: 'one', method: 'tools/list' };
    const encoded = encodeRpc(request, { replyTo: 'opaque-reply-address' });
    const decoded = amqpMessage.decode(amqpMessage.encode(encoded));
    expect(decodeRpc(decoded)).toEqual(request);
    expect(decoded.content_type).toBe('application/json');
    expect(decoded.reply_to).toBe('opaque-reply-address');
    expect(decoded.subject).toBe('tools/list');
    expect(encoded.message_id).not.toBe(request.id);
  });

  it('preserves response correlation without adding reply-to or subject', () => {
    const response = { jsonrpc: '2.0' as const, id: 7, result: {} };
    const encoded = encodeRpc(response, {
      route: { address: 'reply', messageId: types.wrap_ulong(7) },
    });
    expect(encoded.correlation_id).toEqual(types.wrap_ulong(7));
    expect(encoded.reply_to).toBeUndefined();
    expect(encoded.subject).toBeUndefined();
    expect(decodeRpc(amqpMessage.decode(amqpMessage.encode(encoded)))).toEqual(response);
  });

  it('does not put reply-to on notifications, including notification batches', () => {
    const notification = {
      jsonrpc: '2.0' as const,
      method: 'notifications/cancelled',
      params: { requestId: 7 },
    };
    expect(encodeRpc(notification, { replyTo: 'unused' }).reply_to).toBeUndefined();
    expect(encodeRpc([notification], { replyTo: 'unused' }).reply_to).toBeUndefined();
  });

  it('keeps a mixed batch in one message without a subject', () => {
    const batch = [
      { jsonrpc: '2.0' as const, id: 1, method: 'ping' },
      { jsonrpc: '2.0' as const, method: 'notifications/cancelled', params: { requestId: 2 } },
    ];
    const encoded = encodeRpc(batch, { replyTo: 'reply' });
    expect(encoded.subject).toBeUndefined();
    expect(decodeRpc(amqpMessage.decode(amqpMessage.encode(encoded)))).toEqual(batch);
  });

  it('rejects requests without reply addresses and invalid wire bodies', () => {
    expect(() => encodeRpc({ jsonrpc: '2.0', id: 1, method: 'ping' })).toThrow('reply address');
    expect(() => decodeRpc({ content_type: 'text/plain' })).toThrow('content-type');
    expect(() => decodeRpc({ content_type: 'application/json', body: 'value' })).toThrow(
      'data section',
    );
    const decoded = amqpMessage.decode(
      amqpMessage.encode(encodeRpc({ jsonrpc: '2.0', method: 'ping' })),
    );
    expect(() => decodeRpc(decoded, 1)).toThrow('size limit');
  });

  it('requests a sender-created, nondurable, delete-on-close temporary queue', () => {
    const options = temporaryQueueOptions();
    expect(options.source).toEqual({});
    expect(options.target).toMatchObject({
      dynamic: true,
      durable: 0,
      expiry_policy: 'link-detach',
      capabilities: ['temporary-queue'],
    });
    const target = options.target as {
      dynamic_node_properties: Record<string, { descriptor: { value: number } }>;
    };
    expect(target.dynamic_node_properties['lifetime-policy']?.descriptor.value).toBe(0x2b);
  });

  it('retains the original AMQP id type, including a 16-byte binary id and large ulong', () => {
    enableTypedIdCapture();
    enableTypedIdCapture();
    for (const original of [
      types.wrap_binary(Buffer.alloc(16, 1)),
      types.wrap_uuid(Buffer.alloc(16, 2)),
      types.wrap_ulong(Buffer.from('ffffffffffffffff', 'hex')),
      types.wrap_string('request-id'),
    ]) {
      const decoded = amqpMessage.decode(
        amqpMessage.encode({
          ...encodeRpc({ jsonrpc: '2.0', id: 1, method: 'ping' }, { replyTo: 'reply' }),
          message_id: original,
        }),
      );
      const identifier = readMessageId(decoded)!;
      expect(messageIdKey(identifier)).toBe(messageIdKey(original));
      const response = amqpMessage.decode(
        amqpMessage.encode(
          encodeRpc(
            { jsonrpc: '2.0', id: 1, result: {} },
            { route: { address: 'reply', messageId: identifier } },
          ),
        ),
      );
      expect(messageIdKey(readMessageId(response, 'correlation_id')!)).toBe(messageIdKey(original));
    }
  });

  it('rejects multi-section, non-UTF-8, and malformed JSON bodies', () => {
    for (const body of [
      amqpMessage.data_sections([Buffer.from('{}'), Buffer.from('{}')]),
      amqpMessage.data_section(Buffer.from([0xff])),
      amqpMessage.data_section(Buffer.from('{')),
    ]) {
      expect(() =>
        decodeRpc(
          amqpMessage.decode(amqpMessage.encode({ content_type: 'application/json', body })),
        ),
      ).toThrow();
    }
    expect(readMessageId({})).toBeUndefined();
    expect(readMessageId({ message_id: 0 })).toBe(0);
  });
});
