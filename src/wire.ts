// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0. See LICENSE at the package root.

import { randomUUID } from 'node:crypto';
import { JSONRPCMessageSchema, JSONValueSchema } from '@modelcontextprotocol/core';
import type { JSONRPCMessage, JSONRPCRequest, JSONValue } from '@modelcontextprotocol/server';
import {
  message as amqpMessage,
  types,
  type Message,
  type SenderOptions,
  type Source,
  type Typed,
} from 'rhea-promise';
import * as z from 'zod/v4';

const nullErrorSchema = z.object({
  jsonrpc: z.literal('2.0'),
  id: z.null(),
  error: z.object({
    code: z.number().int(),
    message: z.string(),
    data: JSONValueSchema.optional(),
  }),
});
/** An MCP message or a JSON-RPC parse/invalid-request error with a null body ID. */
export type RpcMessage = JSONRPCMessage | z.infer<typeof nullErrorSchema>;
/** One AMQP payload; a batch remains one envelope throughout forwarding. */
export type RpcEnvelope = RpcMessage | RpcMessage[];
/** AMQP identifiers retain their wire type, including ulong, UUID, binary, and string. */
export type AmqpMessageId = string | number | Buffer | Typed;

/** Rhea exposes different message types for decoding and delivery; both require runtime validation. */
type InboundMessage = Partial<Message> | ReturnType<typeof amqpMessage.decode>;

/** Validated reply destination and the original request's AMQP identifier. */
export interface ReplyRoute {
  address: string;
  /** Copied unchanged to correlation-id; distinct from the JSON-RPC body ID. */
  messageId: AmqpMessageId;
}

const codecs = types as typeof types & {
  Reader: new (buffer: Buffer) => { remaining(): number; read(): Typed };
  Writer: new () => { write(value: Typed): void; toBuffer(): Buffer };
};
const identifiers = new WeakMap<object, { message_id?: Typed; correlation_id?: Typed }>();
let capturing = false;

/**
 * Installs a process-wide, idempotent Rhea decode adapter before connections are opened.
 * @remarks Weak references preserve wire identifier types without retaining decoded messages.
 * Rhea's decoded values alone cannot distinguish UUID from binary or preserve every ulong.
 * The pinned codec is inspected before values are coerced; keep the identifier tests when upgrading it.
 */
export function enableTypedIdCapture(): void {
  if (capturing) {
    return;
  }
  capturing = true;
  const decode = amqpMessage.decode;
  amqpMessage.decode = (buffer: Buffer) => {
    const decoded = decode(buffer);
    const reader = new codecs.Reader(buffer);
    while (reader.remaining()) {
      const section = reader.read();
      if (
        section.descriptor?.value === 0x73 ||
        section.descriptor?.value === 'amqp:properties:list'
      ) {
        // The properties list stores message-id at index 0 and correlation-id at index 5.
        const fields = section.value as Typed[];
        identifiers.set(decoded, {
          ...(fields[0] ? { message_id: fields[0] } : {}),
          ...(fields[5] ? { correlation_id: fields[5] } : {}),
        });
        break;
      }
    }
    return decoded;
  };
}

/** Returns a supported identifier without type coercion, or undefined if absent or unsupported. */
export function readMessageId(
  encoded: InboundMessage,
  field: 'message_id' | 'correlation_id' = 'message_id',
): AmqpMessageId | undefined {
  const typed = identifiers.get(encoded)?.[field];
  if (typed) {
    // AMQP message-id permits ulong (zero/small/full), UUID, binary (short/long), and string (short/long).
    return [0x44, 0x53, 0x80, 0x98, 0xa0, 0xb0, 0xa1, 0xb1].includes(typed.type.typecode)
      ? typed
      : undefined;
  }
  const value: unknown = encoded[field];
  if (
    typeof value === 'string' ||
    (typeof value === 'number' && value >= 0) ||
    Buffer.isBuffer(value)
  ) {
    return value;
  }
  if (value && typeof value === 'object' && 'type' in value && 'value' in value) {
    return value as Typed;
  }
  return undefined;
}

/** Encodes both type and value into a correlation key; never equates string and numeric IDs. */
export function messageIdKey(identifier: AmqpMessageId): string {
  const writer = new codecs.Writer();
  writer.write(types.wrap_message_id(identifier));
  return writer.toBuffer().toString('hex');
}

/** Requests a nondurable dynamic target whose queue lives until its owner link closes. */
export function temporaryQueueOptions(): SenderOptions {
  return {
    name: `mcp-reply-owner-${randomUUID()}`,
    source: {} as Source,
    target: {
      dynamic: true,
      durable: 0,
      expiry_policy: 'link-detach',
      dynamic_node_properties: {
        // AMQP delete-on-close (0x2b) ties queue lifetime to this sender, not its receiver.
        'lifetime-policy': types.wrap_described([], 0x2b),
      },
      capabilities: ['temporary-queue'],
    },
  };
}

/** Narrows requests without mistaking a zero or empty-string body ID for a notification. */
export function isRequest(envelope: RpcMessage): envelope is JSONRPCRequest {
  return 'method' in envelope && 'id' in envelope && envelope.id !== undefined;
}

/** Reports whether an envelope contains at least one request requiring a response. */
export function hasRequests(envelope: RpcEnvelope): boolean {
  return Array.isArray(envelope) ? envelope.some(isRequest) : isRequest(envelope);
}

/**
 * Encodes UTF-8 JSON in one AMQP data section with a fresh message-id by default.
 * @remarks Reply routes copy the original AMQP ID, independently of body IDs.
 * Requests use reply-to; responses and request-scoped notifications use correlation-id.
 * A batch has one AMQP message-id and no subject, even when it contains several requests.
 * @throws Error when a request lacks a reply destination.
 * @see https://github.com/oasis-tcs/amqp-specs/blob/master/jsonrpc/jsonrpc-amqp-binding-spec.md
 */
export function encodeRpc(
  envelope: RpcEnvelope,
  options: {
    replyTo?: string;
    route?: ReplyRoute;
    messageId?: string;
  } = {},
): Message {
  const encoded: Message = {
    message_id: options.messageId ?? randomUUID(),
    content_type: 'application/json',
    body: amqpMessage.data_section(Buffer.from(JSON.stringify(envelope), 'utf8')),
  };
  if (options.route) {
    // Preserve the original identifier's AMQP type as well as its value; UUID and binary differ.
    encoded.to = options.route.address;
    encoded.correlation_id = options.route.messageId as NonNullable<Message['correlation_id']>;
  } else if (hasRequests(envelope) || (Array.isArray(envelope) && envelope.length === 0)) {
    if (!options.replyTo) {
      throw new Error('A JSON-RPC request requires a reply address.');
    }
    encoded.reply_to = options.replyTo;
  }
  if (!Array.isArray(envelope) && 'method' in envelope) {
    encoded.subject = envelope.method;
  }
  return encoded;
}

/**
 * Validates content type, one data section, byte limit, UTF-8, and JSON value shape.
 * @throws Error for invalid framing, excess size, invalid UTF-8, or invalid JSON.
 */
export function decodeJsonBody(encoded: InboundMessage, maxBytes = 1024 * 1024): JSONValue {
  if (encoded.content_type !== 'application/json') {
    throw new Error('Expected content-type application/json.');
  }
  const body = encoded.body as { typecode?: number; content?: Buffer } | undefined;
  if (body?.typecode !== 0x75 || !Buffer.isBuffer(body.content)) {
    throw new Error('Expected one AMQP data section.');
  }
  if (body.content.length > maxBytes) {
    throw new Error('JSON-RPC message exceeds the size limit.');
  }
  return JSONValueSchema.parse(
    JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(body.content)),
  );
}

/** Decodes a bounded envelope and validates every member, including null-ID JSON-RPC errors. */
export function decodeRpc(encoded: InboundMessage, maxBytes = 1024 * 1024): RpcEnvelope {
  const parsed = decodeJsonBody(encoded, maxBytes);
  if (Array.isArray(parsed)) {
    return parsed.map((entry) => JSONRPCMessageSchema.or(nullErrorSchema).parse(entry));
  }
  return JSONRPCMessageSchema.or(nullErrorSchema).parse(parsed);
}
