# AMQP MCP transport

`amqp-mcp-transport` adds Advanced Message Queuing Protocol (AMQP) 1.0
request/reply transport to the Model Context Protocol (MCP) TypeScript
software development kit (SDK).
Applications supply their own tool handlers; the package handles message
delivery and reply routing.

The `amqp-mcp-proxy` executable connects a client's standard input and output
(stdio) to the AMQP transport. It reads and writes newline-delimited JSON-RPC
messages, the request and response format used by MCP.

The binding uses stateless MCP revision `2026-07-28`, with `server/discover`
instead of `initialize`. The OASIS [JSON-RPC binding](https://github.com/oasis-tcs/amqp-specs/blob/master/jsonrpc/jsonrpc-amqp-binding-spec.md)
and [MCP binding](https://github.com/oasis-tcs/amqp-specs/blob/master/mcp/mcp-amqp-binding-spec.md)
are experimental working drafts, not published OASIS Standards.

This multilateral demo tests interoperability for the binding work under OASIS
governance. Its implementation and test results are demonstration material,
not an OASIS Standard or conformance certification.

The source is available under the [Apache License, Version 2.0](LICENSE).
Dependencies and broker products retain their own licenses.

Watch the [five-broker VS Code demonstration](videos/amqp-mcp-vscode-demo-chronological-2026-10-12.mp4)
and follow the [demo guide](DEMO.md) to reproduce the tool calls.

## Build and install

Run commands from the repository root. The transport and example server
share an npm workspace; their configuration and build output stay in this project.

Node 22 or later is required at runtime. Development uses Node 22.12+, Node 24,
or Node 26+ for Vitest 5.

```sh
npm ci
npm run verify
npm pack
```

The archive includes compiled JavaScript, declarations, source maps, source,
this documentation, and the [Apache-2.0 license](LICENSE) and [notice](NOTICE).
It excludes the example package, broker manifests, tests, and demo evidence.
Install the archive into another Node project:

```sh
npm install ./amqp-mcp-transport-0.1.0.tgz @modelcontextprotocol/core@2.2.0 @modelcontextprotocol/server@2.2.0
```

The MCP core and server SDKs are peer dependencies: the consuming application
must install the exact versions shown above. Client applications also install
`@modelcontextprotocol/client@2.2.0`.

The adapter selects the fixed protocol revision through a protected SDK
extension point. Validate the adapter before upgrading the SDK.

Registry publication requires package ownership and the applicable repository
contribution approvals. `npm publish` runs the full verification gate before
publication. The example workspace remains private and is excluded from the
transport archive.

## Public API

| Export                                | Purpose                                                                                                                                                                                                              |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AmqpClientTransport`                 | SDK `Transport` implementation with a temporary or preconfigured reply queue. Use `start`, `send`, and `close` through the SDK client. `sendEnvelope` and `onEnvelope` support whole JSON-RPC batches for the proxy. |
| `AmqpServer`                          | AMQP listener constructed with validated configuration and a fresh-server factory. Supplies bounded dispatch, reply routing, manual settlement, cancellation, and cleanup.                                           |
| `StatelessMcpServer`                  | SDK `Server` subclass fixed to revision `2026-07-28`. Applications register their own handlers on each instance.                                                                                                     |
| `McpServerFactory`                    | `() => Server`. Must return a new configured SDK server for every exchange; do not return a shared singleton.                                                                                                        |
| `MCP_PROTOCOL_VERSION`                | The fixed revision string.                                                                                                                                                                                           |
| `parseConfig`, `loadConfig`           | Strict configuration validation with defaults; `loadConfig` reads a UTF-8 JSON file or validates AMQP_MCP environment settings when called without a path.                                                           |
| `Config`, `RpcEnvelope`, `RpcMessage` | Configuration and envelope types.                                                                                                                                                                                    |

The optional `amqp-mcp-transport/cli` entry exports `runApplication`, `runProxy`,
and `CliApplication` to manage command-line startup and shutdown. Importing
either entry does not connect to a broker, start a process, or register tools.

Leave `onEnvelope` unset when connecting an SDK client: it overrides the SDK's
`onmessage` delivery. Use it only for whole-envelope consumers such as the proxy.

### SDK client

```ts
import { Client } from '@modelcontextprotocol/client';
import { AmqpClientTransport, loadConfig, MCP_PROTOCOL_VERSION } from 'amqp-mcp-transport';

const config = await loadConfig('broker.json');
const client = new Client(
  { name: 'my-client', version: '1.0.0' },
  { versionNegotiation: { mode: { pin: MCP_PROTOCOL_VERSION } } },
);
try {
  await client.connect(new AmqpClientTransport(config));
  const catalog = await client.listTools();
  console.log(catalog.tools);
} finally {
  await client.close();
}
```

### Application server

```ts
import { AmqpServer, loadConfig, StatelessMcpServer } from 'amqp-mcp-transport';

const config = await loadConfig('broker.json');
const listener = new AmqpServer(config, () => {
  const server = new StatelessMcpServer(
    { name: 'my-tools', version: '1.0.0' },
    { capabilities: { tools: {} } },
  );
  server.setRequestHandler('tools/list', async () => ({ tools: [] }));
  return server;
});
listener.onerror = (error) => console.error(error.message);
await listener.start();
process.once('SIGINT', () => void listener.close());
```

Register application handlers in the factory. Each exchange gets a new SDK
server instance. Store application state outside that instance if the state
must survive individual calls.

`close()` aborts active exchanges and waits for tracked work to finish.
`drain()` waits for the work tracked at the time of the call; it does not wait
for future requests.

The listener caches reply clients, which are AMQP sender links, by exact reply
address on its shared AMQP session. Concurrent requests for the same address
share the sender, including an attachment still in progress. Each envelope
keeps its own correlation identifier.

`replyClientIdleTimeoutMs` defaults to 60000 milliseconds. Any active request
for an address suspends its idle timer, including while a reply is being sent.
The last request to finish starts a full idle interval. When the timer expires,
the listener closes the sender link; the shared session and the client's reply
queue remain open.

Failed links are removed from the cache without retrying the request. The cache
holds at most `maxConcurrentRequests` idle entries and can remove excess entries
before their timeout. Shutdown drains requests and closes cached reply links
before closing the shared session. Sharing reply links does not share SDK
server instances.

## Proxy entry point

```sh
amqp-mcp-proxy --config broker.json
```

Set `AMQP_MCP_CONFIG` instead of passing `--config`, or configure the connection
entirely through environment variables as shown below. The proxy forwards single
messages and batches without defining tools.

Standard output contains only JSON-RPC. Structured diagnostics go to standard
error. The proxy closes its resources when standard input ends or it receives
SIGINT or SIGTERM.

The AMQP side uses stateless MCP revision `2026-07-28`. For stdio clients such
as VS Code, the proxy maps `initialize` to `server/discover`, returns the
discovered server identity and capabilities in the initialization response,
and consumes `notifications/initialized` locally. It supports client revisions
`2024-11-05`, `2025-03-26`, `2025-06-18`, and `2025-11-25`. Each subsequent
request carries the stateless protocol and client metadata required by the
broker-side server. Native stateless clients pass through unchanged.

The adapter covers request/response tool discovery and calls. It does not
translate sampling, elicitation, server-initiated requests, or subscription
lifecycles, and does not advertise client capabilities for those operations.
It does not change a client's model or chat transport.

## Configuration and authentication

Provision an ordinary request queue that both endpoints can access. The library
does not create or delete that queue.

The Simple Authentication and Security Layer (SASL) authenticates the connection.
Transport Layer Security (TLS) encrypts it. The configuration selects the
authentication mechanism and whether to use TLS. For a local RabbitMQ development
broker:

```json
{
  "endpoint": "amqp://127.0.0.1:25674",
  "node": "/queues/mcp-requests",
  "broker": "rabbitmq",
  "sasl": {
    "mode": "plain",
    "username": "admin",
    "passwordEnv": "AMQP_PASSWORD"
  }
}
```

Set the password through the environment or a secret manager. Do not embed
credentials in configuration, URLs, logs, or reports.

### Reply queues

By default, the client creates a temporary reply queue and holds its ownership
link until the transport closes. To use an existing queue, set `replyAddress`
in the configuration file, for example `"replyAddress": "/queues/mcp-client-replies"`
for RabbitMQ. In environment-only mode, set `AMQP_MCP_REPLY_ADDRESS` instead.
Use the broker's queue address syntax, just as for the request `node`.

Provision the queue and grant the client permission to receive and the server
permission to send. The transport attaches a named receiver, sets each request's
AMQP `reply-to` to the configured address, and opens no temporary queue-owner
link. Shutdown closes the links without requesting deletion of the queue.
The broker's configured retention and expiry policies still apply.

Give each active client transport its own reply queue. Multiple consumers on
one queue compete for messages and can take replies meant for another client.
Unmatched replies are accepted and discarded; a preconfigured queue does not
provide shared-consumer routing or recovery of pending calls after a restart.
If the server uses `replyAddressPrefixes`, the queue address must match its policy.

### Settings in an MCP server definition

Put broker connection settings in the stdio server's `env` block. This avoids
a separate broker configuration file. A standard MCP client does not forward
a custom `amqp` property to the proxy:

```json
{
  "servers": {
    "rabbitmq-tools": {
      "type": "stdio",
      "command": "amqp-mcp-proxy",
      "env": {
        "AMQP_MCP_ENDPOINT": "amqp://127.0.0.1:25674",
        "AMQP_MCP_NODE": "/queues/mcp-requests",
        "AMQP_MCP_BROKER": "rabbitmq",
        "AMQP_MCP_SASL_MODE": "plain",
        "AMQP_MCP_USERNAME": "admin",
        "AMQP_MCP_PASSWORD": "${input:rabbitmq-password}"
      }
    }
  },
  "inputs": [
    {
      "id": "rabbitmq-password",
      "type": "promptString",
      "description": "RabbitMQ password",
      "password": true
    }
  ]
}
```

Install the proxy on the executable search path and provision the request queue.
The client resolves the password input and passes it through the child process's
environment. The returned `Config` stores only the variable name
`AMQP_MCP_PASSWORD`, not the password. Environment validation errors do not echo
supplied values. Both the proxy and example server accept these settings without
command-line arguments.

The proxy reads one configuration source, in this order:

1. The file selected by `--config <path>`.
2. The file selected by `AMQP_MCP_CONFIG`.
3. The `AMQP_MCP_*` environment settings.

Sources are never merged. To use environment-only settings, omit `--config`
and unset `AMQP_MCP_CONFIG`. Use a configuration file for `replyAddressPrefixes`.

Set `AMQP_MCP_REPLY_CLIENT_IDLE_TIMEOUT_MS` to configure the server's reply-client
idle timeout through the MCP `env` block. It accepts an unsigned decimal integer
from 100 to 3600000 milliseconds and defaults to 60000.

| Environment variable                                                                                  | Setting                                                                             |
| ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `AMQP_MCP_ENDPOINT`, `AMQP_MCP_NODE`                                                                  | Required broker URL and request queue address.                                      |
| `AMQP_MCP_REPLY_ADDRESS`                                                                              | Optional preconfigured client reply queue address; unset creates a temporary queue. |
| `AMQP_MCP_BROKER`                                                                                     | Broker profile; defaults to `generic`.                                              |
| `AMQP_MCP_SASL_MODE`                                                                                  | SASL mode; defaults to `anonymous`.                                                 |
| `AMQP_MCP_USERNAME`, `AMQP_MCP_PASSWORD`                                                              | Required for `plain`; password is never copied into `Config`.                       |
| `AMQP_MCP_SASL_HOSTNAME`                                                                              | Optional SASL hostname override.                                                    |
| `AMQP_MCP_TLS_CA_FILE`, `AMQP_MCP_TLS_CERT_FILE`, `AMQP_MCP_TLS_KEY_FILE`, `AMQP_MCP_TLS_SERVER_NAME` | TLS trust, paired client certificate/key, and hostname settings.                    |
| `AMQP_MCP_ALLOW_INSECURE_TRANSPORT`                                                                   | Exactly `true` or `false`; defaults to `false`.                                     |
| `AMQP_MCP_REQUEST_TIMEOUT_MS`, `AMQP_MCP_OPERATION_TIMEOUT_MS`, `AMQP_MCP_SUBSCRIPTION_TIMEOUT_MS`    | Unsigned decimal integers subject to the ranges below.                              |
| `AMQP_MCP_MAX_CONCURRENT_REQUESTS`, `AMQP_MCP_MAX_MESSAGE_BYTES`                                      | Unsigned decimal integers subject to the ranges below.                              |

The server definition configures the child process. The hosting MCP client must
also support the protocol revision used by the proxy.

### Validated settings

| Setting                  | Contract                                                                                                                                                                                                  |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint`, `node`       | Required host-only `amqp://` or `amqps://` URL and request queue address. Default ports are 5672/5671.                                                                                                    |
| `replyAddress`           | Optional client reply queue address (1..4096 characters). Omit to create a temporary queue. Uses the same reply-routing safety checks and prefix policy.                                                  |
| `broker`                 | `generic` (default), `classic`, `artemis`, `rabbitmq`, `servicebus`, or `ibmmq`. RabbitMQ uses `/queues/<name>`; Artemis requires an ANYCAST queue; MQ uses native queue names.                           |
| `tls`                    | Optional `caFile`, paired `certFile`/`keyFile`, and `serverName`. Certificate validation is always enabled; minimum TLS is 1.2.                                                                           |
| `sasl`                   | `none`, `anonymous` (default), `plain`, or `external`. PLAIN requires `username` and `passwordEnv`; EXTERNAL requires TLS with a client certificate/key. Optional `hostname` overrides the SASL hostname. |
| `allowInsecureTransport` | False by default. Plaintext off loopback requires opt-in.                                                                                                                                                 |
| `requestTimeoutMs`       | 60000 by default; 100..3600000. Deadline expiry sends a best-effort cancellation.                                                                                                                         |
| `operationTimeoutMs`     | 10000 by default; 100..60000. Bounds opens, attaches, credit waits, and dispositions.                                                                                                                     |
| `subscriptionTimeoutMs`  | 300000 by default; 100..3600000. The server bounds subscriptions. Set the client request timeout above this value for a full subscription.                                                                |
| `maxConcurrentRequests`  | 16 by default; 1..256. Bounds pending client calls and server work, including batch members.                                                                                                              |
| `maxMessageBytes`        | 1048576 by default; 1024..16777216. Bounds UTF-8 JSON envelopes.                                                                                                                                          |
| `replyAddressPrefixes`   | Empty by default. A nonempty list restricts server reply addresses to matching prefixes. Restricted management nodes and cross-broker routes are always rejected.                                         |

Service Bus requires SASL PLAIN authentication over TLS, with certificate
validation enabled. PLAIN is SASL's username-and-password mechanism; TLS supplies
the encryption.

Set `username` to the shared access policy name and supply that policy's key
through `passwordEnv`. Use a policy scoped to the demo with the permissions
required for request and reply delivery, plus temporary queue creation when
using automatic reply queues. The `servicebus` profile requires `sasl.mode`
to be `plain` and an `amqps://` endpoint.

`replyClientIdleTimeoutMs` sets the server's reply-sender cache idle timeout:
60000 milliseconds by default, with a range of 100..3600000. Active requests
suspend expiry for their reply address. This setting does not change request
deadlines or the lifetime of client-owned temporary queues.

## AMQP binding and failure behavior

Each JSON-RPC envelope travels in an AMQP message with content type
`application/json` and one UTF-8 data section. Requests carry fresh AMQP
`message-id` values and a `reply-to` address. Replies copy the request identifier's
exact AMQP type and value into `correlation-id`. JSON-RPC IDs remain local to
each client and independent of AMQP correlation.

A batch uses one AMQP envelope and one final response envelope. Notifications
have no response and are omitted from the response array. Independent requests
can finish out of order.

With automatic reply queues, the client creates and owns its queue through a dynamic-target
sender link. It keeps that link open while a separate receiver consumes replies.
Closing the receiver alone does not release the queue; the owning sender closes
last. The queue is nondurable, with link-detach expiry and a delete-on-close
policy. A lost connection cannot be transparently resumed.

With `replyAddress`, the client consumes an externally provisioned queue without
owning its lifetime. Both modes use the same correlation checks and deadlines.

Connection loss ends the connection. There is no automatic reconnect or retry.
Manual settlement, where the receiver confirms message processing, does not
guarantee exactly-once execution. A failure after a tool runs but before
settlement can cause redelivery. Non-idempotent operations, such as incrementing
a counter, need the application's own strategy for detecting duplicate calls.

When clients sharing a queue reuse a JSON-RPC ID, cancellation can be ambiguous.
The server ignores such a cancellation rather than cancelling multiple clients.

Broker-specific behavior remains in the transport package. MQ requires explicit
queue termini and `message.to`. Trailing fixed-width padding is removed from
automatically assigned MQ reply addresses; configured addresses remain unchanged.
The MQ profile sends an empty AMQP frame every ten seconds while connected:
the tested broker enforces a thirty-second inactivity limit despite advertising
an idle timeout of zero. Other profiles use their normal negotiated behavior.
Only MQ notification-only envelopes accept the 24-zero-byte `MQCI_NONE`
correlation sentinel. MQ rejects UUID-typed correlations in the tested setup;
string and binary correlations passed. The client generates string identifiers.
The Rhea SASL receive-tail adapter handles a final SASL outcome and AMQP bytes
arriving in the same read. Receiver-created dynamic-source queues are not used.

The live interoperability tests cover Classic, Artemis, RabbitMQ, Service Bus
Premium, and IBM MQ. They exercise the proxy and the separate example package
through stdio. They do not establish full binding conformance or production
reliability. Premium live checks use SASL PLAIN over validated TLS; these tests
do not qualify SASL EXTERNAL.

## Project structure and verification

The library lives in [src](src), with framing in [wire.ts](src/wire.ts),
authentication in [auth.ts](src/auth.ts), client transport in
[transport.ts](src/transport.ts), listener in [responder.ts](src/responder.ts),
and request-scoped SDK dispatch in [sdk.ts](src/sdk.ts). The proxy and common
console lifecycle use [runtime.ts](src/runtime.ts) and [stdio.ts](src/stdio.ts).
The repository contains a separate `amqp-mcp-example-server` package under
`example`; it depends on this package's public exports, not its internal files.

`npm run verify` builds both packages and checks formatting, strict types, and
ESLint with zero warnings. Tests must reach 90% coverage for statements, lines,
and functions, and 80% for branches. Unit tests do not contact brokers.

The release check restricts the archive to allowed paths. It installs
the archive in an isolated consumer, checks its types and imports, and generates
a CycloneDX software bill of materials (SBOM) from that installation. The final
gate audits dependencies for known vulnerabilities.

`build` compiles only the transport/proxy package. `build:example` compiles the
example against the built public declarations; `build:all` runs them in that order.

## Broker verification

The [shared runner](test/interop.mjs) starts the built proxy and example server
and manages their lifetimes for the test. The [broker manifest](test/brokers.json)
references one configuration per broker folder. The runner does not start, stop,
or reconfigure brokers. Run it from this project directory:

```sh
node test/interop.mjs --bootstrap --require-all
```

`--bootstrap` reads credentials from the selected Docker test containers, which
must already be running. Supply Service Bus credentials separately through the
environment. Credentials stay in process environments and do not appear in
reports. Without `--bootstrap`, supply the credential environment variables
named in the manifest.

Use `--broker classic|artemis|rabbitmq|servicebus|ibmmq` to select a broker,
`--manifest <path>` for another manifest, or `--out <path>` for another evidence
directory. A blocked broker fails the run when `--require-all` is set.

The runner writes a self-contained HTML report and JSON evidence under
`tmp/mcp-amqp-demo` by default. MQ qualification results go under
`tmp/ibmmq-qualification`. The project's ignore rules exclude broker evidence,
builds, coverage, and installed dependencies from version control. The npm
release allowlist also excludes broker evidence, coverage, and installed
dependencies from the archive.

### Broker folders and setup

All five tested brokers use the same folder layout and the same interoperability
runner. Local brokers have a `compose.json`; Dockerfiles and broker-specific
configuration live beside it where needed.

| Broker configuration                       | Setup                                                                                                         | Tested version |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------- | -------------- |
| [Classic](test/classic/broker.json)        | [Compose](test/classic/compose.json), [Dockerfile](test/classic/Dockerfile), authenticated AMQP configuration | 6.3.2          |
| [Artemis](test/artemis/broker.json)        | [Compose](test/artemis/compose.json)                                                                          | 2.57.0         |
| [RabbitMQ](test/rabbitmq/broker.json)      | [Compose](test/rabbitmq/compose.json), [Dockerfile](test/rabbitmq/Dockerfile) with rabbitmqadmin 2.35.0       | 4.3.6          |
| [IBM MQ](test/ibmmq/broker.json)           | [Compose](test/ibmmq/compose.json), [Dockerfile](test/ibmmq/Dockerfile), AMQP channel configuration           | 10.0.0.0       |
| [Service Bus](test/servicebus/broker.json) | Existing Premium namespace over validated TLS; no Docker substitute                                           | Premium        |

Validate every local setup without building images or changing containers:

```sh
npm run test:setup:check
```

To provision a local test broker, select its identifier. Setup builds the image
where needed and waits for Compose health checks:

```sh
node test/setup.mjs --broker classic
node test/interop.mjs --bootstrap --broker classic --require-all
```

The same commands accept `artemis`, `rabbitmq`, or `ibmmq`. Setup uses the
selected broker's credential environment variable if supplied, or generates a
random password. Password files stay in an OS temporary directory, outside the
repository, and are retained for container recreation. MQ mounts its password
as a Compose secret; other images receive theirs through environment variables.
Passwords and rendered Compose environments are not printed by the setup command.
`--bootstrap` retrieves the configured credentials when running the tests.

Setup can recreate its named test container. It does not adopt containers
created outside its Compose project. If a container name conflicts, its owner
must resolve the conflict.

The four AMQP ports, `25672` through `25675`, bind only to loopback. These
plaintext development setups do not test production TLS or long-running
reliability. The RabbitMQ build targets Linux x86-64, the tested platform.

The Service Bus test requires a provisioned Premium namespace and a shared
access policy with the permissions needed for the tested dynamic entities.
Supply `AMQP_DEMO_SERVICEBUS_ENDPOINT` (an `amqps://` namespace URL),
`AMQP_DEMO_SERVICEBUS_POLICY` (the policy name), and `AMQP_DEMO_SERVICEBUS_KEY`
(the policy key) through the environment, or configure the endpoint and policy
name in [broker.json](test/servicebus/broker.json). The key remains outside the
configuration file. The emulator is not a replacement for this Premium test.
The setup command does not provision or modify Azure resources.

Broker components retain their own licenses. The MQ Dockerfile adds the matching
AMQP component to `icr.io/ibm-messaging/mq:10.0.0.0-r1` without replacing the
base installation's license state. IBM's developer license applies; the derived
image is not an Apache-licensed part of this package.

The Docker configurations define local test environments. Their images are not
approved for production use. Scan the selected images for vulnerabilities before
use; the npm audit gate covers JavaScript dependencies, not broker images or
their operating systems.

### Broker-specific probes

The [MQ qualification utility](test/ibmmq/qualify.mjs) probes an already-running
container; setup is handled by the shared command above. Its default checks
include twelve authenticated sessions without retries, named-queue delivery,
notification compatibility, and sender-owned temporary queue lifetime.
`--sessions 12` selects authentication only; `--only sender` selects the lifetime
probe. Each run replaces the qualification evidence.

```sh
node test/ibmmq/qualify.mjs --only sender
```

The optional `--dotnet` comparator is for the original AMQP guide workspace
and requires its separately maintained C# sample. That sample is not included
in this repository or required by the standalone Node verification gate.
