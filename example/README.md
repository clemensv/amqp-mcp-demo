# Example AMQP MCP server

`amqp-mcp-example-server` is an application package, separate from
`amqp-mcp-transport`. It defines `echo({ text })`, `add({ left, right })`, and
a bounded subscription over a fixed tool catalog. All AMQP framing,
authentication, reply routing, and SDK dispatch come from the transport package.

[tools.ts](src/tools.ts) returns a fresh `StatelessMcpServer` with application
handlers. [runtime.ts](src/runtime.ts) passes that factory to `AmqpServer` and
composes the shared CLI lifecycle. It imports only the transport package's
public root and `/cli` entries.

From the parent package directory:

```sh
npm ci
npm run build:all
npm run server -- --config broker.json
```

For explicit arguments, run the example directly:

```sh
node example/dist/server.js --config broker.json
```

The example reads the same strict configuration documented by the
[transport package](../README.md). Both processes need access to the configured
request queue. Supply credentials through environment variables, and use
validated TLS outside loopback. Keep stdin open: EOF or SIGINT/SIGTERM stops
the server. Diagnostics go to stderr.

The MCP revision is fixed to `2026-07-28`; there is no `initialize` exchange.
Echo and add are idempotent, but manual settlement is not an exactly-once
guarantee. Replace these handlers in an application package, not in the
transport library. The parent verification and five-broker demo exercise
this implementation alongside the proxy.

The example is private and is not shipped in the transport archive. The
repository source is covered by the parent's [Apache-2.0 license](../LICENSE)
and [notice](../NOTICE); dependency licenses are unchanged.
