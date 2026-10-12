# Demonstrating MCP over AMQP with VS Code

Show VS Code calling the same `echo` and `add` tools through five AMQP 1.0
brokers: ActiveMQ Classic, Apache Artemis, RabbitMQ, IBM MQ, and Service Bus
Premium. The tool implementation stays the same; only the broker configuration
changes. Allow about seven minutes for the recorded walkthrough after rehearsal.

Use the [chronological OBS recording](videos/amqp-mcp-vscode-demo-chronological-2026-10-12.mp4),
which runs for 5:24.27 at 1920x1080, 30 fps, without audio. All twelve native
VS Code calls were executed in the filmed order, starting with an empty Chat
and a fresh log terminal. Each broker's catalog precedes its echo and add
results. The configured RabbitMQ variant runs last. The execution log verifies
each echo completed before add started, all results succeeded, and every sum
was 42.

The edit uses one continuous source recording. It trims idle time and holds
catalog frames for readability; every cut moves forward in that source. The
exporter rejects markers from another recording, reordered markers, and
backward cuts. The MP4 was fully decoded and both panes and result frames were
checked. The uncut source, earlier edits, and execution and media evidence are
retained separately and are not included in this repository.

The published recording lives in `videos/`. Runtime logs, screenshots, and
temporary test evidence stay in the ignored `tmp/` directory. Local credentials
and editor settings are also excluded from Git.

This is a multilateral interoperability demonstration under OASIS governance.
It does not establish OASIS conformance or production reliability.

## Check client compatibility first

The AMQP side implements stateless MCP revision `2026-07-28`, using
`server/discover`. The stdio proxy maps VS Code's `initialize` handshake to
that discovery request and adds the required metadata to later tool calls.
It returns the broker-side server's identity and capabilities to VS Code and
consumes `notifications/initialized` locally. Supported stdio client revisions
are `2024-11-05`, `2025-03-26`, `2025-06-18`, and `2025-11-25`.

Before rehearsing, start one configured server through VS Code and confirm
that its MCP tool catalog contains `echo` and `add`. An AMQP connection alone
does not prove MCP compatibility. If discovery fails, stop and check the client
revision, rebuilt proxy, listener, request queue, and broker permissions before
recording. No separate initialization frontend is required for the supported
client revisions.

If discovery fails during a take, show the compatibility requirement and end
the take. Omit the broker sequence; do not present an SDK-runner recording as
a substitute for VS Code tool calls.

The SDK interoperability runner can verify all brokers independently of this
client requirement. Its results are not evidence of a successful VS Code MCP
session. Do not substitute terminal output for the VS Code tool calls in this
script.

## Prepare off camera

Open the `amqp-mcp` directory as the VS Code workspace. Paths below assume that
its [package.json](package.json), [README.md](README.md), and this script are at
the workspace root. Use the Node versions listed in the README, Docker Compose
for the local brokers, and an existing Service Bus Premium namespace.

Run the build and checks:

```sh
npm ci
npm run verify
npm run test:setup:check
```

Starting or recreating brokers is an explicit operator action. If the local
test brokers are not already prepared, run the following setup commands only
after approving those changes:

```sh
node test/setup.mjs --broker classic
node test/setup.mjs --broker artemis
node test/setup.mjs --broker rabbitmq
node test/setup.mjs --broker ibmmq
```

Setup can recreate its named containers. Use the credentials selected during
setup; do not display passwords, policy keys, password files, or container
environment dumps. These local brokers use plaintext AMQP on loopback only.
Service Bus uses SASL PLAIN over TLS with certificate validation enabled.
Scan the local broker images before use; they are not production images.

### Prepare the request queues

Use an ordinary request queue for each interactive demo. The example does not
create or delete request queues. Have the broker operator provision these
addresses and grant the demo account permission to send and receive:

| Broker              | Endpoint                                   | Request address         | SASL username | Test password variable        |
| ------------------- | ------------------------------------------ | ----------------------- | ------------- | ----------------------------- |
| ActiveMQ Classic    | `amqp://127.0.0.1:25672`                   | `amqp-mcp-demo-classic` | `admin`       | `AMQP_DEMO_CLASSIC_PASSWORD`  |
| Apache Artemis      | `amqp://127.0.0.1:25673`                   | `demo`                  | `admin`       | `AMQP_DEMO_ARTEMIS_PASSWORD`  |
| RabbitMQ            | `amqp://127.0.0.1:25674`                   | `/queues/demo`          | `admin`       | `AMQP_DEMO_RABBITMQ_PASSWORD` |
| IBM MQ              | `amqp://127.0.0.1:25675`                   | `DEV.AMQP.QUALIFY`      | `app`         | `AMQP_DEMO_IBMMQ_PASSWORD`    |
| Service Bus Premium | `amqps://namespace.servicebus.windows.net` | `demo`                  | `mcp-demo`    | `AMQP_DEMO_SERVICEBUS_KEY`    |

Replace the Service Bus hostname and policy name with the prepared namespace
and policy. Use the actual Artemis and RabbitMQ usernames if setup overrides
the defaults. Artemis requires an ANYCAST queue; RabbitMQ uses the `/queues/`
address prefix; IBM MQ uses its native queue name and an enabled AMQP channel.

The recorded run used the existing Service Bus Premium namespace `clemensvams`,
policy `demo`, and request queue `myqueue`. It did not provision Azure resources.

Automatic reply queues require permission to create temporary entities. On
Service Bus, prepare a namespace-scoped shared access policy with the required
delivery and temporary-entity permissions. The local emulator is not a
replacement for this Premium test. No command in this script provisions Azure
resources.

The [interoperability runner](test/interop.mjs) creates isolated request queues
for some brokers and cleans them up afterward. Those generated addresses are
not the ordinary request queues in the table. Prepare the table's queues
separately for the interactive session.

### Start the example listener

For each broker, use a dedicated VS Code terminal and load its password or
policy key into the process environment through your approved secret mechanism.
Configure these nonsecret settings using the corresponding table row:

```powershell
$env:AMQP_MCP_ENDPOINT = 'amqp://127.0.0.1:25674'
$env:AMQP_MCP_NODE = '/queues/demo'
$env:AMQP_MCP_BROKER = 'rabbitmq'
$env:AMQP_MCP_SASL_MODE = 'plain'
$env:AMQP_MCP_USERNAME = 'admin'
Remove-Item Env:AMQP_MCP_CONFIG -ErrorAction SilentlyContinue
Remove-Item Env:AMQP_MCP_REPLY_ADDRESS -ErrorAction SilentlyContinue
$env:AMQP_MCP_PASSWORD = $env:AMQP_DEMO_RABBITMQ_PASSWORD
node example/dist/server.js
```

Repeat with broker profiles `classic`, `artemis`, `ibmmq`, and `servicebus`,
their endpoints, request addresses, usernames, and password variables. Map
`AMQP_DEMO_SERVICEBUS_KEY` to `AMQP_MCP_PASSWORD` for Service Bus. The key is the
password for SASL PLAIN; the policy name is the username.

Keep each listener's terminal open. Wait for its structured ready diagnostic
on stderr before enabling its client. Do not type into or close stdin while
the server is running: EOF stops it. The example listener and the VS Code
proxy are separate processes.

The listener emits started/completed events with its broker, tool, request ID,
and either text length or sum. It does not log raw echo text, raw arguments,
or credentials. Keep these events visible while the native client calls tools.

### Configure VS Code's MCP client

Open `.vscode/mcp.json`, or use **MCP: Open Workspace Folder Configuration**, to
edit the [VS Code-format MCP configuration](https://code.visualstudio.com/docs/agent-customization/mcp-servers).
Configure the proxy, not the example listener, as the stdio
server. The example listener is already running and consumes the request queue.

This script uses an extension-host Chat session that resolves interactive
`${input:...}` password prompts. VS Code does not forward definitions requiring
those prompts to Agent Host sessions. If the selected client runs on Agent Host,
prepare its supported secret-input mechanism separately and repeat the
compatibility gate. The portable `.mcp.json` format uses `mcpServers` rather
than the `servers` object shown here; do not copy this definition unchanged
into that format.

The following is the RabbitMQ definition. Create five definitions with unique
server names and password input IDs, using the table beneath it. Do not add
`AMQP_MCP_CONFIG`; it would override the environment-only connection settings.

```json
{
  "servers": {
    "amqp-rabbitmq": {
      "type": "stdio",
      "command": "node",
      "args": ["${workspaceFolder}/dist/proxy.js"],
      "env": {
        "AMQP_MCP_ENDPOINT": "amqp://127.0.0.1:25674",
        "AMQP_MCP_NODE": "/queues/demo",
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
      "description": "RabbitMQ demo password",
      "password": true
    }
  ]
}
```

| VS Code server name | `AMQP_MCP_BROKER` | Password input ID   | Connection settings                           |
| ------------------- | ----------------- | ------------------- | --------------------------------------------- |
| `amqp-classic`      | `classic`         | `classic-password`  | Classic table row                             |
| `amqp-artemis`      | `artemis`         | `artemis-password`  | Artemis table row                             |
| `amqp-rabbitmq`     | `rabbitmq`        | `rabbitmq-password` | RabbitMQ table row                            |
| `amqp-ibmmq`        | `ibmmq`           | `ibmmq-password`    | IBM MQ table row                              |
| `amqp-servicebus`   | `servicebus`      | `servicebus-key`    | Prepared Premium namespace, policy, and queue |

Each copied definition needs its matching `inputs` entry with `password: true`.
Enter the same account credential used by that broker's example listener.
Use **MCP: List Servers** to start a server, accept the workspace/server trust
prompt when appropriate, and open its output to confirm readiness and discovery.
If VS Code inherits `AMQP_MCP_CONFIG` from its launch environment, clear that
variable and relaunch the editor before testing these definitions.

In Chat, select Agent mode and a model with tool support. Inspect the native
tool picker and expand the selected broker's group to show `echo` and `add`.
Do not change global tool selections. Resolve a `#amqp-rabbitmq` server attachment
in the prompt, or the corresponding name for another broker, to restrict that
turn to the intended server. Verify the server name on each actual invocation.

### Rehearse and verify

Run the two calls in the recording script for each broker. Expand each tool
invocation and verify its server, arguments, and returned content. A model can
answer arithmetic without calling a tool; a chat answer alone is not evidence.
Do not record a broker until its actual invocations pass.

For separate transport evidence, supply the Service Bus environment variables
documented in the README and run:

```sh
node test/interop.mjs --bootstrap --require-all
```

Stop the interactive MCP definitions and example listeners before running this
separate check. The runner starts its own proxy and example processes; Classic
uses the same fixed request queue as the interactive demo, while the other
brokers get isolated request queues. Run this check outside the recording,
then restart the interactive listeners for rehearsal. The runner reads local
container credentials without displaying them.
`--require-all` fails if any broker is blocked or fails. Its report is written
to `tmp/mcp-amqp-demo/report.html`.

## Recording script

Record Classic, Artemis, RabbitMQ, IBM MQ, and Service Bus in that order, then
the configured RabbitMQ variant. Show each catalog before its echo and add
calls. Start with an empty Chat and fresh terminal, keep the same log visible,
and never clear it during the take. If a call requires a retake, repeat the
whole sequence. Use one continuous source and forward-only cuts.

Keep VS Code at a readable zoom. Use the same window size, Chat position, and
tool-result layout throughout. Close notifications and unrelated tabs before
recording. Prepare the terminals, broker views, and tool permissions beforehand.
Password and trust prompts belong off camera.

Use two dedicated window captures in OBS: VS Code occupies the left 1280 pixels
and a listener-log terminal the right 640 pixels of a 1920x1080 canvas. Keep
started/completed events visible during the calls. Exclude unrelated desktop
windows, command history, secret inputs, and account details. For reply queues,
show sanitized assigned addresses and broker queue snapshots in the same log.

Use the following opening narration:

> VS Code is the MCP client. A local stdio proxy sends its tool requests through
> an AMQP broker to the example server. Replies return through a client reply
> queue. The example implements two tools: echo returns text, and add returns
> the sum of two numbers. We will run the same tools through five brokers.

| Time budget | Screen action                                                                               | Narration and evidence                                                                                                                                                                                                                                                                                          |
| ----------- | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0:00-0:35   | Show the proxy mapping and one prepared MCP definition.                                     | "VS Code initializes its stdio connection. The proxy maps that handshake to stateless MCP `2026-07-28` discovery over AMQP." Show the enabled server and successful tool catalog.                                                                                                                               |
| 0:35-1:20   | Enable `amqp-classic`; run the two prompts below, then briefly show `example/src/tools.ts`. | "First, ActiveMQ Classic." Expand both calls and hold the results. Then show the `echo` and `add` handlers: "This is the application code used for every broker."                                                                                                                                               |
| 1:20-2:05   | Disable Classic, enable `amqp-artemis`, and repeat.                                         | "The same tools now use Apache Artemis and an ANYCAST request queue." Show the selected server and both actual calls.                                                                                                                                                                                           |
| 2:05-2:50   | Switch to `amqp-rabbitmq` and repeat.                                                       | "RabbitMQ uses its queue address syntax. The application handlers are unchanged." Show the selected server and results.                                                                                                                                                                                         |
| 2:50-3:35   | Switch to `amqp-ibmmq` and repeat.                                                          | "IBM MQ uses a native queue name. The transport handles its queue-specific framing." Show both calls; do not claim every AMQP identifier type is supported.                                                                                                                                                     |
| 3:35-4:45   | Switch to `amqp-servicebus` and repeat.                                                     | "This connection uses a Premium Service Bus endpoint over `amqps`. The SASL policy credential stays off camera." Show `AMQP_MCP_SASL_MODE: plain` and the nonsecret `amqps://` endpoint, then both tool results. Certificate validation is an implementation requirement, not established by this screen alone. |
| 4:45-6:30   | Show the reply-queue choice described below on one broker.                                  | Contrast client-owned temporary queues with an externally provisioned response queue. Show the actual addresses, successful tool calls, and queue lifetime evidence.                                                                                                                                            |
| 6:30-7:00   | Show the completed five-broker observation table below and return to the tool code.         | "Five brokers, the same example tools. These calls demonstrate interoperability; they do not prove exactly-once execution or production reliability." Label separate SDK-runner evidence as such.                                                                                                               |

For each broker, use a fresh chat or a clearly separated turn and these prompts.
Replace `BROKER_NAME` with the visible broker name. The recorded run used this
combined prompt with a resolved server attachment, changing the server and text
for each broker:

> Use only #amqp-rabbitmq MCP tools, both echo and add from the amqp-rabbitmq
> server. List the discovered echo and add tool names and their input schemas.
> Call echo with text Hello through RabbitMQ. Then call add with left 19 and
> right 23. Call BOTH tools and show their actual results. Do not edit files
> or run commands.

The supported VS Code CLI can submit the same prompt with
`code chat --reuse-window --mode agent "PROMPT"`. The `#` is required.
The `add` schema uses `left` and `right`, not `a` and `b`.

Separate turns can use these prompts, each with the resolved server attachment:

> Use the echo tool from the enabled AMQP MCP server with text
> "Hello through BROKER_NAME". Show the tool result.

Expected tool arguments: `{"text":"Hello through BROKER_NAME"}`. Expected returned
text and structured content contain exactly that text.

> Use the add tool from the same server with left 19 and right 23. Show the tool
> result. Do not answer without calling the tool.

Expected tool arguments: `{"left":19,"right":23}`. Expected text is `42` and
structured content is `{"sum":42}`. Inspect the tool card even if the final
chat answer is correct. If the model chooses another tool or skips the call,
reset the turn and correct the tool selection before recording.

The time budget is an editing target, not a fixed delay. Advance only after
discovery and replies finish. Leave each completed tool card visible for a few
seconds so viewers can read the server, inputs, and result. Trim idle time
between broker changes; do not cut away before a result establishes success.

Complete this table from the observed VS Code tool cards during rehearsal and
show it in Markdown Preview for the final shot. Record the rehearsal date and
client build beside it. Fill every observation cell before using it on camera;
this table summarizes observations and is not live proof or SDK-runner output.

| Broker              | Request address form | Observed echo text                | Observed add result |
| ------------------- | -------------------- | --------------------------------- | ------------------- |
| ActiveMQ Classic    | Named queue          | Hello through ActiveMQ Classic    | `42`, `{"sum":42}`  |
| Apache Artemis      | ANYCAST queue        | Hello through Apache Artemis      | `42`, `{"sum":42}`  |
| RabbitMQ            | `/queues/<name>`     | Hello through RabbitMQ            | `42`, `{"sum":42}`  |
| IBM MQ              | Native queue name    | Hello through IBM MQ              | `42`, `{"sum":42}`  |
| Service Bus Premium | Namespace queue      | Hello through Service Bus Premium | `42`, `{"sum":42}`  |

Observed on 2026-10-12 with VS Code 1.141.0. Each row was verified from native
expanded invocation cards and matching listener execution events.

### Show both reply-queue modes

Use RabbitMQ for this segment so the queue addresses are easy to compare.

1. With `AMQP_MCP_REPLY_ADDRESS` absent, show the proxy's ready diagnostic and
   its assigned reply address. Call `echo` once. If a prepared broker console
   exposes the queue, show its temporary lifetime without displaying credentials.
2. Stop the RabbitMQ MCP definition through VS Code. Have the operator provision
   a separate `/queues/mcp-vscode-replies` queue with send permission for the
   listener and receive permission for the client. Disable queue expiry and
   auto-delete for this retention demonstration.
3. Add `"AMQP_MCP_REPLY_ADDRESS": "/queues/mcp-vscode-replies"` to the
   `amqp-rabbitmq` definition's `env` block. Restart that MCP definition, confirm
   its tool catalog, and call `echo` again. Show the configured reply address.
4. Stop the MCP definition and confirm in the broker console that the ordinary
   reply queue remains. The transport closes its receiver but does not request
   deletion. Keep the listener running while making this comparison.

Use one active client consumer per preconfigured reply queue. Competing
consumers can take each other's responses. This mode does not recover pending
calls after a restart or provide automatic reconnect. Restore the rehearsed
configuration before repeating a take.

In the recorded RabbitMQ comparison, both modes returned the supplied echo text
and sum `42`. The automatically assigned reply queue had one consumer during
the calls and disappeared after its owning proxy detached. The configured
`mcp-vscode-replies` queue had one consumer during its calls and remained with
zero consumers after detachment. RabbitMQ reported `durable: true` and
`auto_delete: false` for both observed entities; the temporary lifetime claim
is based on disappearance, not those management flags. The configuration was
restored to automatic replies, and the existing configured queue was retained.

## Failure checks and shutdown

| Symptom                              | Check before continuing                                                                                                                                                          |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Proxy starts but tools do not appear | Confirm the client uses a supported revision and the proxy was rebuilt. Check the mapped discovery response, example listener, and request queue.                                |
| Authentication or attach failure     | Check the selected endpoint, actual username, credential input, queue existence, and broker permissions. Keep secrets off screen.                                                |
| Request times out                    | Confirm the example listener is ready on the same broker and request address. Check for another consumer on the reply queue. Do not repeatedly retry a state-changing operation. |
| Correct chat answer, no tool card    | The model answered without demonstrating MCP. Select only the intended tools and repeat the turn.                                                                                |
| Wrong broker in the tool card        | Disable the other MCP definitions and recheck the tool picker.                                                                                                                   |

At the end, stop the five MCP definitions through VS Code. Stop each example
listener with Ctrl+C in its own terminal and confirm it exits. Leave existing
broker containers and the Premium namespace running unless the operator
explicitly approves teardown. Delete only demo-owned ordinary queues through
the broker's administration tools when authorized; do not delete shared queues.
