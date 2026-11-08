# switchyardctl

Native administration uses the generated `switchyard.admin.v1` EntityService
client over certificate- and hostname-verified HTTPS/h2. It supports Create,
Get and List for queues, topics and explicit subscriptions. The server must
already have its opt-in administration listener configured. No token signing,
AMQP CBS activation, Delete/Update, Atom or other native services are provided.

Supply an HTTPS **origin**, logical namespace, PEM trust-anchor file and a
regular token file containing one namespace-root Manage JWT. The endpoint host,
logical namespace, token audience and server resource scope are independent;
the CLI does not derive or rewrite claims. Tokens are not accepted in arguments.
At most one surrounding LF or CRLF on either side is accepted; spaces, tabs,
embedded whitespace and multiple credentials are rejected. Token/CA reads are
capped at 16 KiB/1 MiB. There is no insecure TLS override or machine trust change.

```sh
switchyardctl --endpoint https://localhost:9443 --namespace tenant \
  --ca-file /path/to/ca.pem --token-file /path/to/manage.jwt \
  queue create orders --requires-session false --max-delivery-count 10

# The same four connection flags apply to every native command:
switchyardctl --endpoint https://localhost:9443 --namespace tenant \
  --ca-file /path/to/ca.pem --token-file /path/to/manage.jwt topic create news
switchyardctl --endpoint https://localhost:9443 --namespace tenant \
  --ca-file /path/to/ca.pem --token-file /path/to/manage.jwt \
  subscription create news reader --lock-duration-millis 60000
switchyardctl --endpoint https://localhost:9443 --namespace tenant \
  --ca-file /path/to/ca.pem --token-file /path/to/manage.jwt subscription get news reader
switchyardctl --endpoint https://localhost:9443 --namespace tenant \
  --ca-file /path/to/ca.pem --token-file /path/to/manage.jwt \
  queue list --page-size 25 --max-pages 100
```

Create flags preserve optional-field presence, including `false` and `0`.
Defaults and invalid/unsupported combinations are decided by the server, not
duplicated in the client. TTL applies to all kinds; lock duration/delivery count
to queues/subscriptions; message size to queues/topics; sessions and duplicate
detection/history to queues. Explicit zero limits are invalid, except duplicate
history zero retains the native API's default-window meaning. Capacity,
placement, usage and other fields absent from this API are not create flags.
Use `queue get PATH`, `topic get PATH` or `subscription get TOPIC NAME`.
Get refuses a returned entity of a different kind than the command requested.
`subscription list` selects explicit subscriptions across topics.

Successful commands emit one JSON object. Unavailable entity fields are `null`,
not invented zero values. List emits `{"entities":[...],"continuation":null}`
only after the whole traversal succeeds. Page size is 1–100 (default 100), and
the page budget is 1–10,000 (default 100), counting empty continuing pages.
`--continuation BASE64` supplies opaque initial cursor bytes; the CLI only
decodes/echoes and compares cursors, never interprets their internals. Cursor
bytes are capped at 64 KiB; repeated cursors fail. Each decoded RPC response is
capped at 1 MiB. JSON output is capped by `--max-output-bytes` (default 4 MiB,
range 1 KiB–16 MiB), and `--timeout-secs` bounds the entire network operation,
including connection and all pages (default 30, range 1–300). Budget exhaustion,
deadline, cursor cycle or RPC error returns nonzero with no partial catalog on
stdout. A failed/timed-out Create can still have committed: Get before retrying.
Errors preserve the server's status classification without echoing peer
messages, metadata, token contents or transport Debug output.

Offline commands require no connection flags:

```sh
switchyardctl compatibility
switchyardctl store upgrade --data-dir /path/to/stopped-store
```

Stop every server/writer before the existing format-2 to format-3 offline
upgrade. Its content preflight and refusal behavior are unchanged.
