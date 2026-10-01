# AMQP Over WebSockets

The optional WebSocket listener carries the existing AMQP engine and broker
operations. It does not implement a second messaging state machine. TLS, SASL,
CBS authorization, endpoint identity fences, message limits, settlement, and
management requests keep their existing semantics.

## Listener Configuration

The binary enables a separate listener with `--websocket-listen`. For a local
development node, use `--websocket-listen 127.0.0.1:8080`; raw AMQP still defaults
to `127.0.0.1:5672`. WebSockets are disabled unless an address is supplied.
Every configured AMQP, WebSocket, and native administration socket is bound
before any of them starts accepting clients. They are separate listeners, not
HTTP paths multiplexed on the raw AMQP socket.

Supplying `--tls-certificate` and `--tls-private-key` secures both AMQP listeners
and native administration. The WebSocket TLS listener negotiates HTTP/1.1, not
HTTP/2. A shared-access policy applies to both AMQP listeners. Production mode
still requires TLS and authentication; development mode can expose plaintext
WS, which does not protect credentials or messages in transit.

Library callers opt in with `AmqpListener::with_websocket()`. Either ordering
with `with_tls()` selects the same secure transport. Raw AMQP remains the
default. Connection limits are per listener, not one shared node-wide pool.

## Upgrade Contract

The accepted endpoint is exactly `/$servicebus/websocket/`, with no query string
or request body. It uses an HTTP/1.1 GET with a valid Host and standard
WebSocket upgrade headers. Duplicate singleton upgrade headers are refused.
The client must offer the case-sensitive subprotocol `amqp`; the successful
response echoes exactly that value. Multiple offered protocols are permitted,
but an absent, malformed, or differently cased `amqp` offer is refused.
Compression is not negotiated.
Host authorities may not contain userinfo; an optional port must be a nonzero
16-bit integer. These restrictions are local HTTP admission policies.

This path is the official Service Bus client's transport path. The path and
additional HTTP validation are local policies; the
[OASIS binding](https://docs.oasis-open.org/amqp-bindmap/amqp-wsb/v1.0/cs01/amqp-wsb-v1.0-cs01.html)
does not prescribe an endpoint path. The HTTP Host can name the physical
endpoint, such as localhost, while AMQP Open and CBS identify the configured
logical namespace. An HTTP Host is not an authorization grant or namespace
selector.

## Wire And Resource Limits

WebSocket framing, masking, fragmentation, and control frames use the pinned
WebSocket library. The binary payload adapter enforces the AMQP binding:

- Each initial SASL or AMQP protocol header occupies one complete eight-byte
  WebSocket message. The AMQP header after SASL is also standalone.
- Later SASL and AMQP frames may split across binary messages, or several
  frames may share a binary message. WebSocket fragmentation may divide one
  message, including a protocol header.
- Text is refused. Ping, Pong, and empty binary messages are not AMQP bytes or
  an end-of-stream marker. Client frames must be masked.
- The HTTP handshake has a 16 KiB input budget. The library also imposes its
  own bounded header-count and handshake-read safeguards.
- One WebSocket frame and one assembled message each have a 4 MiB payload
  ceiling. The adapter retains at most one incoming binary message at a time.
- The read buffer and each outbound binary chunk are 16 KiB. The library's
  maximum write buffer is 64 KiB; there is no additional outbound message queue.
- Ready control/empty-message processing yields under a bounded polling budget.
  AMQP complete-frame idle deadlines still apply: WebSocket-only traffic does
  not keep an AMQP connection alive indefinitely.

These are local bounds, not Azure quotas or a total process-memory guarantee.
WebSocket buffers are additional to the existing AMQP frame, message, and
retained-content limits. In particular, a 4 MiB WebSocket ceiling does not
increase the engine's advertised 262,144-byte receive-frame maximum or the
broker's producer-message ceiling.

The existing absolute handshake deadline covers TLS, HTTP upgrade, SASL, and
AMQP Open together. Progress in one layer does not restart it. CBS authorization
has its separate post-Open deadline. Excess sockets and failed negotiations
release their listener admission slot without changing broker state.

## Close And Diagnostics

A completed AMQP Close initiates a WebSocket closing handshake. The listener
waits for the engine to relinquish both IO halves, then attempts to close and
flush the WebSocket transport and shut down its underlying socket. Cleanup has
a five-second local deadline, and admission remains held until it finishes or
that deadline expires. Failure or timeout discards the transport; failed writes
do not guarantee a Close acknowledgment or TLS close-notify. A received peer
Close is flushed before normal AMQP end-of-stream. Malformed framing,
non-binary messages, and oversized input select fixed best-effort close reasons
rather than echoing input.

This is connection cleanup, not a new node-wide graceful shutdown contract.
Aborting a listener task does not promise to synchronously drain every already
accepted connection.

The binary unconditionally suppresses the WebSocket dependencies' `log`
targets before forwarding to tracing, even when `RUST_LOG` requests trace.
Those dependencies can log headers and complete wire payloads. Other diagnostic
targets remain available. Applications embedding the listener own their logging
configuration and must apply equivalent filtering.

## Verification Scope

Rust socket tests cover WS and WSS against both storage backends, successful
messaging and clean closing, malformed upgrades and frames, header boundaries,
deadlines, limits, authentication, and endpoint identity fencing. The two
pinned official .NET clients have separate WSS gates in addition to their
existing raw-TLS workflows. The gates exercise queue fidelity, peek, renewal,
redelivery, sessions, state, FIFO, and independent topic subscription copies.
Their public client normalizes the custom endpoint scheme to a TLS transport;
changing that endpoint to `ws://` does not disable TLS. Plain WS coverage comes
from the Rust socket suite, not the official-client gates. See the pinned
[client endpoint construction](https://github.com/Azure/azure-sdk-for-net/blob/Azure.Messaging.ServiceBus_7.21.0/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpClient.cs).

The Linux WSS client tests generate an isolated CA and localhost certificate.
Only their child processes receive `SSL_CERT_FILE` and `SSL_CERT_DIR` overrides.
An untrusted-certificate control must fail without broker mutations; trusted
WSS must then complete. No global certificate trust or
insecure WebSocket certificate callback is installed.
