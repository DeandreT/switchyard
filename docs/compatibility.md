# Compatibility

Switchyard targets the Azure Service Bus Standard messaging model. A capability
is marked supported only after it has protocol-level tests and end-to-end
coverage with the relevant client.

## Client Gates

| Client | Data plane | Administration | Status |
| --- | --- | --- | --- |
| Official .NET SDK, current stable | Planned | Planned | Not implemented |
| Official .NET SDK, previous stable | Planned | Planned | Not implemented |
| Sift pinned revision | Planned | Planned | Not implemented |

## Capability Matrix

| Capability | Target release | Status |
| --- | --- | --- |
| AMQP 1.0 over TLS | Pre-1.0 | Not implemented |
| AMQP over WebSockets | Pre-1.0 | Not implemented |
| SASL PLAIN and CBS SAS/JWT | Pre-1.0 | Not implemented |
| Queue send, receive, peek, and settlement | Pre-1.0 | Not implemented |
| Receive-delete | Pre-1.0 | Not implemented |
| Topics and subscriptions | Pre-1.0 | Not implemented |
| Correlation and SQL filters/actions | Pre-1.0 | Not implemented |
| Scheduling and cancellation | Pre-1.0 | Not implemented |
| Deferral and deferred receive | Pre-1.0 | Not implemented |
| Dead-letter and resubmit | Pre-1.0 | Not implemented |
| Sessions and session state | Pre-1.0 | Not implemented |
| Duplicate detection | Pre-1.0 | Not implemented |
| Same-placement-group transactions | Pre-1.0 | Not implemented |
| Atom/XML entity and rule administration | Pre-1.0 | Not implemented |
| Native gRPC administration | Pre-1.0 | Contract scaffolded |
| Partitioned entities | Later | Out of initial scope |
| Cross-placement-group transactions | Later | Out of initial scope |
| Geo-replication | Later | Out of initial scope |
| Premium-tier features | Uncommitted | Out of scope |

Switchyard intentionally does not reproduce Azure subscription, namespace
capacity, or operations-per-second commercial quotas. It defaults to compatible
wire validation, including the Standard 256 KiB message-size limit, while
allowing operators to configure larger namespace storage quotas.

