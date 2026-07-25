# Threat Model

## Protected Assets

- Message bodies, properties, session state, and dead-letter contents
- Namespace credentials, wrapped data keys, and authorization policy
- Message integrity, ordering guarantees, and settlement state
- Cluster membership, placement, backups, and audit history
- Availability and fair resource access between namespaces

## Trust Boundaries

Clients, public protocol listeners, cluster peers, KMS providers, object
storage, OIDC issuers, and telemetry sinks are separate trust boundaries.
Namespaces are treated as mutually untrusted tenants. Nodes within an admitted
cluster are trusted to run the released Switchyard binary, but every inter-node
connection is mutually authenticated.

## Required Controls

- TLS for production client, administration, and inter-node traffic
- Explicit authentication and scoped authorization for every operation
- Quorum durability and authenticated membership changes
- Per-namespace envelope encryption with external key custody
- Tamper-evident audit records without message bodies
- Bounded request, connection, storage, and audit queues
- Signed release artifacts, backup manifests, and audit batches
- Parser limits and fuzz coverage for every untrusted wire format

## Failure Policy

Switchyard fails closed for authorization uncertainty, missing production KMS
keys, audit backlog exhaustion in compliance mode, storage corruption, unsafe
clock movement, and loss of Raft quorum. It must return an explicit retryable or
terminal error rather than silently weaken the configured guarantee.

## Explicit Non-Claims

Application-level encryption does not protect plaintext while a node is
actively processing an authorized message. Switchyard cannot protect a host
whose kernel or administrator is compromised. Regulatory compliance depends on
the complete deployed environment and operating procedures.

