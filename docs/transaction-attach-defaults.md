# Transaction Attach Defaults

Two independent, additive native opt-ins accommodate specific transaction-link
requests without changing the existing strict APIs. The explicit
[atomic messaging listener](atomic-messaging-ingress.md) selects both; ordinary
listeners and the [posting-only listener](atomic-posting-ingress.md) do not.
This is a wire-profile increment, not an official SDK transaction-scope gate.

## Receiving Settlement Negotiation

`ServerSession::accept_transactional_sender_negotiating_unsettled()` accepts an
original receiver request with sender mode Mixed or Unsettled and receiver mode
Second. It advertises and installs actual Unsettled sender mode. Each outgoing
original is initially unsettled and retains the existing unique sent handle and
exact-original retirement checks. Settled requests, First mode, and early
settled transactional dispositions remain unsupported.

The immutable approval still records the original requested modes. Editing a
request cannot turn a different original profile into an eligible one. Both the
application call and actor acceptance check the same private acceptance policy;
the existing `accept_transactional_sender()` remains strict Unsettled/Second.

This follows the distinction between the receiver's requested sender mode and
the sender's actual mode in [AMQP Transport section
2.7.3](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-attach).
The pinned client's [settlement settings](https://raw.githubusercontent.com/Azure/azure-amqp/v2.7.0/Microsoft.Azure.Amqp/Amqp/AmqpLinkSettings.cs)
omit sender mode for SettleOnDispose, leaving the wire default Mixed while
requesting Second receiver mode.

## Coordinator Count Exception

`ServerConnection::accept_with_transactional_work_defaults()` enables the same
bounded posting and retirement lifecycle as the strict work constructor, plus
one admission exception: a fresh coordinator sender may omit its initial
delivery count, which the native receiver interprets as zero. An explicitly
provided count is still used. This exception does not apply to ordinary producer
links or recovered links.

The ordered actor classifies the request before publishing its approval. Its
immutable coordinator profile records whether omission was admitted. Dedicated
acceptance rechecks that exact profile, so changing between omitted and supplied
counts cannot obtain a different defaulting permission. Existing alias,
generation, recovery, and refusal-history guards still apply.

AMQP Transport section 2.7.3 requires a sender's initial delivery count to be
non-null. This is therefore a narrow interoperability deviation, not a general
AMQP default. The pinned client's [transaction controller](https://raw.githubusercontent.com/Azure/azure-amqp/v2.7.0/Microsoft.Azure.Amqp/Amqp/Transaction/Controller.cs)
omits that field. The default, posting-only, and strict transactional-work
constructors retain their previous behavior.

## Unchanged Boundaries

The messaging adapter still authorizes exact Listen before queue binding and
requires a fixed, non-durable, unfiltered primary non-session queue source.
Actual settlement remains Unsettled/Second; no settled original gains retirement
authority. Coordinator admission still requires a valid authentication grant.

These accommodations do not add management links, cold-first SDK authorization,
transactional acquisition, recovery, cross-queue work, or different retirement
outcomes. Existing resource limits, same-original rollback, exact mixed
preparation, and indeterminate-result rules are unchanged.
