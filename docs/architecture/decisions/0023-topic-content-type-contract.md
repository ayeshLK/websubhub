# ADR 0023: Topic-governed resource content types

- Status: Accepted
- Date: 2026-08-31

## Context

Resource-topic registration already accepts an optional `hub.content_type`,
but state schema version 1 discards it. Each publication currently persists its
own content type and topic ID with the content record. That models every record
as a self-describing envelope instead of treating the registered topic as the
governed channel contract.

WebSubHub needs a broker-style and AsyncAPI-compatible rule: a resource topic
defines one representation contract, publishers write conforming bytes to that
topic, and deliveries identify those bytes with the same media type. Repeating
topic metadata in every ordinary content record is unnecessary because the
MessageStore destination already establishes topic ownership.

## Decision

Every resource topic has one required, immutable content type. Registration
normalizes a valid media type and all of its parameters. An omitted
`hub.content_type` means `application/json`. Re-registering a retained topic ID
with a different normalized content type is denied, including after
deregistration.

An exact-content publication must declare a valid content type whose normalized
media type and complete parameter set equal the registered topic content type.
A mismatch is denied before durable content persistence. WebSubHub does not
parse, serialize, transform, or schema-validate the body: the publisher
serializes it, WebSubHub preserves the exact bytes, and the subscriber
deserializes it according to the topic content type.

Ordinary resource content records contain only a stable product message ID and
the exact body bytes. They contain no content type, topic ID, destination,
provider position, or other topic metadata. The MessageStore destination
selects the topic. A delivery worker receives the canonical topic state and
uses its content type for the subscriber HTTP `Content-Type` header.
The canonical topic management DTO exposes this non-sensitive content type so
operators and tooling can inspect the effective channel contract.

This minimal resource-content envelope does not remove optional typed metadata
from the provider-neutral MessageStore SPI because StateStore records have a
separate versioned media type. A dead-letter operation is also a separate
envelope: it owns source topic ID, subscription ID, failure class, attempt, and
storage diagnostics while retaining the original message body and ID. It must
not add those fields to the ordinary content message.

State event and snapshot schema version 2 adds required `Topic.ContentType` and
uses version 2 StateStore media types. Version 1 records remain rejected at
runtime; startup never guesses or rewrites their topic contracts.

The supported developer-preview transition from version 1 is an explicit
offline clean-state replacement:

1. stop all hubs and consolidators;
2. retain the version 1 state destinations for rollback;
3. configure new, empty version 2 state-event and snapshot destinations;
4. start the version 2 deployment; and
5. re-register topics, explicitly supplying non-JSON content types, then
   recreate subscriptions.

Old content destinations are not rewritten. New subscriptions begin at their
documented latest boundary, so pre-transition content is not replayed through
the new contract. Recovery fixtures must prove that version 1 events and
snapshots fail closed and that version 2 topic content types round-trip
deterministically.

Downgrade requires stopping every version 2 process and restoring both the
version 1 binaries and the retained version 1 state destinations. Version 1 and
version 2 processes must not share state destinations.

## Consequences

- A topic has one stable wire representation contract rather than a
  per-publication content-type choice.
- Content-type parameters are significant; for example, adding or changing a
  `charset` is a contract change and is denied for a retained topic.
- Changing a topic content type requires a future explicit migration or a new
  topic identity; deregistration alone does not change the contract.
- The contract validates media-type agreement, not payload validity. JSON
  syntax or an application schema remains the publisher and subscriber's
  responsibility.
- ADR 0003's universal message content-type statement, ADR 0014's version 1
  registration-content-type decision, and ADR 0015's per-message content-type
  persistence and delivery rules are superseded for ordinary resource content.
