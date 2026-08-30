# ADR 0019: Validate and retain MessageStore subscription options

- Status: Accepted
- Date: 2026-08-29

## Context

WebSub content delivery opens one MessageStore consumer for each durable
subscription. Broker implementations can offer useful subscriber-selected
behavior. For Kafka, subscribers may need competing consumers in one group or
explicit consumption from selected topic partitions. Future IBM MQ and Solace
implementations can have different subscription-level options.

Interpreting those options in the resource protocol adapter or delivery manager
would add broker branches to the core application. Treating MessageStore as a
generic broker library, however, would prevent the provider from seeing the
subscription options that configure a delivery consumer. MessageStore is an
internal WebSubHub SPI and may accept bounded product context while keeping
broker behavior inside provider implementations.

`lib-websubhub` detaches non-standard subscription form parameters into both
`Subscription.Parameters` and `VerifiedSubscription.Parameters`. The current
state model discards them, so a consumer reopened after projection recovery
cannot reproduce customized broker behavior.

The library deliberately separates asynchronous application validation from
the verified persistence handoff. `OnSubscriptionValidation` can reject a
request with `DeniedError` and owns the resulting `hub.mode=denied` callback.
`OnSubscriptionVerified` receives successfully verified intent and persists
application state; its failures use the existing operational `hub-error`
behavior when enabled. Turning the latter into another denial boundary would
break that lifecycle contract to accommodate provider validation.

## Decision

Durable subscription state retains a bounded, detached copy of non-standard
subscription parameters. These parameters are not a credential channel and
must never contain or be treated as provider credentials. They are omitted from
management DTOs, metrics, and logs.
MessageStore defines a provider-neutral subscription-options value containing
only the detached parameter multimap. It does not contain the callback URL,
plaintext or sealed secret, authorization metadata, provider credentials, or a
transient `websubhub.Subscription` value.
The product accepts at most 32 option keys, 8 values per key, 128 bytes per key,
1,024 bytes per value, and 8,192 aggregate key/value bytes. The Kafka consumer
group is additionally limited to 255 valid UTF-8 bytes. Exceeding a bound is a
permanent subscriber configuration rejection.


The MessageStore Administrator exposes a side-effect-free
`ValidateSubscription` operation accepting the content destination and
subscription options. The resource adapter invokes it from
`OnSubscriptionValidation` after its existing topic, callback, and duplicate
checks. The adapter does not inspect broker-specific keys.

The MessageStore consumer creation contract accepts the same subscription
options as optional context. WebSub delivery consumers provide the options
restored from durable state; system consumers such as hub state projections and
the consolidator omit them. No separate provider provisioning operation is
added for this feature.

MessageStore providers own recognition, validation, normalization, and use of
provider-specific subscription parameters. Validation must not create a
consumer, consumer group, destination, offset, or other provider resource.
Consumer opening validates defensively as well, because persisted state may be
invalid or provider topology may change after pre-verification validation.

The Kafka provider recognizes these optional form parameters:

- `kafka.consumer_group`: the Kafka consumer group for competing delivery;
- `kafka.topic_partitions`: a comma-separated list of non-negative Kafka
  partition IDs for explicit assignment.

They are mutually exclusive. Kafka validates single-value occurrence, bounded
length, partition syntax, duplicates, and destination membership. Explicit
assignment may still use the subscription's provider-derived internal group to
restore and commit offsets, but the subscriber cannot customize that group at
the same time.

## Subscription lifecycle

Validation and persistence retain their existing library meanings:

1. the protocol handler returns `202 Accepted`;
2. `OnSubscriptionValidation` performs the existing product checks and calls
   `Administrator.ValidateSubscription`;
3. a confirmed invalid subscriber configuration is mapped to
   `websubhub.DeniedError`, and the library sends the existing best-effort
   `hub.mode=denied` status callback;
4. only successful validation proceeds to callback intent verification;
5. `OnSubscriptionVerified` seals any plaintext subscription secret and
   appends the durable subscription, including the detached options;
6. projection reconciliation opens the delivery consumer with those options.

Validation classifies failures without exposing raw provider errors. Malformed,
mutually exclusive, nonexistent, or out-of-range recognized options are
permanent subscriber rejections with a bounded safe reason. Broker
unavailability, timeouts, authorization failures, and indeterminate metadata
lookups are infrastructure errors. They retain the library's existing optional
`hub.mode=hub-error` behavior and never masquerade as subscriber denials.

Validation is side-effect-free and therefore safe before proof of callback
control. It is not a reservation: provider topology can change between
validation and consumer opening. If defensive validation of restored or raced
state fails permanently, the delivery worker appends one safe stale transition
and stops instead of entering its infrastructure reconnect loop. Transient
consumer-opening failures retain bounded, context-cancellable reconnect
behavior.

Provider group names and partition offsets remain non-product identities.
Allowing a subscriber to supply delivery configuration does not make that
configuration a topic ID, subscription ID, consumer receipt, or management
resource identity. This decision narrowly amends ADRs 0003 and 0007, which
previously excluded all group names and partitions from public API values. It
does not change the verified-subscription sequencing established by ADR 0014.

## Default behavior and compatibility

When neither recognized Kafka parameter is present, behavior is unchanged:

- the existing stable subscription consumer ID derives the existing unique
  provider-safe Kafka group;
- Kafka uses ordinary topic subscription and managed partition assignment;
- every ordinary WebSub subscription has an independent group and receives the
  complete content stream;
- new identities start at `StartLatest`, reopened identities resume committed
  progress, and permanent closure removes the unique group.

A nil parameter map, an empty map, and unrelated extension parameters are
equivalent for Kafka. Custom behavior is strictly opt-in. A shared custom group
must not be deleted when one member unsubscribes. Explicit assignment honors
the `ConsumerSpec` start position when no committed offset exists.

No WebSubHub release or image predates this decision, so the subscription
parameter field is incorporated directly into the initial state schema version
1. Persisted development state produced by earlier unreleased commits has no
compatibility guarantee and must be discarded and recreated rather than
migrated. This is a pre-release exception, not a change to the compatibility
rule in ADR 0004. After the first release, persisted-schema changes require a
new version and an explicit offline migration; runtime startup continues to
reject unknown versions and never silently rewrites state.

## Consequences

- The existing `lib-websubhub` v0.6.0 lifecycle is sufficient; no new
  post-verification denial behavior or library release is required.
- MessageStore remains provider-neutral while its implementations can validate
  and use subscription-level behavior without broker conditionals above the
  provider boundary.
- Validation can perform provider metadata reads but cannot provision or reserve
  resources, so consumer opening remains the authoritative runtime check.
- Provider implementations receive only the option multimap, not callback or
  secret fields, reducing accidental disclosure risk.
- Tests must distinguish permanent configuration denial from transient
  validation failure and verify that only the former emits `hub.mode=denied`.
- Kafka tests must cover default compatibility, shared-group lifecycle,
  explicit assignment and offset recovery, invalid and mutually exclusive
  parameters, topology races, reconnect, and permanent closure.
- End-to-end tests must continue to prove that two subscriptions without
  customization each receive the same published content at least once.
- Selecting partitions above zero requires a Kafka content destination with
  those partitions. Subscriber parameters never create or resize a destination.

## Related work

- [WebSubHub issue 9](https://github.com/ayeshLK/websubhub/issues/9) tracks the
  product implementation of this decision.
- [lib-websubhub issue 26](https://github.com/ayeshLK/lib-websubhub/issues/26)
  proposed treating a `DeniedError` from `OnSubscriptionVerified` as a
  subscriber denial. It was closed without a library change because validation
  belongs to `OnSubscriptionValidation`; the verified callback remains the
  application persistence handoff.
