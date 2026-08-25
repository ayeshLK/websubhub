# ADR 0007: Use franz-go for the Kafka provider

- Status: Accepted
- Date: 2026-08-25

## Context

The Kafka BYOB preview needs idempotent production, `acks=all`, bounded polling,
manual contiguous offset commits, consumer-group lifecycle control, topic
administration, TLS/SASL composition, and testable request behavior. Kafka
client types must remain inside the provider implementation.

## Decision

Use `github.com/twmb/franz-go` v1.21.6 and its `pkg/kadm` v1.18.0 module. Both
match the product's Go 1.25 minimum and use a compatible BSD license.

The provider always requests all in-sync replica acknowledgements and retains
franz-go's idempotent producer default. Consumers disable auto-commit, bound
each poll, block rebalances while records are unresolved, and synchronously
commit only the contiguous head. Product consumer IDs are hashed into
provider-safe Kafka group names. Kafka offsets, partitions, group names, and
records never become product identities or public API values.

DLQ handling is an emulated publish-then-commit boundary with a documented
crash duplicate window. Kafka transactions are reported as restricted because
the v0.5 MessageStore SPI does not expose them.

## Consequences

`franz-go` and `kadm` are reviewed direct dependencies. Kafka-specific options
and types stay under `internal/persistence/messagestore/kafka`. TLS and SASL are
supported by the typed provider configuration; their canonical file and
environment syntax remains behind the existing configuration decision gate.
