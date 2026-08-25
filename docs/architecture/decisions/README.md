# Architecture decision records

ADRs record product decisions whose consequences must remain visible in code,
tests, configuration, and release claims.

- [ADR 0001: Product and protocol-library boundary](0001-product-library-boundary.md)
- [ADR 0002: Storage profiles and Kafka-first release](0002-storage-profiles.md)
- [ADR 0003: MessageStore, StateStore, and capabilities](0003-persistence-contracts.md)
- [ADR 0004: State records, revisions, and snapshots](0004-state-records-and-snapshots.md)
- [ADR 0005: Acknowledgement and duplicate semantics](0005-acknowledgement-and-duplicates.md)
- [ADR 0006: Durable mutation visibility](0006-mutation-visibility.md)
- [ADR 0007: Use franz-go for the Kafka provider](0007-franz-go-kafka-provider.md)
- [Open implementation gates](open-gates.md)

Persisted-schema changes require an explicit replacement ADR, offline migration
plan, downgrade boundary, and recovery fixtures.
