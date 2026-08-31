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
- [ADR 0008: TOML configuration and hierarchical environment overrides](0008-toml-configuration.md)
- [ADR 0009: Optional mTLS for the internal snapshot endpoint](0009-internal-mtls.md)
- [ADR 0010: Provider-neutral consumer catch-up boundary](0010-consumer-catch-up-boundary.md)
- [ADR 0011: Barrier snapshot for gap-free hub startup](0011-snapshot-startup-barrier.md)
- [ADR 0012: Process-specific configuration roots](0012-process-specific-configuration.md)
- [ADR 0013: State runtime configuration and ownership](0013-state-runtime-configuration.md)
- [ADR 0014: Resource protocol adapter and preview lifecycle](0014-resource-protocol-adapter.md)
- [ADR 0015: Exact content ingestion and delivery state machine](0015-content-ingestion-and-delivery.md)
- [ADR 0016: v0.5 endpoint and callback security profile](0016-v0.5-security-profile.md)
- [ADR 0017: Internal management query API](0017-internal-management-query-api.md)
- [ADR 0018: Lockstep component releases and distribution](0018-release-distribution.md)
- [ADR 0019: Validate and retain MessageStore subscription options](0019-subscription-context-message-store-consumers.md)
- [ADR 0020: Automated release authority](0020-automated-release-authority.md)
- [ADR 0021: Explicit API authentication modes](0021-explicit-api-authentication-modes.md)
- [ADR 0022: Structured runtime logging](0022-structured-runtime-logging.md)
- [ADR 0023: Topic-governed resource content types](0023-topic-content-type-contract.md)
- [Open implementation gates](open-gates.md)

Persisted-schema changes require an explicit replacement ADR, offline migration
plan, downgrade boundary, and recovery fixtures.
