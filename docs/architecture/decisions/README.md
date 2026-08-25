# Architecture decision records

ADRs record product decisions whose consequences must remain visible in code,
tests, configuration, and release claims.

- [ADR 0001: Product and protocol-library boundary](0001-product-library-boundary.md)
- [ADR 0002: Storage profiles and Kafka-first release](0002-storage-profiles.md)
- [Open implementation gates](open-gates.md)

The next implementation slice must record the MessageStore, StateStore,
capability vocabulary, state schema, acknowledgement, mutation-visibility, and
snapshot decisions before persisting product data.
