# ADR 0005: Acknowledgement and duplicate semantics

- Status: Accepted
- Date: 2026-08-25

## Decision

Delivery is at least once. A consumer advances durable progress only through
the highest contiguous successfully acknowledged or durably dead-lettered
record. Nack requests redelivery without advancing progress. Temporary close
preserves consumer identity and progress; permanent close deletes them.

HTTP success followed by a crash before durable acknowledgement may duplicate
delivery. DLQ publication and source progress may have a provider-declared
duplicate window unless the provider reports a tested atomic boundary.

## Consequences

Consumers reject acknowledgement beyond an unresolved earlier record. Delivery
is sequential per subscription and includes a stable product message ID.
Exactly-once is not a product claim.
