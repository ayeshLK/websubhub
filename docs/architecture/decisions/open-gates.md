# Open implementation gates

These decisions are intentionally unresolved. Work may continue only up to the
slice that depends on each answer.

## Event-stream API

Define the exact CloudEvents topic, publication, subscription, progress, and
replay contract before implementing event-stream behavior. Resource-topic work
is independent.

The initial security profile is resolved by ADR 0016. JWT endpoint security,
dial-time callback SSRF controls, and file-backed secret encryption block the
public preview. Hub-to-consolidator authentication remains governed by ADR
0009.

## Docker Hub identity

Reserve the organization and final component repository names before container
publication is implemented.
