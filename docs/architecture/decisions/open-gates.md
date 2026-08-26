# Open implementation gates

These decisions are intentionally unresolved. Work may continue only up to the
slice that depends on each answer.

## Event-stream API

Define the exact CloudEvents topic, publication, subscription, progress, and
replay contract before implementing event-stream behavior. Resource-topic work
is independent.

## Initial security profile

Choose which inbound authentication, authorization, callback SSRF, and secret
protection controls block the public preview and which remain explicit
deployment controls before implementing the operational/safety slice.

Hub-to-consolidator authentication is resolved by ADR 0009; the remaining gate
covers public endpoints and callback policy.

## Docker Hub identity

Reserve the organization and final component repository names before container
publication is implemented.
