# Open implementation gates

These decisions are intentionally unresolved. Work may continue only up to the
slice that depends on each answer.

## Event-stream API

Define the exact CloudEvents topic, publication, subscription, progress, and
replay contract before implementing event-stream behavior. Resource-topic work
is independent.

## Configuration syntax

Choose the canonical file format and environment override convention before
implementing configuration loading. Preserve one typed hierarchical schema.

## Initial security profile

Choose which inbound authentication, authorization, callback SSRF, and secret
protection controls block the public preview and which remain explicit
deployment controls before implementing the operational/safety slice.

## Docker Hub identity

Reserve the organization and final component repository names before container
publication is implemented.
