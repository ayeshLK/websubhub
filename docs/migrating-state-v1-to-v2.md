# Offline state schema v1-to-v2 migration

State schema version 2 adds the optional detached subscription-parameter
multimap. Runtime processes accept only version 2 after this upgrade. They do
not rewrite version 1 events or snapshots during startup.

No release predating schema version 2 was published. This procedure exists for
development or preview deployments that retained version 1 Kafka state.

## Boundary

Perform the migration while every `websubhub` and
`websubhub-consolidator` process is stopped. Back up both state destinations
and their consumer-group offsets before changing records. Kafka records are
immutable, so migrate into new event and snapshot destinations rather than
modifying the existing destinations in place.

The transformer accepts one exported JSON record body on standard input and
writes one version 2 JSON body on standard output:

```sh
go run ./internal/tools/statev1tov2 event < event-v1.json > event-v2.json
go run ./internal/tools/statev1tov2 snapshot < snapshot-v1.json > snapshot-v2.json
```

The input is bounded to 16 MiB. The transformer accepts exactly version 1,
initializes subscription parameters as absent, validates the complete version 2
result with the runtime decoder, and rejects unknown fields or versions.

## Kafka record migration

For each source record, preserve its MessageStore message ID, Kafka ordering,
and safe metadata while replacing only the transformed JSON body and content
type:

- events: `application/vnd.websubhub.state-event+json; version=2`;
- snapshots: `application/vnd.websubhub.state-snapshot+json; version=2`.

Migrate every retained event in source order. Migrate snapshots independently;
the consolidator selects the greatest valid snapshot revision. Do not copy
provider offsets into state payloads.

After importing the replacement destinations, update the state destination
configuration, initialize consumer progress at the intended migrated boundary,
and start the consolidator before hubs. Verify the canonical snapshot revision
and management view before reopening public mutations.

## Downgrade

Once a version 2 event is appended or snapshot is published, a version-1-only
binary cannot recover the state. Downgrade requires stopping all processes and
restoring the pre-migration destinations and consumer progress from backup.
