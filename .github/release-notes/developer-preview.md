> [!IMPORTANT]
> This is a pre-1.0 developer preview, not a production-readiness claim.

## Delivery and compatibility

WebSubHub delivery is at least once. Subscribers must tolerate duplicate
notifications around retries, acknowledgement failures, consumer recovery, and
process restarts. The release does not claim end-to-end exactly-once delivery.

`websubhub` and `websubhub-consolidator` use one lockstep product version and
should be rolled out together. Consult the release-specific state and migration
notes before upgrading or rolling back; binary versions alone do not establish
persisted-state compatibility.

## Preview limitations

Automatic subscription renewal, lease expiry, ownership transfer and fencing,
event-stream APIs, and complete failure qualification are deferred. macOS
archives are not Apple-notarized and Windows archives are not Authenticode
signed. Checksums, Sigstore bundles, SBOMs, and provenance are provided for the
portable artifacts.
