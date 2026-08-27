# Security policy

## Supported versions

WebSubHub has not published a supported runtime release yet. Security fixes are
currently made on the main development branch. This table will be replaced by
an explicit supported-version policy before the first preview publication.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for the repository. If
that channel is unavailable, contact the repository owner privately. Do not
open a public issue with exploit details, credentials, callback URLs, payloads,
or subscription secrets.

Include the affected revision, impact, reproduction steps, and any suggested
mitigation. You should receive an acknowledgement within seven days; a
severity-based remediation and disclosure timeline will be agreed after
triage.

## Current status

The repository is implementing a `v0.5.0` Kafka BYOB developer preview. It
includes JWT authentication and scope authorization, callback SSRF defenses,
encrypted persisted subscription secrets, durable state/content handling,
at-least-once delivery, and protected operational inspection. It has not
completed release, failure-qualification, upgrade, or production-hardening
gates and must not be treated as a supported production deployment.
