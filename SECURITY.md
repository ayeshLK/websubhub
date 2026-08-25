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

The repository is only a build and quality scaffold. It is not safe to deploy
as a hub. Authentication, authorization, callback SSRF defenses, secret
encryption, persistence, delivery, and operational controls have not yet been
implemented.
