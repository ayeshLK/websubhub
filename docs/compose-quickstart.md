# Interactive Docker Compose quickstart

This guide starts the Kafka-backed WebSubHub preview and leaves it running so
you can exercise the complete resource-topic journey:

~~~text
register topic -> verify subscription -> publish exact content
               -> receive signed HTTP delivery -> inspect operations
~~~

The topology contains Kafka 4.1.0, the consolidator, two hub instances, a test
JWT issuer, and a controlled subscriber callback. It is for local evaluation
only.

> [!WARNING]
> The generated certificates, JWT key, and subscription-encryption key are
> short-lived or local test credentials. Never reuse them outside this Compose
> topology.

## Prerequisites

- Go 1.26.8 or a newer supported Go release
- Docker with Compose v2
- OpenSSL
- `curl`
- `jq` and `base64` for convenient response inspection

Run every command from the repository root.

This topology builds a local development image from the checked-out source. It
does not pull the public `ayeshalmeida/websubhub` or
`ayeshalmeida/websubhub-consolidator` images and should not be used as a
production deployment template. The fixture binary, local plaintext Kafka,
generated credentials, and relaxed callback policy exist only to make the
protocol journey reproducible. For released binary and container deployment,
follow the [installation and deployment guide](installing.md).

## 1. Prepare and start the topology

Build the three local static binaries and generate ignored test credentials:

~~~sh
sh deploy/compose/prepare.sh
~~~

Start the topology and wait for every service to become healthy:

~~~sh
docker compose \
  -f deploy/compose/compose.yaml \
  up -d --build --wait
~~~

Inspect the running services:

~~~sh
docker compose -f deploy/compose/compose.yaml ps
~~~

The host-facing endpoints are:

| Component | Endpoint |
|---|---|
| Hub A public endpoint | `http://localhost:8080/websub` |
| Hub A operations | `http://localhost:9090` |
| Hub B public endpoint | `http://localhost:8083/websub` |
| Hub B operations | `http://localhost:9091` |
| Test JWT issuer | `https://localhost:8443` |
| Subscriber fixture controls | `http://localhost:8082` |

Kafka and the consolidator remain internal to the Compose network.

## 2. Obtain a test JWT

Request a short-lived token from the bundled issuer, trusting only the
generated Compose CA:

~~~sh
TOKEN="$(curl --silent --show-error \
  --cacert deploy/compose/.generated/ca.crt \
  https://localhost:8443/token)"
~~~

Check Hub A readiness:

~~~sh
curl -i http://localhost:9090/health/ready
~~~

The expected response is `200 OK`.

## 3. Register a resource topic

~~~sh
curl -i \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "hub.mode=register" \
  --data-urlencode "hub.topic=https://publisher.example.test/orders" \
  --data-urlencode "hub.content_type=application/json" \
  http://localhost:8080/websub
~~~

The expected response is `200 OK`.

`hub.content_type` is optional and defaults to `application/json`. It becomes
the immutable representation contract for this topic. A later publication must
declare the same complete media type, including parameters.

WebSubHub acknowledges the durable state append before every hub projection is
guaranteed to have observed it. Inspect the canonical topic query view:

~~~sh
curl --silent \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:9090/v1/topics | jq
~~~

A response revision of at least `1` confirms that the consolidator query view
contains the durable registration. It does not guarantee that Hub A's local
admission projection has caught up. If subscription returns `409 Conflict`,
retry it.

## 4. Configure the subscriber fixture

Configure `/callback-demo` to acknowledge delivery with `204 No Content`:

~~~sh
curl -i -X POST \
  "http://localhost:8082/control?path=%2Fcallback-demo&status=204"
~~~

The subscription callback must use the Compose service name because the hub
connects to it from inside the Docker network:

~~~text
http://fixture:8082/callback-demo
~~~

Using `http://localhost:8082/callback-demo` in the subscription would point
back to the hub container itself, not the host fixture.

## 5. Create and verify the subscription

~~~sh
curl -i \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "hub.mode=subscribe" \
  --data-urlencode "hub.topic=https://publisher.example.test/orders" \
  --data-urlencode "hub.callback=http://fixture:8082/callback-demo" \
  --data-urlencode "hub.verify=sync" \
  --data-urlencode "hub.lease_seconds=300" \
  --data-urlencode "hub.secret=local-demo-secret" \
  http://localhost:8080/websub
~~~

The fixture echoes the WebSub challenge, and a successful verified
subscription returns `202 Accepted`.

Inspect the canonical, redacted subscription view:

~~~sh
curl --silent \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:9090/v1/subscriptions | jq
~~~

The callback capability query and subscription secret are not returned.

## 6. Publish exact content

The preview publisher extension is explicitly non-W3C behavior and must be
enabled in configuration. The bundled topology enables it.

~~~sh
curl -i -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Go-Publisher: publish" \
  -H "Content-Type: application/json" \
  --data-binary '{"order_id":"order-42","status":"created"}' \
  "http://localhost:8080/websub?hub.mode=publish&hub.topic=https%3A%2F%2Fpublisher.example.test%2Forders"
~~~

The expected response is `202 Accepted`, which means the exact bytes crossed
the configured durable persistence boundary after the publication content type
matched the topic contract.

## 7. Inspect the delivered request

~~~sh
curl --silent http://localhost:8082/received | jq
~~~

The fixture records:

- callback path and attempt count;
- exact body encoded as base64;
- the topic-defined complete content type;
- stable `X-Hub-MessageId`;
- WebSub HMAC signature.

Decode the body delivered to `/callback-demo`:

~~~sh
curl --silent http://localhost:8082/received |
  jq -r '.[] | select(.path == "/callback-demo") | .body_base64' |
  base64 --decode
~~~

The expected body is:

~~~json
{"order_id":"order-42","status":"created"}
~~~

## 8. Observe the second hub

Both hubs consume all state changes:

~~~sh
curl --silent \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:9091/v1/subscriptions | jq
~~~

You can publish through Hub B:

~~~sh
curl -i -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Go-Publisher: publish" \
  -H "Content-Type: application/json" \
  --data-binary '{"order_id":"order-43","status":"updated"}' \
  "http://localhost:8083/websub?hub.mode=publish&hub.topic=https%3A%2F%2Fpublisher.example.test%2Forders"
~~~

The persisted subscription remains owned by Hub A, so Hub A performs delivery
even when Hub B accepts publication. Automatic ownership transfer is not part
of the preview.

## 9. Inspect operations

Effective capabilities:

~~~sh
curl --silent \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:9090/v1/system/capabilities | jq
~~~

Canonical topic and subscription state:

~~~sh
curl --silent \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:9090/v1/topics | jq

curl --silent \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:9090/v1/subscriptions?status=active" | jq
~~~

Both responses carry the consolidator snapshot revision. The management views
are an internal preview contract for a future control-plane BFF. DLQ browsing
is not exposed in v0.5.

Prometheus-compatible metrics:

~~~sh
curl --silent \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:9090/metrics
~~~

## 10. Inspect logs and restart a hub

Follow Hub A logs:

~~~sh
docker compose \
  -f deploy/compose/compose.yaml \
  logs -f hub-a
~~~

Follow consolidator logs:

~~~sh
docker compose \
  -f deploy/compose/compose.yaml \
  logs -f consolidator
~~~

Restart Hub A and wait for it to recover from the consolidator snapshot:

~~~sh
docker compose -f deploy/compose/compose.yaml restart hub-a
~~~

~~~sh
curl -i http://localhost:9090/health/ready
~~~

Publishing another message after readiness should continue the existing
subscription's delivery progress.

## 11. Stop and clean up

This command removes the containers, network, and local Kafka data volume:

~~~sh
docker compose \
  -f deploy/compose/compose.yaml \
  down -v --remove-orphans
~~~

Generated binaries and test credentials remain ignored under
`deploy/compose/.generated/`. Rerunning `prepare.sh` refreshes binaries and
reuses still-present local credentials.

## Automated alternative

To run the same topology as a gated test rather than an interactive
environment:

~~~sh
make compose-smoke
~~~

The smoke target starts the topology, runs the end-to-end acceptance test,
prints diagnostics on failure, and always removes its containers and volume.

## Troubleshooting

### A mutation returns `401 Unauthorized`

Obtain a fresh token. Fixture tokens expire after 15 minutes.

### A subscription returns `409 Conflict`

The topic registration may be durable but not visible in the local projection
yet. The `/v1/topics` revision reports consolidator visibility, not a local
hub synchronization barrier. Retry the subscription; WebSubHub will accept it
after the local projection catches up.

### Callback verification is rejected

Use the internal callback URL exactly:

~~~text
http://fixture:8082/callback-demo
~~~

The preview callback policy permits that host and port specifically.

### A service is unhealthy

~~~sh
docker compose -f deploy/compose/compose.yaml ps
docker compose -f deploy/compose/compose.yaml logs --tail=200
~~~

### Port conflicts

The topology requires host ports `8080`, `8082`, `8083`, `8443`, `9090`, and
`9091`. Stop the conflicting process or adapt the host-side port mappings in a
local Compose override.
