# Copyright 2026 Ayesh Almeida
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# syntax=docker/dockerfile:1.18

FROM --platform=$BUILDPLATFORM golang:1.25.8-alpine3.23@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
      -buildvcs=false -trimpath \
      -ldflags "-s -w -X github.com/ayeshLK/websubhub/internal/buildinfo.version=$VERSION -X github.com/ayeshLK/websubhub/internal/buildinfo.commit=$COMMIT -X github.com/ayeshLK/websubhub/internal/buildinfo.date=$BUILD_DATE" \
      -o /out/websubhub ./cmd/websubhub && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
      -buildvcs=false -trimpath \
      -ldflags "-s -w -X github.com/ayeshLK/websubhub/internal/buildinfo.version=$VERSION -X github.com/ayeshLK/websubhub/internal/buildinfo.commit=$COMMIT -X github.com/ayeshLK/websubhub/internal/buildinfo.date=$BUILD_DATE" \
      -o /out/websubhub-consolidator ./cmd/websubhub-consolidator

FROM gcr.io/distroless/static-debian13:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7 AS runtime
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.authors="Ayesh Almeida and WebSubHub contributors" \
      org.opencontainers.image.created=$BUILD_DATE \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision=$COMMIT \
      org.opencontainers.image.source="https://github.com/ayeshLK/websubhub" \
      org.opencontainers.image.version=$VERSION
USER nonroot:nonroot

FROM runtime AS websubhub
LABEL org.opencontainers.image.title="WebSubHub" \
      org.opencontainers.image.description="HTTP event broker for durable, at-least-once WebSub delivery backed by your Kafka."
COPY --from=build --chown=nonroot:nonroot /src/LICENSE /licenses/websubhub/LICENSE
COPY --from=build --chown=nonroot:nonroot /src/NOTICE /licenses/websubhub/NOTICE
COPY --from=build --chown=nonroot:nonroot /out/websubhub /usr/local/bin/websubhub
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/websubhub"]

FROM runtime AS websubhub-consolidator
LABEL org.opencontainers.image.title="WebSubHub Consolidator" \
      org.opencontainers.image.description="Canonical state and snapshot service for WebSubHub deployments."
COPY --from=build --chown=nonroot:nonroot /src/LICENSE /licenses/websubhub-consolidator/LICENSE
COPY --from=build --chown=nonroot:nonroot /src/NOTICE /licenses/websubhub-consolidator/NOTICE
COPY --from=build --chown=nonroot:nonroot /out/websubhub-consolidator /usr/local/bin/websubhub-consolidator
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/websubhub-consolidator"]
