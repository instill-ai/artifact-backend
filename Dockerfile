ARG GOLANG_VERSION
FROM --platform=$TARGETPLATFORM golang:${GOLANG_VERSION} AS build

WORKDIR /src

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .

ARG SERVICE_NAME TARGETOS TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -o /${SERVICE_NAME} ./cmd/main

# Build the migration tool
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -o /${SERVICE_NAME}-migrate ./cmd/migration


FROM golang:${GOLANG_VERSION}

USER nobody:nogroup

ARG SERVICE_NAME

WORKDIR /${SERVICE_NAME}

COPY --from=docker:dind-rootless --chown=nobody:nogroup /usr/local/bin/docker /usr/local/bin

COPY --from=build --chown=nobody:nogroup /src/config ./config
COPY --from=build --chown=nobody:nogroup /src/release-please ./release-please
COPY --from=build --chown=nobody:nogroup /src/pkg/db/migration ./pkg/db/migration
COPY --from=build --chown=nobody:nogroup /${SERVICE_NAME}-migrate ./
COPY --from=build --chown=nobody:nogroup /${SERVICE_NAME} ./
