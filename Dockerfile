ARG GOLANG_VERSION=1.24.4
FROM golang:${GOLANG_VERSION} AS build

WORKDIR /build

ARG SERVICE_NAME SERVICE_VERSION TARGETOS TARGETARCH

RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build -ldflags "-X main.serviceVersion=${SERVICE_VERSION} -X main.serviceName=${SERVICE_NAME}" \
    -o /${SERVICE_NAME} ./cmd/main

RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build -ldflags "-X main.serviceVersion=${SERVICE_VERSION} -X main.serviceName=${SERVICE_NAME}-init" \
    -o /${SERVICE_NAME}-migrate ./cmd/migration

RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build -ldflags "-X main.serviceVersion=${SERVICE_VERSION} -X main.serviceName=${SERVICE_NAME}-init" \
    -o /${SERVICE_NAME}-init ./cmd/init

RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build -ldflags "-X main.serviceVersion=${SERVICE_VERSION} -X main.serviceName=${SERVICE_NAME}-worker" \
    -o /${SERVICE_NAME}-worker ./cmd/worker

RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

FROM golang:${GOLANG_VERSION}

USER nobody:nogroup

ARG SERVICE_NAME SERVICE_VERSION

WORKDIR /${SERVICE_NAME}

COPY --from=build --chown=nobody:nogroup /${SERVICE_NAME}-migrate ./
COPY --from=build --chown=nobody:nogroup /${SERVICE_NAME}-init ./
COPY --from=build --chown=nobody:nogroup /${SERVICE_NAME}-worker ./
COPY --from=build --chown=nobody:nogroup /${SERVICE_NAME} ./
COPY --from=build --chown=nobody:nogroup /go/bin/grpcurl /usr/local/bin/

COPY --chown=nobody:nogroup ./config ./config
COPY --chown=nobody:nogroup ./pkg/db/migration ./pkg/db/migration

ENV SERVICE_NAME=${SERVICE_NAME}
ENV SERVICE_VERSION=${SERVICE_VERSION}
