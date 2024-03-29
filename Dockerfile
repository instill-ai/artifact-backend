ARG GOLANG_VERSION
FROM --platform=$TARGETPLATFORM golang:${GOLANG_VERSION} AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN go get github.com/otiai10/gosseract/v2

ARG SERVICE_NAME TARGETOS TARGETARCH
RUN --mount=target=. --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -tags=ocr -o /${SERVICE_NAME} ./cmd/main
RUN --mount=target=. --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -tags=ocr -o /${SERVICE_NAME}-worker ./cmd/worker
RUN --mount=target=. --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /${SERVICE_NAME}-migrate ./cmd/migration

FROM gcr.io/distroless/base:nonroot

USER nonroot:nonroot

ARG SERVICE_NAME

WORKDIR /${SERVICE_NAME}

COPY --from=build --chown=nonroot:nonroot /src/config ./config
COPY --from=build --chown=nonroot:nonroot /src/release-please ./release-please

COPY --from=build --chown=nonroot:nonroot /${SERVICE_NAME} ./
COPY --from=build --chown=nonroot:nonroot /${SERVICE_NAME}-worker ./
