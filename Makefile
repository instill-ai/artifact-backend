.DEFAULT_GOAL:=help

#============================================================================

# Load environment variables for local development
include .env
export

#============================================================================

.PHONY: dev
dev:							## Run dev container
	@docker compose ls -q | grep -q "instill-core" && true || \
		(echo "Error: Run \"make latest PROFILE=artifact ITMODE_ENABLED=true\" in core repository (https://github.com/instill-ai/core) in your local machine first." && exit 1)
	@docker inspect --type container ${SERVICE_NAME} >/dev/null 2>&1 && echo "A container named ${SERVICE_NAME} is already running." || \
		echo "Run dev container ${SERVICE_NAME}. To stop it, run \"make stop\"."
	@docker run -d --rm \
		-v $(PWD):/${SERVICE_NAME} \
		-p ${SERVICE_PORT}:${SERVICE_PORT} \
		--network instill-network \
		--name ${SERVICE_NAME} \
		instill/${SERVICE_NAME}:dev
.PHONY: run-local
run-local:
	@if docker inspect --type container ${SERVICE_NAME} >/dev/null 2>&1; then \
		echo "A container named ${SERVICE_NAME} is already running. \nRestarting..."; \
		make rm; \
	fi
	@docker run  --rm \
		-p ${SERVICE_PORT}:${SERVICE_PORT} \
		--network instill-network \
		--name ${SERVICE_NAME} \
		instill/${SERVICE_NAME}:local \
		/bin/sh -c "\
		./artifact-backend-migrate && \
		./artifact-backend \
		"

.PHONY: logs
logs:					## Tail service container logs with -n 10
	@docker logs ${SERVICE_NAME} --follow 

.PHONY: stop
stop:							## Stop container
	@docker stop -t 1 ${SERVICE_NAME}

.PHONY: rm
rm:								## Remove container
	@docker rm -f ${SERVICE_NAME}

.PHONY: top
top:							## Display all running service processes
	@docker top ${SERVICE_NAME}
.PHONY: build
build:							## Build dev docker image
	@docker build \
		--build-arg SERVICE_NAME=${SERVICE_NAME} \
		--build-arg GOLANG_VERSION=${GOLANG_VERSION} \
		--build-arg K6_VERSION=${K6_VERSION} \
		-f Dockerfile.dev  -t instill/${SERVICE_NAME}:dev .

.PHONY: build-local
build-local:							## Build dev docker image
	@docker build \
		--no-cache \
		--build-arg SERVICE_NAME=${SERVICE_NAME} \
		--build-arg GOLANG_VERSION=${GOLANG_VERSION} \
		--build-arg K6_VERSION=${K6_VERSION} \
		-f Dockerfile  -t instill/${SERVICE_NAME}:local .

.PHONY: go-gen
go-gen:       					## Generate codes
	go generate ./...

.PHONY: unit-test
unit-test:       				## Run unit test
	@go test -v -race -coverpkg=./... -coverprofile=coverage.out ./...
	@cat coverage.out | grep -v "mock" > coverage.final.out
	@go tool cover -func=coverage.final.out
	@go tool cover -html=coverage.final.out
	@rm coverage.out coverage.final.out

.PHONY: integration-test
integration-test:				## Run integration test
	@exit 0

.PHONY: help
help:       	 				## Show this help
	@echo "\nMakefile for local development"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m (default: help)\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
