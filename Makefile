.DEFAULT_GOAL:=help

#============================================================================

# Load environment variables for local development
include .env
export

# Integration test defaults (for running from host)
API_GATEWAY_PROTOCOL ?= http
API_GATEWAY_URL ?= localhost:8080
DB_HOST ?= localhost

#============================================================================

.PHONY: dev
dev:							## Run dev container
	@docker compose ls -q | grep -q "instill-core" && true || \
		(echo "Error: Run \"make latest\" in instill-core repository (https://github.com/instill-ai/instill-core) in your local machine first and run \"docker rm -f ${SERVICE_NAME} ${SERVICE_NAME}-worker\"." && exit 1)
	@docker inspect --type container ${SERVICE_NAME} >/dev/null 2>&1 && echo "A container named ${SERVICE_NAME} is already running." || \
		echo "Run dev container ${SERVICE_NAME}. To stop it, run \"make stop\"."
	@docker run -d --rm \
		-v $(PWD):/${SERVICE_NAME} \
		-p ${PUBLIC_SERVICE_PORT}:${PUBLIC_SERVICE_PORT} \
		-p ${PRIVATE_SERVICE_PORT}:${PRIVATE_SERVICE_PORT} \
		--network instill-network \
		--name ${SERVICE_NAME} \
		instill/${SERVICE_NAME}:dev

.PHONY: latest
latest: ## Run latest container
	@docker compose ls -q | grep -q "instill-core" && true || \
		(echo "Error: Run \"make latest\" in instill-core repository (https://github.com/instill-ai/instill-core) in your local machine first and run \"docker rm -f ${SERVICE_NAME} ${SERVICE_NAME}-worker\"." && exit 1)
	@docker inspect --type container ${SERVICE_NAME} >/dev/null 2>&1 && echo "A container named ${SERVICE_NAME} is already running." || \
		echo "Run latest container ${SERVICE_NAME} and ${SERVICE_NAME}-worker. To stop it, run \"make stop\"."
	@docker run --network=instill-network \
		--name ${SERVICE_NAME} \
		-p ${PUBLIC_SERVICE_PORT}:${PUBLIC_SERVICE_PORT} \
		-p ${PRIVATE_SERVICE_PORT}:${PRIVATE_SERVICE_PORT} \
		-d instill/${SERVICE_NAME}:latest \
		/bin/sh -c "\
		./${SERVICE_NAME}-migrate && \
		./${SERVICE_NAME}-init && \
		./${SERVICE_NAME} \
		"
	@docker run --network=instill-network \
		--name ${SERVICE_NAME}-worker \
		-d instill/${SERVICE_NAME}:latest ./${SERVICE_NAME}-worker

.PHONY: logs
logs:					## Tail container logs with -n 10
	@docker logs ${SERVICE_NAME} --follow --tail=10

.PHONY: stop
stop:							## Stop all running containers
	@docker stop -t 1 ${SERVICE_NAME} ${SERVICE_NAME}-worker 2>/dev/null || true

.PHONY: rm
rm:								## Remove all running containers
	@docker rm -f ${SERVICE_NAME} ${SERVICE_NAME}-worker >/dev/null 2>&1

.PHONY: top
top:							## Display all running service processes
	@docker top ${SERVICE_NAME}

.PHONY: build-dev
build-dev: ## Build dev docker image
	@docker build \
		--build-arg SERVICE_NAME=${SERVICE_NAME} \
		--build-arg K6_VERSION=${K6_VERSION} \
		--build-arg XK6_VERSION=${XK6_VERSION} \
		--build-arg XK6_SQL_VERSION=${XK6_SQL_VERSION} \
		--build-arg XK6_SQL_POSTGRES_VERSION=${XK6_SQL_POSTGRES_VERSION} \
		-f Dockerfile.dev -t instill/${SERVICE_NAME}:dev .

.PHONY: build-latest
build-latest: ## Build latest docker image
	@docker build \
		--build-arg SERVICE_NAME=${SERVICE_NAME} \
		--build-arg SERVICE_VERSION=dev \
		-t instill/${SERVICE_NAME}:latest .

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
integration-test:		## Run integration tests (parallel by default, sequential if CI=true)
	@if [ -n "${API_GATEWAY_URL}" ]; then \
		echo "✓ Running tests through API Gateway: ${API_GATEWAY_URL}"; \
	else \
		echo "⚠ WARNING: No API_GATEWAY_URL set - using default localhost:8080"; \
	fi
	@echo "  DB_HOST: ${DB_HOST}"
ifeq ($(CI),true)
	@echo "Running integration tests sequentially (CI mode)..."
	@rm -f /tmp/artifact-integration-test.log
	@bash -c 'set -o pipefail; \
	for test in \
		integration-test/rest.js \
		integration-test/rest-object-storage.js \
		integration-test/rest-hash-based-ids.js \
		integration-test/rest-file-type.js \
		integration-test/rest-db.js \
		integration-test/rest-ai-client.js \
		integration-test/rest-kb-e2e-file-process.js \
		integration-test/rest-file-reprocess.js \
		integration-test/rest-kb-delete.js \
		integration-test/grpc.js \
		integration-test/grpc-kb-update.js \
		integration-test/grpc-system-config-update.js \
		integration-test/grpc-system-admin.js; do \
		echo "Running $$test..." | tee -a /tmp/artifact-integration-test.log; \
		TEST_FOLDER_ABS_PATH=$(PWD) k6 run --address="" \
			-e API_GATEWAY_PROTOCOL=$(API_GATEWAY_PROTOCOL) \
			-e API_GATEWAY_URL=$(API_GATEWAY_URL) \
			-e DB_HOST=$(DB_HOST) \
			$$test --no-usage-report 2>&1 | tee -a /tmp/artifact-integration-test.log; \
		if [ $${PIPESTATUS[0]} -ne 0 ]; then exit 1; fi; \
	done'
	@bash integration-test/scripts/report-summary.sh /tmp/artifact-integration-test.log
else
	@echo "Running integration tests in parallel..."
	@rm -f /tmp/artifact-integration-test.log
	# Batch 1: Core REST tests + grpc.js (isolated from other gRPC tests to avoid conflicts)
	@parallel --halt now,fail=1 --tag --line-buffer \
		"TEST_FOLDER_ABS_PATH=${PWD} k6 run --address=\"\" \
		-e API_GATEWAY_PROTOCOL=${API_GATEWAY_PROTOCOL} -e API_GATEWAY_URL=${API_GATEWAY_URL} \
		-e DB_HOST=${DB_HOST} \
		{} --no-usage-report" ::: \
		integration-test/rest.js \
		integration-test/rest-object-storage.js \
		integration-test/rest-hash-based-ids.js 2>&1 | tee -a /tmp/artifact-integration-test.log
	# Batch 2: File processing tests (heavy AI workload)
	@parallel --halt now,fail=1 --tag --line-buffer \
		"TEST_FOLDER_ABS_PATH=${PWD} k6 run --address=\"\" \
		-e API_GATEWAY_PROTOCOL=${API_GATEWAY_PROTOCOL} -e API_GATEWAY_URL=${API_GATEWAY_URL} \
		-e DB_HOST=${DB_HOST} \
		{} --no-usage-report" ::: \
		integration-test/rest-file-type.js \
		integration-test/rest-db.js \
		integration-test/rest-ai-client.js 2>&1 | tee -a /tmp/artifact-integration-test.log
	# Batch 3: E2E and KB management tests
	@parallel --halt now,fail=1 --tag --line-buffer \
		"TEST_FOLDER_ABS_PATH=${PWD} k6 run --address=\"\" \
		-e API_GATEWAY_PROTOCOL=${API_GATEWAY_PROTOCOL} -e API_GATEWAY_URL=${API_GATEWAY_URL} \
		-e DB_HOST=${DB_HOST} \
		{} --no-usage-report" ::: \
		integration-test/rest-kb-e2e-file-process.js \
		integration-test/rest-file-reprocess.js \
		integration-test/rest-kb-delete.js 2>&1 | tee -a /tmp/artifact-integration-test.log
	# Batch 4: gRPC system tests (may modify system config, run separately from grpc.js)
	@parallel --halt now,fail=1 --tag --line-buffer \
		"TEST_FOLDER_ABS_PATH=${PWD} k6 run --address=\"\" \
		-e API_GATEWAY_PROTOCOL=${API_GATEWAY_PROTOCOL} -e API_GATEWAY_URL=${API_GATEWAY_URL} \
		-e DB_HOST=${DB_HOST} \
		{} --no-usage-report" ::: \
		integration-test/grpc.js \
		integration-test/grpc-kb-update.js \
		integration-test/grpc-system-config-update.js \
		integration-test/grpc-system-admin.js 2>&1 | tee -a /tmp/artifact-integration-test.log
	@bash integration-test/scripts/report-summary.sh /tmp/artifact-integration-test.log
endif

.PHONY: help
help:       	 				## Show this help
	@echo "\nMakefile for local development"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m (default: help)\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
