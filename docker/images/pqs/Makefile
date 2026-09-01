# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

## Default CI to false unless it is present:
CI ?= false

## Disable docker buildx bake from restricting filesystem access on newer versions:
export BUILDX_BAKE_ENTITLEMENTS_FS=0

## Include shared docker make functionality:
DEVENV_ROOT ?= $(shell git rev-parse --show-toplevel)
include ${DEVENV_ROOT}/.scaffold/libs/make/docker.mk

## Variables
export PQS_VERSION         ?= $(shell _gen_build_version)
export GAR_REPOSITORY      ?= da-images/playground
export REGISTRY            ?= europe-docker.pkg.dev/$(GAR_REPOSITORY)/docker
export REPOSITORY          ?= participant-query-store
export LEGACY_REPOSITORY   ?= scribe
export IMAGE_TAG           ?= $(shell echo ${PQS_VERSION} | sed 's|^v||g')
export BASE_IMAGE_TAG      ?= jdk

## Variables for image annotation vars:
export RFC_3339_BUILD_DATE := $(shell date --rfc-3339=seconds)
export GIT_SHA             := $(shell git rev-parse HEAD)

## Ensure we bundle the same OTEL version as what we build and test PQS against:
export OTEL_VERSION    ?= $(shell grep openTelemetryAgent ${DEVENV_ROOT}/mill-build/src/millbuild/package.scala | awk '{print $$4}'| sed 's/"//g')

## Set required variables and build args for this image:
REQUIRED_BUILD_VARS := PQS_VERSION BASE_IMAGE_TAG OTEL_VERSION RFC_3339_BUILD_DATE GIT_SHA
BUILD_ARG_VARS      := PQS_VERSION BASE_IMAGE_TAG OTEL_VERSION RFC_3339_BUILD_DATE GIT_SHA

.PHONY: dockerfile
dockerfile: ## Copy Dockerfile to .dist/${PQS_VERSION}/ directory
	head -n -1 Dockerfile > ${DEVENV_ROOT}/.dist/${PQS_VERSION}/Dockerfile
	echo "COPY pqs.jar /pqs.jar" >> ${DEVENV_ROOT}/.dist/${PQS_VERSION}/Dockerfile
	tail -1 Dockerfile >> ${DEVENV_ROOT}/.dist/${PQS_VERSION}/Dockerfile
	cp -av ${DEVENV_ROOT}/LICENSE.txt ${DEVENV_ROOT}/.dist/${PQS_VERSION}/LICENSE.txt
	cat ${DEVENV_ROOT}/.dist/${PQS_VERSION}/Dockerfile

build: dockerfile
build: ## Build multi-arch PQS container
	$(call docker_buildx_bake,${REGISTRY},${REPOSITORY},${IMAGE_TAG},${ADDITIONAL_TAGS},${BUILD_ARG_VARS},${REQUIRED_BUILD_VARS})
	$(call docker_buildx_bake,${REGISTRY},${LEGACY_REPOSITORY},${IMAGE_TAG},${ADDITIONAL_TAGS},${BUILD_ARG_VARS},${REQUIRED_BUILD_VARS})
	rm -v ${DEVENV_ROOT}/.dist/${PQS_VERSION}/Dockerfile
	rm -v ${DEVENV_ROOT}/.dist/${PQS_VERSION}/LICENSE.txt

build-debug: BASE_IMAGE_TAG := dev
build-debug: IMAGE_TAG := $(IMAGE_TAG)-debug
build-debug: build ## Build multi-arch PQS container with debug-tooling

get-otel-hash-github: ## get sha256sum hash of otel agent jar from github artifact
	@curl -sSL "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v$(OTEL_VERSION)/opentelemetry-javaagent.jar" | sha256sum

.PHONY: get-bundled-otel-hash
get-otel-hash-container: ## get sha265sum hash of otel agent jar from container
	@docker run -it --rm otel/autoinstrumentation-java:$(OTEL_VERSION) /bin/sha256sum /javaagent.jar
