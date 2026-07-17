# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

MILL_OPTS ?=
FUNC_TEST_CONTROLS ?=

## Disable Mill server in CI:
ifeq ($(CI),true)
MILL_OPTS += --no-server
endif

###############################################################################
## Generate version for builds ################################################
###############################################################################

## Populate and export PQS_VERSION with generated details above:
## - use svu to generate SemVer compliant version value
##   - increments patch value from most recent release tag

## - Mill sets version in application from PQS_VERSION env var
export PQS_VERSION ?= $(shell _gen_build_version)

.PHONY: version
version:
	@echo "[INFO] build number: ${PQS_VERSION}"

###############################################################################
## Documented Targets #########################################################
###############################################################################

.PHONY: auth.test
auth.test:
	mill ${MILL_OPTS} auth.test

.PHONY: check-scala-format
check-scala-format: ## Scalafmt check
	mill ${MILL_OPTS} __.checkFormat

.PHONY: reformat-scala
reformat-scala: ## Reformat scala
	mill ${MILL_OPTS} __.reformat

.PHONY: bsp
bsp: ## Setup Mill-BSP (Build Server Protocol) bridge
	mill ${MILL_OPTS} mill.bsp.BSP/install

.PHONY: idea
idea: bsp ## Open project in IntelliJ IDEA
	idea .

.PHONY: clean
clean: ## Clean build outputs
	mill ${MILL_OPTS} clean

.PHONY: stop
stop: ## Stop running servers
	mill ${MILL_OPTS} __.stop

.PHONY: build
build: ## Compile all modules
	mill ${MILL_OPTS} __.checkFormat
	mill ${MILL_OPTS} pqs.jar

.PHONY: package
package: ## Package artifacts
	mill ${MILL_OPTS} pqs.assembly

.PHONY: test
test: auth.test ## Unit test all modules
	mill ${MILL_OPTS} __.test.test

.PHONY: func-test
func-test: package ## Run all (expensive) functional tests
	mill ${MILL_OPTS} -k -j 1 pqs.functest.test ${FUNC_TEST_CONTROLS}

.PHONY: func-test-compatibility
func-test-compatibility: ## Run matrix compatibility tests
	./compatibility/run-compatibility-tests.sh

.PHONY: consolidate-test-results
consolidate-test-results: ## Consolidate all test results in one folder
	rm -rf out/tests
	mkdir -p out/tests
	cp -v --parents `find out -path "*/test*.dest/results.xml"` out/tests

.PHONY: updates
updates: ## Show available updates for used libraries
	mill ${MILL_OPTS} mill.scalalib.Dependency/showUpdates

.PHONY: prefetch-deps
prefetch-deps: ## Fetch all dependencies for offline operations
	mill ${MILL_OPTS} _.prepareOffline + __.prepareOffline

.PHONY: populate-dist-dir
populate-dist-dir: ## Create and populate .dist/ directory with pqs.jar
	mkdir -p ../.dist/${PQS_VERSION}/
	cp -v out/pqs/assembly.dest/out.jar ../.dist/${PQS_VERSION}/pqs.jar

.PHONY: populate-blackduck-dir
populate-blackduck-dir:
	mill pqs.pom
	mkdir -p ../.blackduck-input/${PQS_VERSION}/
	cp -v out/pqs/pom.dest/pom.xml ../.blackduck-input/${PQS_VERSION}/pom.xml

###############################################################################
## DPM Assistant Publishing Stuff #############################################
###############################################################################

ASSISTANT_ARGS ?= oci://europe-docker.pkg.dev/da-images/playground/components

.PHONY: populate-component-yaml
populate-component-yaml: ## Render dpm component.yaml
	cp ../LICENSE.txt ../.dist/${PQS_VERSION}/LICENSE
	@printf 'apiVersion: digitalasset.com/v1\nkind: Component\nspec:\n  jar-commands:\n    - path: ./pqs.jar\n      name: pqs\n      desc: participant query store\n' \
		| tee ../.dist/${PQS_VERSION}/component.yaml

.PHONY: publish-component
publish-component: ## publish dpm component
	dpm publish component \
		-p generic=../.dist/${PQS_VERSION}/ $(ASSISTANT_ARGS)/participant-query-store:$(patsubst v%,%,$(PQS_VERSION)) \
		--extra-tags 3.6
	dpm publish component \
		-p generic=../.dist/${PQS_VERSION}/ $(ASSISTANT_ARGS)/scribe:$(patsubst v%,%,$(PQS_VERSION)) \
		--extra-tags 3.6
