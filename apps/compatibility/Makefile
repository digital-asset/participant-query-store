# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

.PHONY: matrix-test
matrix-test: ## run matrix compatibility test tool use MATRIX_TEST_ARGS to pass arguments
	./run-compatibility-tests.sh ${MATRIX_TEST_ARGS}

validate-matrix-workflow: ## ensure test count in parameters.csv and CircleCI config match
	./run-compatibility-tests.sh -v

render-matrix-workflow: ## renders .circleci/configs/workflow/matrix-compatibility-func-test.yml
	./run-compatibility-tests.sh -r
