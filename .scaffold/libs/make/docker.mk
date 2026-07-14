# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

## Snowball™ multi-purpose docker buildx bake functions for multi-arch container creation and publishing

## Know the arch we're running on, this is mostly for local builds:
export ARCH ?= $(shell uname -p)

## CI awareness:
export CI   ?= $(shell [ -f /.dockerenv ] && echo 'true' || echo 'false')

## If we are running in CI, build for both amd64 and arm64 images by default
## If not in CI, build the image that is native to your host hardware and load it in to your docker service
## - operators can override the architecture and actions by setting BAKE_ARCH and BAKE_ACTION as desired
ifeq ($(CI),true)
	BAKE_ARCH    ?= linux/amd64,linux/arm64/v8
	BAKE_ACTION  ?=
else
ifeq ($(ARCH),arm)
	BAKE_ARCH    ?= linux/arm64/v8
	BAKE_ACTION  ?= --load
else
	BAKE_ARCH    ?= linux/amd64
	BAKE_ACTION  ?= --load
endif
endif

## Checks that given variables are set and have non-empty values,
## fail with an error otherwise...
##
## Params:
##   $(1) Variable name(s) to test.
##   $(2) (optional) Error message to print.
define check_defined
    $(strip $(foreach 1,$1, \
        $(call __check_defined,$1,$(strip $(value 2)))))
endef

## Second half of the function above:
define __check_defined
    $(if $(value $1),, \
      $(error Required variable $1$(if $2, ($2)) is undefined, please set it))
endef

## Function to check if input is empty:
##
## Params:
##    $(1) Item you want to validate is not empty
##    $(2) Error message printed if the item is empty
define __is_empty
	$(if $(1),,$(error $(2)))
endef

define __check_buildx_bake_input
	$(call __is_empty,$(1),[ERROR] docker-buildx-bake argument in position $(2) is empty - please populate $(3))
endef

## Functions to populate and generate build args:
##
## Params:
##    $(1) service name to target build arg for
##    $(2) env variable set in calling make file and it's value
define generate_buildx_set_args
BAKE_ARGS += --set $(1).args.$(2)="${$(2)}"
endef

define generate_docker_tags
BAKE_ARGS += --set $(1).tags="$(2)"
endef

## Wrapper function to iterate over all provided variable names and generate args used for container build:
##
## Params:
##    $(1) list of variable names to be converted in to build args
define generate_buildx_cli_args
$(foreach item,$(1),$(eval $(call generate_buildx_set_args,*,$(item))))
endef

## Helper to ensure we have a build context set up...
## yeah, we eat the errors so that it always succeeds because computers are hard
define _ensure_docker_buildx_context
	@printf "[INFO] Ensuring context buildcontext exists... "
	@docker context create buildcontext || exit 0
	@printf "[INFO] Ensuring build instance buildcontext exists... "
	@docker buildx create buildcontext --name buildcontext --use || exit 0
endef

## First time setup for Linux/Ubuntu users:
## - not baking this in to anything yet but it may be helpful
define buildx_ubuntu_setup
	docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
	docker run --privileged --rm tonistiigi/binfmt --install arm64
endef

## Build docker image(s) using buildx bake and docker-compose file:
##
## Params:
##    $(1) docker image registry (required)
##    $(2) docker image repository name (required)
##    $(3) primary image tag (required)
##    $(4) array of additional tags (optional)
##    $(5) array of variables to map to build arguments (optional)
##    $(6) array of variables that are required to be populated (optional)
define docker_buildx_bake
	$(call __check_buildx_bake_input,$(1),1,docker image registry)
	$(call __check_buildx_bake_input,$(2),2,docker image repository)
	$(call __check_buildx_bake_input,$(3),3,primary image tag)
	$(call check_defined,$(6))
	$(call generate_buildx_cli_args,$(5))
	$(eval $(call generate_docker_tags,*,$(1)/$(2):$(3)))
	$(foreach item,$(4),$(eval $(call generate_docker_tags,*,$(1)/$(2):$(item))))
	$(call _ensure_docker_buildx_context)
	@echo "[INFO] building $(1)/$(2):$(3)"
	docker buildx bake -f docker-compose.yml \
		--set *.platform="${BAKE_ARCH}" \
		${BAKE_ARGS} \
		${BAKE_ACTION} \
		--progress plain
endef
