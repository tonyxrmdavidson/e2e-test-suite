ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
DOCKER ?= docker
KUBECONFIG ?= $(HOME)/.kube/config

TMP_ENV := $(shell mktemp)

ifdef PROFILE
	PROFILE_ARGS = "-P$(PROFILE)"
endif

ifdef TESTCASE
	TESTCASE_ARGS = "-Dtest=$(TESTCASE)"
endif

clean:
	mvn clean

build:
	mvn test -DskipTests

test:
	mvn test $(TESTCASE_ARGS) $(PROFILE_ARGS)

pipeline:
	./pipeline.sh

ci/pipeline:
	env > $(TMP_ENV)
	$(DOCKER) pull quay.io/app-sre/mk-ci-tools:latest
	$(DOCKER) run -v $(ROOT_DIR):/opt/mk-e2e-test-suite \
	            -w /opt/mk-e2e-test-suite \
	            --env-file=$(TMP_ENV) \
				-e HOME=/tmp \
				-v $(KUBECONFIG):/tmp/.kube/config \
				-e GOPATH=/tmp \
				-e TESTCASE=${TESTCASE} \
				-e PROFILE=${PROFILE} \
				-u $(shell id -u) \
				quay.io/app-sre/mk-ci-tools:latest make pipeline

.PHONY: clean build test