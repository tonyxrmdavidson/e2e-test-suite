ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
DOCKER ?= docker
KUBECONFIG ?= $(HOME)/.kube/config

# Test ENVs
TESTCASE ?=
PROFILE ?=
LOG_DIR ?=
CONFIG_PATH ?=
SERVICE_API_URI ?=
SSO_REDHAT_KEYCLOAK_URI ?=
SSO_REDHAT_REALM ?=
SSO_REDHAT_CLIENT_ID ?=
SSO_REDHAT_REDIRECT_URI ?=
SSO_USERNAME ?=
SSO_PASSWORD ?=
REPORTPORTAL_UUID ?=

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
	$(DOCKER) pull quay.io/app-sre/mk-ci-tools:latest
	$(DOCKER) run -v $(ROOT_DIR):/opt/mk-e2e-test-suite \
		-w /opt/mk-e2e-test-suite \
		-e HOME=/tmp \
		-v $(KUBECONFIG):/tmp/.kube/config \
		-e GOPATH=/tmp \
		-e TESTCASE=${TESTCASE} \
		-e PROFILE=${PROFILE} \
		-e LOG_DIR=${LOG_DIR} \
        -e CONFIG_PATH=${CONFIG_PATH} \
        -e SERVICE_API_URI=${SERVICE_API_URI} \
        -e SSO_REDHAT_KEYCLOAK_URI=${SSO_REDHAT_KEYCLOAK_URI} \
        -e SSO_REDHAT_REALM=${SSO_REDHAT_REALM} \
        -e SSO_REDHAT_CLIENT_ID=${SSO_REDHAT_CLIENT_ID} \
        -e SSO_REDHAT_REDIRECT_URI=${SSO_REDHAT_REDIRECT_URI} \
        -e SSO_USERNAME=${SSO_USERNAME} \
        -e SSO_PASSWORD=${SSO_PASSWORD} \
        -e REPORTPORTAL_UUID=${REPORTPORTAL_UUID} \
		-u $(shell id -u) \
		quay.io/app-sre/mk-ci-tools:latest make pipeline

.PHONY: clean build test