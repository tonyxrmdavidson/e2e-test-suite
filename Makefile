ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
DOCKER ?= docker
KUBECONFIG ?= $(HOME)/.kube/config

IMAGE_REGISTRY ?= quay.io
IMAGE_REPO ?= bf2
IMAGE_NAME ?= e2e-test-suite
IMAGE_TAG ?= latest
IMAGE = "${IMAGE_REGISTRY}/${IMAGE_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

# Test ENVs
TESTCASE ?=
PROFILE ?=
CONFIG_FILE ?=
LOG_DIR ?=
PRIMARY_USERNAME ?=
PRIMARY_PASSWORD ?=
SECONDARY_USERNAME ?=
SECONDARY_PASSWORD ?=
ALIEN_USERNAME ?=
ALIEN_PASSWORD ?=
OPENSHIFT_API_URI ?=
REDHAT_SSO_URI ?=
OPENSHIFT_IDENTITY_URI ?=
DEV_CLUSTER_SERVER ?=
DEV_CLUSTER_NAMESPACE ?=
DEV_CLUSTER_TOKEN ?=
CLI_VERSION ?=
LAUNCH_KEY ?=
SKIP_TEARDOWN ?=
PROMETHEUS_PUSH_GATEWAY ?=
REPORTPORTAL_ENABLE ?=
REPORTPORTAL_ENDPOINT ?=
REPORTPORTAL_UUID ?=
REPORTPORTAL_LAUNCH ?=
BUILD_URL ?=
ENABLE_TEST ?=

# if set to false the tests will not be executed
# this is a workaround because the app-interface jenkins doesn't support a easy
# way of disabling a job, but we can use the vault to change this value on demand
# and disable the tests when needed
ENABLE_TEST ?= true

ifeq (${DOCKER}, podman)
	DOCKER_RUN = ${DOCKER} run -u "0"
else
	DOCKER_RUN = ${DOCKER} run -u "$(shell id -u)"
endif

image/build:
	$(DOCKER) build -t ${IMAGE} .

container/test:
	mkdir -p test-results/failsafe-reports
	mkdir -p test-results/logs
	${DOCKER_RUN} \
		--dns "1.1.1.1" \
		--dns "10.5.30.45" \
		--dns "10.5.30.46" \
		-v "${ROOT_DIR}/test-results/failsafe-reports:/home/jboss/test-suite/target/failsafe-reports:z" \
		-v "${ROOT_DIR}/test-results/logs:/home/jboss/test-suite/target/logs:z" \
		-e TESTCASE="${TESTCASE}" \
        -e PROFILE="${PROFILE}" \
        -e CONFIG_FILE="${CONFIG_FILE}" \
        -e LOG_DIR="${LOG_DIR}" \
        -e PRIMARY_USERNAME="${PRIMARY_USERNAME}" \
        -e PRIMARY_PASSWORD="${PRIMARY_PASSWORD}" \
        -e SECONDARY_USERNAME="${SECONDARY_USERNAME}" \
        -e SECONDARY_PASSWORD="${SECONDARY_PASSWORD}" \
        -e ALIEN_USERNAME="${ALIEN_USERNAME}" \
        -e ALIEN_PASSWORD="${ALIEN_PASSWORD}" \
        -e OPENSHIFT_API_URI="${OPENSHIFT_API_URI}" \
        -e REDHAT_SSO_URI="${REDHAT_SSO_URI}" \
        -e OPENSHIFT_IDENTITY_URI="${OPENSHIFT_IDENTITY_URI}" \
        -e DEV_CLUSTER_SERVER="${DEV_CLUSTER_SERVER}" \
        -e DEV_CLUSTER_NAMESPACE="${DEV_CLUSTER_NAMESPACE}" \
        -e DEV_CLUSTER_TOKEN="${DEV_CLUSTER_TOKEN}" \
        -e CLI_VERSION="${CLI_VERSION}" \
        -e LAUNCH_KEY="${LAUNCH_KEY}" \
        -e SKIP_TEARDOWN="${SKIP_TEARDOWN}" \
        -e PROMETHEUS_PUSH_GATEWAY="${PROMETHEUS_PUSH_GATEWAY}" \
        -e REPORTPORTAL_ENABLE="${REPORTPORTAL_ENABLE}" \
        -e REPORTPORTAL_ENDPOINT="${REPORTPORTAL_ENDPOINT}" \
        -e REPORTPORTAL_UUID="${REPORTPORTAL_UUID}" \
        -e REPORTPORTAL_LAUNCH="${REPORTPORTAL_LAUNCH}" \
        -e BUILD_URL="${BUILD_URL}" \
		-e ENABLE_TEST="${ENABLE_TEST}" \
		${IMAGE}

.PHONY: clean build test image/build container/test
