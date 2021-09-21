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
LOG_DIR ?=
CONFIG_PATH ?=
SERVICE_API_URI ?=
SSO_REDHAT_KEYCLOAK_URI ?=
MAS_SSO_REDHAT_KEYCLOAK_URI ?=
SSO_USERNAME ?=
SSO_PASSWORD ?=
SSO_SECONDARY_USERNAME ?=
SSO_SECONDARY_PASSWORD ?=
SSO_ALIEN_USERNAME ?=
SSO_ALIEN_PASSWORD ?=
DEV_CLUSTER_SERVER ?=
DEV_CLUSTER_NAMESPACE ?=
DEV_CLUSTER_TOKEN ?=
BF2_GITHUB_TOKEN ?=
CLI_VERSION ?=
CLI_ARCH ?=
API_CALL_THRESHOLD ?=
REPORTPORTAL_ENABLE ?=
REPORTPORTAL_ENDPOINT ?=
REPORTPORTAL_UUID ?=
REPORTPORTAL_LAUNCH ?=
KAFKA_POSTFIX_NAME ?=
SKIP_TEARDOWN ?=
BUILD_URL ?=
WHOISXMLAPI_KEY ?=
PROMETHEUS_PUSH_GATEWAY ?=

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

ifdef PROFILE
	PROFILE_ARGS = "-P$(PROFILE)"
endif

ifdef TESTCASE
	TESTCASE_ARGS = "-Dit.test=$(TESTCASE)"
endif

clean:
	mvn clean

build:
	mvn install -DskipTests --no-transfer-progress

test:
	mvn verify $(TESTCASE_ARGS) $(PROFILE_ARGS) --no-transfer-progress

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
		-e TESTCASE=${TESTCASE} \
		-e PROFILE=${PROFILE} \
		-e CONFIG_PATH=${CONFIG_PATH} \
		-e SERVICE_API_URI=${SERVICE_API_URI} \
		-e SSO_REDHAT_KEYCLOAK_URI=${SSO_REDHAT_KEYCLOAK_URI} \
		-e MAS_SSO_REDHAT_KEYCLOAK_URI=${MAS_SSO_REDHAT_KEYCLOAK_URI} \
		-e SSO_USERNAME=${SSO_USERNAME} \
		-e SSO_PASSWORD=${SSO_PASSWORD} \
		-e SSO_SECONDARY_USERNAME=${SSO_SECONDARY_USERNAME} \
		-e SSO_SECONDARY_PASSWORD=${SSO_SECONDARY_PASSWORD} \
		-e SSO_ALIEN_USERNAME=${SSO_ALIEN_USERNAME} \
		-e SSO_ALIEN_PASSWORD=${SSO_ALIEN_PASSWORD} \
		-e DEV_CLUSTER_SERVER=${DEV_CLUSTER_SERVER} \
		-e DEV_CLUSTER_NAMESPACE=${DEV_CLUSTER_NAMESPACE} \
		-e DEV_CLUSTER_TOKEN=${DEV_CLUSTER_TOKEN} \
		-e BF2_GITHUB_TOKEN=${BF2_GITHUB_TOKEN} \
		-e CLI_VERSION=${CLI_VERSION} \
		-e CLI_ARCH=${CLI_ARCH} \
		-e API_CALL_THRESHOLD=${API_CALL_THRESHOLD} \
		-e REPORTPORTAL_ENABLE=${REPORTPORTAL_ENABLE} \
		-e REPORTPORTAL_ENDPOINT=${REPORTPORTAL_ENDPOINT} \
		-e REPORTPORTAL_UUID=${REPORTPORTAL_UUID} \
		-e REPORTPORTAL_LAUNCH=${REPORTPORTAL_LAUNCH} \
		-e WHOISXMLAPI_KEY=${WHOISXMLAPI_KEY} \
		-e PROMETHEUS_PUSH_GATEWAY=${PROMETHEUS_PUSH_GATEWAY} \
		-e KAFKA_POSTFIX_NAME=${KAFKA_POSTFIX_NAME} \
		-e SKIP_TEARDOWN=${SKIP_TEARDOWN} \
		-e BUILD_URL=${BUILD_URL} \
		-e ENABLE_TEST=${ENABLE_TEST} \
		${IMAGE}

.PHONY: clean build test image/build container/test