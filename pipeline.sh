#!/usr/bin/env bash

# List of env variables
# TESTCASE ............ maven surefire testcase format (package.TestClass#testcase)
# PROFILE ............. maven profile of tests (for example "ci")

set -eEu -o pipefail
# shellcheck disable=SC2154
trap 's=$?; echo "$0: error on $0:$LINENO"; exit $s' ERR

SCRIPT=$0

function info() {
  echo "$SCRIPT: info: $1" >&2
}

info "Build the test container"
#docker build -t e2e-test-suite .
set -x

info "Run the test suite"
mkdir -p test-results/failsafe-reports
mkdir -p test-results/logs
docker run \
  -v "${PWD}/test-results/failsafe-reports:/home/jboss/test-suite/target/failsafe-reports" \
  -v "${PWD}/test-results/logs:/home/jboss/test-suite/target/logs" \
  -e TESTCASE="${TESTCASE:-}" \
  -e PROFILE="${PROFILE:-}" \
  -e CONFIG_PATH="${CONFIG_PATH:-}" \
  -e SERVICE_API_URI="${SERVICE_API_URI:-}" \
  -e SSO_REDHAT_KEYCLOAK_URI="${SSO_REDHAT_KEYCLOAK_URI:-}" \
  -e SSO_REDHAT_REALM="${SSO_REDHAT_REALM:-}" \
  -e SSO_REDHAT_CLIENT_ID="${SSO_REDHAT_CLIENT_ID:-}" \
  -e SSO_REDHAT_REDIRECT_URI="${SSO_REDHAT_REDIRECT_URI:-}" \
  -e SSO_USERNAME="${SSO_USERNAME:-}" \
  -e SSO_PASSWORD="${SSO_PASSWORD:-}" \
  -e SSO_SECONDARY_USERNAME="${SSO_SECONDARY_USERNAME:-}" \
  -e SSO_SECONDARY_PASSWORD="${SSO_SECONDARY_PASSWORD:-}" \
  -e SSO_ALIEN_USERNAME="${SSO_ALIEN_USERNAME:-}" \
  -e SSO_ALIEN_PASSWORD="${SSO_ALIEN_PASSWORD:-}" \
  -e DEV_CLUSTER_SERVER="${DEV_CLUSTER_SERVER:-}" \
  -e DEV_CLUSTER_NAMESPACE="${DEV_CLUSTER_NAMESPACE:-}" \
  -e DEV_CLUSTER_TOKEN="${DEV_CLUSTER_TOKEN:-}" \
  -e BF2_GITHUB_TOKEN="${BF2_GITHUB_TOKEN:-}" \
  -e CLI_VERSION="${CLI_VERSION:-}" \
  -e CLI_ARCH="${CLI_ARCH:-}" \
  -e REPORTPORTAL_ENABLE="${REPORTPORTAL_ENABLE:-"true"}" \
  -e REPORTPORTAL_UUID="${REPORTPORTAL_UUID:-}" \
  -e KAFKA_POSTFIX_NAME="${KAFKA_POSTFIX_NAME:-"ci"}" \
  -u "$(id -u)" \
  e2e-test-suite
