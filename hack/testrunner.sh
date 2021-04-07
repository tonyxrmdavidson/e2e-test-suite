#!/usr/bin/env bash

# Inspired from: https://gist.github.com/b1zzu/ccd9ef553d546a2009eca21ab45db97a

set -eEu -o pipefail
# shellcheck disable=SC2154
trap 's=$?; echo "$0: error on $0:$LINENO"; exit $s' ERR

SCRIPT=$0

# Defaults
# ---

PROFILE_DEFAULT="default"
TESTCASE_DEFAULT="io.managed.services.test.**"

# Variables
# ---

PROFILE=${PROFILE:-${PROFILE_DEFAULT}}
TESTCASE=${TESTCASE:-${TESTCASE_DEFAULT}}
REPORTPORTAL_ENABLE=${REPORTPORTAL_ENABLE:-"false"}
REPORTPORTAL_ENDPOINT=${REPORTPORTAL_ENDPOINT:-"https://reportportal-cloud-services.apps.ocp4.prod.psi.redhat.com"}
REPORTPORTAL_UUID=${REPORTPORTAL_UUID:-""}
ENABLE_TEST=${ENABLE_TEST:-"true"}

# Help
# ---

function usage() {
  echo
  echo "Usage: ${SCRIPT} [OPTIONS]"
  echo
  echo "Options:"
  echo "  -p, --profile string       the test profile (default: ${PROFILE_DEFAULT}) (env: PROFILE)"
  echo "  -t, --test string          the class name of the test to run (example: ${SCRIPT} -t ServiceAPITest) (default: all) (env: TESTCASE)"
}

# Utils
# ---

function fatal() {
  echo "$SCRIPT: error: $1" >&2
  return 1
}

function info() {
  echo "$SCRIPT: info: $1" >&2
}

# Main
# ---

while [[ $# -gt 0 ]]; do
  case "$1" in
  -h | --help)
    usage
    exit 0
    ;;
  -p | --profile)
    PROFILE="$2"
    shift
    shift
    ;;
  -t | test)
    TESTCASE="$2"
    shift
    shift
    ;;
  --* | -*)
    usage
    echo
    fatal "unknown option '$1'"
    ;;
  *)
    usage
    echo
    fatal "unknown args '$1'"
    ;;
  esac
done

info "ENVs:"
info "----------------"
env
info "----------------"

if [[ ${ENABLE_TEST} == "false" ]]; then
  info "skip all tests because they are not enable"
  exit 0
fi

exec mvn verify \
  --offline \
  "-P${PROFILE}" \
  "-Dit.test=${TESTCASE}" \
  "-Drp.enable=${REPORTPORTAL_ENABLE}" \
  "-Drp.endpoint=${REPORTPORTAL_ENDPOINT}" \
  "-Drp.api.key=${REPORTPORTAL_UUID}" \
  "-Drp.description=Build: ${BUILD_URL:-"null"}" \
  --no-transfer-progress
