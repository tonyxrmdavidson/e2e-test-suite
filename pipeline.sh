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
make image/build

info "Run the test suite"
make container/test \
  REPORTPORTAL_ENABLE=true
