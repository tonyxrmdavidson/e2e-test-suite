#!/usr/bin/env bash

# List of env variables
# TESTCASE ............ maven surefire testcase format (package.TestClass#testcase)
# PROFILE ............. maven profile of tests (for example "ci")

set -eEu -o pipefail
# shellcheck disable=SC2154
trap 's=$?; echo "$0: error on $0:$LINENO"; exit $s' ERR

TESTCASE=${TESTCASE:-"io.managed.services.test.**"}
PROFILE=${PROFILE:-"ci"}
REPORTPORTAL_UUID=${REPORTPORTAL_UUID:-""}

function info() {
    MESSAGE="${1}"
    echo "[INFO]  [$(date +"%T")] - ${MESSAGE}"
}

function error() {
    MESSAGE="${1}"
    echo "[ERROR] [$(date +"%T")] - ${MESSAGE}"
    exit 1
}

function separator() {
    echo ""
    info "---------------------------------------------------------------------"
    echo ""
}

function check_env_variable() {
    X="${1}"
    info "Checking content of variable ${X}"
    if [[ -z "${!X}" ]]; then
        error "Variable ${X} is not defined, exit!!!"
    fi
}

info "1. Check env variables"
check_env_variable "TESTCASE"
check_env_variable "PROFILE"
separator

info "2. Running tests"
separator
mvn test \
    "-P${PROFILE}" \
    "-Dtest=${TESTCASE}" \
    "-Drp.enable=true" \
    "-Drp.uuid=${REPORTPORTAL_UUID}" \
    --no-transfer-progress
separator
