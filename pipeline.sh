#!/usr/bin/env bash

# List of env variables
# TESTCASE ............ maven surefire testcase format (package.TestClass#testcase)
# PROFILE ............. maven profile of tests (for example "ci")

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
TESTCASE=${TESTCASE:-"io.managed.services.test.**"}
PROFILE=${PROFILE:-"ci"}
trap "echo script failed; exit 1" ERR

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
TESTCASE_ARGS="-Dtest=${TESTCASE}"
PROFILE_ARGS="-P${PROFILE}"
mvn test "${TESTCASE_ARGS}" "${PROFILE_ARGS}" --no-transfer-progress
separator
