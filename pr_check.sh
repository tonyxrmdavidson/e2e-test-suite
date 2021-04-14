#!/bin/bash

TOOLS_IMAGE=${TOOLS_IMAGE:-"quay.io/app-sre/mk-ci-tools:latest"}

docker pull ${TOOLS_IMAGE}
docker run \
    -u $(id -u) \
    -v $(pwd):/opt/mk-e2e-test-suite:z \
    ${TOOLS_IMAGE} \
    "bash" "-c" "cd /opt/mk-e2e-test-suite && mvn install -DskipTests --no-transfer-progress"
