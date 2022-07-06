#!/bin/bash

echo "running E2E tests"
echo "${ADMIN_USERNAME}"

mvn install -DskipTests
mvn verify -Pintegration