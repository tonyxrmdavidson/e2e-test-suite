#!/bin/bash

echo "running E2E tests"
export OPENSHIFT_API_URI="https://$(oc get routes --all-namespaces 2>&1 | grep -o -m1 'kas-fleet-manager-kas\S*')"
echo $OPENSHIFT_API_URI
export OPENSHIFT_IDENTITY_URI="https://sso.stage.redhat.com"
echo $OPENSHIFT_IDENTITY_URI
# mvn install -DskipTests
# mvn verify -Pintegration