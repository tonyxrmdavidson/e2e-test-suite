#!/bin/bash

echo "running E2E tests"
OPENSHIFT_API_URI="https://$(oc get routes --all-namespaces 2>&1 | grep -o -m1 'kas-fleet-manager-kas\S*')"
echo $OPENSHIFT_API_URI
OPENSHIFT_IDENTITY_URI="https://sso.stage.redhat.com"
echo $OPENSHIFT_IDENTITY_URI
REDHAT_SSO_URI="https://sso.redhat.com"
echo $REDHAT_SSO_URI
echo $PRIMARY_USERNAME
mvn install -DskipTests
./hack/testrunner.sh -p integration