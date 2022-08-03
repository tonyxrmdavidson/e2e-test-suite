#!/bin/bash

echo "running E2E tests"
export OPENSHIFT_API_URI="https://$(oc get routes --all-namespaces 2>&1 | grep -o -m1 'kas-fleet-manager-kas\S*')"
echo $OPENSHIFT_API_URI
export OPENSHIFT_IDENTITY_URI="https://sso.stage.redhat.com"
echo $OPENSHIFT_IDENTITY_URI
export LAUNCH_KEY="e2e-tony"
echo $LAUNCH_KEY
export REDHAT_SSO_URI="https://sso.redhat.com"
echo $REDHAT_SSO_URI
export SKIP_TEARDOWN="true"
echo $SKIP_TEARDOWN
echo $PRIMARY_USERNAME
mvn install -DskipTests
./hack/testrunner.sh -p integration