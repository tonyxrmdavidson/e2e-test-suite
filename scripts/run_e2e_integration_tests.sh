#!/bin/bash

echo "running E2E tests"

RESPONSE=$(curl -sXGET -H "Authorization: Bearer $(ocm token)" "https://$(oc get routes/kas-fleet-manager -n $NAMESPACE -o jsonpath='{.spec.host}')/api/kafkas_mgmt/v1/kafkas")
echo ${RESPONSE}
KAFKA_ID=$(echo ${RESPONSE} | jq -r .items[0].id)
echo ${KAFKA_ID}


# mvn install -DskipTests
# mvn verify -Pintegration