#!/bin/bash

echo "getting OPENSHIFT_API_URI"
export OPENSHIFT_API_URI_VALUE="https://$(oc get routes --all-namespaces 2>&1 | grep -o -m1 'kas-fleet-manager-kas\S*')"
echo $OPENSHIFT_API_URI
