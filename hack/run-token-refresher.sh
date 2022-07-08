#!/usr/bin/env bash

docker stop observatorium-token-refresher 2>/dev/null
docker rm observatorium-token-refresher 2>/dev/null
docker run -d -p 8085:8085 \
		--restart always \
		--name observatorium-token-refresher quay.io/rhoas/mk-token-refresher:latest \
		/bin/token-refresher \
		--oidc.issuer-url="${OBSERVATORIUM_OIDC_ISSUER_URL}" \
		--url="${OBSERVATORIUM_URL}" \
		--oidc.client-id="${OBSERVATORIUM_CLIENT_ID}" \
		--oidc.client-secret="${OBSERVATORIUM_CLIENT_SECRET}" \
		--web.listen=":8085"