set -eEu -o pipefail
trap 's=$?; echo "[ERROR] [$(date +"%T")] on $0:$LINENO"; exit $s' ERR

function log() {
    echo "[$1] [$(date +"%T")] - ${2}"
}

function step() {
    log "STEP" "$1"
}

if [[ ! -d ./.git ]]; then
    echo "error: the pr_check.sh script must be executed from the project root"
    exit 1
fi

CONTAINER_ENGINE=${CONTAINER_ENGINE:-"docker"}
TOOLS_IMAGE=${TOOLS_IMAGE:-"quay.io/app-sre/mk-ci-tools:latest"}
TOOLS_HOME=$(mktemp -d /tmp/e2e.XXXX)

function run() {
    ${CONTAINER_ENGINE} run \
        -u ${UID} \
        -v ${TOOLS_HOME}:/thome:z \
        -e HOME=/thome \
        ${TOOLS_IMAGE} \
        $@
}

        # -v ${PWD}:/workspace:z \
        # -w /workspace \

step "Pull tools image"
${CONTAINER_ENGINE} pull ${TOOLS_IMAGE}

step "Build and check checkstyle"
run cd /thome && mvn install -DskipTests
