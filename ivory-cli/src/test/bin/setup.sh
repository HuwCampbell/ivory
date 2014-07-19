TEST=$(dirname $0)
COMMON=${TEST}/../..
export PROJECT=${TEST}/../../../../../..

# Disable Snappy for us poor OSX users
export IVORY_NO_CODEC=1

random() {
    # /bin/sh doesn't have $RANDOM
    date '+%Y%m%d%H%M%S'-$$
}

# Lazily load a specific ivory version from S3 and execute with the supplied extra arguments
ivory_run() {
    VERSION=$1
    shift
    IVORY_CMD=`${COMMON}/ivory-get "${VERSION}"`
    ${IVORY_CMD} "$@"
}

diff_test() {
    diff "$1" "$2"
}

. "${COMMON}/build.sh"
. "${COMMON}/versions.sh"

export TARGET="${PROJECT}/target/regression/"
export REPOSITORY="/tmp/ivory-$(random)"
export INPUT="${TEST}/input"
mkdir -p "${TARGET}"
