TEST=$(dirname $0)
COMMON=${TEST}/../..
export PROJECT=${TEST}/../../../../../..

# Disable Snappy for us poor OSX users
export IVORY_NO_CODEC=1

# Lazily load a specific ivory version from S3 and execute with the supplied extra arguments
ivory_run() {
    VERSION=$1
    SCALA_VERSION=$2
    shift 2
    IVORY_CMD=`${COMMON}/ivory-get "${VERSION}" "${SCALA_VERSION}"`
    ${IVORY_CMD} "$@"
}

diff_test() {
    diff "$1" "$2"
}

. "${COMMON}/build.sh"
. "${COMMON}/versions.sh"

export TARGET="${PROJECT}/target/tmp/regression-$$"
export IVORY_REPOSITORY="${PROJECT}/target/tmp/ivory-$$"
# Deprecated
export REPOSITORY="$IVORY_REPOSITORY"
export INPUT="${TEST}/input"
rm -rf "${TARGET}"
mkdir -p "${TARGET}"
