IVORY=${IVORY:-}

# Only build once, or if you want to override it for testing
if [ ! -n "$IVORY" ]; then
    echo "Building ivory-cli"

    TARGET="${PROJECT}/target/ivory-cli"
    mkdir -p "${TARGET}"
    ${PROJECT}/sbt -ivy ~/.ivy-ivory.cli -Dsbt.log.noformat=true ";project cli; clean; universal:package-zip-tarball"
    tar xvf ${PROJECT}/ivory-cli/target/universal/ivory-cli*.tgz --strip-components 1 -C ${TARGET}
    export IVORY="${PROJECT}/bin/hadoop-dev ${TARGET}/lib/ivory.jar com.ambiata.ivory.cli.main"
fi
