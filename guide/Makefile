MFLAGS = -s
MAKEFLAGS = $(MFLAGS)
SANDBOX = .cabal-sandbox
DEPS = .cabal-sandbox/.cairn

.PHONY: deps repl build rewatch watch publish

default: watch

${SANDBOX}:
	cabal sandbox init

${DEPS}: ${SANDBOX} $(wildcard *.cabal)
	cabal install -j --only-dependencies --enable-tests
	cabal configure --enable-tests ${CABAL_FLAGS}
	touch $@

repl: ${DEPS}
	cabal configure && cabal build && cabal repl

build: ${DEPS}
	cabal configure && cabal build

watch: build
	dist/build/guide/guide watch

rewatch: build
	dist/build/guide/guide clean && dist/build/guide/guide watch
