API Compatibility
=================

This document serves as a list of changes to the Ivory API, both in the file format and in the Scala API.

## 1.0.0-*-3eb8f01

- Introduce json on ingest/extract for formatting structs/lists as JSON.
  Previously struct/list records could only be extracted and not ingested.
  JSON is now the default, but the old behaviour is still available (for now) with the following.

    ivory snapshot -o "sparse:delimited:psv:deprecated=..."

## 1.0.0-*-71d176e

- Introduce keyed_set mode which can handle a list of (mandatory) struct fields that are used to distinguish
  a record from another at the exact same date/time.

## 1.0.0-*-323e06e

- cat-facts cli has been merged into debug-dump-facts. To output to stdout, do not provide a file location
- printFacts in the `Ivory` api has been replaced with `dumpFactsToFile` and `dumpFactsToStdout`.

## 1.0.0-*-7e1afa3

- Introduced the concept of a global versioning, which is respected/required by commands requiring `-r/--repository`.
  The `update` command _must_ be run before Ivory can be used again.

  In this version of Ivory a consistent manifest scheme has been introduced for snapshots and factsets.

## 1.0.0-*-aedcc7c

- New restriction on length of `entity` field in a fact is 256.

## 1.0.0-*-ecefb75

- Ingest now supports multiple inputs, each with their own format and optional namespace.
  NOTE: This breaks backwards compatibility.

      ivory ingest -i my/path1
      ivory ingest -i my/path2 -f text:delimited
      ivory ingest -i my/path3 -f thrift -n foo

  Can now be written as:

      ivory ingest -i sparse:delimited:psv=my/path1 -i sparse:delimited:psv=my/path2 -i sparse:thrift|foo=my/path3

## 1.0.0-*-f7378d0

- Introduced `dense:escaped:thrift` and `sparse:escaped:thrift` output formats for snapshot, and `text:escaped` format
  to ingest. These are symmetrical and result in newlines and the delimited character to be escaped with a backslash `\`.
  `cat-facts` and `debug-*` commands will now always be escaped.

## 1.0.0-*-ed57711

- Introduced `dense:thrift` and `sparse:thrift` output formats for snapshot.

  NOTE: These are currently unsupported by chord.

## 1.0.0-*-fd377e3

- Timezone for ingest is now optional and will fall back to using the timezone configured when Ivory is created.

## 1.0.0-*-226a694

- Repositories now requires a timezone, which is mandated in `create-repository`.
  **WARNING**: Previously created repositories will default to 'Australia/Sydney'.

    ivory create-repository -z Australia/Sydney

  This also has implications for `ingest` which will _no longer_ create a repository if none is found.
- Added the `config` cli command which currently only echos the contents of the configuration, but will eventually
  be able to set/get specific values.

    ivory config

## 1.0.0-*-37b2727

 - `snapshot` no longer has a `--no-incremental` flag.

## 1.0.0-*-1c85f36

- `create-repository` now takes the repository path as an argument, and no longer requires `-p`/`--path`. eg.

    ivory create-repository /path/to/repo

## 1.0.0-*-811f2fb

- Removed `--delim` from `extract-chord` and `extract-snapshot`, use `--format` instead
- Added `dense` formats `dense:csv`, `dense:tsv`
- Added `sparse` formats `sparse:psv`, `sparse:csv`, `sparse:tsv`

## 1.0.0-*-1fab84d

- Removed `extract-pivot` command
- Collapsed `extract-pivot-snapshot` command into `extract-snapshot` via the `-f`/`--format` option

      ivory snapshot -f dense:psv=/path/to/file

- Removed `-t`/`--tmp` and `--pivot` from `extract-chord`, and switch to `-f`/`--format` as well

      ivory chord -f dense:psv=/path/to/file
- Renamed `extract-snapshot` and `extract-chord` to `snapshot` and `chord`

## 1.0.0-*-4010b5f

- `Dictionary` now has `Definition` instead of `FeatureMeta`, which has two constructors `Concrete` and `Virtual` (new)
- Renamed `FeatureMeta` to `ConcreteDefinition`, and wrap in `Concrete` constructor
- `Virtual` is currently unused

## 1.0.0-*-57b95e0

- Added new command `rename` for create a new factset/store containing updated features.

### 1.0.0-*-076ec8a

- Namespaces must have names composed only of `[a-z]`,`[A-Z]`,`[0-9]`,`[-_]`, must not start with '_' and not be empty

## 1.0.0-*-bb4ec85

- `ingest` now supports optional Thrift format (specified by `--format`), which allows the importing of structs/lists

## 1.0.0-*-2433ce9

- `Factset` has been renamed to `FactsetId`

## 1.0.0-*-66d4ed3

- `createFeatureStore` and `importFeatureStore` has been removed.

## 1.0.0-*-e4212cc

- `cat-dictionary` will no longer support custom delimiters and will always use '|'.
- Dictionaries have switched to the following format:

      namespace:id|encoding=string|type=categorical|description=description|tombstone=NA

  A companion `convert-dictionary` command has been added to help automatically upgrade external dictionaries to this
  new format.

## 1.0.0-*-cc96fbc

- `ingest-bulk` in now just `ingest`.
- `ingest` has been removed for more performant `ingest-bulk`.
- `import-facts` has been removed.

## 1.0.0-*-548d896

- `import-dictionary` will now validate non-backwards compatible changes to the dictionary.
  This includes changing feature types. If these changes are intentional then `--force` can be used to ignore the check.

## 1.0.0-*-1c9d86c

`count-facts` cli doesn't require `-p` to be specified, eg. `ivory count-facts /path/to/snapshot` is now valid.

## 1.0.0-*-5a804e8

### Dictionary format

Previously dictionaries were "named", and consumers of the CLI/API would need to identify them manually.
This concept has been removed, and dictionary updates will now be stored under a global, incrementing identifier
in Thrift format (which may change in future).
By default the latest dictionary will be used in almost all cases.

- `ingest` and `ingest-bulk` no longer import dictionaries, and instead just use the latest
- `ingest`, `ingest-bulk`, `validate-store` and `import-fact` all lost the `-d`/`--dictionary` argument and will now
  always use the latest dictionary
- Added `-u`/`--update` to `import-dictionary` to support incremental updates to the latest dictionary
- Introduced `cat-dictionary` to support viewing a text representation of the dictionary

### 1.0.0-*-0e13188

- `ingest` and `ingest-bulk` both lost the `-t`/`--tmp` argument as it was not being used
