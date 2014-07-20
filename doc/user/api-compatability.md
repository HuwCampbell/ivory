API Compatibility
=================

This document serves as a list of changes to the Ivory API, both in the file format and in the Scala API.

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
