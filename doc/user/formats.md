Formats
=======

This document serves as a guide to the current ingest/extract formats supported in Ivory.


Ingest
------

Format                    | Type   | Example                                      | Description
------------------------- | ------ | -------------------------------------------- | -----------
`sparse:[ESCAPING:]DELIM` | text   | `entity|attribute|value|YY-MM-DD'T'HH:MM:SS` | Text delimited by a single character, and optionally escaped.
`sparse:thrift`           | thrift |                                              | See `ThriftFact` in the accompanying `ivory.thrift` file in the Ivory distribution.

NOTE: Currently dense formats are not supported for ingesting.


Extract
-------

These are the formats that can be specified for both `snapshot`/`chord`.

Format                    | Type   | Example                                           | Description
------------------------- | ------ | ------------------------------------------------- | ------------
`dense:[ESCAPING:]DELIM`  | text   | `entity|feature1|NA|feature3`                     | All features/values for a single fact are represented in a single line, in which the order is defined in an accompanying `.dictionary`.
`sparse:[ESCAPING:]DELIM` | text   | `entity|ns:attribute|value|YYYY-MM-DD'T'HH:MM:SS` | Each fact is represented by a separate entry
`dense:thrift`            | thrift |                                                   | Similar in concept to `dense` text, all values for a single entity are contained in a `List` of values in the same order as the included dictionary.
`sparse:thrift`           | thrift |                                                   | Similar in concept to `sparse` text, all values for a single entity are contains in a `Map`, where missing values are not included.


File Types
----------

Ivory currently supports the following file types.

- `text` - A regular text file, separated by new lines.
- `thrift` - A Hadoop [SequenceFile](http://wiki.apache.org/hadoop/SequenceFile) where the key is `null` and the value
  is the relevant bytes of the fact/entity.

### Compression

All file types are currently compressed using [Snappy](https://code.google.com/p/snappy/).


Delimiters
----------

Currently these are the only supported delimiters:

- `psv` - Pipe separatec (ie. `|`)
- `csv` - Comma separated (ie. `,`)
- `tsv` - Tab separated

Text Escaping
-------------

There are currently two forms of escaping supported by ingest/extract:

- `delimited` - Data is not escaped, and is required not to contain newlines or delimiters.
- `escaped` - The delimited and newlines are escaped with a backslash (eg. `my \n value \| with a backslash \\`)
