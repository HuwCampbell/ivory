Big Query Support
=================

This describes additions that can be made to Ivory to make it simpler to load
snapshots/chords in to a BigQuery table.

Loading data into BigQuery
--------------------------

There are number of different ways to [preapre data for loading](https://cloud.google.com/bigquery/preparing-data-for-bigquery)
in to BigQuery but the most appropriate for snapshots/chords will be:

* 1 table per snapshot/chord
* table schema defined in JSON - derrived from snapshot/chord dictionary file
* snapshot/chord represented as dense JSON text files
* Gzip compressed snapshot files


Immediate Ivory requirements
----------------------------

### Dense JSON text output format for snapshots/chords

An output format that is *row oriented* (i.e. row per entity) where each row is a JSON object.
For example:

```
uid  |  a: int  | b: double  | c: string  | d: [string]     | e: {name: string, age: int}
-----|----------|------------|------------|-----------------|-----------------------------
u111 |     3    |   0.67     |   "foo"    |       -         |               -            
u222 |     4    |    -       |   "bar"    |       -         |               -            
u333 |     5    |    -       |     -      | ["foo","bar"]   |               -            
u444 |     6    |    -       |     -      |       -         | {"name": "bob", "age": 42 }
```

Would be represented as:

```
{"uid": "u111", "a": 3, "b": 0.67, "c": "foo"}
{"uid": "u222", "a": 4, "c": "bar"}
{"uid": "u333", "a": 5, "d": ["foo", "bar"]}
{"uid": "u444", "a": 6, "e": {"name": "bob", "age": 42}}
```

Notes:
  * string values containing newlines will need to be escaped so that the output is a JSON
  record per line
  * for feature values that are *dates*, the output will need to comply the data/time formatting
  supported by BigQuery - https://cloud.google.com/bigquery/preparing-data-for-bigquery#datatypes
  * feature names should include their namespace as well, e.g. `"namespace:name": "...`

### Gzip part files

Ability to configure snapshot/chord extracts to be compressed with Gzip on a part file basis.
