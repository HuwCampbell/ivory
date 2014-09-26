

dictionary
----------


#### Ingest

```

  V1       |   TEXT/PSV      |   Fixed columns.                            ** not supported
  V2       |   TEXT/PSV+KV   |   Variable columns labeled by key.          ** current active

```

#### Internal

```

  V1       |   TEXT/PSV      |   Fixed columns. Same as Ingest#V1          ** read only
  V2       |   THRIFT        |   Variable columns in map.                  ** current active
  V3       |   THRIFT        |   Re-structure ADTs for better invariants   ** proposed (v2 would become read-only)

```

#### Extract

```

  V1       |   TEXT/PSV      |   Fixed, numbered columns. not quite Ingest#V1. ** write only

```

facts
-----

#### Ingest

```

  V1       |   TEXT/PSV      |   EAVT (w/ 3 time formats, no structs)      ** current active
  V2       |   THRIFT        |   EAVT (w/ 3 time formats, w/ structs)      ** current active

```

#### Internal

```

  V0       |   TEXT/PSV      |   EAVT (millis time)                        ** not supported at all anymore
  V1       |   THRIFT        |   Null key, ThriftFact EAVT value. Faux.    ** read only.
  V2       |   THRIFT        |   Null key, ThriftFact EAVT value.          ** current active
  V3       |   THRIFT        |   Partition key, ThriftFact EAVT value.     ** proposed (would deprecate snapshot#v1, would live side by side with V2 above for small data)

```

#### Internal (Snapshot only - intention is to deprecate in favour of V3 above). 

```

  V1       |   THRIFT        |   Null key, NamespacedThriftFact value      ** current active (to be deprecated for above)

```


### Extract

```

  V1       |   TEXT/PSV/CSV/TSV |   Dense                                  ** write only
  V2       |   TEXT/PSV/CSV/TSV |   Sparse                                 ** write only

```

stores
------


```

  V1       |   TEXT/LINES   |   Line per factset id.

```
