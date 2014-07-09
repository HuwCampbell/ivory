namespace java com.ambiata.ivory.core.thrift

enum ThriftDictionaryEncoding {
    BOOLEAN = 0,
    INT = 1,
    LONG = 2,
    DOUBLE = 3,
    STRING = 4
}

enum ThriftDictionaryType {
    NUMERICAL = 0,
    CONTINOUS = 1,
    CATEGORICAL = 2,
    BINARY = 3
}

struct ThriftDictionaryFeatureId {
    1: string ns;
    2: string name;
}

struct ThriftDictionaryFeatureMeta {
    1: ThriftDictionaryEncoding encoding;
    2: ThriftDictionaryType type;
    3: string desc;
    4: list<string> tombstoneValue;
}

struct ThriftDictionary {
    1: map<ThriftDictionaryFeatureId, ThriftDictionaryFeatureMeta> meta;
}
