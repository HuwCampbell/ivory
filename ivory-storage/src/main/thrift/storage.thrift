namespace java com.ambiata.ivory.lookup

struct NamespaceLookup {
    1: map<i32, string> namespaces;
}

struct ReducerLookup {
    1: map<i32, i32> reducers;
}

struct FeatureIdLookup {
    1: map<string, i32> ids;
}

struct FactsetLookup {
    1: map<string, i16> priorities;
}

struct FactsetVersionLookup {
    1: map<string, byte> versions;
}

struct FeatureMappingValue {
    1: i32 featureId;
    2: string newName;
}

struct FeatureIdMapping {
    1: map<string, FeatureMappingValue> mapping;
}

struct SnapshotWindowLookup {
    1: map<i32, i32> window;
}

struct FeatureReduction {
    1: string ns;
    2: string source;
    3: string expression;
    4: string encoding;
    5: optional string filter;
    6: optional i32 date;
}

struct FeatureReductionLookup {
    1: map<i32, list<FeatureReduction>> reductions;
}
