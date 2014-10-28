namespace java com.ambiata.ivory.lookup

struct NamespaceLookup {
    1: map<i32, string> namespaces;
}

struct ReducerLookup {
    1: map<i32, i32> reducers;
}

struct FlagLookup {
    1: map<i32, bool> flags;
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
    2: string name;
    3: string source;
    4: string expression;
    5: string encoding;
    6: optional string filter;
    7: optional i32 date;
}

struct FeatureReductionLookup {
    1: map<i32, list<FeatureReduction>> reductions;
}

// The following has been hacked to convert the list to an array for performance reasons
/*
struct ChordEntities {
    1: map<string, list<i32>> entities;
}
*/
