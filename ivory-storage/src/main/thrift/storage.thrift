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

struct RenameFeatureMappingValue {
    1: i32 featureId;
    2: string newName;
}

struct RenameFeatureIdMapping {
    1: map<string, RenameFeatureMappingValue> mapping;
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
    6: i32 window;
    7: optional string filter;
}

struct FeatureReductionLookup {
    1: map<i32, list<FeatureReduction>> reductions;
}

struct EntityFilterLookup {
    1: list<string> features;
    2: list<string> entities;
}

// The following has been hacked to convert the list to an array for performance reasons
/*
struct ChordEntities {
    1: map<string, list<i32>> entities;
}
*/

struct ThriftFeatureIdMappings {
    1: list<string> features;
}

struct ThriftNumericalStats {
    1: i64 count;
    2: double sum;
    3: double sqsum;
}

struct ThriftHistogramEntry {
    1: string value;
    2: i64 count;
}

struct ThriftCategoricalStats {
    1: i16 categories;
    2: list<ThriftHistogramEntry> histogram;
}
