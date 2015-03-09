namespace java com.ambiata.ivory.core.thrift

struct ThriftTombstone {}

// Unfortunately Thrift doesn't (yet) support recursive types, or this could be ThriftFactValue :(
union ThriftFactPrimitiveValue {
    1: string s;
    2: i32 i;
    3: i64 l;
    4: double d;
    5: bool b;
    // DEPRECATED - DO NOT USE
    6: ThriftTombstone t;
    7: i32 date;
}

struct ThriftFactStructSparse {
    1: map<string, ThriftFactPrimitiveValue> v;
}

union ThriftFactListValue {
    1: ThriftFactPrimitiveValue p;
    2: ThriftFactStructSparse s;
}

struct ThriftFactList {
    1: list<ThriftFactListValue> l;
}

// NOTE: For the next Fact version remove the primitive values here and replace with ThriftFactPrimitiveValue
union ThriftFactValue {
    1: string s;
    2: i32 i;
    3: i64 l;
    4: double d;
    5: bool b;
    6: ThriftTombstone t;
    7: ThriftFactStructSparse structSparse;
    8: ThriftFactList lst;
    9: i32 date;
}

struct ThriftFact {
    1: string entity;
    2: string attribute;
    3: ThriftFactValue value;
    4: optional i32 seconds;
}

struct NamespacedThriftFact {
    1: ThriftFact fact;
    2: string nspace;
    3: i32 yyyyMMdd; // this is a packed int, with a the first 16 bits representing the year, the next 8 the month and the final 8 the day
}

struct ThriftV1ErrorData {
    1: binary bytes;
}

struct TextErrorData {
    1: string line;
}

union ParseErrorData {
    1: TextErrorData text;
    2: ThriftV1ErrorData thriftV1;
}

struct ThriftParseError {
    1: string message;
    2: ParseErrorData data;
}

enum ThriftDictionaryEncoding {
    BOOLEAN = 0,
    INT = 1,
    LONG = 2,
    DOUBLE = 3,
    STRING = 4,
    STRUCT = 5,
    DATE = 6
}

enum ThriftDictionaryType {
    NUMERICAL = 0,
    CONTINOUS = 1,
    CATEGORICAL = 2,
    BINARY = 3
}

union ThriftDictionaryStructMetaOpts {
    1: bool isOptional;
}

/**
 * NOTE: The current encoding of structs is far from optimal/ideal, and is to avoid an early bump in Dictionary verions.
 * When we decide to make a new version of the dictionary, something like the following would be better.
 *
 * struct IntEncoding {}
 * struct ...Encoding {}
 * struct StringEncoding {}
 * struct StructEncoding {
 *  1:  list<ThriftDictionaryStructMeta> values;
 * }
 * union ThriftDicitonaryEncoding {
 *  1: IntEncoding intEncoding;
 *  2: ...
 *  n: StructEncoding structEncoding;
 * }
 */
struct ThriftDictionaryStructMeta {
    1: string name;
    2: ThriftDictionaryEncoding encoding;
    3: ThriftDictionaryStructMetaOpts opts
}

struct ThriftDictionaryStruct {
    1: list<ThriftDictionaryStructMeta> values;
}

union ThriftDictionaryList {
    1: ThriftDictionaryEncoding encoding;
    2: ThriftDictionaryStruct   structEncoding;
}

union ThriftDictionaryFeatureValue {
    1: ThriftDictionaryStruct structValue;
    2: ThriftDictionaryList   listValue;
}

struct ThriftDictionaryFeatureId {
    1: string ns;
    2: string name;
}


enum ThriftDictionaryMode {
    STATE = 1,
    SET = 2
}

union ThriftDictionaryModeV2 {
    1: ThriftDictionaryMode mode,
    2: string keyedSet
    3: list<string> keyedSetMulti
}

struct ThriftDictionaryFeatureMeta {
    1: ThriftDictionaryEncoding encoding;
    2: optional ThriftDictionaryType type;
    3: string desc;
    4: list<string> tombstoneValue;
    5: optional ThriftDictionaryFeatureValue value;
    6: optional ThriftDictionaryMode mode;
    7: optional ThriftDictionaryModeV2 modeV2;
}

enum ThriftDictionaryWindowUnit {
    DAYS = 1,
    WEEKS = 2,
    MONTHS = 3,
    YEARS = 4
}

struct ThriftDictionaryWindow {
    1: i32 length;
    2: ThriftDictionaryWindowUnit unit;
}

struct ThriftDictionaryExpression {
    1: string expression;
    2: optional string filter;
}

struct ThriftDictionaryVirtual {
    1: ThriftDictionaryFeatureId sourceName;
    2: optional ThriftDictionaryWindow window;
    3: optional ThriftDictionaryExpression expression;
}

union ThriftDictionaryDefinition {
    1: ThriftDictionaryFeatureMeta concrete;
    2: ThriftDictionaryVirtual virt;
}

struct ThriftDictionaryV2 {
    1: map<ThriftDictionaryFeatureId, ThriftDictionaryDefinition> meta;
}

struct ThriftDictionary {
    // Deprecated, please remove when we upgrade the struct format - https://github.com/ambiata/ivory/issues/137
    1: map<ThriftDictionaryFeatureId, ThriftDictionaryFeatureMeta> meta;
    2: optional ThriftDictionaryV2 dict;
}
