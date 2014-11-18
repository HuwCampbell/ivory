struct ThriftTombstone {}

union ThriftFactPrimitiveValue {
    1: string s;
    2: i32 i;
    3: i64 l;
    4: double d;
    5: bool b;
    6: string date;
}

struct ThriftFactStruct {
    1: map<string, ThriftFactPrimitiveValue> v;
}

union ThriftFactListValue {
    1: ThriftFactPrimitiveValue p;
    2: ThriftFactStruct s;
}

struct ThriftFactList {
    1: list<ThriftFactListValue> l;
}

union ThriftFactValue {
    1: ThriftFactPrimitiveValue primitive;
    2: ThriftFactStruct strct;
    3: ThriftFactList lst;
    4: ThriftTombstone tombstone;
}

struct ThriftFact {
    1: string entity;
    2: string attribute;
    3: ThriftFactValue value;
    4: string datetime;
}

/* Represents a dense-dense fact where every value is either set or a tombstone */
struct ThriftFactDense {
    1: string entity;
    2: list<ThriftFactValue> value;
}

/* Represents a dense-sparse fact where the map only contains the value if they are set */
struct ThriftFactSparse {
    1: string entity;
    2: map<string, ThriftFactValue> value;
}
