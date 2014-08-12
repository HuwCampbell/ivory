namespace java com.ambiata.ivory.core.thrift

include "../../main/thrift/core.thrift"

struct ThriftFactTest {
    1: string entity;
    2: string attribute;
    3: core.ThriftFactValue value;
    4: i32 seconds;
}
