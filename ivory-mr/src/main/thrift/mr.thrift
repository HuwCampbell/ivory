namespace java com.ambiata.ivory.mr.thrift

struct InputSpecificationThrift {
    1: string format;
    2: string mapper;
    3: list<string> paths;
}

struct InputSpecificationsThrift {
    1: list<InputSpecificationThrift> inputs;
}
