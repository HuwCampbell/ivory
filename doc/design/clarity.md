anatomy of ivory
================



A top down look at ivory functions and how they relate to each other.

```

*----
everyone else


                  ************************************  +-----------+
                  *            public users          *  |  example  |
                  ************************************  +-----------+
                                   |                          |
                                   v                          v

*----
public interface

                          +-----------+             +-----------+
                          |    cli    | ----------> |    api    |
                          +-----------+             +-----------+
                                                         |
                                                         |  (re-exports only)
                                                         |
                                                    -------------
                                                    v  v  v  v  v


*-----
core operations (batch jobs, etc...)


                          +-------------------------------------+
                          |              operation              |
                          +-------------------------------------+
                                             |
                                             |
                                             |
                                       -------------
                                       v  v  v  v  v

*---------
storage layer (primitives for dealing with factsets, partitions, repositories, etc...)

                          +-------------------------------------+
                          |               storage               |
                          +-------------------------------------+
                                             |
                                             |
                                             |
                                       -------------
                                       v  v  v  v  v


*---------
data layer (generic tooling for dealing with non ivory specific concerns like dates, identifiers etc...)


                          +-------------------------------------+
                          |                data                 |
                          +-------------------------------------+

```


`api`, `cli`, `operation`, `storage` and `data` would become the essential top level projects.
There is also room for a couple of other top-level tooling projects (such as `benchmark`,
`performance`, `example` and `regression`). Note there are no "generic" buckets here for
scoobi/thrift/mr, they will evolve over time, and may stand on their own outside of ivory
or be folded into one of the above.


Differences to now:
 - Current ingest/extract/tools move to operation.
 - Some of the "operation" code is currently in storage and would be moved up accordingly.
 - Storage needs to be aggressively tidied. Remove legacy (which is close now). Ensure nice
   categorization of sub-namespaces.
 - Core is currently a mish-mash. Pure data things like "Date" etc... move down to data.
   Ivory concept things move up to "storage".


Guidelines for APIs between components / layers:

 * Don't expose interface/layer splits based on implementation
   detail. Basically any difference between HDFS/S3/LOCAL (and those
   varieties with SYNC steps) needs to be resolved internally to
   sub-projects, otherwise the nice split doesn't buy as anything. We
   need to be able to reason about "dictionary import" not "dictionary
   import on HDFS" or this won't work

 * A follow on to this is, don't export implementation specific
   effects. For example the storage layer may have a mix of `S3, HDFS`
   actions internally, but they should be unified to generic
   structures that don't have a reader of some config blob (this may
   be `ResultT[IO, _]`, `\/`, `Validation` or any number of things,
   but importantly lifecycle and control of implementation specific
   things should not accumulate through interfaces.

 * Don't sacrifice performance for neat interfaces, we need to work
   out the best granularity of interface to achieve this and it will
   likely take some trial and error but we need to work towards a mix
   of safety/performance rather than avoiding it.

 * Always try to support non-hadoop implementations where
   possible. This helps with testing, usability and performance in the
   face of small-to-moderate sized data.

 * Any binary format should have adequate tooling. At a minimum a
   `cat` like tool and an upload tool, but ideally (where data size
   permits) being able to open in `$EDITOR` or manipulate with command
   line actions is highly desirable.
