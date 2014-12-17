Using Generated Data to Debug Performance Issues
================================================


### Getting a dataset

We want to start with a fairly uniform dataset. This can be downloaded
from http, or generated yourself. The dataset itself is ~15 GB spread
across 2 namespaces for a single day (the generator tool can be used
to generate slightly different data sets.)


To download:
```
curl -O https://ambiata-ivory.s3.amazonaws.com/data/benchmark-data.tar.gz
tar xvfz benchmark-data.tar.gz
```

To generate yourself
```
curl -O https://ambiata-ivory.s3.amazonaws.com/data/ivory-data-generator-0.1.tar.gz
tar xvfz ivory-data-generator-0.1.tar.gz
./ivory-data-generator-0.1/bin/generator 200000000 1 benchmark-data
```

Either way you should end up with something like this:

```
benchmark-data
├── 2014-01-01
│   ├── jam
│   │   └── 2014-01-01.psv
│   └── sauerkraut
│       └── 2014-01-01.psv
└── dictionary.psv
```

### Preparation

 1. Sync to cluster (ensure a reasonable block size is set, depending on cluster size either 32MB or 64MB would be safe).
 2. Create an ivory repository.
 3. Import dictionary.
 4. Ingest dataset (ensure a reasonable block size is set, and slow start is at > 90%)


A (rough) outline of commands (note this is yarn - may need some options tweaked
for mr1):

```
hadoop fs -Ddfs.block.size=67108864 -put benchmark-data /benchmark-data

ivory create-repository /ivory -z Australia/Sydney

ivory import-dictionary -r /ivory -p file:benchmark-data/dictionary.psv

MAPREDUCE_OPTS="\
  -Dmapreduce.reduce.shuffle.input.buffer.percent=0.3 \
  -Dmapreduce.reduce.shuffle.parallelcopies=5 \
  -Dshuffle.input.buffer.percent=0.3 \
  -Dmapreduce.map.memory.mb=1546 \
  -Dmapreduce.reduce.memory.mb=3000 \
  -Dmapreduce.map.java.opts=-Xmx768M  \
  -Dmapreduce.reduce.java.opts=-Xmx2G \
  -Ddfs.block.size=33554432 \
  -Dmapreduce.job.reduce.slowstart.completedmaps=0.9"

time ivory ingest ${MAPREDUCE_OPTS} -r /ivory -i sparse:delimited:psv=/benchmark-data/2014-01-01
```

### Gathering Data

Required information:

 - Resource Details:
   - number of physical machines
   - number of virtual slots (map + reduce slots on mr1 or no of containers on yarn)
   - physical memory & cpu of machines
   - heap size of tasks

 - Job output:
   - Number of map tasks
   - Number of reduce tasks
   - Size on disk of input dataset via `hadoop fs du -s -h /path/to/dataset`
   - Size of ivory on disk after ingest `hadoop fs du -s -h /path/to/ivory`
   - Block size of input dataset `hadoop fs -stat %o /path/to/file/in/dataset`
   - Block size of factset on disk `hadoop fs -stat %o /path/to/part/file/in/ivory/factsets/000001/..../part-*`
   - All size/time related job counters (data size read/written etc...)
   - Counters around number of records processed at each stage
   - Counters around number of _spilled_ records.
   - Wall time of entire job.
   - Avg. Min. Max. time of each map task and each reduce task.
   - Rough GC time vs CPU time while job is running (this is easy on yarn but quite difficult on mr1 - gangalia, cloudera manager etc... may be able to help here)
   - At least look at (or if possible provide) a log from one of the slower map and reduce tasks. Looking for spills, warnings, or other errors.
   - Overall cluster perf while job is running (disk i/o / cpu load)
