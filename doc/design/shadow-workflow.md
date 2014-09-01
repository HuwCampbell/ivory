# Shadow Workflow

## Workflow

Example : ingest

`ivory ingest input-dataset`
- update metadata
- plan a job
    (Dataset, Plan)
- prepare a job
    (Dataset, Repository) -> ShadowRepository
- run a job ## Input / Output
    (ShadowRepository, Plan) -> (Dataset')
- finalize a job (`sync`)
    (Dataset', ShadowRepository) -> Repository

## Input

#### functions
InputDataset -> ShadowInputDataSet


## Output

#### functions
ShadowOutputDataset -> OutputDataSet


`Where ``InputDataset`` / ``OutputDataSet`` represents a location on S3 / HDFS / local`


## Sync

Synchronise `Datasets` between a `Repository` and a `ShadowRepository` or vice versa

#### Supported sync operations

 Origin           |   Source          |  Implementation
 ---------------- | ----------------- | ---------------------
 S3               | HDFS(Shadow)      |
 local            | HDFS(Shadow)      | poacher (Hdfs)
 HDFS             | HDFS(Shadow)      | no data moves
 HDFS(Shadow)     | HDFS              | no data moves
 HDFS(Shadow)     | S3                |
 HDFS(Shadow)     | local             | poacher (Hdfs)
