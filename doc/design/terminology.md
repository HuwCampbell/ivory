This document presents the list of the different concepts handled in Ivory.

Current state
-------------

 Name                             |   Description
 -------------------------------- | -----------------------------------------------------------------------------
 `FeatureId`                      | A characteristic of an `Entity` (the age of a customer for example). It belongs to a namespace and has a name
 `Fact`                           | A value for an entity at a given point in time. Represented as (namespace, `FeatureId`, `Entity`, `Date`, `Time`, `Value`)
 `FeatureStore`                   | A coherent list of facts, formed by several factsets. Identified by a `FeatureStoreId`. Represented by list of (`Priority`, `FactsetId`)
 `FeatureStoreId`                 | Identifier for a `FeatureStore`. Represented by an `OldIdentifier`
 `Priority`                       | Represented by a `Short`
 `FactsetId`                      | Identifier for a `FactSet`. Represented by an `OldIdentifier`
 `FactsetDataset`                 | Represents a set of `Facts`, ingested as a whole. Identified by a `FactsetId`. Represented by a list of `Partitions`.
 `Partition`                      | Represents a set of `Facts`, on a given `Date`. Represented by a `FactsetId`, a namespace, a `Date`, and a path to files containing `Facts`
 `OldIdentifier`                  | Integer from `0` to `99999`
 `SnapshotMeta`                   | Represents a set of `Facts` being the latest values of all features of a `FeatureStore` on a given `Date`. Represented by a `Date` and a `FeatureStoreId`
 `SnapshotId`                     | Identifier for a set of `Facts` representing the latest values of all features. Represented by an `Identifier`
 `Identifier`                     | Integer from `0` to `99999999`
 `Dictionary`                     | Represents a list of features which can be associated to entities. Represented as `Map[FeatureId, Definition]`
 `ConcreteDefinition`             | Previously `FeatureMeta`, which represents a feature with real facts
 `VirtualDefinition`              | Represents a derived (aliased) feature definition, over a window (eg. 6 months)
 `Repository`                     | Describe the structure of the Ivory repository in terms of metadata and data
 `Dataset`                        | Represents either a FactsetDataset or SnapshotDataset
 `SnapshotDataset`                | Represents a single Snapshot to read from
 `Datasets`                       | Represents a list of Datasets. This is used as input to operations like Snapshot and Chord



Desired state
-------------

 Name                             |   Description
 -------------------------------- | -----------------------------------------------------------------------------
 `FeatureId`                      | A characteristic of an `Entity` (the age of a customer for example). It belongs to a namespace and has a name
 `Namespace`                      | Represents a group of `FeatureIds`. Represented as a string with some constraints (it must contain no slash for example)
 `Fact`                           | A value for an entity at a given point in time. Represented as (namespace, `FeatureId`, `Entity`, `Date`, `Time`, `Value`)
 `FeatureStore`                   | A prioritized list of `FactsetIds`. Identified by a `FeatureStoreId` and contains a list of (`Priority`, `FactsetId`)
 `FeatureStoreId`                 | Identifier for a `FeatureStore`. Represented by an `OldIdentifier`
 `Priority`                       | Represented by a `Short`
 `FactsetId`                      | Identifier for a `FactSet`. Represented by an `Identifier`
 `Partition`                      | Represents a set of `Facts` in a given `Namespace` for a set of `Dates`
 `Snapshot`                       | Represents a set of `Facts` representing the latest values of all features of a `FeatureStore` on a given `Date`. Represented as `SnapshotId`, `FeatureStoreId`, `Date`
 `SnapshotId`                     | Identifier for a `Snapshot`. Represented as an `Identifier`
 `Dictionary`                     | A list of features which can be associated to entities. It is represented as a map of `Map[FeatureId, Definition]`
 `ConcreteDefinition`             | Previously `FeatureMeta`, which represents a feature with real facts
 `VirtualDefinition`              | Represents a derived (aliased) feature definition, with some specific calculation over a window (eg. 6 months)
 `Repository`                     | Describe the structure of the Ivory repository in terms of metadata and data
 `InputDataSet`                   | Represents data that needs to be loaded into ivory from an external location
 `OutputDataSet`                  | Represents data that should not be stored in ivory, which will be output to some other location.
 `Shadow*`                        | Primarily is where the "work" happens, and the types are specific enough to exclude the possibility of data being somewhere we can't do processing on.
 `ShadowRepository`               | Represents a `Repository` which is specialized to only those stores that we can perform batch operations on. Right now this is only HDFS, in the future it may include other file systems or streaming processes.
 `ShadowInputDataSet`             | Represents the `InputDataSet` that will be loaded for any ivory operation on the `ShadowRepository`
 `ShadowOutputDataSet`            | Represents the `OutputDataSet` generated from any ivory operation on the `ShadowRepository` where the data needs to be exported outside of ivory
 ~~ShadowDataset~~                | There is no need for ~~ShadowDataset~~ as it is simply represented by the `ShadowRepository` which is synced to the `Repository`
`*Format`                         | Version tag for an on-disk data format.
`MetadataVersion`                 | Global version tag for all metadata.
