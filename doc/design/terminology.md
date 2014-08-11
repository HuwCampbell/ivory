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
 `Dictionary`                     | Represents a list of features which can be associated to entities. Represented as `Map[FeatureId, FeatureMeta]`
 `Repository`                     | Describe the structure of the Ivory repository in terms of metadata and data
 



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
 `FactsetDataset`                 | Represents a set of `Facts`, ingested as a whole. Identified by a `FactsetId`. Represented by a list of `Partitions`.
 `Partition`                      | Represents a set of `Facts` in a given `Namespace` for a set of `Dates`
 `Snapshot`                       | Represents a set of `Facts` representing the latest values of all features of a `FeatureStore` on a given `Date`. Represented as `SnapshotId`, `FeatureStoreId`, `Date`
 `SnapshotId`                     | Identifier for a `Snapshot`. Represented as an `Identifier`
 `Dictionary`                     | A list of features which can be associated to entities. It is represented as a map of `Map[FeatureId, FeatureMeta]`
 `Repository`                     | Describe the structure of the Ivory repository in terms of metadata and data
