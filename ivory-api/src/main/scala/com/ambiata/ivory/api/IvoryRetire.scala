package com.ambiata.ivory.api

<<<<<<< HEAD
import com.ambiata.ivory.storage.repository.Repositories
=======
import com.ambiata.ivory.operation.ingestion.Ingest
>>>>>>> removed "hole in the middle" funtion when ingesting facts

/**
 * The ivory "retire" API forms an exported API for "deprecated",
 * "legacy" or "dangerous" compontents. The goal of these APIs is
 * to eventually phase them out, and replace them with better,
 * stable APIs.
 */
object IvoryRetire {
  /** Some ivory API's require currently force explicit use of scoobi,
     this component is generally how those API's are exposed, however
     in the future we will move to more general, implementation
     neutral APIs. */
  type ScoobiAction[A] = com.ambiata.poacher.scoobi.ScoobiAction[A]
  val ScoobiAction = com.ambiata.poacher.scoobi.ScoobiAction
  type HdfsRepository = com.ambiata.ivory.storage.repository.HdfsRepository

  /**
   * Storage types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  type IvoryScoobiLoader[A] = com.ambiata.ivory.storage.legacy.IvoryScoobiLoader[A]
  type IvoryScoobiStorer[A, +B] = com.ambiata.ivory.storage.legacy.IvoryScoobiStorer[A, B]

  val writeFactsetVersion = com.ambiata.ivory.storage.legacy.IvoryStorage.writeFactsetVersion _

  val snapshotFromHdfs = com.ambiata.ivory.storage.legacy.SnapshotStorageV1.snapshotFromHdfs _
  val snapshotToHdfs = com.ambiata.ivory.storage.legacy.SnapshotStorageV1.snapshotToHdfs _

  val createRepository = Repositories.create _

  /**
   * Ingest types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  val importWorkflow = Ingest.ingestFacts _
  val importDictionary = com.ambiata.ivory.operation.ingestion.DictionaryImporter.fromPath _
  val dictionaryFromIvory = com.ambiata.ivory.storage.metadata.Metadata.dictionaryFromIvory _
  val dictionaryToString = com.ambiata.ivory.storage.metadata.DictionaryTextStorageV2.delimitedString _

  implicit def DListToIvoryFactStorage(dlist: com.nicta.scoobi.core.DList[Ivory.Fact]): com.ambiata.ivory.storage.legacy.IvoryStorage.IvoryFactStorage =
    com.ambiata.ivory.storage.legacy.IvoryStorage.IvoryFactStorage(dlist)

  /**
   * Extract types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  val snapshot = com.ambiata.ivory.operation.extraction.Snapshot.snapshot _
  val takeSnapshot = com.ambiata.ivory.operation.extraction.Snapshot.takeSnapshot _
  val chord = com.ambiata.ivory.operation.extraction.Chord.createChord _
  val pivot = com.ambiata.ivory.operation.extraction.Pivot.createPivot _
  val pivotFromSnapshot = com.ambiata.ivory.operation.extraction.Pivot.createPivotFromSnapshot _

  val Codec = com.ambiata.ivory.storage.repository.Codec

  /**
   * Bespoke debugging tools.
   */
  val countFacts = com.ambiata.ivory.operation.statistics.FactCount.flatFacts _
  val diffFacts = com.ambiata.ivory.operation.diff.FactDiff.flatFacts _
}
