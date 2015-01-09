package com.ambiata.ivory.api

import com.ambiata.ivory.operation.ingestion.Ingest

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
  type HdfsRepository = com.ambiata.ivory.core.HdfsRepository

  /**
   * Storage types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  val createRepository = com.ambiata.ivory.storage.repository.Repositories.create _

  /**
   * Ingest types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  val importWorkflow = Ingest.ingestFacts _
  val importDictionary = com.ambiata.ivory.operation.ingestion.DictionaryImporter.importFromPath _
  val dictionaryFromIvory = com.ambiata.ivory.storage.metadata.Metadata.latestDictionaryFromIvory _
  val dictionaryToString = com.ambiata.ivory.storage.metadata.DictionaryTextStorageV2.delimitedString _

  /**
   * Extract types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  val takeSnapshot = com.ambiata.ivory.operation.extraction.Snapshots.takeSnapshot _
  val chord = com.ambiata.ivory.operation.extraction.Chord.createChordWithSquash _
  val dense = com.ambiata.ivory.operation.extraction.output.GroupByEntityOutput.createWithDictionary _
  val sparse = com.ambiata.ivory.operation.extraction.output.SparseOutput.extractWithDictionary _

  val Codec = com.ambiata.ivory.storage.repository.Codec

  /**
   * Bespoke debugging tools.
   */
  val countFacts = com.ambiata.ivory.operation.statistics.FactCount.flatFacts _
  val statsFacts = com.ambiata.ivory.operation.statistics.FactStats.statisticsForFactSet _
}
