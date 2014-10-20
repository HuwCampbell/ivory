package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy.SnapshotMeta
import com.ambiata.ivory.storage.lookup.ReducerSize
import com.ambiata.ivory.operation.extraction.squash.{SquashConfig, SquashJob}
import com.ambiata.ivory.operation.extraction.Snapshot
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._

/**
 * Takes a snapshot containing EAVTs
 *
 * and create a "dense" file where there is one line per entity id and all the values for that entity
 */
object PivotOutput {

  /**
   * Take a snapshot first then extract a pivot
   */
  def createPivotFromSnapshot(repository: Repository, output: IvoryLocation, delim: Char, missing: String,
                              meta: SnapshotMeta, squash: SquashConfig): ResultTIO[Unit] = for {
    dict <- Snapshot.dictionaryForSnapshot(repository, meta)
    _    <- SquashJob.squashFromSnapshotWith(repository, dict, meta, output, squash)(path =>
      createPivotWithDictionary(repository, repository.toIvoryLocation(path), output, dict, delim, missing))
  } yield ()

  /**
   * Extract a pivot from a given snapshot (input) to output
   */
  def createPivot(repository: Repository, input: IvoryLocation, output: IvoryLocation, delim: Char, missing: String): ResultTIO[Unit] = for {
    dictionary  <- latestDictionaryFromIvory(repository)
    _            = NotImplemented.chordSquash()
    _           <- createPivotWithDictionary(repository, input, output, dictionary.removeVirtualFeatures, delim, missing)
  } yield ()

  /**
   * Create a pivot on a HdfsRepository
   */
  def createPivotWithDictionary(repository: Repository, input: IvoryLocation, output: IvoryLocation, dictionary: Dictionary, delim: Char, missing: String): ResultTIO[Unit] = for {
    hdfsRepo       <- downcast[Repository, HdfsRepository](repository, s"Pivot only works with Hdfs repositories currently, got '$repository'")
    inputLocation  <- downcast[IvoryLocation, HdfsIvoryLocation](input, s"Pivot can only read from HDFS currently, got '${input.show}'")
    in             =  inputLocation.toHdfsPath
    outputLocation <- downcast[IvoryLocation, HdfsIvoryLocation](output, s"Pivot can only read from HDFS currently, got '${output.show}'")
    out            =  outputLocation.toHdfsPath
    reducers       <- ReducerSize.calculate(in, 256.mb).run(hdfsRepo.configuration)
  } yield PivotOutputJob.run(hdfsRepo.configuration, dictionary, in, out, missing, delim, reducers, hdfsRepo.codec)
}