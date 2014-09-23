package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.operation.pivot.PivotJob
import com.ambiata.ivory.storage.legacy.SnapshotMeta
import com.ambiata.ivory.storage.lookup.ReducerSize
import com.ambiata.ivory.operation.extraction.squash.SquashJob
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.poacher.hdfs._

/**
 * Takes a snapshot containing EAVTs
 *
 * and create a "dense" file where there is one line per entity id and all the values for that entity
 */
object Pivot {

  /**
   * Take a snapshot first then extract a pivot
   */
  def createPivotFromSnapshot(repository: Repository, output: ReferenceIO, delim: Char, tombstone: String, meta: SnapshotMeta): ResultTIO[Unit] = for {
    dict <- Snapshot.dictionaryForSnapshot(repository, meta)
    path <- SquashJob.squashFromSnapshot(repository, dict, meta)
    _    <- createPivotWithDictionary(repository, repository.toReference(path._1), output, dict, delim, tombstone)
    // Delete the temporary squashed output - no longer required
    _    <- ResultT.when(path._2, repository.toStore.deleteAll(path._1))
  } yield ()

  /**
   * Extract a pivot from a given snapshot (input) to output
   */
  def createPivot(repository: Repository, input: ReferenceIO, output: ReferenceIO, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    dictionary  <- latestDictionaryFromIvory(repository)
    _            = NotImplemented.chordSquash()
    _           <- createPivotWithDictionary(repository, input, output, removeVirtualFeatures(dictionary), delim, tombstone)
  } yield ()

  /**
   * Create a pivot on a HdfsRepository
   */
  def createPivotWithDictionary(repository: Repository, input: ReferenceIO, output: ReferenceIO, dictionary: Dictionary, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    hdfsRepo    <- downcast[Repository, HdfsRepository](repository, s"Pivot only works with Hdfs repositories currently, got '$repository'")
    inputStore  <- downcast[Any, HdfsStore](input.store, s"Pivot can only read from HDFS currently, got '$input'")
    in          =  (inputStore.base </> input.path).toHdfs
    outputStore <- downcast[Any, HdfsStore](output.store, s"Pivot can only read from HDFS currently, got '$output'")
    out         =  (outputStore.base </> output.path).toHdfs
    reducers    <- ReducerSize.calculate(in, 1.gb).run(hdfsRepo.configuration)
    } yield PivotJob.run(hdfsRepo.configuration, dictionary, in, out, tombstone, delim, reducers, hdfsRepo.codec)

  /** Only required until chord supports windows as well */
  def removeVirtualFeatures(dictionary: Dictionary): Dictionary = Dictionary(
    dictionary.definitions.filter {
      case Concrete(_, _) => true
      case Virtual(_, _)  => false
    }
  )
}
