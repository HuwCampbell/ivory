package com.ambiata.ivory.operation.extraction

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi._
import FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.scoobi._
import scalaz._, Scalaz._

/**
 * Takes a snapshot containing EAVTs
 *
 * and create a "dense" file where there is one line per entity id and all the values for that entity
 */
object Pivot {

  /**
   * Take a snapshot first then extract a pivot
   */
  def createPivotFromSnapshot(repository: Repository, output: ReferenceIO, delim: Char, tombstone: String, date: Date): ResultTIO[Unit] = for {
    meta <- Snapshot.takeSnapshot(repository, date, incremental = true)
    ref  =  repository.toReference(Repository.snapshot(meta.snapshotId))
    _    <- createPivot(repository, ref, output, delim, tombstone)
  } yield ()

  /**
   * Extract a pivot from a given snapshot (input) to output
   */
  def createPivot(repository: Repository, input: ReferenceIO, output: ReferenceIO, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    dictionary  <- dictionaryFromIvory(repository)
    hdfsRepo    <- downcast[Repository, HdfsRepository](repository, s"Pivot only works with Hdfs repositories currently, got '$repository'")
    inputStore  <- downcast[Any, HdfsStore](input.store, s"Pivot can only read from HDFS currently, got '$input'")
    in          =  (inputStore.base </> input.path).toHdfs
    outputStore <- downcast[Any, HdfsStore](output.store, s"Pivot can only read from HDFS currently, got '$output'")
    out         =  (outputStore.base </> output.path).toHdfs
    _           <- createPivotWithDictionary(hdfsRepo, in, out, dictionary, delim, tombstone)
  } yield ()

  /**
   * Create a pivot on a HdfsRepository
   */
  def createPivotWithDictionary(repository: HdfsRepository, input: Path, output: Path, dictionary: Dictionary, delim: Char, tombstone: String): ResultTIO[Unit] = {
    val storer = DenseRowTextStorageV1.DenseRowTextStorer(output.toString, dictionary, delim, tombstone)
    ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>
      val facts = valueFromSequenceFile[Fact](input.toString)
      persist(storer.storeScoobi(facts))
      ()
    }.run(repository.scoobiConfiguration) >>
    storer.storeMeta.run(repository.configuration)
  }
}
