package com.ambiata.ivory.operation.extraction

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, effect._
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

/**
 * Takes a snapshot containing EAVTs
 *
 * and create a "dense" file where there is one line per entity id and all the values for that entity
 */
object Pivot {

  /**
   * Take a snapshot first then extract a pivot
   */
  def createPivotFromSnapshot(repo: Repository, output: ReferenceIO, delim: Char, tombstone: String, date: Date): ResultTIO[Unit] = for {
    meta <- Snapshot.takeSnapshot(repo, date, true)
    ref  = repo.toReference(Repository.snapshot(meta.snapshotId))
    _    <- createPivot(repo, ref, output, delim, tombstone)
  } yield ()

  /**
   * Take a snapshot first then extract a pivot from a given snapshot (input) to output
   */
  def createPivot(repo: Repository, input: ReferenceIO, output: ReferenceIO, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    dictionary <- dictionaryFromIvory(repo)
    hdfsRepo <- repo match {
      case r: HdfsRepository => ResultT.ok[IO, HdfsRepository](r)
      case _                 => ResultT.fail[IO, HdfsRepository](s"Pivot only works with Hdfs repositories currently, got '$repo'")
    }
    in <- input match {
      case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
      case _                                => ResultT.fail[IO, Path](s"Pivot can only read from HDFS currently, got '$input'")
    }
    out <- output match {
      case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
      case _                                => ResultT.fail[IO, Path](s"Pivot can only output to HDFS currently, got '$output'")
    }
    _ <- createPivotWithDictionary(hdfsRepo, in, out, dictionary, delim, tombstone)
  } yield ()

  def createPivotWithDictionary(repo: HdfsRepository, input: Path, output: Path, dictionary: Dictionary, delim: Char, tombstone: String): ResultTIO[Unit] = {
    val storer = DenseRowTextStorageV1.DenseRowTextStorer(output.toString, dictionary, delim, tombstone)
    for {
      _ <- exportFacts(input, storer).run(repo.scoobiConfiguration)
      _ <- storer.storeMeta.run(repo.configuration)
    } yield ()
  }

  private def exportFacts(inputSnapshot: Path, storer: DenseRowTextStorageV1.DenseRowTextStorer): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>
      val facts = valueFromSequenceFile[Fact](inputSnapshot.toString)
      persist(storer.storeScoobi(facts))
    }
}
