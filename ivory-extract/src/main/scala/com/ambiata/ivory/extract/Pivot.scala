package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi._
import WireFormats._
import FactFormats._
import SeqSchemas._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.alien.hdfs._

/**
 * Takes a snapshot containing EAVTs
 *
 * and create a "dense" file where there is one line per entity id and all the values for that entity
 */
object Pivot {

  def onStoreFromSnapshot(repo: Repository, output: ReferenceIO, delim: Char, tombstone: String, date: Date, codec: Option[CompressionCodec]): ResultTIO[Unit] = for {
    snap <- Snapshot.takeSnapshot(repo, date, true, codec)
    (store, snapId) = snap
    ref = repo.toReference(Repository.snapshot(snapId))
    _    <- onStore(repo, ref, output, delim, tombstone)
  } yield ()

  def onStore(repo: Repository, input: ReferenceIO, output: ReferenceIO, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    d <- dictionaryFromIvory(repo)
    _ <- withDictionary(repo, input, output, d, delim, tombstone)
  } yield ()

  def withDictionary(repo: Repository, input: ReferenceIO, output: ReferenceIO, dictionary: Dictionary, delim: Char, tombstone: String): ResultTIO[Unit] = {
    for {
      r <- repo match {
             case r: HdfsRepository => ResultT.ok[IO, HdfsRepository](r)
             case _                 => ResultT.fail[IO, HdfsRepository](s"Pivot only works with Hdfs repositories currently, got '${repo}'")
           }
      i <- input match {
             case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
             case _                                => ResultT.fail[IO, Path](s"Pivot can only read from HDFS currently, got '${input}'")
           }
      o <- output match {
             case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
             case _                                => ResultT.fail[IO, Path](s"Pivot can only output to HDFS currently, got '${output}'")
           }
      s = DenseRowTextStorageV1.DenseRowTextStorer(o.toString, dictionary, delim, tombstone)
      _ <- r.run.runScoobi(scoobiJob(i, s))
      _ <- s.storeMeta.run(r.conf)
    } yield ()
  }

  def scoobiJob(input: Path, storer: DenseRowTextStorageV1.DenseRowTextStorer): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>
      val facts = valueFromSequenceFile[Fact](input.toString)
      persist(storer.storeScoobi(facts))
    }
}
