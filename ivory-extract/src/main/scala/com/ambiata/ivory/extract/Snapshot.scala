package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.joda.time.LocalDate
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.data._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.SeqSchemas._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata._, Metadata._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.alien.hdfs._
import MemoryConversions._

case class Snapshot(repo: Repository, store: String, entities: Option[ReferenceIO], snapshot: Date, output: ReferenceIO, incremental: Option[(Identifier, SnapshotMeta)], codec: Option[CompressionCodec]) {
  import IvoryStorage._

  def run: ResultTIO[Unit] = for {
    hr  <- repo match {
      case h: HdfsRepository => ResultT.ok[IO, HdfsRepository](h)
      case _                 => ResultT.fail[IO, HdfsRepository]("Snapshot only works on HDFS repositories at this stage.")
    }
    out <- output match {
      case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
      case _                                => ResultT.fail[IO, Path](s"Snapshot output path must be on hdfs, got '${output}'")
    }
    d   <- dictionaryFromIvory(repo)
    s   <- storeFromIvory(repo, store)
    es  <- entities.traverseU(_.run(store => store.linesUtf8.read))
    in  <- incremental.traverseU({ case (id, sm) => for {
            _ <- ResultT.ok[IO, Unit]({
                   println(s"Previous store was '${sm.store}'")
                   println(s"Previous date was '${sm.date.string("-")}'")
                 })
            s <- storeFromIvory(repo, sm.store)
          } yield (repo.snapshot(id).toHdfs, s, sm) })
    _   <- job(hr, s, in, out, codec).run(hr.conf)
    _   <- DictionaryTextStorageV2.toStore(output </> FilePath(".dictionary"), d)
    _   <- SnapshotMeta(snapshot, store).toReference(output </> SnapshotMeta.fname)
  } yield ()

  def job(repo: HdfsRepository, store: FeatureStore, incremental: Option[(Path, FeatureStore, SnapshotMeta)], outputPath: Path, codec: Option[CompressionCodec]): Hdfs[Unit] = for {
    conf  <- Hdfs.configuration
    globs <- Hdfs.fromResultTIO(Snapshot.storePaths(repo, store, snapshot, incremental))
    paths  = globs.flatMap(_.partitions.map(p => new Path(p.path))) ++ incremental.map(_._1).toList
    size  <- paths.traverse(Hdfs.size).map(_.sum)
    _     = println(s"Total input size: ${size}")
    reducers = size.toBytes.value / 768.mb.toBytes.value + 1 // one reducer per 768MB of input
    _     = println(s"Number of reducers: ${reducers}")
    _     <- Hdfs.safe(SnapshotJob.run(conf, reducers.toInt, snapshot, globs, outputPath, incremental.map(_._1), codec))
  } yield ()
}

object Snapshot {
  val SnapshotName: String = "ivory-incremental-snapshot"

  def takeSnapshot(repo: Repository, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ResultTIO[(String, Identifier)] =
    fatrepo.ExtractLatestWorkflow.onStore(repo, extractLatest(codec), date, incremental)

  /* This is exposed through the external API */
  def snapshot(repoPath: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[Path] = for {
    sc   <- ScoobiAction.scoobiConfiguration
    repo <- Repository.fromHdfsPath(repoPath.toString.toFilePath, sc).pure[ScoobiAction]
    snap <- ScoobiAction.fromResultTIO(takeSnapshot(repo, date, incremental, codec).map(res => repo.snapshot(res._2).toHdfs))
  } yield snap

  def extractLatest(codec: Option[CompressionCodec])(repo: Repository, store: String, date: Date, output: ReferenceIO, incremental: Option[(Identifier, SnapshotMeta)]): ResultTIO[Unit] =
    Snapshot(repo, store, None, date, output, incremental, codec).run

  def storePaths(repo: Repository, store: FeatureStore, latestDate: Date, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): ResultTIO[List[FactsetGlob]] =
    incremental match {
      case None =>
        StoreGlob.before(repo, store, latestDate)
      case Some((p, s, sm)) => for {
        o <- StoreGlob.between(repo, s, sm.date, latestDate) // read facts from already processed store from the last snapshot date to the latest date
        sd = store diff s
        _  = println(s"Reading factsets '${sd.factsets}' up to '${latestDate}'")
        n <- StoreGlob.before(repo, sd, latestDate) // read factsets which haven't been seen up until the 'latest' date
      } yield FactsetGlob.groupByVersion(o ++ n)
    }
}
