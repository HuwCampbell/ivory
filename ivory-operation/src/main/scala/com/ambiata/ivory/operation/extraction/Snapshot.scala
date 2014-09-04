package com.ambiata.ivory.operation.extraction

import scalaz.{DList => _, _}, Scalaz._, effect._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata._, Metadata._
import com.ambiata.ivory.storage.store._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.scoobi._
import MemoryConversions._

case class Snapshot(repo: Repository, store: FeatureStoreId, entities: Option[ReferenceIO], snapshot: Date, output: ReferenceIO, incremental: Option[(SnapshotId, SnapshotMeta)]) {
  def run: ResultTIO[Unit] = for {
    hr  <- repo match {
      case h: HdfsRepository => ResultT.ok[IO, HdfsRepository](h)
      case _                 => ResultT.fail[IO, HdfsRepository]("Snapshot only works on HDFS repositories at this stage.")
    }
    out <- output match {
      case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
      case _                                => ResultT.fail[IO, Path](s"Snapshot output path must be on hdfs, got '${output}'")
    }
    ds  <- (dictionaryFromIvoryT tuple featureStoreFromIvoryT(store)).run(IvoryRead.prod(repo))
    (d, s) = ds
    es  <- entities.traverseU(_.run(store => store.linesUtf8.read))
    in  <- incremental.traverseU({ case (id, sm) => for {
            _ <- ResultT.ok[IO, Unit]({
                   println(s"Previous store was '${sm.featureStoreId}'")
                   println(s"Previous date was '${sm.date.string("-")}'")
                 })
            s <- featureStoreFromIvory(repo, sm.featureStoreId)
          } yield (repo.snapshot(id).toHdfs, s, sm) })
    _   <- job(hr, s, in, out, hr.codec).run(hr.configuration)
    _   <- DictionaryTextStorageV2.toStore(output </> FilePath(".dictionary"), d)
    _   <- SnapshotMeta(snapshot, store).toReference(output </> SnapshotMeta.fname)
  } yield ()

  def job(repo: HdfsRepository, store: FeatureStore, incremental: Option[(Path, FeatureStore, SnapshotMeta)], outputPath: Path, codec: Option[CompressionCodec]): Hdfs[Unit] = for {
    conf  <- Hdfs.configuration
    globs <- Hdfs.fromResultTIO(Snapshot.featureStorePaths(repo, store, snapshot, incremental))
    paths  = globs.flatMap(_.value.paths.map(_.toHdfs)) ++ incremental.map(_._1).toList
    size  <- paths.traverse(Hdfs.size).map(_.sum)
    _     = println(s"Total input size: ${size}")
    reducers = size.toBytes.value / 2.gb.toBytes.value + 1 // one reducer per 2GB of input
    _     = println(s"Number of reducers: ${reducers}")
    _     <- Hdfs.safe(SnapshotJob.run(conf, reducers.toInt, snapshot, globs, outputPath, incremental.map(_._1), codec))
  } yield ()
}

object Snapshot {
  val SnapshotName: String = "ivory-incremental-snapshot"

  def takeSnapshot(repo: Repository, date: Date, incremental: Boolean): ResultTIO[(FeatureStoreId, SnapshotId)] =
    fatrepo.ExtractLatestWorkflow.onStore(repo, extractLatest, date, incremental)

  /** This is exposed through the external API */
  def snapshot(repoPath: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[Path] = for {
    sc   <- ScoobiAction.scoobiConfiguration
    repo <- Repository.fromHdfsPath(repoPath.toString.toFilePath, sc).pure[ScoobiAction]
    snap <- ScoobiAction.fromResultTIO(takeSnapshot(repo, date, incremental).map(res => repo.snapshot(res._2).toHdfs))
  } yield snap

  def extractLatest(repo: Repository, store: FeatureStoreId, date: Date, output: ReferenceIO, incremental: Option[(SnapshotId, SnapshotMeta)]): ResultTIO[Unit] =
    Snapshot(repo, store, None, date, output, incremental).run

  def featureStorePaths(repo: Repository, store: FeatureStore, latestDate: Date, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): ResultTIO[List[Prioritized[FactsetGlob]]] =
    incremental match {
      case None =>
        FeatureStoreGlob.before(repo, store, latestDate).map(_.globs)
      case Some((p, s, sm)) => for {
        // read facts from already processed store from the last snapshot date to the latest date
        o <- FeatureStoreGlob.between(repo, s, sm.date, latestDate).map(_.globs)
        sd = store diff s
        _  = println(s"Reading factsets '${sd.factsets}' up to '${latestDate}'")
        // read factsets which haven't been seen up until the 'latest' date
        n <- FeatureStoreGlob.before(repo, sd, latestDate).map(_.globs)
      } yield o ++ n
    }
}
