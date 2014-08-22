package com.ambiata.ivory.operation.extraction

import org.apache.commons.logging.LogFactory

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

object Snapshot {
  private implicit val logger = LogFactory.getLog("ivory.operation.Snapshot")

  /** take a new snapshot, possibly an incremental one */
  def takeSnapshot(repository: Repository, date: Date, incremental: Boolean): ResultTIO[SnapshotMeta] =
    if (incremental) takeIncrementalSnapshot(repository, date)
    else             takeNewSnapshot(repository, date)

  /**
   * If we are using incremental snapshots we need to first check the state of
   * the latest available incremental snapshot. If it is not:
   *
   *  - up to date with respect to the latest store
   *  - up to date with the date for which we want to take a snapshot
   *
   *  then we take a new snapshot
   */
  def takeIncrementalSnapshot(repository: Repository, date: Date): ResultTIO[SnapshotMeta] =
    for {
      storeId <- Metadata.latestFeatureStoreIdOrFail(repository)
      latest  <- SnapshotMeta.latestIncrementalSnapshot(repository, date, storeId)
      meta    <- latest match {
        case Some(m) => ResultT.safe[IO, SnapshotMeta](m).info(s"Not running snapshot as already have a snapshot for '${date.hyphenated}' and '$storeId'")
        case None    => takeNewSnapshot(repository, date)
      }
    } yield meta

  /**
   * take a new snapshot, allocating a new snapshot id first
   */
  def takeNewSnapshot(repository: Repository, date: Date): ResultTIO[SnapshotMeta] =
    for {
      storeId    <- Metadata.latestFeatureStoreIdOrFail(repository)
      snapshotId <- SnapshotMeta.allocateId(repository)
      output     =  repository.toReference(Repository.snapshot(snapshotId))
      meta       <- runSnapshot(repository, storeId, date, output, snapshotId, None).info(s"""
                                 | Running extractor on:
                                 |
                                 | Repository     : ${repository.root.path}
                                 | Feature Store  : ${storeId.render}
                                 | Date           : ${date.hyphenated}
                                 | Output         : $output
                                 |
                                 """.stripMargin)
    } yield meta

  private def runSnapshot(repository: Repository, storeId: FeatureStoreId, snapshotDate: Date, output: ReferenceIO, snapshotId: SnapshotId, incremental: Option[SnapshotMeta]) =
    for {
      hr  <- repository match {
        case h: HdfsRepository => ResultT.ok[IO, HdfsRepository](h)
        case _                 => ResultT.fail[IO, HdfsRepository]("Snapshot only works on HDFS repositories at this stage.")
      }
      out <- output match {
        case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
        case _                                => ResultT.fail[IO, Path](s"Snapshot output path must be on hdfs, got '$output'")
      }
      dictionary           <- dictionaryFromIvory(repository)
      store                <- featureStoreFromIvory(repository, storeId)
      featureStoreSnapshot <- incremental.traverse(FeatureStoreSnapshot.fromSnapshotMeta(repository))
      _                    <- job(hr, store, featureStoreSnapshot, snapshotDate, out, hr.codec).run(hr.configuration)
      _                    <- DictionaryTextStorageV2.toStore(output </> FilePath(".dictionary"), dictionary)
      meta                 <- SnapshotMeta.save(snapshotId, snapshotDate, storeId, output)
    } yield meta

  private def job(repository: HdfsRepository, store: FeatureStore,
                  previous: Option[FeatureStoreSnapshot], snapshotDate: Date, outputPath: Path, codec: Option[CompressionCodec]): Hdfs[Unit] =
    for {
      conf                 <- Hdfs.configuration
      globs                <- Hdfs.fromResultTIO(Snapshot.featureStorePaths(repository, store, snapshotDate, previous))
      incrementalPath      =  previous.map(meta => repository.snapshot(meta.snapshotId).toHdfs)
      paths                =  globs.flatMap(_.value.paths.map(_.toHdfs)) ++ incrementalPath.toList
      size                 <- paths.traverse(Hdfs.size).map(_.sum)
      _                    <- Hdfs.log(s"Total input size: $size")
      reducers             =  size.toBytes.value / 2.gb.toBytes.value + 1 // one reducer per 2GB of input
      _                    <- Hdfs.log(s"Number of reducers: $reducers")
      _                    <- Hdfs.safe(SnapshotJob.run(conf, reducers.toInt, snapshotDate, globs, outputPath, incrementalPath, codec))
    } yield ()

  def featureStorePaths(repository: Repository, store: FeatureStore, latestDate: Date, incremental: Option[FeatureStoreSnapshot]): ResultTIO[List[Prioritized[FactsetGlob]]] =
    incremental match {
      case None => FeatureStoreGlob.before(repository, store, latestDate).map(_.globs)

      case Some(fss) => for {
        // read facts from already processed store from the last snapshot date to the latest date
        oldOnes    <- FeatureStoreGlob.between(repository, fss.store, fss.date, latestDate)
        difference = store diff fss.store
        _          = logInfo(s"Reading factsets '${difference.factsets}' up to '$latestDate'")
        // read factsets which haven't been seen up until the 'latest' date
        newOnes    <- FeatureStoreGlob.before(repository, difference, latestDate)
      } yield oldOnes.globs ++ newOnes.globs
    }

  /** This is exposed through the external API */
  def snapshot(repoPath: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[Path] = for {
    sc   <- ScoobiAction.scoobiConfiguration
    repo <- Repository.fromHdfsPath(repoPath.toString.toFilePath, sc).pure[ScoobiAction]
    snap <- ScoobiAction.fromResultTIO(takeSnapshot(repo, date, incremental).map(res => repo.snapshot(res.snapshotId).toHdfs))
  } yield snap

}
