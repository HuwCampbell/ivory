package com.ambiata.ivory.operation.extraction

import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.operation.extraction.snapshot._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._
import com.ambiata.mundane.store._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._

import scalaz.{DList => _, _}, Scalaz._, effect._

/**
 * Snapshots are used to store the latest feature values at a given date.
 * This can be done incrementally in order to be fast.
 *
 * Snapshots are also used to extract chords and pivots.
 *
 * After each snapshot is taken, metadata is saved to know exactly:
 *
 *  - when the snapshot was taken
 *  - on which FeatureStore it was taken
 *
 *
 * Note that in between 2 snapshots, the FeatureStore might have changed
 */
object Snapshot {
  private implicit val logger = LogFactory.getLog("ivory.operation.Snapshot")

  /** 
   * Take a new snapshot
   * If incremental is true, take a incremental snapshot (based off the previous one), unless the previous one is up to date
   */
  def takeSnapshot(repository: Repository, date: Date, incremental: Boolean): ResultTIO[SnapshotMeta] =
    if (incremental) takeIncrementalSnapshot(repository, date)
    else             takeNewSnapshot(repository, date)

  /**
   * We need to create a new incremental snapshot if the previous one is not up to date any more
   *
   *  - if it corresponds to an old store
   *  - if there were partitions created after the snapshot has been taken
   */
  def takeIncrementalSnapshot(repository: Repository, date: Date): ResultTIO[SnapshotMeta] =
    for {
      latest  <- SnapshotMeta.latestUpToDateSnapshot(repository, date)
      meta    <- latest match {
        case Some(m) => ResultT.safe[IO, SnapshotMeta](m).info(s"Not running snapshot as already have a snapshot for '${date.hyphenated}' and '${m.featureStoreId}'")
        case None    => SnapshotMeta.latestSnapshot(repository, date) >>= createSnapshot(repository, date)
      }
    } yield meta

  /**
   * take a new snapshot, without considering any previous incremental snapshot
   */
  def takeNewSnapshot(repository: Repository, date: Date): ResultTIO[SnapshotMeta] =
    createSnapshot(repository, date)(None)

  /**
   * create a new snapshot at a given date, using the previous snapshot data if present
   */
  def createSnapshot(repository: Repository, date: Date): Option[SnapshotMeta] => ResultTIO[SnapshotMeta] = (previousSnapshot: Option[SnapshotMeta]) =>
    for {
      newSnapshot <- SnapshotMeta.createSnapshotMeta(repository, date)
      _           <- runSnapshot(repository, newSnapshot, previousSnapshot, date, newSnapshot.snapshotId).info(s"""
                                 | Running extractor on:
                                 |
                                 | Repository     : ${repository}
                                 | Feature Store  : ${newSnapshot.featureStoreId.render}
                                 | Date           : ${date.hyphenated}
                                 | Output         : ${Repository.snapshot(newSnapshot.snapshotId).name}
                                 |
                                 """.stripMargin)
    } yield newSnapshot

  /**
   * Run a snapshot on a given repository using the previous snapshot in case of an incremental snapshot
   */
  def runSnapshot(repository: Repository, newSnapshot: SnapshotMeta, previousSnapshot: Option[SnapshotMeta], date: Date, newSnapshotId: SnapshotId): ResultTIO[Unit] =
    for {
      hr              <- downcast[Repository, HdfsRepository](repository, s"Snapshot only works with Hdfs repositories currently, got '$repository'")
      output          =  repository.toFilePath(Repository.snapshot(newSnapshot.snapshotId)).toDirPath
      dictionary      <- latestDictionaryFromIvory(repository)
      windows         =  SnapshotWindows.planWindow(dictionary, date)
      newFactsetGlobs <- calculateGlobs(repository, dictionary, windows, newSnapshot, previousSnapshot, date)
      _               <- job(hr, previousSnapshot, newFactsetGlobs, date, output.toHdfs, windows, hr.codec).run(hr.configuration)
      _               <- DictionaryTextStorageV2.toKeyStore(repository, Repository.snapshot(newSnapshot.snapshotId) / ".dictionary", dictionary)
      _               <- SnapshotMeta.save(repository, newSnapshot)
    } yield ()

  def calculateGlobs(repository: Repository, dictionary: Dictionary, windows: SnapshotWindows, newSnapshot: SnapshotMeta,
                     previousSnapshot: Option[SnapshotMeta], date: Date): ResultTIO[List[Prioritized[FactsetGlob]]] =
    for {
      currentFeatureStore <- Metadata.latestFeatureStoreOrFail(repository)
      parts           <- previousSnapshot.cata(sm => for {
        prevStore     <- featureStoreFromIvory(repository, sm.featureStoreId)
        pw            =  SnapshotWindows.planWindow(dictionary, sm.date)
        sp            =  SnapshotPartition.partitionIncremental(currentFeatureStore, prevStore, date, sm.date)
        spw           =  SnapshotPartition.partitionIncrementalWindowing(prevStore, sm.date, windows, pw)
      } yield sp ++ spw, ResultT.ok[IO, List[SnapshotPartition]](SnapshotPartition.partitionAll(currentFeatureStore, date)))
      newFactsetGlobs <- newFactsetGlobs(repository, parts)
    } yield newFactsetGlobs

  /**
   * create a new snapshot as a Map-Reduce job
   */
  private def job(repository: Repository, previousSnapshot: Option[SnapshotMeta],
                  factsetsGlobs: List[Prioritized[FactsetGlob]], snapshotDate: Date, outputPath: Path,
                  windows: SnapshotWindows, codec: Option[CompressionCodec]): Hdfs[Unit] =
    for {
      conf            <- Hdfs.configuration
      incrementalPath =  previousSnapshot.map(meta => repository.toFilePath(Repository.snapshot(meta.snapshotId)).toHdfs)
      paths           =  factsetsGlobs.flatMap(_.value.keys.map(key => repository.toFilePath(key).toHdfs)) ++ incrementalPath.toList
      size            <- paths.traverse(Hdfs.size).map(_.sum)
      _               <- Hdfs.log(s"Total input size: $size")
      reducers        =  size.toBytes.value / 2.gb.toBytes.value + 1 // one reducer per 2GB of input
      _               <- Hdfs.log(s"Number of reducers: $reducers")
      _               <- Hdfs.safe(SnapshotJob.run(repository, conf, reducers.toInt, snapshotDate, factsetsGlobs, outputPath, windows, incrementalPath, codec))
    } yield ()

  /** This is exposed through the external API */
  def snapshot(repoPath: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[Path] = for {
    sc         <- ScoobiAction.scoobiConfiguration
    repository <- ScoobiAction.fromResultTIO(Repository.fromUriResultTIO(repoPath.toString, IvoryConfiguration.fromScoobiConfiguration(sc)))
    snap       <- ScoobiAction.fromResultTIO(takeSnapshot(repository, date, incremental).map(res => repository.toFilePath(Repository.snapshot(res.snapshotId)).toHdfs))
  } yield snap

  def dictionaryForSnapshot(repository: Repository, meta: SnapshotMeta): ResultTIO[Dictionary] =
    meta.commitId.cata(
      commitId => for {
        commit <- commitFromIvory(repository, commitId)
        dict   <- dictionaryFromIvory(repository, commit.dictionaryId)
      } yield dict,
      latestDictionaryFromIvory(repository)
    )

  def newFactsetGlobs(repo: Repository, partitions: List[SnapshotPartition]): ResultTIO[List[Prioritized[FactsetGlob]]] =
    partitions.traverseU(s => FeatureStoreGlob.between(repo, s.store, s.start, s.end).map(_.globs)).map(_.flatten)
}
