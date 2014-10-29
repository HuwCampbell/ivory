package com.ambiata.ivory.operation.extraction

import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.snapshot._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import IvorySyntax._
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
  def takeSnapshot(repository: Repository, date: Date, incremental: Boolean): ResultTIO[SnapshotManifest] =
    if (incremental) takeIncrementalSnapshot(repository, date)
    else             takeNewSnapshot(repository, date).map(SnapshotManifest.snapshotManifestNew)

  /**
   * We need to create a new incremental snapshot if the previous one is not up to date any more
   *
   *  - if it corresponds to an old store
   *  - if there were partitions created after the snapshot has been taken
   */
  def takeIncrementalSnapshot(repo: Repository, date: Date): ResultTIO[SnapshotManifest] =
    for {
      latest  <- SnapshotManifest.latestUpToDateSnapshot(repo, date).run
      meta    <- latest match {
        case Some(m) =>
          for {
            storeId <- SnapshotManifest.getFeatureStoreId(repo, m)
            _ <- ResultT.fromIO(IO.putStrLn(s"Not running snapshot as already have a snapshot for '${date.hyphenated}' and '${storeId}'"))
            x <- ResultT.safe[IO, SnapshotManifest](m)
          } yield x
        case None    => (SnapshotManifest.latestSnapshot(repo, date).run >>= createSnapshot(repo, date)).map(SnapshotManifest.snapshotManifestNew)
      }
    } yield meta

  /**
   * take a new snapshot, without considering any previous incremental snapshot
   */
  def takeNewSnapshot(repository: Repository, date: Date): ResultTIO[NewSnapshotManifest] =
    createSnapshot(repository, date)(None)

  /**
   * create a new snapshot at a given date, using the previous snapshot data if present
   */
  def createSnapshot(repo: Repository, date: Date): Option[SnapshotManifest] => ResultTIO[NewSnapshotManifest] = (previousSnapshot: Option[SnapshotManifest]) =>
    for {
      newSnapshot <- NewSnapshotManifest.createSnapshotManifest(repo, date)
      _           <- NewSnapshotManifest.getFeatureStoreId(repo, newSnapshot).flatMap((featureStoreId: FeatureStoreId) => runSnapshot(repo, newSnapshot, previousSnapshot, date, newSnapshot.snapshotId).info(s"""
                                 | Running extractor on:
                                 |
                                 | Repository     : ${repo}
                                 | Feature Store  : ${featureStoreId.render}
                                 | Date           : ${date.hyphenated}
                                 | Output         : ${Repository.snapshot(newSnapshot.snapshotId).name}
                                 |
                                 """.stripMargin))
    } yield newSnapshot

  /**
   * Run a snapshot on a given repository using the previous snapshot in case of an incremental snapshot
   */
  def runSnapshot(repository: Repository, newSnapshot: NewSnapshotManifest, previousSnapshot: Option[SnapshotManifest], date: Date, newSnapshotId: SnapshotId): ResultTIO[Unit] =
    for {
      hr              <- downcast[Repository, HdfsRepository](repository, s"Snapshot only works with Hdfs repositories currently, got '$repository'")
      output          =  hr.toIvoryLocation(Repository.snapshot(newSnapshot.snapshotId))
      dictionary      <- latestDictionaryFromIvory(repository)
      windows         =  SnapshotWindows.planWindow(dictionary, date)
      newFactsetGlobs <- calculateGlobs(repository, dictionary, windows, newSnapshot, previousSnapshot, date)
      _               <- job(hr, dictionary, previousSnapshot, newFactsetGlobs, date, output.toHdfsPath, windows, hr.codec).run(hr.configuration)
      _               <- DictionaryTextStorageV2.toKeyStore(repository, Repository.snapshot(newSnapshot.snapshotId) / ".dictionary", dictionary)
      _               <- NewSnapshotManifest.save(repository, newSnapshot)
    } yield ()

  def calculateGlobs(repo: Repository, dictionary: Dictionary, windows: SnapshotWindows, newSnapshot: NewSnapshotManifest,
                     previousSnapshot: Option[SnapshotManifest], date: Date): ResultTIO[List[Prioritized[FactsetGlob]]] =
    for {
      currentFeatureStore <- Metadata.latestFeatureStoreOrFail(repo)
      parts           <- previousSnapshot.cata(sm => for {
        featureStoreId <- SnapshotManifest.getFeatureStoreId(repo, sm)
        prevStore     <- featureStoreFromIvory(repo, featureStoreId)
        pw            =  SnapshotWindows.planWindow(dictionary, sm.date)
        sp            =  SnapshotPartition.partitionIncremental(currentFeatureStore, prevStore, date, sm.date)
        spw           =  SnapshotPartition.partitionIncrementalWindowing(prevStore, sm.date, windows, pw)
      } yield sp ++ spw, ResultT.ok[IO, List[SnapshotPartition]](SnapshotPartition.partitionAll(currentFeatureStore, date)))
      newFactsetGlobs <- newFactsetGlobs(repo, parts)
    } yield newFactsetGlobs

  /**
   * create a new snapshot as a Map-Reduce job
   */
  def job(repository: HdfsRepository, dictionary: Dictionary, previousSnapshot: Option[SnapshotManifest],
                  factsetsGlobs: List[Prioritized[FactsetGlob]], snapshotDate: Date, outputPath: Path,
                  windows: SnapshotWindows, codec: Option[CompressionCodec]): Hdfs[Unit] =
    for {
      conf            <- Hdfs.configuration
      incrementalPath =  previousSnapshot.map(meta => repository.toIvoryLocation(Repository.snapshot(meta.snapshotId)).toHdfsPath)
      paths           =  factsetsGlobs.flatMap(_.value.keys.map(key => repository.toIvoryLocation(key).toHdfsPath)) ++ incrementalPath.toList
      size            <- paths.traverse(Hdfs.size).map(_.sum)
      _               <- Hdfs.log(s"Total input size: $size")
      reducers        =  size.toBytes.value / 2.gb.toBytes.value + 1 // one reducer per 2GB of input
      _               <- Hdfs.log(s"Number of reducers: $reducers")
      _               <- Hdfs.safe(SnapshotJob.run(repository, conf, dictionary, reducers.toInt, snapshotDate, factsetsGlobs, outputPath, windows, incrementalPath, codec))
    } yield ()

  /** This is exposed through the external API */
  def snapshot(repoPath: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[Path] = for {
    sc         <- ScoobiAction.scoobiConfiguration
    repository <- ScoobiAction.fromResultTIO(Repository.fromUri(repoPath.toString, IvoryConfiguration.fromScoobiConfiguration(sc)))
    hr         <- ScoobiAction.fromResultTIO(downcast[Repository, HdfsRepository](repository, s"Snapshot only works with Hdfs repositories currently, got '$repository'"))
    snap       <- ScoobiAction.fromResultTIO(takeSnapshot(hr, date, incremental).map(res => hr.toIvoryLocation(Repository.snapshot(res.snapshotId)).toHdfsPath))
  } yield snap

  def dictionaryForSnapshot(repository: Repository, meta: SnapshotManifest): ResultTIO[Dictionary] =
    meta.storeOrCommitId.b.cata(
      commitId => for {
        commit <- commitFromIvory(repository, commitId)
        dict   <- dictionaryFromIvory(repository, commit.dictionaryId)
      } yield dict,
      latestDictionaryFromIvory(repository)
    )

  def newFactsetGlobs(repo: Repository, partitions: List[SnapshotPartition]): ResultTIO[List[Prioritized[FactsetGlob]]] =
    partitions.traverseU(s => FeatureStoreGlob.between(repo, s.store, s.start, s.end).map(_.globs)).map(_.flatten)
}
