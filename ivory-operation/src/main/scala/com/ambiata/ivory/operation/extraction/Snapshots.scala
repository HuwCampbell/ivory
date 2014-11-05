package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.snapshot._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.poacher.hdfs._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import scalaz.{DList => _, _}, Scalaz._, effect._

/**
 * Contains information that would be handy to return to the user of the CLI or
 * Ivory "as a library" that doesnt have to go in the snapshot metadata for persistence
 * in the repository itself.
 */
case class SnapshotJobSummary[A](
    meta: A
  , incremental: Option[SnapshotManifest]) {

  def map[B](f: A => B): SnapshotJobSummary[B] = SnapshotJobSummary(f(meta), incremental)
}

/**
 * Snapshots are used to store the latest feature values at a given date.
 * This can be done incrementally in order to be fast.
 *
 * After each snapshot is taken, metadata is saved to know exactly:
 *
 *  - when the snapshot was taken
 *  - on which FeatureStore it was taken
 *
 *
 * Note that in between 2 snapshots, the FeatureStore might have changed
 */
object Snapshots {
  /**
   * Take a new snapshot as at the specified date.
   */
  def takeSnapshot(repository: Repository, date: Date): ResultTIO[SnapshotJobSummary[SnapshotManifest]] =
    for {
      latest    <- SnapshotManifest.latestUpToDateSnapshot(repository, date).run
      result    <- latest match {
        case Some(m) =>
          for {
            storeId <- SnapshotManifest.getFeatureStoreId(repository, m)
            _ <- ResultT.fromIO(IO.putStrLn(s"Not running snapshot as already have a snapshot for '${date.hyphenated}' and '${storeId}'"))
            x <- ResultT.safe[IO, SnapshotManifest](m)
          } yield SnapshotJobSummary(x, latest)
        case None    => (SnapshotManifest.latestSnapshot(repository, date).run >>= createSnapshot(repository, date)).map(_.map(SnapshotManifest.snapshotManifestNew))
      }
    } yield result

  /**
   * create a new snapshot at a given date, using the previous snapshot data if present
   */
  def createSnapshot(repository: Repository, date: Date): Option[SnapshotManifest] => ResultTIO[SnapshotJobSummary[NewSnapshotManifest]] = (previousSnapshot: Option[SnapshotManifest]) =>
    for {
      newSnapshot <- NewSnapshotManifest.createSnapshotManifest(repository, date)
      _           <- NewSnapshotManifest.getFeatureStoreId(repository, newSnapshot).flatMap((featureStoreId: FeatureStoreId) => for {
        _ <- runSnapshot(repository, newSnapshot, previousSnapshot, date, newSnapshot.snapshotId)
        _ <- ResultT.fromIO(IO.putStrLn(s"""| Running extractor on:
                                            |
                                            | Repository     : ${repository.root.show}
                                            | Feature Store  : ${featureStoreId.render}
                                            | Date           : ${date.hyphenated}
                                            | Output         : ${Repository.snapshot(newSnapshot.snapshotId).name}
                                            |""".stripMargin))
      } yield ())
    } yield SnapshotJobSummary(newSnapshot, previousSnapshot)

  /**
   * Run a snapshot on a given repository using the previous snapshot in case of an incremental snapshot
   */
  def runSnapshot(repository: Repository, newSnapshot: NewSnapshotManifest, previousSnapshot: Option[SnapshotManifest], date: Date, newSnapshotId: SnapshotId): ResultTIO[Unit] =
    for {
      dictionary      <- latestDictionaryFromIvory(repository)
      windows         =  SnapshotWindows.planWindow(dictionary, date)
      newFactsetGlobs <- calculateGlobs(repository, dictionary, windows, newSnapshot, previousSnapshot, date)

      /* DO NOT MOVE CODE BELOW HERE, NOTHING BESIDES THIS JOB CALL SHOULD MAKE HDFS ASSUMPTIONS. */
      hr              <- repository.asHdfsRepository[IO]
      output          =  hr.toIvoryLocation(Repository.snapshot(newSnapshot.snapshotId))
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

  def newFactsetGlobs(repo: Repository, partitions: List[SnapshotPartition]): ResultTIO[List[Prioritized[FactsetGlob]]] =
    partitions.traverseU(s => FeatureStoreGlob.between(repo, s.store, s.start, s.end).map(_.globs)).map(_.flatten)
}
