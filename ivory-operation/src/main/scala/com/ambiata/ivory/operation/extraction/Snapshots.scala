package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.snapshot._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.manifest._
import com.ambiata.ivory.storage.plan._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.mundane.io.MemoryConversions._
import org.apache.hadoop.fs.Path
import scalaz.{DList => _, _}, Scalaz._

/**
 * Contains information that would be handy to return to the user of the CLI or
 * Ivory "as a library" that doesnt have to go in the snapshot metadata for persistence
 * in the repository itself.
 */
case class SnapshotJobSummary[A](
    meta: A
  , incremental: Option[SnapshotMetadata]) { // FIX why is this relevant, I think it can only lead to a bug that it is available

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
  def takeSnapshot(repository: Repository, date: Date): RIO[SnapshotJobSummary[SnapshotMetadata]] =
    for {
      commit <- CommitStorage.head(repository)
      ids <- SnapshotStorage.ids(repository)
      plan <- SnapshotPlan.pessimistic(date, commit, ids, Kleisli[RIO, SnapshotId, Snapshot](id => SnapshotStorage.byId(repository, id)))
      summary <- plan.exact match {
        case Some(snapshot) =>
          SnapshotJobSummary(snapshot.toMetadata, snapshot.toMetadata.some).pure[RIO]
        case None =>
          createSnapshot(repository, date, commit, plan)
      }
    } yield summary

    // 1. allocate id
    // 2. run job
    // 3. save dictionary
    // 4. save manifest
    // 5. save stats
  def createSnapshot(repository: Repository, date: Date, commit: Commit, plan: SnapshotPlan): RIO[SnapshotJobSummary[SnapshotMetadata]] = for {
    // FIX detangle IO and pure code here...
    manifest <- SnapshotMetadataStorage.createSnapshotManifest(repository, date) // WTF FIX pass in commit? WTF is this magic commit thing?
    metadata <- SnapshotMetadataStorage.toMetadata(repository, manifest) // WTF why is this not pure?
    _        <- RIO.putStrLn(s"Total input size: ${plan.datasets.bytes}")
    reducers =  (plan.datasets.bytes.toLong / 2.gb.toBytes.value + 1).toInt // one reducer per 2GB of input
    _        <- RIO.putStrLn(s"Number of reducers: $reducers")
    /* DO NOT MOVE CODE BELOW HERE, NOTHING BESIDES THIS JOB CALL SHOULD MAKE HDFS ASSUMPTIONS. */
    hr       <- repository.asHdfsRepository
    output   =  hr.toIvoryLocation(Repository.snapshot(manifest.snapshot))
    stats    <- SnapshotJob.run(hr, plan, reducers, output.toHdfsPath)
    _        <- DictionaryTextStorageV2.toKeyStore(repository, Repository.snapshot(manifest.snapshot) / ".dictionary", commit.dictionary)
    _        <- SnapshotManifest.io(repository, manifest.snapshot).write(manifest)
    _        <- SnapshotStats.save(repository, manifest.snapshot, stats)
  } yield SnapshotJobSummary(metadata, plan.snapshot.map(_.toMetadata))
}
