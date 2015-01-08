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
  def takeSnapshot(repository: Repository, flags: IvoryFlags, date: Date): RIO[Snapshot] =
    for {
      commit <- CommitStorage.head(repository)
      ids <- SnapshotStorage.ids(repository)
      snapshot <- takeSnapshotOn(repository, flags, commit, ids, date)
    } yield snapshot

  def takeSnapshotOn(repository: Repository, flags: IvoryFlags, commit: Commit, ids: List[SnapshotId], date: Date): RIO[Snapshot] =
    for {
      plan <- SnapshotPlan.pessimistic(date, commit, ids, SnapshotStorage.source(repository))
      snapshot <- plan.exact match {
        case Some(snapshot) =>
          snapshot.pure[RIO]
        case None =>
          createSnapshot(repository, date, commit, plan)
      }
    } yield snapshot

    // 1. allocate id
    // 2. run job
    // 3. save dictionary
    // 4. save manifest
    // 5. save stats
  def createSnapshot(repository: Repository, date: Date, commit: Commit, plan: SnapshotPlan): RIO[Snapshot] = for {
    id       <- SnapshotStorage.allocateId(repository)
    _        <- RIO.putStrLn(s"Total input size: ${plan.datasets.bytes}")
    reducers =  (plan.datasets.bytes.toLong / 2.gb.toBytes.value + 1).toInt // one reducer per 2GB of input
    _        <- RIO.putStrLn(s"Number of reducers: $reducers")
    /* DO NOT MOVE CODE BELOW HERE, NOTHING BESIDES THIS JOB CALL SHOULD MAKE HDFS ASSUMPTIONS. */
    hr       <- repository.asHdfsRepository
    output   =  hr.toIvoryLocation(Repository.snapshot(id))
    stats    <- SnapshotJob.run(hr, plan, reducers, output.toHdfsPath)
    _        <- DictionaryTextStorageV2.toKeyStore(repository, Repository.snapshot(id) / ".dictionary", commit.dictionary.value)
    _        <- SnapshotManifest.io(repository, id).write(SnapshotManifest.createLatest(commit.id, id, date))
    _        <- SnapshotStats.save(repository, id, stats)
    bytes    <- SnapshotStorage.size(repository, id)
  } yield Snapshot(id, date, commit.store, commit.dictionary.some, bytes)
}
