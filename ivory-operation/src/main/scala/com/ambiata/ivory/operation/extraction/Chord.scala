package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.squash.{SquashConfig, SquashJob}
import com.ambiata.ivory.storage.entities._
import com.ambiata.ivory.storage.manifest._
import com.ambiata.ivory.storage.legacy.FeatureStoreSnapshot
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.plan._
import com.ambiata.notion.core.KeyName
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import scalaz._

/**
 * A Chord is the extraction of feature values for some entities at some dates
 *
 * Use the latest snapshot (if available) to get the latest values
 */
object Chord {
  /**
   * Create a chord from a list of entities
   * If takeSnapshot = true, take a snapshot first, otherwise use the latest available snapshot
   */
  def createChordWithSquash(repository: Repository, entitiesLocation: IvoryLocation, takeSnapshot: Boolean,
                            config: SquashConfig, cluster: Cluster): RIO[(ShadowOutputDataset, Dictionary)] = for {
    commit   <- CommitStorage.head(repository)
    entities <- Entities.readEntitiesFrom(entitiesLocation)
    out      <- createChordRaw(repository, entities, commit, takeSnapshot)
    (o, dict) = out
    hr       <- repository.asHdfsRepository
    job      <- SquashJob.initChordJob(hr.configuration, entities)
    // We always need to squash because the entities need to be rewritten, which is _only_ handled by squash
    // This can technically be optimized to do the entity rewriting in the reducer - see the Git history for an example
    r        <- SquashJob.squash(repository, dict, o, config, job, cluster)
    _        <- ChordExtractManifest.io(cluster.toIvoryLocation(r.location)).write(ChordExtractManifest.create(commit.id))
  } yield r -> dict

  def createChordRaw(repository: Repository, entities: Entities, commit: Commit, takeSnapshot: Boolean): RIO[(ShadowOutputDataset, Dictionary)] = for {
    plan     <- planning(repository, entities, commit, takeSnapshot)
    output   <- Repository.tmpLocation(repository, "chord").flatMap(_.asHdfsIvoryLocation)
    _        <- RIO.putStrLn(s"Total input size: ${plan.datasets.bytes}")
    reducers =  (plan.datasets.bytes.toLong / 2.gb.toBytes.value + 1).toInt // one reducer per 2GB of input
    _        <- RIO.putStrLn(s"Number of reducers: $reducers")
    /* DO NOT MOVE CODE BELOW HERE, NOTHING BESIDES THIS JOB CALL SHOULD MAKE HDFS ASSUMPTIONS. */
    hr       <- repository.asHdfsRepository
    _        <- ChordJob.run(hr, plan, reducers, output.toHdfsPath)
  } yield (ShadowOutputDataset.fromIvoryLocation(output), commit.dictionary.value)

  def planning(repository: Repository, entities: Entities, commit: Commit, takeSnapshot: Boolean): RIO[ChordPlan] =
    SnapshotStorage.ids(repository).flatMap(ids => takeSnapshot match {
      case true =>
        Snapshots.takeSnapshotOn(repository, commit, ids, entities.earliestDate).map(snapshot =>
          ChordPlan.inmemory(entities, commit, List(snapshot)))
      case false =>
        // FIX this shouldn't be or fail.
        val source = Kleisli[RIO, SnapshotId, Snapshot](id => SnapshotStorage.byIdOrFail(repository, id))
        ChordPlan.pessimistic(entities, commit, ids, source)
    })
}
