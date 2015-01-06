package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core._ //, Lists.findMapM
import com.ambiata.ivory.storage.entities._
import scalaz._//, Scalaz._

case class ChordPlan(entities: Entities, commit: Commit, snapshot: Option[Snapshot], datasets: Datasets)

/**
 * This planner is responsible for producing the minimal set of data to read for a
 * chord operation.
 */
object ChordPlan {
  /**
   * Determine the plan datasets for the given chord entites, and repository
   * state using an in memory strategy to determine validity using specified
   * snapshots and select best using a weighting function.
   */
  def inmemory(
    entities: Entities
  , commit: Commit
  , snapshots: List[Snapshot]
  ): ChordPlan =
    ???
  /**
   * Determine the plan datasets for the given chord entites, and repository
   * state using a pessismistic strategy to determine validity using all
   * snapshots and select best using a weighting function.
   */
  def pessimistic[F[_]: Monad](
    entities: Entities
  , commit: Commit
  , snapshots: List[SnapshotId]
  , getSnapshot: Kleisli[F, SnapshotId, Snapshot]
  ): F[ChordPlan] =
    ???

  /**
   * Determine the plan datasets for the given chord entites, and repository
   * state using an optimistic strategy to using SnapshotMetadata to filter
   * candidates and order by 'likelihood', and then take the first valid
   * snapshot that it hits.
   */
  def optimistic[F[_]: Monad](
    entities: Entities
  , commit: Commit
  , snapshots: List[SnapshotMetadata]
  , getSnapshot: Kleisli[F, SnapshotId, Snapshot]
  ): F[ChordPlan] =
    ???


/*
  def calculateGlobs(repository: Repository, featureStore: FeatureStore, latestDate: Date,
                     featureStoreSnapshot: Option[FeatureStoreSnapshot]): RIO[List[Prioritized[FactsetGlob]]] =
    featureStoreSnapshot.cata(snapshot => for {
      oldGlobs    <- FeatureStoreGlob.strictlyAfterAndBefore(repository, snapshot.store, snapshot.date, latestDate).map(_.globs)
      newFactsets  = featureStore diff snapshot.store
      _            = println(s"Reading factsets up to '$latestDate'\n${newFactsets.factsets}")
      newGlobs    <- FeatureStoreGlob.before(repository, newFactsets, latestDate).map(_.globs)
    } yield oldGlobs ++ newGlobs, FeatureStoreGlob.before(repository, featureStore, latestDate).map(_.globs))
*/

}
