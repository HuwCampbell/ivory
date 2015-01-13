package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.entities._
import scalaz._, Scalaz._

case class ChordPlan(entities: Entities, commit: Commit, snapshot: Option[Snapshot], datasets: Datasets)

/**
 * This planner is responsible for producing the minimal set of data to read for a
 * chord operation.
 */
object ChordPlan {
  /**
   * Determine the plan datasets for the given chord entities, and repository
   * state using an in memory strategy to determine validity using specified
   * snapshots and select best using a weighting function.
   */
  def inmemory(
    entities: Entities
  , commit: Commit
  , snapshots: List[Snapshot]
  ): ChordPlan = {
    val p = SnapshotPlan.inmemory(entities.earliestDate, commit, snapshots)
    val datasets = build(entities, commit, p)
    ChordPlan(entities, commit, p.snapshot, datasets)
  }

  /**
   * Determine the plan datasets for the given chord entities, and repository
   * state using a pessimistic strategy to determine validity using all
   * snapshots and select best using a weighting function.
   */
  def pessimistic[F[_]: Monad](
    entities: Entities
  , commit: Commit
  , snapshots: List[SnapshotId]
  , getSnapshot: Kleisli[F, SnapshotId, Option[Snapshot]]
  ): F[ChordPlan] =
    SnapshotPlan.pessimistic(entities.earliestDate, commit, snapshots, getSnapshot).map(p =>
      ChordPlan(entities, commit, p.snapshot, build(entities, commit, p)))

  /**
   * Determine the plan datasets for the given chord entities, and repository
   * state using a conservative strategy to determine validity using all
   * snapshots and select best using a weighting function. This is the
   * same as pessimistic this uses requires memory at the cost of more io.
   */
  def conservative[F[_]: Monad](
    entities: Entities
  , commit: Commit
  , snapshots: List[SnapshotId]
  , getSnapshot: Kleisli[F, SnapshotId, Option[Snapshot]]
  ): F[ChordPlan] =
    SnapshotPlan.conservative(entities.earliestDate, commit, snapshots, getSnapshot).map(p =>
      ChordPlan(entities, commit, p.snapshot, build(entities, commit, p)))

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
  , getSnapshot: Kleisli[F, SnapshotId, Option[Snapshot]]
  ): F[ChordPlan] =
    SnapshotPlan.optimistic(entities.earliestDate, commit, snapshots, getSnapshot).map(p =>
      ChordPlan(entities, commit, p.snapshot, build(entities, commit, p)))


  /**
   * ChordPlan is effectively a special case of SnapshotPlan where we use the
   * most recent date in the entities file as the 'at' date. This assumes that the
   * provided SnapshotPlan is for the oldest date in the entities file, this makes
   * this method _unsafe_ to call without the contextual checks in the planning
   * methods above, and as such is being made private to prevent it from being
   * called to ensure the invariant can't be violated.
   */
  private def build(entities: Entities, commit: Commit, snapshot: SnapshotPlan): Datasets =
    snapshot.snapshot.flatMap(SnapshotPlan.evaluate(entities.latestDate, commit, _)).map(_.datasets).getOrElse(
      Datasets(Dataset.to(commit.store, entities.latestDate)))
}
