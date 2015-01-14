package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core._, Lists.findMapM
import scalaz._, Scalaz._

case class SnapshotPlan(date: Date, commit: Commit, snapshot: Option[Snapshot], datasets: Datasets){
  def exact: Option[Snapshot] =
    snapshot.filter(s => s.date === date && s.store === commit.store && s.dictionary.forall(_.id === commit.dictionary.id))
}

/**
 * This planner is responsible for producing the minimal set of data to read for a
 * snapshot operation.
 */
object SnapshotPlan {
  /**
   * Determine the plan datasets for the given snapshot 'at' date, and repository
   * state using an in-memory strategy to determine validity using provided
   * snapshots and select best using a weighting function.
   */
  def inmemory(
    at: Date
  , commit: Commit
  , snapshots: List[Snapshot]
  ): SnapshotPlan =
    snapshots.flatMap(evaluate(at, commit, _).toList).sortBy(weight).headOption.getOrElse(fallback(at, commit))

  /**
   * Determine the plan datasets for the given snapshot 'at' date, and repository
   * state using a pessimistic strategy to determine validity using all
   * snapshots and select best using a weighting function.
   */
  def pessimistic[F[_]: Monad](
    at: Date
  , commit: Commit
  , snapshots: List[SnapshotId]
  , getSnapshot: Kleisli[F, SnapshotId, Option[Snapshot]]
  ): F[SnapshotPlan] =
    snapshots.traverse(getSnapshot.run(_).map(s => s.flatMap(evaluate(at, commit, _))))
      .map(_.flatten.sortBy(weight).headOption.getOrElse(fallback(at, commit)))

  /**
   * Determine the plan datasets for the given snapshot 'at' date, and repository
   * state using a conservative strategy to determine validity using all
   * snapshots and select best using a weighting function. This is the same as the
   * pessimist strategy except that it gains a smaller memory footprint by
   * performing additional IO.
   */
  def conservative[F[_]: Monad](
    at: Date
  , commit: Commit
  , snapshots: List[SnapshotId]
  , getSnapshot: Kleisli[F, SnapshotId, Option[Snapshot]]
  ): F[SnapshotPlan] =
    snapshots.traverse(getSnapshot.run(_).map(s => for {
      snapshot <- s
      plan <- evaluate(at, commit, snapshot)
    } yield snapshot.id -> weight(plan))).flatMap(_.flatten.sortBy(_._2).headOption.traverse({
      case (id, _) =>
        getSnapshot(id).map(_.flatMap(evaluate(at, commit, _)))
    })).map(_.flatten.getOrElse(fallback(at, commit)))

  /**
   * Determine the plan datasets for the given snapshot 'at' date, and repository
   * state using an optimistic strategy to using SnapshotMetadata to filter
   * candidates and order by 'likelihood', and then take the first valid
   * snapshot that it hits.
   */
  def optimistic[F[_]: Monad](
    at: Date
  , commit: Commit
  , snapshots: List[SnapshotMetadata]
  , getSnapshot: Kleisli[F, SnapshotId, Option[Snapshot]]
  ): F[SnapshotPlan] = {
    val candidates = snapshots.filter(_.date <= at).sortBy(metadata => ~metadata.date.int)
    findMapM(candidates)(metadata => {
      getSnapshot.run(metadata.id).map(s => s.flatMap(evaluate(at, commit, _)))
    }).map(_.getOrElse(fallback(at, commit)))
  }

  /**
   * Attempt to construct the set of datasets required to be read for a snapshot, given
   * the specified feature store and incremental snapshot. This shall build a data set
   * iff if the snapshot 'isValid' for the given date and store. The dataset shall be
   * the _minimal_ dataset required for the operation.
   */
  def evaluate(at: Date, commit: Commit, snapshot: Snapshot): Option[SnapshotPlan] =
    isValid(at, commit, snapshot).option({
      // work out what data is included in the snapshot
      val included = snapshot.store.unprioritizedIds.toSet
      // This is all the factset data that was available at the snapshot date, but not included in the snapshot
      val before = Dataset.to(commit.store.filterByFactsetId(id => !included.contains(id)), snapshot.date)
      // This is all the factset data after the snapshot date and before the at date (from both, before and after snapshot)
      val after = Dataset.within(commit.store, snapshot.date, at)
      // include the snapshot itself in the dataset
      val s = Dataset.prioritizedSnapshot(snapshot)
      SnapshotPlan(at, commit, snapshot.some, Datasets(s :: before ::: after).prune) })

  /**
   * Determine the weight of the dataset, a lower weight is better, and the lowest weight
   * will be selected as the best candidate. We currently just use data size as the
   * weight indicator, in the future this may be extended to include health checks, i.e.
   * it *may* be better to source equivalent data from less fragmented data with a smaller
   * number of partitions.
   */
  def weight(plan: SnapshotPlan): Long =
    plan.datasets.bytes.toLong

  /**
   * Determine if a given snapshot is valid for the specified snapshot 'at' date
   * and store.
   */
  def isValid(at: Date, commit: Commit, snapshot: Snapshot): Boolean =
    snapshot.date.isBeforeOrEqual(at) && snapshot.store.subsetOf(commit.store) &&
      (sameDictionary(commit, snapshot) || noWindows(commit, snapshot) || containedBy(at, commit, snapshot))

  /**
   * Determine if the commit and snapshot have the same dictionary. This is a
   * fairly aggressive optimization, that we do, because the containedBy check
   * is relatively expensive and the common case is the dictionaries are the
   * same.
   */
  def sameDictionary(commit: Commit, snapshot: Snapshot): Boolean =
    snapshot.dictionary.exists(_.id === commit.dictionary.id)

  /**
   * A snapshot from the pre-commit world is valid if and only if the current dictionary doesn't
   * have any windows.
   */
  def noWindows(commit: Commit, snapshot: Snapshot): Boolean =
    !snapshot.dictionary.isDefined && !commit.dictionary.value.windows.hasWindows

  /**
   * We need to check that any feature ids that exists at the time of the old snapshot,
   * are fully contained by the snapshot windows for the given commit/at combination,
   * i.e. for each feature in the commit, the snapshot must contain all data from at or
   * before the earliest required date for that feature (taking into account the potential
   * windows for virtual features etc...)
   *
   * We simplify and do this at a namespace granularity only, because that is all we can select the
   * actual dataset by.
   */
  def containedBy(at: Date, commit: Commit, snapshot: Snapshot): Boolean =
    snapshot.dictionary.exists(dictionary => {
      // if the old dictionary doesn't know about the feature, we know it is ok
      // because the relevant data will only be in the new commits, so we can
      // ignore them and happily use a snapshot that doesn't know about those
      // features, even for overlapping date ranges.
      val restricted = commit.dictionary.value.forFeatureIds(dictionary.value.featureIds)
      restricted.windows.byNamespace(at).containedBy(dictionary.value.windows.byNamespace(at))
    })

  /**
   * When we don't have any snapshot to optimise with, we fallback to reading all
   * data for the current feature store at or before the specified date.
   */
  def fallback(at: Date, commit: Commit): SnapshotPlan =
    SnapshotPlan(at, commit, none, Datasets(Dataset.to(commit.store, at)).prune)
}
