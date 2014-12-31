package com.ambiata.ivory.storage.plan
/*

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._

object SnapshotPlannerSpec extends Specification with ScalaCheck { def is = s2"""

SnapshotPlanner
===============

Scenario 1 - Planning a Snapshot, where there is a valid incremental snapshot
-----------------------------------------------------------------------------

  Given there is a valid snapshot, there should be exactly one snapshot in the plan
  dataset.
    ${scenario1.solidarity}

  When the 'at' date and the current feature store matches an incremental snapshot
  date/store, the plan dataset should only contain the snapshot, i.e. no fact sets.
    ${scenario1.exact}

  There must not be factset partitions in the plan dataset before any snapshot
  included in the plan unless they are from a factset id not included in the store
  from the incremental snapshot.
    ${scenario1.optimal}

  The selected snapshot should always be the _latest_ valid snapshot.
    ${scenario1.latest}

  All partitions at or before the snapshot 'at' date and after the incremental
  snapshot date must be included (and no others).
    ${scenario1.soundness}


Scenario 2 - Planning a Snapshot, where there is _no_ valid incremental snapshot
--------------------------------------------------------------------------------

  Given there are no valid snapshots, there should only be factsets in the
  plan dataset.
    ${scenario2.factsets}

  All partitions at or before the snapshot date must be included.
    ${scenario2.soundness}


Scenario 3 - Planning a Snapshot, independent of incremental snapshot state
---------------------------------------------------------------------------

  There should never be partitions in the plan dataset included after the snapshot
  date.
    ${scenario3.optimal}


Support 1 - Attempting to build a dataset form a snapshot
---------------------------------------------------------

  Never build a dataset if the snapshot store is not a subset of the current store.
    ${support1.subset}

  Never include a snapshot from a future date.
    ${support1.future}

  When sucessful, output datasets must not include any date after 'at' date.
    ${support1.snapshot}


Support 2 - Determining if a given snapshot is valid
----------------------------------------------------

  If the snapshot date is before stored snapshot date, it is always invalid.
    ${support2.future}

  If there are factsets in the snapshot not in the store, it is always invalid.
    ${support2.disjoint}

  If the snapshot date is equal to stored snapshot date and valid store, is is valid.
    ${support2.now}

  If the snapshot date is after stored snapshot date and valid store, is is valid.
    ${support2.past}


Support 3 - Fallback behaviour when there is no incremental snapshot to load from
---------------------------------------------------------------------------------

  Output datasets must not include any date after 'at' date.
    ${support3.soundness}

"""

  object scenario1 {

    def solidarity =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        SnapshotPlanner.plan(scenario.at, scenario.store, scenario.metadata, source(scenario)) must beSome((datasets: Datasets) =>
          countSnapshots(datasets) must_== 1) })

    def exact =
      prop((snapshot: Snapshot) => {
        val scenario = RepositoryScenario(snapshot.store, List(snapshot), snapshot.date, snapshot.date)
        SnapshotPlanner.plan(snapshot.date, scenario.store, scenario.metadata, source(scenario)) must beSome(
          Datasets(List(Prioritized(Priority.Max, SnapshotDataset(snapshot))))) })

    def optimal =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        SnapshotPlanner.plan(scenario.at, scenario.store, scenario.metadata, source(scenario)) must beSome((datasets: Datasets) =>
          findSnapshot(datasets).exists(snapshot =>
            factsetsLike(factset =>
              if (snapshot.store.unprioritizedIds.contains(factset.id))
                factset.partitions.forall(p => p.date > snapshot.date && p.date <= scenario.at)
              else
                factset.partitions.forall(p => p.date <= scenario.at))(datasets)))})

    def latest =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        SnapshotPlanner.plan(scenario.at, scenario.store, scenario.metadata, source(scenario)) must beSome((datasets: Datasets) =>
          findSnapshotDate(datasets).exists(date =>
            scenario.snapshots.filter(SnapshotPlanner.isValid(scenario.at, scenario.store, _)).forall(_.date <= date))) })

    def soundness =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        SnapshotPlanner.plan(scenario.at, scenario.store, scenario.metadata, source(scenario)) must beSome((datasets: Datasets) => {
          findSnapshotDate(datasets).exists(date =>
            allPartitions(datasets).sorted == scenario.partitions.filter(partition =>
              partition.date > date && partition.date <= scenario.at)) }) })
  }

  object scenario2 {

    def factsets =
      prop((scenario: RepositoryScenario) =>
        SnapshotPlanner.plan(scenario.at, scenario.store, Nil, source(scenario)) must beSome((datasets: Datasets) =>
          countSnapshots(datasets) must_== 0) )

    def soundness =
      prop((scenario: RepositoryScenario) =>
        SnapshotPlanner.plan(scenario.at, scenario.store, Nil, source(scenario)) must beSome((datasets: Datasets) =>
          allPartitions(datasets).sorted must_== scenario.partitions.filter(_.date <= scenario.at)))
  }

  object scenario3 {

    def optimal =
      prop((scenario: RepositoryScenario) =>
        SnapshotPlanner.plan(scenario.at, scenario.store, scenario.metadata, source(scenario)) must beSome(
          allBefore(scenario.at)))
  }

  object support1 {

    def subset =
      prop((at: Date, store: FeatureStore, snapshot: Snapshot) => !snapshot.store.subsetOf(store) ==> {
        !SnapshotPlanner.build(at, store, snapshot).isDefined })

    def future =
      prop((at: Date, store: FeatureStore, snapshot: Snapshot) => snapshot.date > at ==> {
        !SnapshotPlanner.build(at, store, snapshot).isDefined })

    def snapshot =
      prop((at: Date, store: FeatureStore, snapshot: Snapshot) => (snapshot.date <= at && snapshot.store.subsetOf(store)) ==> {
        SnapshotPlanner.build(at, store, snapshot).exists(allBefore(at)) })
  }

  object support2 {

    def now =
      prop((store: FeatureStore, snapshot: Snapshot) => snapshot.store.subsetOf(store) ==> {
        SnapshotPlanner.isValid(snapshot.date, store, snapshot) must beTrue })

    def past =
      prop((at: Date, store: FeatureStore, snapshot: Snapshot) => (snapshot.store.subsetOf(store) && at > snapshot.date) ==> {
        SnapshotPlanner.isValid(at, store, snapshot) must beTrue })

    def future =
      prop((at: Date, store: FeatureStore, snapshot: Snapshot) => at < snapshot.date ==> {
        SnapshotPlanner.isValid(at, store, snapshot) must beFalse })

    def disjoint =
      prop((at: Date, store: FeatureStore, snapshot: Snapshot) => !snapshot.store.subsetOf(store) ==> {
        SnapshotPlanner.isValid(at, store, snapshot) must beFalse })
  }

  object support3 {

    def soundness =
      prop((at: Date, store: FeatureStore) =>
        allBefore(at) { SnapshotPlanner.fallback(at, store) })
  }

  def validSnapshot(scenario: RepositoryScenario): Boolean =
    scenario.snapshots.exists(SnapshotPlanner.isValid(scenario.at, scenario.store, _))

  def source(scenario: RepositoryScenario): Kleisli[Option, SnapshotId, Snapshot] =
    Kleisli(id => scenario.snapshots.find(_.id == id))

  def allBefore(at: Date): Datasets => Boolean =
    datasets => datasets.sets.forall({
      case Prioritized(_, FactsetDataset(factset)) =>
        factset.partitions.forall(_.date <= at)
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        snapshot.date <= at
    })

  def factsetsAfter(at: Date): Datasets => Boolean =
    factsetsLike(_.partitions.forall(_.date > at))

  def factsetsLike(pred: Factset => Boolean): Datasets => Boolean =
    datasets => datasets.sets.forall({
      case Prioritized(_, FactsetDataset(factset)) =>
        pred(factset)
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        true
    })

  def allPartitions(datasets: Datasets): List[Partition] =
    datasets.sets.flatMap({
      case Prioritized(_, FactsetDataset(factset)) =>
        factset.partitions
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        Nil
    })

  def findSnapshotDate(datasets: Datasets): Option[Date] =
    findSnapshot(datasets).map(_.date)

  def findSnapshot(datasets: Datasets): Option[Snapshot] =
    datasets.sets.collect({
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        snapshot
    }).headOption

  def countSnapshots(datasets: Datasets): Int =
    datasets.sets.filter(_.value.isSnapshot).length

}
*/
