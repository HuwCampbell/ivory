package com.ambiata.ivory.storage.plan

import com.ambiata.disorder.DistinctPair
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._, scalacheck.ScalazArbitrary._

object SnapshotPlanSpec extends Specification with ScalaCheck { def is = s2"""

SnapshotPlan
============

Scenario 1 - Planning a Snapshot, where there are valid incremental snapshots
-----------------------------------------------------------------------------

  Given there is a valid snapshot, there should be exactly one snapshot in the plan
  dataset.
    ${scenario1.solidarity}

  When the 'at' date and the current feature store matches an incremental snapshot
  date/store, the plan dataset should only contain the snapshot, i.e. no fact sets.
    ${scenario1.exactDataset}

  When the 'at' date and the current feature store matches an incremental snapshot
  date/store, the plan dataset should be available as the 'exact' match in the plan.
    ${scenario1.exact}

  There must not be factset partitions in the plan dataset before any snapshot
  included in the plan unless they are from a factset id not included in the store
  from the incremental snapshot.
    ${scenario1.optimal}

  The selected snapshot should always be the _latest_ valid snapshot when doing an
  optimistic plan.
    ${scenario1.optimistic}

  The selected datasets should always be the smallest valid combination when doing a
  pessimisitc plan.
    ${scenario1.pessimistic}

  The pessismistic and inmemory strategies should always return the same result.
    ${scenario1.consistency}

  The conservative and inmemory strategies should always return the same result.
    ${scenario1.consistency2}

  All partitions at or before the snapshot 'at' date and after the incremental
  snapshot date must be included (and no others).
    ${scenario1.soundness}


Scenario 2 - Planning a Snapshot, where there are _no_ valid incremental snapshot
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


Support 1 - Attempting to build a dataset from a snapshot
---------------------------------------------------------

  Never build a dataset if the snapshot store is not a subset of the current store.
    ${support1.subset}

  Never include a snapshot from a future date.
    ${support1.future}

  When successful, output datasets must not include any date after 'at' date.
    ${support1.snapshot}


Support 2 - Determining if a given snapshot is valid
----------------------------------------------------

  If the snapshot date is before stored snapshot date, it is always invalid.
    ${support2.future}

  If there are factsets in the snapshot not in the store, it is always invalid.
    ${support2.disjoint}

  If the snapshot date is equal to stored snapshot date and valid store, it is valid.
    ${support2.now}

  If the snapshot date is after stored snapshot date and valid store, it is valid.
    ${support2.past}

  If one concrete feature mode changes, it is always invalid.
    ${support2.mode}


Support 3 - Fallback behaviour when there is no incremental snapshot to load from
---------------------------------------------------------------------------------

  Output datasets must not include any date after 'at' date.
    ${support3.soundness}


Invariants 1 - Things that always hold true for SnapshotPlan
------------------------------------------------------------

  The request commit should always be maintained as is in the plan.
    ${invariants1.commit}

  The request date should always be maintained as is in the plan.
    ${invariants1.date}

  The selected snapshot should be defined if there is a valid snapshot.
    ${invariants1.snapshot}

  The selected snapshot should be defined if there is a snapshot in the datasets.
    ${invariants1.datasets}

"""

  object scenario1 {

    def solidarity =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots)
        countSnapshots(plan.datasets) ==== 1 })

    def exactDataset =
      prop((scenario: RepositoryScenario, id: SnapshotId, bytes: Bytes \/ List[Sized[Namespace]], format: SnapshotFormat) => {
        val snapshot = Snapshot(id, scenario.at, scenario.commit.store, scenario.commit.dictionary.some, bytes, format)
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, List(snapshot))
        plan.datasets ==== Datasets(List(Prioritized(Priority.Max, SnapshotDataset(snapshot)))) })

    def exact =
      prop((scenario: RepositoryScenario, id: SnapshotId, bytes: Bytes \/ List[Sized[Namespace]], format: SnapshotFormat) => {
        val snapshot = Snapshot(id, scenario.at, scenario.commit.store, scenario.commit.dictionary.some, bytes, format)
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, List(snapshot))
        plan.exact.exists(_ === snapshot) })

    def optimal =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots)
        val datasets = plan.datasets
        findSnapshot(datasets).exists(snapshot =>
          factsetsLike(factset =>
            if (snapshot.store.unprioritizedIds.contains(factset.id))
              factset.partitions.forall(p => p.value.date > snapshot.date && p.value.date <= scenario.at)
            else
              factset.partitions.forall(p => p.value.date <= scenario.at))(datasets)) })

    def optimistic =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        val plan = SnapshotPlan.optimistic(scenario.at, scenario.commit, scenario.snapshots.map(_.toMetadata), source(scenario.snapshots))
        val datasets = plan.datasets
        findSnapshotDate(datasets).exists(date =>
          scenario.snapshots.filter(SnapshotPlan.isValid(scenario.at, scenario.commit, _)).forall(_.date <= date)) })

    def pessimistic =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots)
        val datasets = plan.datasets
        findSnapshotId(datasets).exists(id => {
          val evaluated = scenario.snapshots.map(s => s.id -> SnapshotPlan.evaluate(scenario.at, scenario.commit, s))
          val filtered = evaluated.filter(_._2.isDefined)
          val sorted = filtered.sortBy(_._2.cata(_.datasets.bytes.toLong, 0L))
          sorted.headOption.forall(_._1 === id) }) })

    def consistency =
      prop((scenario: RepositoryScenario) =>
        SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots) ====
          SnapshotPlan.pessimistic(scenario.at, scenario.commit, scenario.snapshots.map(_.id), source(scenario.snapshots)))

    def consistency2 =
      prop((scenario: RepositoryScenario) =>
        SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots) ====
          SnapshotPlan.conservative(scenario.at, scenario.commit, scenario.snapshots.map(_.id), source(scenario.snapshots)))

    // this is a critical test, be very, very careful if you ever want to change this, the basic
    // idea is that we are doing a cross check and calculating the datasets in a different way
    // to the production code, in production we use the latest commit and carefully pick out the
    // two classes of data (new - up to the at date, and old - between the snapshot and at date),
    // in the test case we calculate the old data using the store from the snapshot, this should
    // always give the same answer and is a good way to ensure no additional or missing partitions
    // in the final dataset result.
    def soundness =
      prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots)
        val datasets = plan.datasets
        findSnapshot(datasets) must beSome((snapshot: Snapshot) => {
          def select(factsets: List[Prioritized[Factset]]): List[(FactsetId, Partition)] =
            factsets.flatMap(f => f.value.partitions.map(p => f.value.id -> p.value))
          val included = snapshot.store.factsets.map(_.value.id).toSet
          val newData = select(scenario.commit.store.factsets.filter(f => !included.contains(f.value.id)))
          val oldData = select(snapshot.store.factsets)
          val newSelected = newData.filter(_._2.date <= scenario.at)
          val oldSelected = oldData.filter(p => p._2.date > snapshot.date && p._2.date <= scenario.at)
          allFactsetPartitions(datasets).sorted ==== (newSelected ++ oldSelected).sorted }) })
  }

  object scenario2 {

    def factsets =
      prop((scenario: RepositoryScenario) => {
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, Nil)
        val datasets = plan.datasets
        countSnapshots(datasets) ==== 0 })

    def soundness =
      prop((scenario: RepositoryScenario) => {
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, Nil)
        val datasets = plan.datasets
        allPartitions(datasets).sorted ==== scenario.partitions.filter(_.date <= scenario.at) })
  }

  object scenario3 {

    def optimal =
      prop((scenario: RepositoryScenario) => {
        val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots)
        val datasets = plan.datasets
        allBefore(scenario.at)(datasets) })
  }

  object support1 {

    def subset =
      prop((at: Date, commit: Commit, snapshot: Snapshot) => !snapshot.store.subsetOf(commit.store) ==> {
        !SnapshotPlan.evaluate(at, commit, snapshot).isDefined })

    def future =
      prop((at: Date, commit: Commit, snapshot: Snapshot) => snapshot.date > at ==> {
        !SnapshotPlan.evaluate(at, commit, snapshot).isDefined })

    def snapshot =
      prop((at: Date, commit: Commit, snapshot: Snapshot) => (snapshot.date <= at && snapshot.store.subsetOf(commit.store)) ==> {
        SnapshotPlan.evaluate(at, commit, snapshot).exists(p => allBefore(at)(p.datasets)) })
  }

  object support2 {

    def now =
      prop((commit: Commit, snapshot: Snapshot) => snapshot.store.subsetOf(commit.store) ==> {
        SnapshotPlan.isValid(snapshot.date, commit, snapshot) must beTrue })

    def past =
      prop((at: Date, commit: Commit, snapshot: Snapshot) => (snapshot.store.subsetOf(commit.store) && at > snapshot.date) ==> {
        SnapshotPlan.isValid(at, commit, snapshot) must beTrue })

    def future =
      prop((at: Date, commit: Commit, snapshot: Snapshot) => at < snapshot.date ==> {
        SnapshotPlan.isValid(at, commit, snapshot) must beFalse })

    def disjoint =
      prop((at: Date, commit: Commit, snapshot: Snapshot) => !snapshot.store.subsetOf(commit.store) ==> {
        SnapshotPlan.isValid(at, commit, snapshot) must beFalse })

    def mode =
      prop((commit: Commit, snapshot: Snapshot, cg: ConcreteGroupFeature, dictionary: Dictionary,
            ids: DistinctPair[DictionaryId], modes: DistinctPair[Mode]) => {
        SnapshotPlan.isValid(snapshot.date,
          commit.copy(dictionary = Identified(ids.first, cg.withMode(modes.first).dictionary.append(dictionary))),
          snapshot.copy(dictionary = Some(Identified(ids.second, cg.withMode(modes.second).dictionary.append(dictionary))))
        ) must beFalse})
  }

  object support3 {

    def soundness =
      prop((at: Date, commit: Commit) =>
        allBefore(at) { SnapshotPlan.fallback(at, commit).datasets })
  }

  object invariants1 {
    def commit = prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
      SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots).commit ==== scenario.commit })

    def date = prop((scenario: RepositoryScenario) => validSnapshot(scenario) ==> {
      SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots).date ==== scenario.at })

    def snapshot = prop((scenario: RepositoryScenario) =>
      SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots).snapshot.isDefined ==== validSnapshot(scenario) )

    def datasets = prop((scenario: RepositoryScenario) => {
      val plan = SnapshotPlan.inmemory(scenario.at, scenario.commit, scenario.snapshots)
      plan.snapshot.isDefined ====  plan.datasets.summary.snapshot.isDefined })
  }

  def validSnapshot(scenario: RepositoryScenario): Boolean =
    validSnapshotAt(scenario, scenario.at)

  def validSnapshotAt(scenario: RepositoryScenario, at: Date): Boolean =
    scenario.snapshots.exists(SnapshotPlan.isValid(at, scenario.commit, _))

  def source(snapshots: List[Snapshot]): Kleisli[Id, SnapshotId, Option[Snapshot]] =
    Kleisli[Id, SnapshotId, Option[Snapshot]](id => snapshots.find(_.id == id))

  def allBefore(at: Date): Datasets => Boolean =
    datasets => datasets.sets.forall({
      case Prioritized(_, FactsetDataset(factset)) =>
        factset.partitions.forall(_.value.date <= at)
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        snapshot.date <= at
    })

  def factsetsAfter(at: Date): Datasets => Boolean =
    factsetsLike(_.partitions.forall(_.value.date > at))

  def factsetsLike(pred: Factset => Boolean): Datasets => Boolean =
    datasets => datasets.sets.forall({
      case Prioritized(_, FactsetDataset(factset)) =>
        pred(factset)
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        true
    })

  def allFactsetPartitions(datasets: Datasets): List[(FactsetId, Partition)] =
    datasets.sets.flatMap({
      case Prioritized(_, FactsetDataset(factset)) =>
        factset.partitions.map(p => factset.id -> p.value)
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        Nil
    })

  def allPartitions(datasets: Datasets): List[Partition] =
    datasets.sets.flatMap({
      case Prioritized(_, FactsetDataset(factset)) =>
        factset.partitions.map(_.value)
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        Nil
    })

  def findSnapshotDate(datasets: Datasets): Option[Date] =
    findSnapshot(datasets).map(_.date)

  def findSnapshotId(datasets: Datasets): Option[SnapshotId] =
    findSnapshot(datasets).map(_.id)

  def findSnapshot(datasets: Datasets): Option[Snapshot] =
    datasets.sets.collect({
      case Prioritized(_, SnapshotDataset(snapshot)) =>
        snapshot
    }).headOption

  def countSnapshots(datasets: Datasets): Int =
    datasets.sets.filter(_.value.isSnapshot).length

}
