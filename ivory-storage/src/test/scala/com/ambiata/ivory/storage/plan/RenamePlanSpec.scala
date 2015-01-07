package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

object RenamePlanSpec extends Specification with ScalaCheck { def is = s2"""

RenamePlan
==========

Scenario 1 - Rename is straight forward and always has an answer based on commit
--------------------------------------------------------------------------------

  Include all factset data when there is at least one feature from every namespace
  we have a partition for.
    ${scenario1.everything}

  Include only relevant factset data when there are features from only some of the
  namespaces we have a partition for, this should not include data from irrelevant
  namespaces.
    ${scenario1.some}

  Include all partitions from commit where namespace is selected.
    ${scenario1.all}

  Include no factset data when there are no features from any namespace we have
  a partition for.
    ${scenario1.none}

Invariants 1 - Things that always hold true for RenamePlan
----------------------------------------------------------

  The request commit should always be maintained as is in the plan.
    ${invariants1.commit}

  There should never be snapshots in the plan datasets.
    ${invariants1.noSnapshot}

"""

  object scenario1 {
    def everything = prop((c: Commit) =>
      RenamePlan.inmemory(c, allNamespaces(c)).datasets ==== Datasets(c.store.toDataset))

    def some = prop((c: Commit) => {
      val features = someNamespaces(c)
      val namespaces = features.map(_.namespace).toSet
      val plan = RenamePlan.inmemory(c, features)
      val datasets = plan.datasets
      datasets.sets.forall(d => d.value match {
        case FactsetDataset(factset) =>
          factset.partitions.forall(p => namespaces.contains(p.value.namespace))
        case SnapshotDataset(snapshot) =>
          true
      }) })

    def all = prop((c: Commit) => {
      val features = someNamespaces(c)
      val namespaces = features.map(_.namespace).toSet
      val plan = RenamePlan.inmemory(c, features)
      val datasets = plan.datasets
      val included = datasets.sets.flatMap(d => d.value match {
        case FactsetDataset(factset) =>
          factset.partitions.map(_.value)
        case SnapshotDataset(snapshot) =>
          Nil
      })
      val required = c.store.factsets.flatMap(_.value.partitions.map(_.value))
      (included.size -> included.toSet) ==== (required.size -> required.toSet)  })

    def none = prop((c: Commit) =>
      RenamePlan.inmemory(c, Nil).datasets ==== Datasets(Nil))
  }

  object invariants1 {
    def commit = prop((c: Commit, ids: List[FeatureId]) =>
      RenamePlan.inmemory(c, ids).commit ==== c)

    def noSnapshot = prop((c: Commit) =>
      !RenamePlan.inmemory(c, allNamespaces(c)).datasets.summary.snapshot.isDefined)
  }

  def allNamespaces(c: Commit): List[FeatureId] =
    c.store.factsets.flatMap(
      _.value.partitions.map(p => FeatureId(p.value.namespace, "feature")))

  def someNamespaces(c: Commit): List[FeatureId] =
    allNamespaces(c).zipWithIndex.filter(_._2 % 2 == 0).map(_._1)
}
