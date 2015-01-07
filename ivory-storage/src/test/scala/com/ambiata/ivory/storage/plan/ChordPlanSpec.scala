package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.storage.entities._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scala.collection.JavaConverters._


object ChordPlanSpec extends Specification with ScalaCheck { def is = s2"""

ChordPlan
============

Scenario 1 - Planning a Chord, where there are valid incremental snapshots
--------------------------------------------------------------------------

  The SnapshotPlan for the earliest Entities entry should have the same incremental
  snapshot as the ChordPlan.

    ${scenario1.consistency}


Invariants 1 - Things that always hold true for ChordPlan
---------------------------------------------------------

  The request commit should always be maintained as is in the plan.
    ${invariants1.commit}

  The request entities should always be maintained as is in the plan.
    ${invariants1.entities}

  The selected snapshot should be defined if there is a valid snapshot.
    ${invariants1.snapshot}

  The selected snapshot should be defined if there is a snapshot in the datasets.
    ${invariants1.datasets}

"""

  import SnapshotPlanSpec._

  object scenario1 {

    def consistency = prop((scenario: RepositoryScenario) => {
      val entities = Entities(scenario.entities.asJava)
      val chord = ChordPlan.inmemory(entities, scenario.commit, scenario.snapshots)
      val snapshot = SnapshotPlan.inmemory(entities.earliestDate, scenario.commit, scenario.snapshots)
      chord.snapshot ==== snapshot.snapshot })
  }

  object invariants1 {
    def commit = prop((scenario: RepositoryScenario) => validSnapshotEntities(scenario) ==> {
      val entities = Entities(scenario.entities.asJava)
      ChordPlan.inmemory(entities, scenario.commit, scenario.snapshots).commit ==== scenario.commit })

    def entities = prop((scenario: RepositoryScenario) => validSnapshotEntities(scenario) ==> {
      val entities = Entities(scenario.entities.asJava)
      ChordPlan.inmemory(entities, scenario.commit, scenario.snapshots).entities ==== entities })

    def snapshot = prop((scenario: RepositoryScenario) => {
      val entities = Entities(scenario.entities.asJava)
      ChordPlan.inmemory(entities, scenario.commit, scenario.snapshots).snapshot.isDefined ==== validSnapshotEntities(scenario) })

    def datasets = prop((scenario: RepositoryScenario) => {
      val entities = Entities(scenario.entities.asJava)
      val plan = ChordPlan.inmemory(entities, scenario.commit, scenario.snapshots)
      plan.snapshot.isDefined ====  plan.datasets.summary.snapshot.isDefined })
  }

  def validSnapshotEntities(scenario: RepositoryScenario): Boolean =
    validSnapshotAt(scenario, Entities(scenario.entities.asJava).earliestDate)

}
