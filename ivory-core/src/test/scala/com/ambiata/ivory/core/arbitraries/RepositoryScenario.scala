package com.ambiata.ivory.core.arbitraries
/*
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen.GenDate
import Arbitraries._

import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._, scalacheck.ScalaCheckBinding._

/**
 * Represents a somewhat realistic set of repository metadata.
 *
 * Effectively you end up with a feature store that has been built up over
 * a series of days from 'start' to 'end'. At each day there may have been
 * new data ingested and/or a snapshot taken as at the current date.
 */
case class RepositoryScenario(commit: Commit, snapshots: List[Snapshot], epoch: Date, at: Date) {
  def metadata: List[SnapshotMetadata] =
    snapshots.map(snapshot => SnapshotMetadata(snapshot.id, snapshot.date, snapshot.store.id, snapshot.dictionary.map(_._1)))

  def partitions: List[Partition] =
    commit.store.factsets.flatMap(_.value.partitions).sorted

  override def toString = {
    def partitions(ps: List[Partition]): String =
      Partition.intervals(ps).groupBy(_._1.namespace).toList.map({
        case (namespace, intervals) =>
          val dates = intervals.map({ case (min, max) => s"      ${min.date.hyphenated}->${max.date.hyphenated}" }).mkString("\n")
          s"    ${namespace}\n${dates}"
      }).mkString("\n")

    def renderFactset(factset: Prioritized[Factset]) =
      s"""Factset(${factset.value.id.render}) @ ${factset.priority}
         |${partitions(factset.value.partitions)}
         |""".stripMargin

    def renderSnapshot(snapshot: Snapshot) =
      s"""Snapshot(${snapshot.id.render}) @ FeatureStore(${snapshot.store.id.render}) / ${snapshot.date.hyphenated}
         |  ${snapshot.store.factsets.sorted.map(renderFactset).mkString("\n  ")}
         |""".stripMargin

    s"""Repository Scenario ============================================================
       |
       |Repository Epoch:                 ${epoch.hyphenated}
       |Suggested Snapshot 'at' Date:     ${at.hyphenated}
       |
       |FeatureStore(${commit.store.id.render})
       |  ${commit.store.factsets.sorted.map(renderFactset).mkString("\n  ")}
       |
       |${snapshots.map(renderSnapshot)}
       |""".stripMargin
  }
}

object RepositoryScenario {
  implicit def RepositoryScenarioArbitrary: Arbitrary[RepositoryScenario] = Arbitrary(for {
      // We pick some date to start the repository at
      epoch   <- GenDate.dateIn(Date(1900, 1, 1), Date(2100, 12, 31))

      // We will be generating 'span' days with of events (ingestions and/or snapshots).
      span    <- Gen.choose(2, 10)

      // Constant dictionary for now, this could be improved to vary the dictionary by small
      // increments over time
      dictionary <- arbitrary[Dictionary]
      dictionaryId <- arbitrary[DictionaryId]

      // The repository will contain 'n' different namespaces.
      n       <- Gen.choose(2, 4)
      names   <- Gen.listOfN(n, arbitrary[Name])

      // Work out if we want the snapshot 'at' date to be in the future, past, or mid-repository
      delta   <- Gen.choose(1, span)
      at      <- Gen.frequency(
          2 -> takeDays(epoch, delta)
        , 6 -> addDays(epoch, delta)
        , 2 -> addDays(epoch, span + delta)
        )

      // We start with an empty repository and will iterate each day until we end up with a complete view
      init    =  RepositoryScenario(FeatureStore(FeatureStoreId.initial, Nil), Nil, epoch, at)

      // For each day, take the current feature store, and then:
      //  - calculate a new factset for 'ingestion' using our determined namespaces
      //  - potentiolly generate a snapshot for today (1 out of 2 chance)
      //  - increment the state of the current scenario to track the new snapshot and head of the feature store
      r       <- (1 to span).toList.foldLeftM(init)((acc, day) => for {
        chance    <- Gen.choose(1, 10).map(_ < 3)
        earliest  <- Gen.choose(1, day * span * 5)
        latest    <- Gen.choose(1, day * span * 5)
        // Tend towards single day chunks (helps with debugging and is closer to reality, throw in larger chunk every once in a while)
        chunk     <- Gen.frequency(99 -> Gen.const(1), 1 -> Gen.choose(1, 3))
        today     =  addDays(acc.epoch, day)
        store     =  nextStore(acc.store, names, today, earliest, latest, chunk)
        snapshots =  nextSnapshots(store, acc.snapshots, today, dictionaryId -> dictionary, chance)
        } yield RepositoryScenario(store, snapshots, acc.epoch, acc.at))
      } yield r)

  def nextSnapshots(store: FeatureStore, snapshots: List[Snapshot], today: Date, dictionary: (DictionaryId, Dictionary), create: Boolean) = {
    val current = if (snapshots.isEmpty) SnapshotId.initial.some else snapshots.map(_.id).maximum
    val next = current.flatMap(_.next)
    snapshots ++ next.map(id => Snapshot(id, today, store, dictionary.some)).filter(_ => create).toList
  }

  def nextStore(store: FeatureStore, names: List[Name], today: Date, earliest: Int, latest: Int, chunk: Int): FeatureStore = (for {
    id        <- store.id.next
    priority  <- if (store.factsets.isEmpty) Priority.Min.some else store.factsets.map(_.priority).maximum.flatMap(_.next)
    factsetId <- if (store.factsets.isEmpty) FactsetId.initial.some else store.factsets.map(_.value.id).maximum.flatMap(_.next)
    factset   =  Factset(factsetId, for {
                   name <- names
                   date <- (1 to earliest by chunk).toList.map(takeDays(today, _)) ++ List(today) ++ (1 to latest by chunk).toList.map(addDays(today, _))
    } yield Partition(name, date))
    nu        =  List(Prioritized(priority, factset))
  } yield FeatureStore(id, store.factsets ++ nu)).getOrElse(store)

  def addDays(date: Date, days: Int): Date =
    Date.fromLocalDate(date.localDate.plusDays(days))

  def takeDays(date: Date, days: Int): Date =
    Date.fromLocalDate(date.localDate.minusDays(days))

}
*/
