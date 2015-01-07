package com.ambiata.ivory.core.arbitraries

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
case class RepositoryScenario(commits: NonEmptyList[Commit], snapshots: List[Snapshot], epoch: Date, at: Date, entities: Map[String, Array[Int]]) {
  def commit: Commit =
    commits.head

  def metadata: List[SnapshotMetadata] =
    snapshots.map(_.toMetadata)

  def partitions: List[Partition] =
    commit.store.factsets.flatMap(_.value.partitions.map(_.value)).sorted

  override def toString = {
    def partitions(ps: List[Partition]): String =
      Partition.intervals(ps).groupBy(_._1.namespace).toList.map({
        case (namespace, intervals) =>
          val dates = intervals.map({ case (min, max) => s"      ${min.date.hyphenated}->${max.date.hyphenated}" }).mkString("\n")
          s"    ${namespace}\n${dates}"
      }).mkString("\n")

    def renderFactset(factset: Prioritized[Factset]) =
      s"""Factset(${factset.value.id.render}) @ ${factset.priority}
         |${partitions(factset.value.partitions.map(_.value))}
         |""".stripMargin

    def renderSnapshot(snapshot: Snapshot) =
      s"""Snapshot(${snapshot.id.render}) @ FeatureStore(${snapshot.store.id.render}) / ${snapshot.date.hyphenated}
         |  ${snapshot.store.factsets.sorted.map(renderFactset).mkString("\n  ")}
         |""".stripMargin

    def renderSnapshotNoStore(snapshot: Snapshot) =
      s"""Snapshot(${snapshot.id.render}) @ FeatureStore(${snapshot.store.id.render}) / ${snapshot.date.hyphenated}
         |""".stripMargin

    s"""Repository Scenario ============================================================
       |
       |Repository Epoch:                 ${epoch.hyphenated}
       |Suggested Snapshot 'at' Date:     ${at.hyphenated}
       |
       |FeatureStore(${commit.store.id.render})
       |  ${commit.store.factsets.sorted.map(renderFactset).mkString("\n  ")}
       |
       |Snapshots:
       |  ${snapshots.map(renderSnapshotNoStore).mkString("\n  ")}
       |
       |""".stripMargin
  }
}

object RepositoryScenario {
  implicit def RepositoryScenarioArbitrary: Arbitrary[RepositoryScenario] = Arbitrary(for {
      // We pick some date to start the repository at
      epoch   <- GenDate.dateIn(Date(1900, 1, 1), Date(2100, 12, 31))

      // We will be generating 'span' days worth of events (ingestions and/or snapshots).
      span    <- Gen.choose(2, 10)

      // Constant dictionary for now, this could be improved to vary the dictionary by small
      // increments over time
      dictionary <- arbitrary[Identified[DictionaryId, Dictionary]]

      // The repository will contain 'n' different namespaces.
      n       <- Gen.choose(2, 4)
      names   <- Gen.listOfN(n, arbitrary[Namespace])

      // Work out if we want the snapshot 'at' date to be in the future, past, or mid-repository
      delta   <- Gen.choose(1, span)
      at      <- Gen.frequency(
          2 -> takeDays(epoch, delta)
        , 6 -> addDays(epoch, delta)
        , 2 -> addDays(epoch, span + delta)
        )

      // Pick an end dates and build an entities file.
      entitiesEnd  <- Gen.frequency(
          1 -> Gen.const(at)
        , 4 -> addDays(at, delta)
        )

      entities = Map(
          "start" -> Array(at.int)
        , "end" -> Array(entitiesEnd.int)
        )

      // We start with an empty repository and will iterate each day until we end up with a complete view
      first   =  Commit(CommitId.initial, dictionary, FeatureStore(FeatureStoreId.initial, Nil), Identified(RepositoryConfigId.initial, RepositoryConfig.testing).some)
      init    =  RepositoryScenario(NonEmptyList(first), Nil, epoch, at, entities)

      // For each day, take the current feature store, and then:
      //  - calculate a new factset for 'ingestion' using our determined namespaces
      //  - potentiolly generate a snapshot for today (1 out of 2 chance)
      //  - increment the state of the current scenario to track the new snapshot and head of the feature store
      r       <- (1 to span).toList.foldLeftM(init)((acc, day) => for {
        chance    <- Gen.choose(1, 10).map(_ < 3)
        earliest  <- Gen.choose(1, day * span * 5)
        latest    <- Gen.choose(1, day * span * 5)
        bytes     <- arbitrary[Bytes]
        format    <- arbitrary[FactsetFormat]
        today     =  addDays(acc.epoch, day)
        // just do 1 day chunks, makes debugging significantly easier, and doesn't really help
        // coverage to do wider spans.
        store     =  nextStore(acc.commit.store, names, today, earliest, latest, format, bytes, 1)
        snapshots =  nextSnapshots(store, acc.snapshots, today, dictionary, chance, bytes)
        commit    = Commit(acc.commit.id.next.get, dictionary, store, acc.commit.config)
        } yield RepositoryScenario(commit <:: acc.commits, snapshots, acc.epoch, acc.at, acc.entities))
      } yield r)

  def nextSnapshots(store: FeatureStore, snapshots: List[Snapshot], today: Date, dictionary: Identified[DictionaryId, Dictionary], create: Boolean, bytes: Bytes) = {
    val current = if (snapshots.isEmpty) SnapshotId.initial.some else snapshots.map(_.id).maximum
    val next = current.flatMap(_.next)
    snapshots ++ next.map(id => Snapshot(id, today, store, dictionary.some, bytes)).filter(_ => create).toList
  }

  def nextStore(store: FeatureStore, names: List[Namespace], today: Date, earliest: Int, latest: Int, format: FactsetFormat, bytes: Bytes, chunk: Int): FeatureStore = (for {
    id        <- store.id.next
    priority  <- if (store.factsets.isEmpty) Priority.Min.some else store.factsets.map(_.priority).maximum.flatMap(_.next)
    factsetId <- if (store.factsets.isEmpty) FactsetId.initial.some else store.factsets.map(_.value.id).maximum.flatMap(_.next)
    factset   =  Factset(factsetId, format, for {
                   name <- names
                   date <- (1 to earliest by chunk).toList.map(takeDays(today, _)) ++ List(today) ++ (1 to latest by chunk).toList.map(addDays(today, _))
    } yield Sized(Partition(name, date), bytes))
    nu        =  List(Prioritized(priority, factset))
  } yield FeatureStore(id, store.factsets ++ nu)).getOrElse(store)

  def addDays(date: Date, days: Int): Date =
    Date.fromLocalDate(date.localDate.plusDays(days))

  def takeDays(date: Date, days: Int): Date =
    Date.fromLocalDate(date.localDate.minusDays(days))

}
