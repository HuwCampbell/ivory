package com.ambiata.ivory.operation.extraction

import org.specs2._, execute.{Failure => SpecsFailure}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._, arbitraries._, Arbitraries._

import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDate

import scalaz._, Scalaz._

class SnapshotsSpec extends Specification with SampleFacts with ScalaCheck { def is = s2"""

  A snapshot of the features can be extracted as a sequence file $snapshot         ${tag("mr")}
  A snapshot of the features can be extracted over a window      $windowing        ${tag("mr")}
  A snapshot builds incrementally on the last if it exists       $incremental      ${tag("mr")}
  A snapshot of the set features can be extracted over a window  $sets             ${tag("mr")}

"""

  def snapshot =
    RepositoryBuilder.using { repo => for {
      _ <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
      _ <- Snapshots.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now))
    } yield ()} must beOk

  def windowing = propNoShrink((vdict: VirtualDictionaryWindow, fact: Fact) => {
    val facts = List(
      fact,
      fact.withTime(Time.unsafe(fact.time.seconds + 1)),
      fact.withDate(Date.fromLocalDate(Window.startingDate(vdict.window)(fact.date).localDate.plusDays(1)))
    ).map(_.withFeatureId(vdict.vdict.vd.source))

    val deprioritized = List(
      fact.withValue(StringValue("override-by-priority")),
      fact.withTime(Time.unsafe(fact.time.seconds + 1))
          .withValue(StringValue("override-by-priority"))
    )

    val oldfacts = List(
      fact.withDate(Window.startingDate(vdict.window)(fact.date))
    ).map(_.withFeatureId(vdict.vdict.vd.source))

    RepositoryBuilder.using { repo => for {
        _ <- RepositoryBuilder.createRepo(repo, vdict.vd.dictionary, List(deprioritized, facts ++ oldfacts))
        s <- Snapshots.takeSnapshot(repo, fact.date)
        f  = valueFromSequenceFile[Fact](repo.toIvoryLocation(Repository.snapshot(s.meta.snapshotId)).toHdfs).run(repo.scoobiConfiguration)
      } yield f
    }.map(_.toSet) must beOkValue((oldfacts.sortBy(_.date).lastOption.toList ++ facts).toSet)
  }).set(minTestsOk = 3)

  def incremental = prop(
    (dictionary: Dictionary, featureId: FeatureId, fact: Fact) => {
      val feature = dictionary.definitions.headOption.cata(_.featureId, featureId)
      val facts = fact.withFeatureId(feature).pure[List]
      val facts2 = facts.map(_.withDate(Date.fromLocalDate(fact.date.localDate.plusDays(1))))
      RepositoryBuilder.using((repo: HdfsRepository) =>
        for {
          _ <- RepositoryBuilder.createRepo(repo, dictionary, facts.pure[List])
          res <- Snapshots.takeSnapshot(repo, fact.date)
          _ <- RepositoryBuilder.createFactset(repo, facts2)
          res2 <- Snapshots.takeSnapshot(repo, fact.date)
        } yield (res, res2.incremental.map(_.snapshotId))) must beOkLike(
          (t: (SnapshotJobSummary[SnapshotManifest], Option[SnapshotId])) =>
            t._2.cata((sid: SnapshotId) => sid must_== t._1.meta.snapshotId, SpecsFailure("No incremental was used in the second snapshot")))
    }).set(minTestsOk = 3)

  def sets = propNoShrink((concrete: FeatureId, virtual: FeatureId, window: Window, date: Date, time: Time, entity: Int) => {
    val dictionary = Dictionary(List(
      Definition.concrete(concrete, IntEncoding, Mode.Set, None, concrete.toString, Nil)
    , Definition.virtual(virtual, concrete, Query(Count, None), Some(window))
    ))

    def fact(d: Date, t: Time, v: Int): Fact =
      Fact.newFact(s"E${entity}", concrete.namespace.name, concrete.name, d, t, IntValue(v))

    val firstDayOfWindow = Date.fromLocalDate(Window.startingDate(window)(date).localDate.plusDays(1))
    val facts = List(
        fact(date, time, 1)
      , fact(date, time, 2)
      , fact(firstDayOfWindow, time, 3)
      , fact(firstDayOfWindow, time, 4)
      )

    // Currently we're keeping the last fact outside the window, which is incorrect
    // https://github.com/ambiata/ivory/issues/376
    val outer = List(
        fact(Window.startingDate(window)(date), Time(0), 5)
      , fact(Window.startingDate(window)(date), Time(1), 6)
      )

    RepositoryBuilder.using { repo => for {
        _ <- RepositoryBuilder.createRepo(repo, dictionary, List(facts ++ outer))
        s <- Snapshots.takeSnapshot(repo, date)
        f  = valueFromSequenceFile[Fact](repo.toIvoryLocation(Repository.snapshot(s.meta.snapshotId)).toHdfs).run(repo.scoobiConfiguration)
      } yield f
    }.map(_.toSet) must beOkValue((outer.sortBy(f => f.datetime.long).lastOption.toList ++ facts).toSet)
  }).set(minTestsOk = 3)
}
