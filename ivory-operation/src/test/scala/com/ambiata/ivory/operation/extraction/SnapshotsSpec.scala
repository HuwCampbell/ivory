package com.ambiata.ivory.operation.extraction

import org.specs2._, execute.{Failure => SpecsFailure}
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._, arbitraries._, Arbitraries._

import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDate

class SnapshotsSpec extends Specification with SampleFacts with ScalaCheck { def is = s2"""

  A snapshot of the features can be extracted as a sequence file $snapshot         ${tag("mr")}
  A snapshot of the features can be extracted over a window      $windowing        ${tag("mr")}
  A snapshot of the set features can be extracted over a window  $sets             ${tag("mr")}

"""

  def snapshot = {
    RepositoryBuilder.using { repo => for {
      _ <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
      s <- Snapshots.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now))
      f  = valueFromSequenceFile[Fact](repo.toIvoryLocation(Repository.snapshot(s.id)).toHdfs).run(repo.scoobiConfiguration)
    } yield f.map(_.featureId).toSet} must beOkValue(
      // FIX: Capture "simple" snapshot logic which handles priority and set/state so we can check the counts
      sampleFacts.flatten.map(_.featureId).toSet
    )
  }

  def windowing = propNoShrink((vdict: VirtualDictionaryWindow, fact: Fact) => {
    val facts = List(
      fact,
      fact.withTime(Time.unsafe(fact.time.seconds + 1)),
      fact.withDate(Date.fromLocalDate(Window.startingDate(vdict.window, fact.date).localDate.plusDays(1)))
    ).map(_.withFeatureId(vdict.vdict.vd.source))

    val deprioritized = List(
      fact.withValue(StringValue("override-by-priority")),
      fact.withTime(Time.unsafe(fact.time.seconds + 1))
          .withValue(StringValue("override-by-priority"))
    )

    val oldfacts = List(
      fact.withDate(Window.startingDate(vdict.window, fact.date))
    ).map(_.withFeatureId(vdict.vdict.vd.source))

    RepositoryBuilder.using { repo => for {
        _ <- RepositoryBuilder.createRepo(repo, vdict.vd.dictionary, List(deprioritized, facts ++ oldfacts))
        s <- Snapshots.takeSnapshot(repo, fact.date)
        f  = valueFromSequenceFile[Fact](repo.toIvoryLocation(Repository.snapshot(s.id)).toHdfs).run(repo.scoobiConfiguration)
      } yield f
    }.map(_.toSet) must beOkValue((oldfacts.sortBy(_.date).lastOption.toList ++ facts).toSet)
  }).set(minTestsOk = 3)

  def sets = propNoShrink((concrete: FeatureId, virtual: FeatureId, window: Window, date: Date, time: Time, entity: Int) => {
    val dictionary = Dictionary(List(
      Definition.concrete(concrete, IntEncoding, Mode.Set, None, concrete.toString, Nil)
    , Definition.virtual(virtual, concrete, Query(Count, None), Some(window))
    ))

    def fact(d: Date, t: Time, v: Int): Fact =
      Fact.newFact(s"E${entity}", concrete.namespace.name, concrete.name, d, t, IntValue(v))

    val firstDayOfWindow = Date.fromLocalDate(Window.startingDate(window, date).localDate.plusDays(1))
    val facts = List(
        fact(date, time, 1)
      , fact(date, time, 2)
      , fact(firstDayOfWindow, time, 3)
      , fact(firstDayOfWindow, time, 4)
      )

    // Currently we're keeping the last fact outside the window, which is incorrect
    // https://github.com/ambiata/ivory/issues/376
    val outer = List(
        fact(Window.startingDate(window, date), Time(0), 5)
      , fact(Window.startingDate(window, date), Time(1), 6)
      )

    RepositoryBuilder.using { repo => for {
        _ <- RepositoryBuilder.createRepo(repo, dictionary, List(facts ++ outer))
        s <- Snapshots.takeSnapshot(repo, date)
        f  = valueFromSequenceFile[Fact](repo.toIvoryLocation(Repository.snapshot(s.id)).toHdfs).run(repo.scoobiConfiguration)
      } yield f
    }.map(_.toSet) must beOkValue((outer.sortBy(f => f.datetime.long).lastOption.toList ++ facts).toSet)
  }).set(minTestsOk = 3)
}
