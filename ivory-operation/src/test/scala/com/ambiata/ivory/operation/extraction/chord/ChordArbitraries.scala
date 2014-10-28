package com.ambiata.ivory.operation.extraction.chord

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.ChordEntities
import com.ambiata.ivory.core.arbitraries._, ArbitraryFacts._, ArbitraryValues._, ArbitraryFeatures._
import org.scalacheck._
import scala.collection.JavaConverters._

object ChordArbitraries {

  def incValue(f: Fact, post: String): Fact =
    f.withValue(StringValue(Value.toStringWithStruct(f.value, "") + post))

  /** Represents a single entity with a number of dates, and for each date a number of possible facts */
  case class ChordEntity(entity: String, dates: List[(Date, List[Date])], above: List[Date]) {
    lazy val dateArray: Array[Int] =
      dates.map(_._1.int).sorted.reverse.toArray

    def fromFact(fact: Fact): List[(Date, List[Fact])] =
      dates.sortBy(_._1).map({ case (date, dates) => date ->
        dates.sorted.map(fact.withDate).zipWithIndex.map({
          case (fact, index) =>
            fact.withValue(StringValue(List(entity, date.hyphenated, index).mkString(" ")))
        })
      })

    lazy val isEmpty: Boolean = dates.forall(_._2.isEmpty)

    /** Creates facts but ensures each have a unique value to confirm priority actually works */
    def fromFactWithPriority(fact: Fact): List[(Date, List[Fact])] =
      fromFact(fact).map({ case (date, facts) =>
        date -> facts.zip(facts.map(incValue(_, "p"))).flatMap(f => List(f._1, f._2)) })

    def fromFactWithMode(fact: Fact, mode: Mode): List[(Date, List[Fact])] = {
      mode match {
        case Mode.State => fromFact(fact)
        case Mode.Set   => fromFactWithPriority(fact)
      }
    }

    def toFacts[A](fact: Fact, mode: Mode)(t: (Date, List[Fact], List[Fact]) => List[A]): List[A] =
      fromFactWithMode(fact, mode).foldLeft(List[Fact]() -> List[A]())({ case ((prev, as), (date, fact)) =>
        (prev ++ fact) -> (as ++ t(date, fact, prev))
      })._2

    def facts(fact: Fact, mode: Mode): List[Fact] =
      toFacts(fact, mode)((_, fs, _) => fs).map(_.withEntity(entity))

    def expected(fact: Fact, mode: Mode): List[Fact] =
      // Take the latest fact, which may come from a previous chord period if this one is empty
      toFacts(fact, mode)(expecteds)

    def expecteds: (Date, List[Fact], List[Fact]) => List[Fact] =
      (d, fs, prev) => (prev ++ fs).lastOption.map(_.withEntity(entity + ":" + d.hyphenated)).toList

    def expectedWindow(fact: Fact, mode: Mode, window: Option[Window]): List[Fact] =
      toFacts(fact, mode)(expectedWindows(window)).distinct

    def expectedWindows: Option[Window] => (Date, List[Fact], List[Fact]) => List[Fact] =
      window => (d, fs, prev) =>
        window.map(Window.startingDate(_)(d)).map {
          sd =>
            // _Always_ emit the last fact before the window (for state-based features)
            (prev ++ fs).filter(_.date.int < sd.int).lastOption.toList ++
              // All the facts from the window
              fs.filter(_.date.int >= sd.int)
        }.getOrElse((prev ++ fs).lastOption.toList).map(_.withEntity(entity))
  }

  /** A single entity, for testing [[com.ambiata.ivory.operation.extraction.ChordReducer]] */
  case class ChordFact(ce: ChordEntity, fact: Fact, window: Option[Window]) {
    lazy val facts: List[Fact] = ce.facts(fact, Mode.State)
    lazy val factsWithPriority: List[Fact] = ce.facts(fact, Mode.Set)
    lazy val expected: List[Fact] = ce.expected(fact, Mode.State)
    lazy val expectedSet: List[Fact] = ce.expected(fact, Mode.Set)
    lazy val expectedWindow: List[Fact] = ce.expectedWindow(fact, Mode.State, window)
    lazy val expectedWindowSet: List[Fact] = ce.expectedWindow(fact, Mode.Set, window)
    lazy val windowDateArray: Option[Array[Int]] = window.map {
      win => ce.dates.map(_._1).map(Window.startingDate(win)(_).int).sorted.reverse.toArray
    }
  }

  case class ChordFacts(ces: List[ChordEntity], fid: FeatureId, factAndMeta: SparseEntities, window: Option[Window],
                        other: List[Fact]) {
    lazy val facts: List[Fact] = ces.flatMap(_.facts(factAndMeta.fact, Mode.State))
    lazy val allFacts: List[List[Fact]] = {
      List(
        // The first factset, with the exact same commits but with a different value
        // Depending on the mode, different facts will be expected
        facts.map(incValue(_, "p")),
        facts ++ above ++ other.map(_.withFeatureId(factAndMeta.fact.featureId))
      )
    }
    lazy val above: List[Fact] = ces.flatMap(_.above.map(factAndMeta.fact.withDate))
    lazy val expected: List[Fact] = ces.flatMap(_.expected(factAndMeta.fact, factAndMeta.meta.mode))
    lazy val expectedWindow: List[Fact] = ces.flatMap(_.expectedWindow(factAndMeta.fact, factAndMeta.meta.mode, window))
    lazy val dictionary: Dictionary = Dictionary(List(
      factAndMeta.meta.toDefinition(factAndMeta.fact.featureId),
      VirtualDefinition(factAndMeta.fact.featureId, Query(Count, None), window).toDefinition(fid)
    ))
    // If the oldest chord has no facts then don't capture a snapshot (it will error at the moment)
    // https://github.com/ambiata/ivory/issues/343
    lazy val takeSnapshot: Boolean = factAndMeta.meta.mode match {
      // Currently the priority/window in snapshots/sets is slightly off - we keep more than we need
      // Disabling snapshots here is easier than complicating the "expected" code with that logic
      // https://github.com/ambiata/ivory/issues/376
      case Mode.Set => false
      case Mode.State => ces.sortBy(_.dates.headOption.map(_._1).getOrElse(Date.maxValue)).headOption
        .flatMap(_.dates.headOption).flatMap(_._2.headOption).isDefined
    }
    lazy val expectedSquash: List[Fact] = ces.sortBy(_.entity).flatMap {
      ce => ce.toFacts(factAndMeta.fact, Mode.State) { (d, fs, prev) =>
        val sd = window.map(Window.startingDate(_)(d)).getOrElse(Date.minValue)
        val pfs = prev ++ fs
        val last = pfs.lastOption
        val inWindow = pfs.filter(_.date.int >= sd.int)
        (last.map { f =>
          val cfid = factAndMeta.fact.featureId
          Fact.newFact("", cfid.namespace.name, cfid.name, d, Time(0), f.value)
        }.toList ++
          List(Fact.newFact("", fid.namespace.name, fid.name, d, Time(0), LongValue(inWindow.size)))
        ).map(_.withEntity(ce.entity + ":" + d.hyphenated))
      }
    }
    lazy val chord = Entities.fromChordEntities(new ChordEntities(ces.map(ce => ce.entity -> ce.dateArray).toMap.asJava))
  }

  implicit def ChordFactArbitrary: Arbitrary[ChordFact] = Arbitrary(for {
    e     <- chordEntityGen(0)
    // Just generate one stub fact - we only care about the entity and date
    fact  <- Arbitrary.arbitrary[Fact]
    win   <- Arbitrary.arbitrary[Option[Window]]
  } yield ChordFact(e, fact, win))

  implicit def ChordFactsArbitrary: Arbitrary[ChordFacts] = Arbitrary(for {
    n     <- Gen.choose(1, 20)
    dates <- Gen.sequence[List, ChordEntity]((0 until n).map(chordEntityGen)).map(_.filter(!_.isEmpty))
    // Just generate one stub fact - we only care about the entity and date
    fact  <- Arbitrary.arbitrary[SparseEntities]
    fid   <- Arbitrary.arbitrary[FeatureId]
    win   <- Arbitrary.arbitrary[Option[Window]]
    // Generate other facts that aren't related in any way - they should be ignored
    o     <- Gen.choose(1, 3).flatMap(i => Gen.listOfN(i, Arbitrary.arbitrary[Fact]))
  } yield ChordFacts(dates, fid, fact, win, o))

  /** Use an increasing number to represent the entity to avoid name clashes */
  def chordEntityGen(e: Int): Gen[ChordEntity] = for {
    m     <- Gen.choose(2, 5)
    dates <- Gen.listOfN(m, Arbitrary.arbitrary[Date]).flatMap {
      ds => Gen.sequence[List, (Date, List[Date])](ds.sorted.sliding(2).filter(_.length == 2).zipWithIndex.map {
        case (List(d1, d2), i) => for {
          // NOTE: We're possibly generating an empty set of fact dates for _all_ chord dates,
          // which is technically impossible but is gracefully handled in ChordJob.reduce to make this simpler
          i  <- Gen.choose(0, 3)
          df <- Gen.listOfN(i, genDate(Date.fromLocalDate(d1.localDate.plusDays(1)), d2))
        } yield d2 -> df.distinct
      }.toList)
    }
    // Generate a few dates above the highest chord date, only needed by ChordSpec but easier to do here
    last   = dates.map(_._1).last
    above <- Gen.choose(1, 3).flatMap(i => Gen.listOfN(i, genDate(Date.fromLocalDate(last.localDate.plusDays(1)), Date.maxValue)))
  } yield ChordEntity(e.toString, dates, above)
}
