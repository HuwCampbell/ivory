package com.ambiata.ivory.operation.extraction.chord

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWindows
import org.scalacheck._

object ChordArbitraries {

  def incValue(f: Fact, post: String): Fact =
    f.withValue(StringValue(Value.toStringWithStruct(f.value, "") + post))

  /** Represents a single entity with a number of dates, and for each date a number of possible facts */
  case class ChordEntity(entity: String, dates: List[(Date, List[Date])], above: List[Date]) {
    lazy val dateArray: Array[Int] =
      dates.map(_._1.int).sorted.reverse.toArray
    /** Creates facts but ensures each have a unique value to confirm priority actually works */
    def toFacts[A](fact: Fact)(t: (Date, List[Fact], List[Fact]) => List[A]): List[A] = {
      // Accumulate over the facts and keep track of everything up until this point (see expected())
      dates.sortBy(_._1).foldLeft((List[Fact](), List[A]())){ case ((prev, as), dd) =>
        val now = dd._2.sorted.map(fact.withDate).zipWithIndex.map {
          case (f, i) => f.withValue(StringValue(List(entity, dd._1.hyphenated, i).mkString(" ")))
        }
        (prev ++ now) -> (as ++ t(dd._1, now, prev))
      }._2
    }

    def facts(fact: Fact): List[Fact] =
      toFacts(fact)((_, fs, _) => fs).map(_.withEntity(entity))
    def expected(fact: Fact): List[Fact] =
      // Take the latest fact, which may come from a previous chord period if this one is empty
      toFacts(fact)((d, fs, prev) => (prev ++ fs).lastOption.map(_.withEntity(entity + ":" + d.hyphenated)).toList)
    def expectedWindow(fact: Fact, window: Option[Window]): List[Fact] =
      toFacts(fact) {(d, fs, prev) =>
        val inWindow = window.map(SnapshotWindows.startingDate(_, d)).toList.flatMap {
          sd => fs.filter(_.date.int >= sd.int)
        }
        (if (inWindow.isEmpty) (prev ++ fs).lastOption.toList else inWindow).map(_.withEntity(entity))
      }.distinct
  }

  /** A single entity, for testing [[com.ambiata.ivory.operation.extraction.ChordReducer]] */
  case class ChordFact(ce: ChordEntity, fact: Fact, window: Option[Window]) {
    lazy val facts: List[Fact] = ce.facts(fact)
    // Duplicate every fact with priority version but different value
    lazy val factsWithPriority: List[Fact] = facts.zip(facts.map(incValue(_, "p"))).flatMap(f => List(f._1, f._2))
    lazy val expected: List[Fact] = ce.expected(fact)
    lazy val expectedWindow: List[Fact] = ce.expectedWindow(fact, window)
    lazy val windowDateArray: Option[Array[Int]] = window.map {
      win => ce.dates.map(_._1).map(SnapshotWindows.startingDate(win, _).int).sorted.reverse.toArray
    }
  }

  case class ChordFacts(ces: List[ChordEntity], fid: FeatureId, factAndMeta: SparseEntities, other: List[Fact]) {
    lazy val facts: List[Fact] = (ces.flatMap(_.facts(factAndMeta.fact)) ++ above ++ other).map(_.withFeatureId(fid))
    lazy val above: List[Fact] = ces.flatMap(_.above.map(factAndMeta.fact.withDate))
    lazy val expected: List[Fact] = ces.flatMap(_.expected(factAndMeta.fact)).map(_.withFeatureId(fid))
    lazy val dictionary: Dictionary = Dictionary(List(factAndMeta.meta.toDefinition(fid)))
    // If the oldest chord has no facts then don't capture a snapshot (it will error at the moment)
    // https://github.com/ambiata/ivory/issues/343
    lazy val takeSnapshot: Boolean = ces.sortBy(_.dates.headOption.map(_._1).getOrElse(Date.maxValue)).headOption
      .flatMap(_.dates.headOption).flatMap(_._2.headOption).isDefined
  }
 
  implicit def ChordFactArbitrary: Arbitrary[ChordFact] = Arbitrary(for {
    e     <- Arbitrary.arbitrary[ChordEntity]
    // Just generate one stub fact - we only care about the entity and date
    fact  <- Arbitrary.arbitrary[Fact]
    win   <- Arbitrary.arbitrary[Option[Window]]
  } yield ChordFact(e, fact, win))

  implicit def ChordFactsArbitrary: Arbitrary[ChordFacts] = Arbitrary(for {
    n     <- Gen.choose(1, 20)
    dates <- Gen.listOfN(n, Arbitrary.arbitrary[ChordEntity])
    // Just generate one stub fact - we only care about the entity and date
    fact  <- Arbitrary.arbitrary[SparseEntities]
    fid   <- Arbitrary.arbitrary[FeatureId]
    // Generate other facts that aren't related in any way - they should be ignored
    o     <- Gen.choose(1, 3).flatMap(i => Gen.listOfN(i, Arbitrary.arbitrary[Fact]))
  } yield ChordFacts(dates, fid, fact, o))

  implicit def ChordEntityArbitrary: Arbitrary[ChordEntity] = Arbitrary(for {
    e     <- Gen.identifier
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
  } yield ChordEntity(e, dates, above))
}
