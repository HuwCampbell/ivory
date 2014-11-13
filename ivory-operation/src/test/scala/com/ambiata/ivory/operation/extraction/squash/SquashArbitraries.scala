package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.gen._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWindows
import org.scalacheck._
import scalaz._, Scalaz._

object SquashArbitraries {

  /** A snapshot date, and associated facts that are likely to be captured by a snapshot for a _single_ feature */
  case class SquashFacts(date: Date, dict: ConcreteGroupFeature, facts: NonEmptyList[Fact]) {

    lazy val factsSorted = facts.list.sortBy(fact => (fact.entity, fact.datetime.long))

    /** Return the squashed view of entities to their expected facts for each feature, given the window */
    def expected: Map[String, Map[FeatureId, (Option[Fact], List[Fact])]] =
      facts.list.groupBy(_.entity).mapValues { facts =>
        ((dict.fid -> None) :: dict.cg.virtual.map(vd => vd._1 -> vd._2.window)).toMap.mapValues { window =>
          facts.sortBy(_.datetime.long)
            // Either filter by window, or get everything
            .partition(ef => window.cata(w => !Window.isFactWithinWindow(Window.startingDate(w)(date), ef), false)) match {
            case (a, b) => (a.lastOption, b)
          }
        }
      }

    def expectedFactsWithCount: List[Fact] =
      for {
        (e, fs)                       <- expected.toList
        (fid, (oldFact, windowFacts)) <- fs.toList
        value                         <-
          if (fid == dict.fid) (oldFact ++ windowFacts).lastOption.filterNot(_.isTombstone).map(_.value).toList
          else                 LongValue(windowFacts.filterNot(_.isTombstone).size) :: Nil
      } yield Fact.newFact(e, fid.namespace.name, fid.name, date, Time(0), value)

    def removeNonWindowFeatures: SquashFacts =
      copy(dict = dict.copy(cg = dict.cg.copy(virtual = dict.cg.virtual.filter(_._2.window.isDefined))))
  }

  /** A snapshot date, and associated facts that are likely to be captured by a snapshot */
  case class SquashFactsMultiple(date: Date, facts: NonEmptyList[SquashFacts]) {
    lazy val allFacts: List[Fact] = facts.list.flatMap(_.facts.list)
    lazy val dict: Dictionary = facts.map(_.dict).foldLeft(Dictionary.empty) {
      // Set the expression for all features to count for simplicity, we test all the expression logic elsewhere
      case (d, vd) => d append vd.withExpression(Count).dictionary
    }
    lazy val hasVirtual = facts.list.exists(_.dict.dictionary.hasVirtual)
  }

  implicit def SquashFactMultipleArbitrary: Arbitrary[SquashFactsMultiple] = Arbitrary(for {
    d <- Arbitrary.arbitrary[Date]
    i <- Gen.choose(2, 5)
    l <- Gen.listOfN(i, squashFactsArbitraryFromDate(d))
      // https://github.com/ambiata/ivory/issues/441
      .map(_.map(_.removeNonWindowFeatures))
  } yield SquashFactsMultiple(d, NonEmptyList(l.head, l.tail: _*)))

  implicit def SquashFactsSingleArbitrary: Arbitrary[SquashFacts] =
    Arbitrary(Arbitrary.arbitrary[Date].flatMap(squashFactsArbitraryFromDate))

  def squashFactsArbitraryFromDate(date: Date): Gen[SquashFacts] = for {
    w <- Arbitrary.arbitrary[ConcreteGroupFeature].flatMap { w =>
      // Make sure we have _at least_ one virtual feature
      if (w.dictionary.hasVirtual) Gen.const(w)
      else GenDictionary.virtual(w.fid -> w.cg.definition, 0).map(virt => w.copy(cg = w.cg.copy(virtual = virt :: w.cg.virtual)))
    }.map {
      // My kingdom for a lens :(
      // Disable filtering in squash tests, handled in FilterReductionSpec and window cli test
      cgf => cgf.copy(cg = cgf.cg.copy(virtual = cgf.cg.virtual.map {
        case (fid, vd) => fid -> vd.copy(query = vd.query.copy(filter = None))
      }))
    }
    // We should only ever see 1 fact when no window is defined
    i <- startingDate(w, date).cata(_ => Gen.choose(2, 10), Gen.const(1))
    f <- Gen.listOfN(i, Arbitrary.arbitrary[Fact].flatMap {
      // NOTE: We're not testing snapshot logic here, so there's no point generating facts after the snapshot date
      fact => Gen.choose(0, 100).map(o => fact.withDate(Date.fromLocalDate(date.localDate.minusDays(o))))
    })
    fs = f.map(_.withFeatureId(w.fid))
    fa = w.cg.definition.mode.fold (
      // For state it's impossible to have a duplicate entity + datetime (the result is non-deterministic)
      fs.groupBy1(f => f.entity -> f.datetime).values.map(_.head).toList,
      fs
    )
  } yield SquashFacts(date, w, NonEmptyList(fa.head, fa.tail: _*))


  def startingDate(vd: ConcreteGroupFeature, date: Date): Option[Date] =
    vd.cg.virtual.flatMap(_._2.window.map(w => Window.startingDate(w)(date))).sorted.headOption
}
