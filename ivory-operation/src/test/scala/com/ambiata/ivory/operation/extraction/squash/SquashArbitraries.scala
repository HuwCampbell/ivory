package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWindows
import org.scalacheck._
import scalaz._, Scalaz._

object SquashArbitraries {

  /** A snapshot date, and associated facts that are likely to be captured by a snapshot for a _single_ feature */
  case class SquashFacts(date: Date, dict: ConcreteGroupFeature, facts: NonEmptyList[Fact]) {

    /** Return the squashed view of entities to their expected facts for each feature, given the window */
    def expected: Map[String, Map[FeatureId, (Option[Fact], List[Fact])]] =
      facts.list.groupBy(_.entity).mapValues { facts =>
        ((dict.fid -> None) :: dict.cg.virtual.map(vd => vd._1 -> vd._2.window)).toMap.mapValues { window =>
          facts.sortBy(_.datetime.long)
            // Either filter by window, or get everything
            .partition(ef => window.cata(SnapshotWindows.startingDate(_, date) > ef.date, false)) match {
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
  }

  /** A snapshot date, and associated facts that are likely to be captured by a snapshot */
  case class SquashFactsMultiple(date: Date, facts: NonEmptyList[SquashFacts])

  implicit def SquashFactMultipleArbitrary: Arbitrary[SquashFactsMultiple] = Arbitrary(for {
    d <- Arbitrary.arbitrary[Date]
    i <- Gen.choose(2, 5)
    l <- Gen.listOfN(i, squashFactsArbitraryFromDate(d))
  } yield SquashFactsMultiple(d, NonEmptyList(l.head, l.tail: _*)))

  implicit def SquashFactsSingleArbitrary: Arbitrary[SquashFacts] =
    Arbitrary(Arbitrary.arbitrary[Date].flatMap(squashFactsArbitraryFromDate))

  def squashFactsArbitraryFromDate(date: Date): Gen[SquashFacts] = for {
    w <- Arbitrary.arbitrary[ConcreteGroupFeature].flatMap { w =>
      // Make sure we have _at least_ one virtual feature
      if (w.dictionary.hasVirtual) Gen.const(w)
      else virtualDefGen(w.fid -> w.cg.definition).map(virt => w.copy(cg = w.cg.copy(virtual = virt :: w.cg.virtual)))
    }.map {
      // My kingdom for a lens :(
      // Disable filtering in squash tests, handled in FilterReductionSpec and window cli test
      cgf => cgf.copy(cg = cgf.cg.copy(virtual = cgf.cg.virtual.map {
        case (fid, vd) => fid -> vd.copy(filter = None)
      }))
    }
    // We should only ever see 1 fact when no window is defined
    i <- startingDate(w, date).cata(_ => Gen.choose(2, 10), Gen.const(1))
    f <- Gen.listOfN(i, Arbitrary.arbitrary[Fact].flatMap {
      // NOTE: We're not testing snapshot logic here, so there's no point generating facts after the snapshot date
      fact => Gen.choose(0, 100).map(o => fact.withDate(Date.fromLocalDate(date.localDate.minusDays(o))))
    })
    fa = NonEmptyList(f.head, f.tail: _*).map(_.withFeatureId(w.fid))
  } yield SquashFacts(date, w, fa)


  def startingDate(vd: ConcreteGroupFeature, date: Date): Option[Date] =
    vd.cg.virtual.flatMap(_._2.window.map(w => SnapshotWindows.startingDate(w, date))).sorted.headOption
}
