package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift.ThriftFactValue
import com.ambiata.ivory.core.{Fact, Date}
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.scalacheck._, Prop._, Arbitrary._

object ReducerUtil {

  def updateAll(r: Reduction, facts: List[Fact]): ThriftFactValue = {
    r.clear()
    facts.sortBy(_.datetime.underlying).foreach(r.update)
    r.save
  }

  def run[A, B, C](r: ReductionFold[A, B, C], l: List[B]): C =
    r.aggregate(l.foldLeft(r.initial) { (a, v) => r.fold(a, v) })

  def runWithTombstones[A, B, C](r: ReductionFold[A, B, C], l: List[Option[B]]): C =
    r.aggregate(l.foldLeft(r.initial) { (a, v) => v.map(r.fold(a, _)).getOrElse(r.tombstone(a)) })

  def run2[A, B1, B2, C](r: ReductionFold2[A, B1, B2, C], l: List[(B1, B2)]): C = {
    val d = Date.unsafeFromInt(0)
    r.aggregate(l.foldLeft(r.initial) { case (a, (v1, v2)) => r.fold(a, v1, v2, d) })
  }

  def runWithDates[A, B, C](r: ReductionFoldWithDate[A, B, C], l: List[(B, Date)]): C =
    r.aggregate(l.foldLeft(r.initial) { case (a, (v, d)) => r.foldWithDate(a, v, d) })

  def reduceDates[A](doc: DatesOfCount, r: DateReducer[A]): A = {
    val dates = doc.offsets.createSet
    doc.allDates.foreach(dates.inc)
    r.aggregate(dates)
  }

  def reduceDatesLaws[A](r: DateOffsets => DateReducer[A]): Prop =
    laws("ReductionFold laws", arbitrary[DatesOfCount].map(doc => (r(doc.offsets), doc.offsets.createSet) -> doc.allDates)) {
      case ((r2, dates), l) =>
        dates.clear()
        l.foreach(dates.inc)
        r2.aggregate(dates)
    }

  def reductionLaws(r: Reduction): Prop =
    laws("Reduction laws", arbitrary[List[Fact]].map(r ->))((r, l) => deepCopy(updateAll(r, l)))

  def reductionFoldLaws[A, B: Arbitrary, C](r: ReductionFold[A, B, C]): Prop =
    laws("ReductionFold laws", arbitrary[List[B]].map(r ->))(run)

  def reductionFold2Laws[A, B1: Arbitrary, B2: Arbitrary, C](r: ReductionFold2[A, B1, B2, C]): Prop =
    laws("ReductionFold2 laws", arbitrary[List[(B1, B2)]].map(r ->))(run2)

  def reductionFoldWithDateLaws[A, B: Arbitrary, C](r: DateOffsets => ReductionFoldWithDate[A, B, C]): Prop =
    laws("ReductionFoldWithDate laws", arbitrary[ValuesWithDate[B]]
      .map(dos => r(dos.offsets) -> dos.ds))((r, l) => runWithDates(r, l))

  def reductionWithDatesLaws(r: DateOffsets => Reduction): Prop =
    laws("ReductionFoldWithDate laws", arbitrary[FactsWithDate]
      .map(dos => r(dos.offsets) -> dos.ds))((r, l) => deepCopy(updateAll(r, l)))

  def laws[A, B, R](name: String, gen: Gen[(R, List[A])])(f: (R, List[A]) => B): Prop = new Properties(name) {
    property("consecutive") =
      forAll(gen)((l) => {
        f.tupled(l) ?= f.tupled(l)
      })

    property("interleaved") =
      forAll(gen)((l) => {
        val a = f.tupled(l)
        f.tupled(l._1 -> l._2.reverse)
        val b = f.tupled(l)
        a ?= b
      })
  }

  def deepCopy(t: ThriftFactValue): ThriftFactValue =
    if (t == null) t else t.deepCopy
}
