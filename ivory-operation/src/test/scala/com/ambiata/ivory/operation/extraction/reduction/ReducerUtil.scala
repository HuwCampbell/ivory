package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift.ThriftFactValue
import com.ambiata.ivory.core.{Fact, Date}
import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries.DatesOfCount

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

  def consecutive[A, B, C](r: ReductionFold[A, B, C], l: List[B]): Boolean =
    run(r, l) == run(r, l)

  def interleaved[A, B, C](r: ReductionFold[A, B, C], l: List[B], x: List[B]): Boolean = {
    val a = run(r, l)
    run(r, x)
    val b = run(r, l)
    a == b
  }


  def buildDateOffsets[A](ds: List[(A, Date)]) = DateOffsets.compact(ds.headOption.map(_._2).getOrElse(Date.minValue),
      ds.lastOption.map(_._2).getOrElse(Date.minValue))
}
