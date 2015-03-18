package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core.{Fact, Value, Crash, TimeDivision, WeekEndWeekDay, TimeOfDay, DateTimeUtil}

import scala.collection.JavaConverters._

class TimePartitionReducer(d: TimeDivision, f: () => Reduction) extends Reduction {

  val reducers: Array[Reduction] = d match {
    case WeekEndWeekDay => Array.fill(2)(f())
    case TimeOfDay      => Array.fill(4)(f())
  } 

  def clear(): Unit = {
    reducers.foreach(_.clear())
  }

  def update(fact: Fact): Unit = {
    d match {
      case WeekEndWeekDay =>
        // Server time is UTC, but the time we care about is AEST (+10). So a fact which is at 2pm UTC, would be midnight the next day. 
        val offSetDayFromUTC = if (fact.datetime.time.hours > 13) 1 else 0
        // Magic +2 ahead is because 1600-03-01 was a Wednesday, but also day 0, so to bring it up to Monday is 0, Sunday is 6, we add 2.
        if ((DateTimeUtil.toDays(fact.datetime.date) + offSetDayFromUTC + 2) % 7 < 5) reducers(0).update(fact) else reducers(1).update(fact)
      case TimeOfDay      => {
        val hour = (fact.datetime.time.hours + 10) % 24
        if (hour < 6)        reducers(0).update(fact)
        else if (hour < 12)  reducers(1).update(fact)
        else if (hour < 18)  reducers(2).update(fact)
        else                 reducers(3).update(fact)
      }
    }
  }

  def skip(f: Fact, reason: String): Unit = reducers.foreach(_.skip(f, reason))

  def save: ThriftFactValue = {
    val xs = reducers.map(_.save).toList
    ThriftFactValue.lst(new ThriftFactList(xs.map {
      case tv if tv.isSetStructSparse => ThriftFactListValue.s(tv.getStructSparse)
      case tv => Value.toPrimitive(tv) match {
        case Some(tpv) => ThriftFactListValue.p(tpv)
        case _         => Crash.error(Crash.CodeGeneration, s"You have hit an expression error as a list fact has been passed into the time partition reducer.'")
      }
    }.asJava))
  }
}
