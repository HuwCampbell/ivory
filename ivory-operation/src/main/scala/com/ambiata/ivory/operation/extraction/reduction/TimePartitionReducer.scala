package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core.{Fact, Value, Crash, TimeDivision, WeekEndWeekDay, TimeOfDay}
import org.joda.time.DateTimeZone

import scala.collection.JavaConverters._

class TimePartitionReducer(d: TimeDivision, f: () => Reduction) extends Reduction {

  val reducers: Array[Reduction] = d match {
    case WeekEndWeekDay => Array.fill(2)(f())
    case TimeOfDay      => Array.fill(4)(f())
  } 

  val datetimezone = DateTimeZone.forID("Australia/Sydney")

  def clear(): Unit = {
    reducers.foreach(_.clear())
  }

  def update(fact: Fact): Unit = {
    val t = fact.datetime.joda(datetimezone)
    d match {
      case WeekEndWeekDay => if (t.getDayOfWeek() < 6) reducers(0).update(fact) else reducers(1).update(fact)
      case TimeOfDay      => {
        val hour = t.getHourOfDay
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
