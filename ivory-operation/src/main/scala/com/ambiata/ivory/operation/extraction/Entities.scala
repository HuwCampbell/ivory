package com.ambiata.ivory.operation.extraction

import java.util.HashMap

import com.ambiata.ivory.core.{Priority, Fact, Date}
import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._

import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scalaz.Order
import scalaz.Scalaz._
import Entities._

case class Entities(entities: Mappings) {
  type PrioritizedFact = (Priority, Fact)

  private case class DateRange(earliest: Date, latest: Date)

  private lazy val dateRange: DateRange = entities.values.asScala.foldLeft(DateRange(Date.maxValue, Date.minValue)) { case (DateRange(lmin, lmax), ds) =>
    val min = Date.unsafeFromInt(ds.min)
    val max = Date.unsafeFromInt(ds.max)
    DateRange(Date.min(min, lmin), Date.max(max, lmax))
  }

  lazy val earliestDate = dateRange.earliest
  lazy val latestDate = dateRange.latest

  /** @return true if this fact concerns one of the entities and happened before the last required date for that entity */
  def keep(f: Fact) = {
    val dates = entities.get(f.entity)
    dates != null && Date.ymdToInt(f.date.year, f.date.month, f.date.day) <= dates(0)
  }

  def keepBestFact(entityId: String, facts: Iterable[PrioritizedFact]): Array[(Int, Priority, Option[Fact])] = {
    // lexical order for a pair (Fact, Priority) so that
    // p1 < p2 <==> f1.datetime > f2.datetime || f1.datetime == f2.datetime && priority1 < priority2
    implicit val ord: Order[(Fact, Priority)] = Order.orderBy { case (f, p) => (-f.datetime.long, p) }

    // the required dates
    val dates = entities.get(entityId)

    // we traverse all facts and for each required date
    // we keep the "best" fact which date is just before that date
    facts.foldLeft(dates.map((_, Priority.Min, none[Fact]))) { case (result, (priority, fact)) =>
      val factDate = fact.date.int
      result.map {
        // we found a first suitable fact for that date
        case (date, p, None)    if factDate <= date                               => (date, priority, Some(fact))
        // we found a better priority at a later time
        case (date, p, Some(f)) if factDate <= date && (fact, priority).<((f, p)) => (date, priority, Some(fact))
        case previous                                                             => previous
      }
    }
  }
}

object Entities {

  type Mappings = HashMap[String, Array[Int]]

  def serialiseEntities(entities: Entities, ref: ReferenceIO): ResultTIO[Unit] = {
    import java.io.ObjectOutputStream
    ref.run(store => path => store.unsafe.withOutputStream(path)(os => ResultT.safe({
      val bOut = new ObjectOutputStream(os)
      bOut.writeObject(entities.entities)
      bOut.close()
    })))
  }

  // TODO Change to thrift serialization, see #131
  def deserialiseEntities(ref: ReferenceIO): ResultTIO[Entities] = {
    import java.io.{ByteArrayInputStream, ObjectInputStream}
    ref.run(store => path => store.bytes.read(path).flatMap(bytes =>
      ResultT.safe(Entities(new ObjectInputStream(new ByteArrayInputStream(bytes.toArray)).readObject.asInstanceOf[Mappings]))))
  }

  /**
   * read entities from a file
   */
  def readEntitiesFrom(ref: ReferenceIO): ResultTIO[Entities] = {
    val DatePattern = """(\d{4})-(\d{2})-(\d{2})""".r

    ref.run(s => s.linesUtf8.read).map { lines =>
      val mappings = new HashMap[String, Array[Int]](lines.length)
      lines.map(parseLine(DatePattern)).groupBy(_._1).foreach { case (k, v) =>
        mappings.put(k, v.map(_._2).toArray.sorted.reverse)
      }
      Entities(mappings)
    }
  }

  private def parseLine(DatePattern: Regex): String => (String, Int) = (line: String) =>
    line.split("\\|").toList match {
      case h :: DatePattern(y, m, d) :: Nil => (h, Date.ymdToInt(y.toShort, m.toByte, d.toByte))
      case _                                => sys.error("Can't parse the line "+line+". Expected: entity id|yyyy-MM-dd")
    }

}
