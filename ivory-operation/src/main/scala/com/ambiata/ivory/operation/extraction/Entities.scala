package com.ambiata.ivory.operation.extraction

import java.util.HashMap

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scalaz.Order
import scalaz.Scalaz._
import Entities._

/**
 * This class stores a map of (entities name, dates)
 * which describes the points in time at which we want to retrieve facts for a given entity
 */
case class Entities(entities: Mappings) {
  type PrioritizedFact = (Priority, Fact)

  private case class DateRange(earliest: Date, latest: Date)

  /** get the earliest date and the latest date across all entities */
  private val dateRange: DateRange = entities.values.asScala.foldLeft(DateRange(Date.maxValue, Date.minValue)) { case (DateRange(lmin, lmax), ds) =>
    val min = Date.unsafeFromInt(ds.last) // last is the minimum date because the array is sorted
    val max = Date.unsafeFromInt(ds.head) // head is the maximum date because the array is sorted
    DateRange(Date.min(min, lmin), Date.max(max, lmax))
  }

  def earliestDate = dateRange.earliest
  def latestDate   = dateRange.latest

  /** @return true if this fact concerns one of the entities and happened before the last required date for that entity */
  def keep(f: Fact): Boolean = {
    val dates = entities.get(f.entity)
    dates != null && f.date.int <= dates(0)
  }

  /**
   * From a list of prioritized facts and a given entity
   * @return the list of (Date, Priority, Fact) where we keep the best fact for a given date
   *         the "best" facts must be the closest to the required date and then the fact with the highest
   *         priority
   */
  def keepBestFacts(entityId: String, facts: Iterable[PrioritizedFact]): Array[(Int, Priority, Option[Fact])] = {
    // lexical order for a pair (Fact, Priority) so that
    // p1 < p2 <==> f1.datetime > f2.datetime || f1.datetime == f2.datetime && priority1 < priority2
    implicit val ord: Order[(Fact, Priority)] = Order.orderBy { case (f, p) => (-f.datetime.long, p) }

    // the required dates
    val dates = Option(entities.get(entityId)).getOrElse(Array[Int]())

    // we traverse all facts and for each required date
    // we keep the "best" fact which date is just before that date
    facts.foldLeft(dates.map((_, Priority.Min, none[Fact]))) { case (result, (priority, fact)) =>
      val factDate = fact.date.int
      result.map {
        // we found a first suitable fact for that date
        case (date, p, None)    if factDate <= date                               => (date, priority, Some(fact))
        // we found a better priority at an acceptable date but later time or better priority
        case (date, p, Some(f)) if factDate <= date && (fact, priority).<((f, p)) => (date, priority, Some(fact))
        case previous                                                             => previous
      }
    }
  }
}

object Entities {

  /**
   * Map of entity name to a sorted non-empty array of Dates, represented as Ints.
   * The dates array is sorted from latest to earliest
   */
  type Mappings = HashMap[String, Array[Int]]

  def serialiseEntities(repository: Repository, entities: Entities, key: Key): ResultTIO[Unit] = {
    import java.io.ObjectOutputStream
    repository.store.unsafe.withOutputStream(key)(os => ResultT.safe({
      val bOut = new ObjectOutputStream(os)
      bOut.writeObject(entities.entities)
      bOut.close()
    }))
  }

  // TODO Change to thrift serialization, see #131
  def deserialiseEntities(repository: Repository, chordKey: Key): ResultTIO[Entities] = {
    import java.io.{ByteArrayInputStream, ObjectInputStream}
    repository.store.bytes.read(chordKey).flatMap(bytes =>
      ResultT.safe(Entities(new ObjectInputStream(new ByteArrayInputStream(bytes.toArray)).readObject.asInstanceOf[Mappings])))
  }

  /**
   * read entities from a file
   */
  def readEntitiesFrom(location: IvoryLocation): ResultTIO[Entities] = {
    val DatePattern = """(\d{4})-(\d{2})-(\d{2})""".r

    IvoryLocation.readLines(location).map { lines =>
      val mappings = new HashMap[String, Array[Int]](lines.length)
      lines.map(parseLine(DatePattern)).groupBy(_._1).foreach { case (k, v) =>
        mappings.put(k, v.map(_._2).toArray.sorted.reverse)
      }
      Entities(mappings)
    }
  }

  def empty = Entities(new java.util.HashMap[String, Array[Int]])

  private def parseLine(DatePattern: Regex): String => (String, Int) = (line: String) =>
    line.split("\\|").toList match {
      case h :: DatePattern(y, m, d) :: Nil => (h, Date.unsafeYmdToInt(y.toShort, m.toByte, d.toByte))
      case _                                => Crash.error(Crash.DataIntegrity, "Can't parse the line "+line+". Expected: entity id|yyyy-MM-dd")
    }

}
