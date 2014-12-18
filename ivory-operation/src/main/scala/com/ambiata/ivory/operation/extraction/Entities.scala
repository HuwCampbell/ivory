package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.ChordEntities
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import scala.collection.JavaConverters._
import scala.util.Sorting
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

  private case class DateRange(earliest: Date, latest: Date, maxCount: Int)

  /** get the earliest date and the latest date across all entities */
  private val dateRange: DateRange = {
    // Going full-mutable to avoid creating GC pressure on Hadoop
    var lmin = Date.maxValue.underlying
    var lmax = Date.minValue.underlying
    var maxCount = 0
    entities.values.asScala.foreach { ds =>
      lmin = Math.min(lmin, ds.last) // last is the minimum date because the array is sorted
      lmax = Math.max(lmax, ds.head) // head is the maximum date because the array is sorted
      maxCount = Math.max(ds.length, maxCount)
    }
    DateRange(Date.unsafeFromInt(lmin), Date.unsafeFromInt(lmax), maxCount)
  }

  def earliestDate = dateRange.earliest
  def latestDate   = dateRange.latest
  def maxChordSize = dateRange.maxCount

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
  type Mappings = java.util.Map[String, Array[Int]]

  def serialiseEntities(repository: Repository, entities: Entities, key: Key): ResultTIO[Unit] = {
    import java.io.ObjectOutputStream
    repository.store.unsafe.withOutputStream(key)(os => ResultT.safe({
      val bOut = new ObjectOutputStream(os)
      bOut.writeObject(entities.entities)
      bOut.close()
    }))
  }

  /**
   * read entities from a file
   */
  def readEntitiesFrom(location: IvoryLocation): ResultTIO[Entities] = {
    val DatePattern = """(\d{4})-(\d{2})-(\d{2})""".r

    // This file can be _really_ big - we want to make this as memory efficient as possible
    IvoryLocation.streamLinesUTF8(location, new java.util.HashMap[String, Array[Int]]) { (line, mappings) =>
      val (entity, date) = parseLine(DatePattern)(line)
      val dates = mappings.get(entity)
      // We don't know ahead of time how many dates we have for each entity, we have to manually grow the array each time
      mappings.put(entity,
        if (dates == null) Array[Int](date.int)
        else if (dates.contains(date.int)) dates
        else {
          val dates2 = dates :+ date.int
          // Don't use Array.sorted - we save an extra, unnecessary allocation this way
          Sorting.quickSort[Int](dates2)(Ordering[Int].reverse)
          dates2
        })
      mappings
    }.map(Entities(_))
  }

  /** For testing only - does everything in memory */
  def writeEntitiesTesting(entities: Entities, location: IvoryLocation): ResultTIO[Unit] =
    IvoryLocation.writeUtf8Lines(location, entities.entities.asScala.flatMap {
      case (entity, dates) => dates.map(d => entity + "|" + Date.unsafeFromInt(d).hyphenated)
    }.toList)

  /** WARNING: This shares the _same_ mutable entities map as [[ChordEntities]] for performance */
  def fromChordEntities(chordEntities: ChordEntities): Entities =
    Entities(chordEntities.entities)

  /** WARNING: This shares the _same_ internal mutable entities map with [[ChordEntities]] for performance */
  def toChordEntities(entities: Entities): ChordEntities =
    new ChordEntities(entities.entities)

  def empty = Entities(new java.util.HashMap[String, Array[Int]])

  private def parseLine(DatePattern: Regex): String => (String, Date) = (line: String) =>
    line.split("\\|").toList match {
      case h :: DatePattern(y, m, d) :: Nil => (h, Date.unsafe(y.toShort, m.toByte, d.toByte))
      case _                                => Crash.error(Crash.DataIntegrity, "Can't parse the line "+line+". Expected: entity id|yyyy-MM-dd")
    }

}
