package com.ambiata.ivory.operation.extraction

import java.util.HashMap

import com.ambiata.ivory.core.TemporaryLocations._
import com.ambiata.ivory.core.{Date, Fact}
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.operation.extraction.Chord.PrioritizedFact
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core.TemporaryType
import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import org.specs2.matcher.ThrownExpectations
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._
import scalaz._, Scalaz._

class EntitiesSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

 The Entities class provides a list of dates where we wish to get values for each entity

   the keep method keeps a Fact if:
     the fact concerns one of the entities and the fact has a date earlier than the required date $keepFact

   the keepBest method finds facts in a list of facts that have the closest datetime
    to the required date and the best priority $keepBestFact

 Read the entities file from a location $readEntities
 Reading the entities file from a location should ignore dupes $dupes
"""

  def keepFact = prop { (fact: Fact, o: Byte) =>
    val unsafeDate = Date.fromLocalDate(fact.date.localDate.plusDays(Math.abs(o.toInt)))
    val safeDate = if (Date.isValid(unsafeDate.year, unsafeDate.month, unsafeDate.day)) unsafeDate else Date.maxValue
    val entities = add(Entities.empty, fact.entity, safeDate)
    val diagnostic = Seq(
      s"safeDate is $safeDate",
      s"fact is ${(fact.entity, fact.date, fact.date.int)}",
      s"entities are ${entities.entities.asScala.map { case (e, ds) => (e, ds.mkString(",")) }.mkString("\n") }").mkString("\n", "\n", "\n")

    entities.keep(fact) aka diagnostic must beTrue

  }.set(maxSize = 3, minTestsOk= 1000)

  def keepBestFact = prop { (h: PrioritizedFact, t: List[PrioritizedFact]) =>
    val facts = (h +: t).sortBy(_._1)
    val head = facts.head
    val tail = facts.tail

    // create Entities from the existing facts
    val entities = createEntitiesFromFactsWithOneMoreDate(facts)
    val (priority1, fact1) = head
    val (entity1, date1)   = (fact1.entity, fact1.date.int)

    val best = entities.keepBestFacts(entity1, facts).toList

    "there are 'best' facts for entity1, given how we've built the Entities object" ==> {
      best must not(beEmpty)
    }

    "there is a fact for date1" ==> {
      val diagnostic =
        s"\n\nentity1: $entity1, date1: ${date1}, priority1: $priority1\n\n" +
        facts.collect { case (p, f) => (p, f.entity, f.date.int) }.mkString("FACTS are\n", "\n", "\n\n") +
        best.collect { case (d, p, Some(f)) => (d, p, f.entity, f.date.int) }.mkString("BEST is\n", "\n", "\n\n")

      best.find(_._1 == date1).flatMap(_._3) aka diagnostic must beSome(fact1)
    }
  }.set(maxSize = 3, minTestsOk= 1000)

  /**
   * ARBITRARIES
   */

  def createEntitiesFromFactsWithOneMoreDate(facts: List[PrioritizedFact]) = {
    facts.foldLeft(Entities(new HashMap[String, Array[Int]])) { case (entities, (p, f)) =>
      add(entities, f.entity, f.date)
    }
  }

  /** add a new entity name and date to the entities list */
  def add(entities: Entities, entity: String, date: Date) = {
    val dates =
      Option(entities.entities.get(entity))
        .map(ds => (ds :+ date.int).toArray).getOrElse(Array(date.int)).sorted.reverse

    entities.entities.put(entity, dates)
    entities
  }

  def genEntityDates: Gen[(Entity, List[Date])] = for {
    id          <- arbitrary[Entity]
    datesNumber <- Gen.choose(1, 4)
    dates       <- Gen.listOfN(datesNumber, DateArbitrary.arbitrary)
    // Make sure we filter/sort the result in the way that is expected
  } yield (id, dates.distinct.sorted.reverse)

  def genEntities: Gen[Entities] = Gen.sized { n =>
    Gen.listOfN(n + 1, genEntityDates).map { list =>
      val mappings = new java.util.HashMap[String, Array[Int]]
      list.foreach { case (entity, dates) => mappings.put(entity.value, dates.map(_.int).toArray) }
      Entities(mappings)
    }
  }

  implicit def EntitiesArbitrary: Arbitrary[Entities] =
    Arbitrary(genEntities)

  def readEntities = prop { entities: Entities =>
    def toMapForEquals(e: Entities) = e.entities.asScala.mapValues(_.toList).toMap
    withIvoryLocationFile(TemporaryType.Hdfs) { location =>
      Entities.writeEntitiesTesting(entities, location) >>
        Entities.readEntitiesFrom(location).map(toMapForEquals)
    } must beOkValue(toMapForEquals(entities))
  }.set(minTestsOk = 10)

  def dupes = prop { entities: Entities =>
    def toMapForEquals(e: Entities) = e.entities.asScala.mapValues(_.toList).toMap
    withIvoryLocationFile(TemporaryType.Hdfs) { location =>
      Entities.writeEntitiesTesting(Entities(entities.entities.asScala.map(x => x._1 -> (x._2 ++ x._2)).asJava), location) >>
        Entities.readEntitiesFrom(location).map(toMapForEquals)
    } must beOkValue(toMapForEquals(entities))
  }.set(minTestsOk = 3)
}
