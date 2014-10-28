package com.ambiata.ivory.core
package arbitraries

import org.joda.time.DateTimeZone
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary._
import ArbitraryFeatures._
import ArbitraryValues._
import ArbitraryEncodings._

/**
 * Arbitraries for generating facts
 */
trait ArbitraryFacts {

  def factWithZoneGen(entity: Gen[String], mgen: Gen[ConcreteDefinition]): Gen[(ConcreteDefinition, Fact, DateTimeZone)] = for {
    f <- arbitrary[FeatureId]
    m <- mgen
    dtz <- arbitrary[DateTimeWithZone]
    f <- factGen(entity, f, m, dtz.datetime)
  } yield (m, f, dtz.zone)

  def factGen(entity: Gen[String], f: FeatureId, m: ConcreteDefinition, dt: DateTime): Gen[Fact] = for {
    e <- entity
    // Don't generate a Tombstone if it's not possible
    v <- Gen.frequency((if (m.tombstoneValue.nonEmpty) 1 else 0) -> Gen.const(TombstoneValue), 99 -> valueOf(m.encoding, m.tombstoneValue))
  } yield Fact.newFact(e, f.namespace.name, f.name, dt.date, dt.time, v)

  /**
   * Create an arbitrary fact and timezone such that the time in the fact is valid given the timezone
   */
  implicit def SparseEntitiesArbitrary: Arbitrary[SparseEntities] =
    Arbitrary(factWithZoneGen(Gen.choose(0, 1000).map(testEntityId), arbitrary[ConcreteDefinition]).map(SparseEntities.tupled))

  implicit def FactsWithDictionaryArbitrary: Arbitrary[FactsWithDictionary] = Arbitrary(for {
    cg <- arbitrary[ConcreteGroupFeature]
    n <- Gen.choose(2, 10)
    dt <- arbitrary[DateTime]
    facts <- Gen.listOfN(n, factGen(Gen.choose(0, 1000).map(testEntityId), cg.fid, cg.cg.definition, dt))
  } yield FactsWithDictionary(cg, facts))

  implicit def FactArbitrary: Arbitrary[Fact] =
    Arbitrary(arbitrary[SparseEntities].map(_.fact))

  implicit def FactsWithFilterArb: Arbitrary[FactsWithQuery] = Arbitrary(for {
    d <- arbitrary[DefinitionWithQuery]
    n <- Gen.choose(0, 10)
    entities = Gen.choose(0, 1000).map(testEntityId)
    f <- Gen.listOfN(n, for {
      f <- factWithZoneGen(entities, Gen.const(d.cd))
      // This is probably going to get pretty hairy when we add more filter operations
      v = d.filter.fold({
        case FilterEquals(ev) => ev
      })(identity, (k, v) => StructValue(Map(k -> v))) {
        case (op, h :: t) => op.fold(t.foldLeft(h) {
          case (StructValue(m1), StructValue(m2)) => StructValue(m1 ++ m2)
          case (a, _) => a
        }, h)
        case (_, Nil) => StringValue("")
      }
    } yield f._2.withValue(v))
    o <- Gen.choose(0, 10).flatMap(n => Gen.listOfN(n, factWithZoneGen(entities, Gen.const(d.cd)).map(_._2)).map(_.filterNot {
      // Filter out facts that would otherwise match
      fact => FilterTester.eval(d.filter, fact)
    }))
  } yield FactsWithQuery(d, f, o))

  implicit def ArbitraryFactAndPriority: Arbitrary[FactAndPriority] = Arbitrary(for {
    f <- Arbitrary.arbitrary[Fact]
    p <- Arbitrary.arbitrary[Priority]
  } yield new FactAndPriority(f, p))

}

/** All generated SparseEntities will have a large range of possible entity id's */
case class SparseEntities(meta: ConcreteDefinition, fact: Fact, zone: DateTimeZone)

/** Facts for a _single_ [[ConcreteDefinition]] (feel free to generate a [[List]] of them if you need more) */
case class FactsWithDictionary(cg: ConcreteGroupFeature, facts: List[Fact]) {
  def dictionary: Dictionary = cg.dictionary
}

case class FactAndPriority(f: Fact, p: Priority)
case class FactsetList(factsets: List[Factset])
/** list of facts with a query and the corresponding result */
case class FactsWithQuery(filter: DefinitionWithQuery, facts: List[Fact], other: List[Fact])

object ArbitraryFacts extends ArbitraryFacts