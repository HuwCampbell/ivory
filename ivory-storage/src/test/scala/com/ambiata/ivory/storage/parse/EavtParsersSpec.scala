package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._

import org.joda.time.DateTimeZone
import org.scalacheck._, Arbitrary._
import org.specs2._

import scalaz.{Name => _, Value => _, _}, Scalaz._

class EavtParsersSpec extends Specification with ScalaCheck { def is = s2"""

Eavt Parse Formats
------------------

 Can parse date only                     $date
 Can parse legacy date-time format       $legacy
 Can parse standard date-time format     $standard
 Can parse with different time zones     $zones
 Must fail with bad value in EAVT        $badvalue
 Must fail with bad attribute in EAVT    $badattribute
 Must fail with struct EAVT string       $structFail

"""

  case class PrimitiveSparseEntities(meta: ConcreteDefinition, fact: Fact, zone: DateTimeZone)
  implicit def PrimitiveSpareEntitiesArbitrary: Arbitrary[PrimitiveSparseEntities] = Arbitrary(
    factWithZoneGen(Gen.oneOf(testEntities(1000)), featureMetaGen(arbitrary[PrimitiveEncoding])).map(PrimitiveSparseEntities.tupled)
  )

  def date = prop((fz: PrimitiveSparseEntities) =>
    factDateSpec(fz, DateTimeZone.getDefault, fz.fact.date.hyphenated) must_== Success(fz.fact.withTime(Time(0)))
  )

  def legacy = prop((fz: PrimitiveSparseEntities) =>
    factDateSpec(fz, DateTimeZone.getDefault, fz.fact.date.hyphenated + " " + fz.fact.time.hhmmss) must_== Success(fz.fact)
  )

  def standard = prop((fz: PrimitiveSparseEntities) =>
    factDateSpec(fz, fz.zone, fz.fact.datetime.iso8601(fz.zone)) must_== Success(fz.fact)
  )

  def zones = prop((fz: PrimitiveSparseEntities) =>
    factDateSpec(fz, fz.zone, fz.fact.date.hyphenated + " " + fz.fact.time.hhmmss) must_== Success(fz.fact)
  )

  def factDateSpec[A](fz: PrimitiveSparseEntities, z: DateTimeZone, dt: String): Validation[String, Fact] = {
    import fz._
    EavtParsers.fact(Dictionary(List(meta.toDefinition(fact.featureId))), fact.namespace, z).run(List(
        fact.entity
      , fact.feature
      , Value.toString(fact.value, meta.tombstoneValue.headOption).getOrElse("")
      , dt
    ))
  }

  def badvalue =
    prop((entity: Entity, bad: BadValue, feature: FeatureId, date: Date, zone: DateTimeZone) =>
      EavtParsers.fact(
        Dictionary(List(Concrete(feature, bad.meta)))
      , feature.namespace
      , zone
      ).run(List(feature.namespace.name, entity.value, feature.name, bad.value, date.hyphenated)).toOption must beNone)

  def badattribute =
    prop((entity: Entity, value: Value, feature: FeatureId, date: Date, zone: DateTimeZone) => {
      EavtParsers.fact(
        Dictionary.empty
      , feature.namespace
      , zone
      ).run(List(entity.value, feature.name, Value.toString(value, None).getOrElse("?"), date.hyphenated)).toOption must beNone})

  def structFail = {
    val dict = Dictionary(List(Definition.concrete(FeatureId(Name("ns"), "a"), StructEncoding(Map()), None, "", Nil)))
    EavtParsers.parse("e|a|v|t", dict, Name("ns"), DateTimeZone.getDefault).toOption must beNone
  }

  def genBadDouble: Gen[DoubleValue] =
    Gen.oneOf(Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity).map(DoubleValue)

  def genBadValue(good: ConcreteDefinition, bad: ConcreteDefinition): Gen[Value] =
    genValue(bad).filter(Value.toString(_, None).exists(s => !validString(s, good.encoding) && !good.tombstoneValue.contains(s)))

  case class BadValue(meta: ConcreteDefinition, value: String)

  implicit def BadValueArbitrary: Arbitrary[BadValue] = Arbitrary(Gen.oneOf(
    /** A bad BooleanValue, examplified by an integer  */
    arbitrary[Int] map (v =>
      BadValue(ConcreteDefinition(BooleanEncoding, None, "bad-boolean-test-case", Nil), v.toString))

    /** A bad IntValue, examplified by a string  */
  , arbitrary[String] map (v =>
      BadValue(ConcreteDefinition(IntEncoding, None, "bad-int-test-case", Nil), v)) filter (x =>
        !x.value.parseInt.toOption.isDefined)

    /** A bad LongValue, examplified by a double  */
  , arbitrary[Double] map (v =>
      BadValue(ConcreteDefinition(LongEncoding, None, "bad-long-test-case", Nil), v.toString))

    /** A bad DoubleValue, examplified by a string  */
  , arbitrary[String] map (v =>
      BadValue(ConcreteDefinition(DoubleEncoding, None, "bad-double-test-case", Nil), v)) filter (x =>
        !x.value.parseInt.toOption.isDefined)

    /** Edge cases for a bad DoubleValue (note this should include Long.MaxValue, but see #187)  */
  , Gen.oneOf(Double.NaN.toString, Double.NegativeInfinity.toString, Double.PositiveInfinity.toString) map (v =>
      BadValue(ConcreteDefinition(DoubleEncoding, None, "bad-double-edge-test-case", Nil), v))
  ))

  def validString(s: String, e: Encoding): Boolean = e match {
    case StringEncoding  => true
    case IntEncoding     => s.parseInt.isSuccess
    case DoubleEncoding  => s.parseDouble.fold(_ => false, Value.validDouble)
    case BooleanEncoding => s.parseBoolean.isSuccess
    case LongEncoding    => s.parseLong.isSuccess
    case _: StructEncoding => false
    case _: ListEncoding   => false
  }

  def genValue(m: ConcreteDefinition): Gen[Value] =
    Gen.frequency(1 -> Gen.const(TombstoneValue), 99 -> valueOf(m.encoding, m.tombstoneValue))
}
