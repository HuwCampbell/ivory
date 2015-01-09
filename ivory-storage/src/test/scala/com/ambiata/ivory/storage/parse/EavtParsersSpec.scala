package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.gen._
import org.joda.time.DateTimeZone
import org.scalacheck._, Arbitrary._
import org.specs2._

import scalaz.{Value => _, _}, Scalaz._

class EavtParsersSpec extends Specification with ScalaCheck { def is = s2"""

Eavt Parse Formats
------------------

 Can split EAV line                      $splitLineEAV
 Can split non-EAV line                  $splitLineOther
 Can parse date only                     $date
 Can parse legacy date-time format       $legacy
 Can parse standard date-time format     $standard
 Can parse with different time zones     $zones
 Must fail with bad value in EAVT        $badvalue
 Must fail with bad attribute in EAVT    $badattribute
 Must fail with struct EAVT string       $structFail

"""

  def splitLineEAV = prop { (c: Char, c2: Char, e: String, a: String, v: String, t: String) => {
    val l = List(e, a, v, t).map(_.replace(c.toString, "")).filter(!_.isEmpty)
    if (l.isEmpty || l.size != 4) true ==== true
    else EavtParsers.splitLine(c, l.mkString(c.toString)) ==== List(l(0), l(1), l(2), l(3).trim)
  }}

  def splitLineOther = prop { (c: Char, l2: List[String]) =>
    val l = l2.map(_.replace(c.toString, "")).filter(!_.isEmpty)
    if (l.isEmpty || l.size == 4) true ==== true else EavtParsers.splitLine(c, l.mkString(c.toString)) ==== l
  }

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
    EavtParsers.fact(Dictionary(List(meta.toDefinition(fact.featureId))), fact.namespace, z, z).run(List(
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
      , zone
      ).run(List(feature.namespace.name, entity.value, feature.name, bad.value, date.hyphenated)).toOption must beNone)

  def badattribute =
    prop((entity: Entity, value: PrimitiveValue, feature: FeatureId, date: Date, zone: DateTimeZone) => {
      EavtParsers.fact(
        Dictionary.empty
      , feature.namespace
      , zone
      , zone
      ).run(List(entity.value, feature.name, Value.toString(value, None).getOrElse("?"), date.hyphenated)).toOption must beNone})

  def structFail = {
    val dict = Dictionary(List(Definition.concrete(FeatureId(Namespace("ns"), "a"), StructEncoding(Map()), Mode.State, None, "", Nil)))
    EavtParsers.parser(dict, Namespace("ns"), DateTimeZone.getDefault, DateTimeZone.getDefault).run(List("e", "a", "v", "t")).toOption must beNone
  }

  def genBadDouble: Gen[DoubleValue] =
    Gen.oneOf(Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity).map(DoubleValue)

  def genBadValue(good: ConcreteDefinition, bad: ConcreteDefinition): Gen[Value] =
    genValue(bad).filter(Value.toString(_, None).exists(s => !validString(s, good.encoding) && !good.tombstoneValue.contains(s)))

  case class BadValue(meta: ConcreteDefinition, value: String)

  implicit def BadValueArbitrary: Arbitrary[BadValue] = Arbitrary(Gen.oneOf(
    /** A bad BooleanValue, examplified by an integer  */
    arbitrary[Int] map (v =>
      BadValue(ConcreteDefinition(BooleanEncoding, Mode.State, None, "bad-boolean-test-case", Nil), v.toString))

    /** A bad IntValue, examplified by a string  */
  , arbitrary[String] map (v =>
      BadValue(ConcreteDefinition(IntEncoding, Mode.State, None, "bad-int-test-case", Nil), v)) filter (x =>
        !x.value.parseInt.toOption.isDefined)

    /** A bad LongValue, examplified by a double  */
  , arbitrary[Double] map (v =>
      BadValue(ConcreteDefinition(LongEncoding, Mode.State, None, "bad-long-test-case", Nil), v.toString))

    /** A bad DoubleValue, examplified by a string  */
  , arbitrary[String] map (v =>
      BadValue(ConcreteDefinition(DoubleEncoding, Mode.State, None, "bad-double-test-case", Nil), v)) filter (x =>
        !x.value.parseInt.toOption.isDefined)

    /** Edge cases for a bad DoubleValue (note this should include Long.MaxValue, but see #187)  */
  , Gen.oneOf(Double.NaN.toString, Double.NegativeInfinity.toString, Double.PositiveInfinity.toString) map (v =>
      BadValue(ConcreteDefinition(DoubleEncoding, Mode.State, None, "bad-double-edge-test-case", Nil), v))
  ))

  def validString(s: String, e: Encoding): Boolean = e match {
    case StringEncoding  => true
    case IntEncoding     => s.parseInt.isSuccess
    case DoubleEncoding  => s.parseDouble.fold(_ => false, Value.validDouble)
    case BooleanEncoding => s.parseBoolean.isSuccess
    case LongEncoding    => s.parseLong.isSuccess
    case DateEncoding    => Dates.date(s).isDefined
    case _: StructEncoding => false
    case _: ListEncoding   => false
  }

  def genValue(m: ConcreteDefinition): Gen[Value] =
    Gen.frequency(1 -> Gen.const(TombstoneValue), 99 -> GenValue.valueOf(m.encoding, m.tombstoneValue))
}
