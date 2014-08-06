package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._

import org.joda.time.DateTimeZone
import org.scalacheck._, Arbitrary._
import org.specs2._

import scalaz.{Value => _, _}, Scalaz._

class EavtParsersSpec extends Specification with ScalaCheck { def is = s2"""

Eavt Parse Formats
------------------

 Can parse date only                     $date
 Can parse legacy date-time format       $legacy
 Can parse standard date-time format     $standard
 Can parse with different time zones     $zones
 Must fail with bad EAVT string          $parsefail
 Must fail with struct EAVT string       $structFail

"""

  case class PrimitiveSparseEntities(meta: FeatureMeta, fact: Fact, zone: DateTimeZone)
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
    EavtParsers.fact(Dictionary(Map(fact.featureId -> meta)), fact.namespace, z).run(List(
      fact.entity
      , fact.feature
      , Value.toString(fact.value, meta.tombstoneValue.headOption).getOrElse("")
      , dt
    ))
  }

  def parsefail = prop((bad: BadEavt) =>
    EavtParsers.fact(Dictionary(Map(FeatureId(bad.namespace, bad.name) -> bad.meta)), bad.namespace, bad.timezone).run(bad.string.split("\\|").toList).toOption must beNone)

  def structFail = {
    val dict = Dictionary(Map(FeatureId("ns", "a") -> FeatureMeta(StructEncoding(Map()), None, "")))
    EavtParsers.parse("e|a|v|t", dict, "ns", DateTimeZone.getDefault).toOption must beNone
  }

  def genBadDouble: Gen[DoubleValue] =
    Gen.oneOf(Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity).map(DoubleValue)

  def genBadValue(good: FeatureMeta, bad: FeatureMeta): Gen[Value] =
    genValue(bad).filter(Value.toString(_, None).exists(s => !validString(s, good.encoding) && !good.tombstoneValue.contains(s)))

  /**
   * Arbitrary to create invalid EAVT strings such that the structure is correct, but the content is wrong in some way
   */
  case class BadEavt(example: Int, string: String, namespace: String, name: String, meta: FeatureMeta, timezone: DateTimeZone)
  implicit def BadEavtArbitrary: Arbitrary[BadEavt] = Arbitrary(for {
    e                   <- Gen.oneOf(testEntities(10000))
    (i, a, v, t, f, m, z) <- Gen.oneOf(for {
      (f, m) <- arbitrary[(FeatureId, FeatureMeta)]
      a      <- Gen.identifier.map(s => s + (if (s == f.name) "?" else ""))
      v      <- genValue(m)
      dtz    <- arbitrary[DateTimeWithZone]
    } yield (0, a, v, dtz.datetime, f, m, dtz.zone), for {
      (f, m) <- arbitrary[(FeatureId, FeatureMeta)].retryUntil(_._2.encoding != StringEncoding)
      bm     <- arbitrary[(FeatureId, FeatureMeta)].map(_._2).retryUntil(bm => !compatible(bm.encoding, m.encoding))
      v      <- Gen.oneOf(genBadValue(m, bm), if(m.encoding == DoubleEncoding) genBadDouble else genBadValue(m, bm))
      dtz    <- arbitrary[DateTimeWithZone]
    } yield (1, f.name, v, dtz.datetime, f, m, dtz.zone), for {
      (f, m) <- arbitrary[(FeatureId, FeatureMeta)]
      v      <- genValue(m)
      (t, z) <- arbitrary[BadDateTime].map(b => (b.datetime, b.zone))
    } yield (2, f.name, v, t, f, m, z))
  } yield BadEavt(i, s"$e|${f.name}|${Value.toString(v, m.tombstoneValue.headOption).getOrElse("?")}|${t.localIso8601}", f.namespace, a, m, z))

  def compatible(from: Encoding, to: Encoding): Boolean =
    if(from == to) true else (from, to) match {
      case (_, StringEncoding)            => true
      case (IntEncoding, DoubleEncoding)  => true
      case (IntEncoding, LongEncoding)    => true
      case (LongEncoding, IntEncoding)    => true
      case (LongEncoding, DoubleEncoding) => true
      case _                              => false
    }

  def validString(s: String, e: Encoding): Boolean = e match {
    case StringEncoding  => true
    case IntEncoding     => s.parseInt.isSuccess
    case DoubleEncoding  => s.parseDouble.fold(_ => false, Value.validDouble)
    case BooleanEncoding => s.parseBoolean.isSuccess
    case LongEncoding    => s.parseLong.isSuccess
    case _: StructEncoding => false
    case _: ListEncoding   => false
  }

  def genValue(m: FeatureMeta): Gen[Value] =
    Gen.frequency(1 -> Gen.const(TombstoneValue()), 99 -> valueOf(m.encoding, m.tombstoneValue))
}
