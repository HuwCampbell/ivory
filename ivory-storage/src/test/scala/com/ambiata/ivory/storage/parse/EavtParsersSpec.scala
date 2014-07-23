package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._

import org.joda.time.DateTimeZone
import org.scalacheck._, Arbitrary._
import org.specs2._, matcher._, specification._

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
  def date = prop((fact: Fact) =>
    EavtParsers.fact(TestDictionary, fact.namespace, DateTimeZone.getDefault).run(List(
      fact.entity
    , fact.feature
    , Value.toString(fact.value, None).getOrElse("?")
    , fact.date.hyphenated
    )) must_== Success(fact.withTime(Time(0))))

  def legacy = prop((fact: Fact) =>
    EavtParsers.fact(TestDictionary, fact.namespace, DateTimeZone.getDefault).run(List(
      fact.entity
    , fact.feature
    , Value.toString(fact.value, None).getOrElse("?")
    , fact.date.hyphenated + " " + fact.time.hhmmss
    )) must_== Success(fact))

  def standard = prop((fz: SparseEntities) => {
    EavtParsers.fact(TestDictionary, fz.fact.namespace, fz.zone).run(List(
      fz.fact.entity
    , fz.fact.feature
    , Value.toString(fz.fact.value, None).getOrElse("?")
    , fz.fact.datetime.iso8601(fz.zone)
    )) must_== Success(fz.fact)
  })

  def zones = prop((fz: SparseEntities) => {
    EavtParsers.fact(TestDictionary, fz.fact.namespace, fz.zone).run(List(
      fz.fact.entity
    , fz.fact.feature
    , Value.toString(fz.fact.value, None).getOrElse("?")
    , fz.fact.date.hyphenated + " " + fz.fact.time.hhmmss
    )) must_== Success(fz.fact)
  })

  def parsefail = prop((bad: BadEavt) =>
    EavtParsers.fact(TestDictionary, bad.namespace, bad.timezone).run(bad.string.split("\\|").toList).toOption must beNone)

  def structFail = {
    val dict = Dictionary(Map(FeatureId("ns", "a") -> FeatureMeta(StructEncoding(Map()), None, "")))
    EavtParsers.parse("e|a|v|t", dict, "ns", DateTimeZone.getDefault).toOption must beNone
  }

  def genBadDouble: Gen[DoubleValue] =
    Gen.oneOf(Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity).map(DoubleValue)

  def genBadValue(good: FeatureMeta, bad: FeatureMeta): Gen[Value] =
    genValue(bad).retryUntil(Value.toString(_, None).map(s => !validString(s, good.encoding)).getOrElse(false))

  /**
   * Arbitrary to create invalid EAVT strings such that the structure is correct, but the content is wrong in some way
   */
  case class BadEavt(string: String, namespace: String, timezone: DateTimeZone)
  implicit def BadEavtArbitrary: Arbitrary[BadEavt] = Arbitrary(for {
    e                   <- Gen.oneOf(testEntities(10000))
    (a, v, t, ns, m, z) <- Gen.oneOf(for {
      (f, m) <- Gen.oneOf(TestDictionary.meta.toList)
      a      <- arbitrary[String].retryUntil(s => !TestDictionary.meta.toList.exists(_._1.name == s))
      v      <- genValue(m)
      dtz    <- arbitrary[DateTimeWithZone]
    } yield (a, v, dtz.datetime, f.namespace, m, dtz.zone), for {
      (f, m) <- Gen.oneOf(TestDictionary.meta.toList).retryUntil(_._2.encoding != StringEncoding)
      a      <- Gen.const(f.name)
      bm     <- Gen.oneOf(TestDictionary.meta.toList).map(_._2).retryUntil(bm => !compatible(bm.encoding, m.encoding))
      v      <- Gen.oneOf(genBadValue(m, bm), if(m.encoding == DoubleEncoding) genBadDouble else genBadValue(m, bm))
      dtz    <- arbitrary[DateTimeWithZone]
    } yield (a, v, dtz.datetime, f.namespace, m, dtz.zone), for {
      (f, m) <- Gen.oneOf(TestDictionary.meta.toList)
      a      <- Gen.const(f.name)
      v      <- genValue(m)
      (t, z) <- arbitrary[BadDateTime].map(b => (b.datetime, b.zone))
    } yield (a, v, t, f.namespace, m, z))
  } yield BadEavt(s"$e|$a|${Value.toString(v, None).getOrElse(m.tombstoneValue.head)}|${t.localIso8601}", ns, z))

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
    case _: StructEncoding => sys.error("Encoding of structs as strings not supported!")
    case _: ListEncoding   => sys.error("Encoding of lists as strings not supported!")
  }

  def genValue(m: FeatureMeta): Gen[Value] =
    Gen.frequency(1 -> Gen.const(TombstoneValue()), 99 -> valueOf(m.encoding))
}
