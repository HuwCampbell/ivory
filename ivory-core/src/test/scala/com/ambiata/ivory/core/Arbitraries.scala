package com.ambiata.ivory.core

import org.scalacheck._, Arbitrary._

import org.joda.time.DateTimeZone
import scala.collection.JavaConverters._

import scalaz._, Scalaz._

object Arbitraries {
  implicit def PriorityArbitrary: Arbitrary[Priority] =
    Arbitrary(Gen.choose(Priority.Min.toShort, Priority.Max.toShort).map(Priority.unsafe))

  implicit def DateArbitrary: Arbitrary[Date] =
    Arbitrary(for {
      y <- Gen.choose(1970, 3000)
      m <- Gen.choose(1, 12)
      d <- Gen.choose(1, 31)
      r = Date.create(y.toShort, m.toByte, d.toByte)
      if r.isDefined
    } yield r.get)

  implicit def TimeArbitrary: Arbitrary[Time] =
    Arbitrary(Gen.frequency(
      3 -> Gen.const(Time(0))
    , 1 -> Gen.choose(0, (60 * 60 * 24) - 1).map(Time.unsafe)
    ))

  implicit def DateTimeArbitrary: Arbitrary[DateTime] =
    Arbitrary(for {
      d <- arbitrary[Date]
      t <- arbitrary[Time]
    } yield d.addTime(t))

  case class DateTimeWithZone(datetime: DateTime, zone: DateTimeZone)
  implicit def DateTimeWithZoneArbitrary: Arbitrary[DateTimeWithZone] =
    Arbitrary(for {
      dt <- arbitrary[DateTime]
      z  <- arbitrary[DateTimeZone].retryUntil(z => try { dt.joda(z); true } catch { case e: java.lang.IllegalArgumentException => false })
    } yield DateTimeWithZone(dt, z))

  case class BadDateTime(datetime: DateTime, zone: DateTimeZone)
  implicit def BadDateTimeArbitrary: Arbitrary[BadDateTime] =
    Arbitrary(for {
      dt  <- arbitrary[DateTime]
      opt <- arbitrary[DateTimeZone].map(z => Dates.dst(dt.date.year, z).flatMap({ case (firstDst, secondDst) =>
               val unsafeFirst = unsafeAddSecond(firstDst)
               val unsafeSecond = unsafeAddSecond(secondDst)
               try {
                 unsafeFirst.joda(z)
                 try { unsafeSecond.joda(z); None } catch { case e: java.lang.IllegalArgumentException => Some((unsafeSecond, z)) }
               } catch {
                 case e: java.lang.IllegalArgumentException => Some((unsafeFirst, z))
               }
             })).retryUntil(_.isDefined)
      (bad, z) = opt.get
    } yield BadDateTime(bad, z))

  def unsafeAddSecond(dt: DateTime): DateTime = {
    val (d, h, m, s) = (dt.date.day.toInt, dt.time.hours, dt.time.minuteOfHour, dt.time.secondOfMinute) match {
      case (d, 23, 59, 59) => (d + 1, 0, 0, 0)
      case (d, h, 59, 59)  => (d, h + 1, 0, 0)
      case (d, h, m, 59)   => (d, h, m + 1, 0)
      case (d, h, m, s)    => (d, h, m, s + 1)
    }
    DateTime.unsafe(dt.date.year, dt.date.month, d.toByte, (h * 60 * 60) + (m * 60) + s)
  }

  lazy val TestDictionary: Dictionary = Dictionary(Map(
    FeatureId("fruit", "apple") -> FeatureMeta(LongEncoding, ContinuousType, "Reds and Greens", "?" :: Nil)
  , FeatureId("fruit", "orange") -> FeatureMeta(StringEncoding, CategoricalType, "Oranges", "?" :: Nil)
  , FeatureId("vegetables", "potatoe") -> FeatureMeta(DoubleEncoding, ContinuousType, "Browns", "?" :: Nil)
  , FeatureId("vegetables", "yams") -> FeatureMeta(BooleanEncoding, CategoricalType, "Sweets", "?" :: Nil)
  , FeatureId("vegetables", "peas") -> FeatureMeta(IntEncoding, ContinuousType, "Greens", "?" :: Nil)
  ))

  def testEntities(n: Int): List[String] =
    (1 to n).toList.map(i => "T+%05d".format(i))

  def valueOf(encoding: Encoding): Gen[Value] = encoding match {
    case BooleanEncoding =>
      arbitrary[Boolean].map(BooleanValue)
    case IntEncoding =>
      arbitrary[Int].map(IntValue)
    case LongEncoding =>
      arbitrary[Long].map(LongValue)
    case DoubleEncoding =>
      arbitrary[Double].map(DoubleValue)
    case StringEncoding =>
      arbitrary[String].map(StringValue)
   }

  def factWithZoneGen(entity: Gen[String]): Gen[(Fact, DateTimeZone)] = for {
    e      <- entity
    (f, m) <- Gen.oneOf(TestDictionary.meta.toList)
    dtz    <- arbitrary[DateTimeWithZone]
    v      <- Gen.frequency(1 -> Gen.const(TombstoneValue()), 99 -> valueOf(m.encoding))
  } yield (Fact.newFact(e, f.namespace, f.name, dtz.datetime.date, dtz.datetime.time, v), dtz.zone)

  case class SparseEntities(fact: Fact, zone: DateTimeZone)
  case class DenseEntities(fact: Fact, zone: DateTimeZone)

  /**
   * Create an arbitrary fact and timezone such that the time in the fact is valid given the timezone
   */
  implicit def SparseEntitiesArbitrary: Arbitrary[SparseEntities] =
   Arbitrary(factWithZoneGen(Gen.oneOf(testEntities(1000))).map(SparseEntities.tupled))

  implicit def DenseEntitiesArbitrary: Arbitrary[DenseEntities] =
   Arbitrary(factWithZoneGen(Gen.oneOf(testEntities(50))).map(DenseEntities.tupled))

  implicit def FactArbitrary: Arbitrary[Fact] = Arbitrary(for {
    es <- arbitrary[SparseEntities]
  } yield es.fact)

  implicit def DateTimeZoneArbitrary: Arbitrary[DateTimeZone] = Arbitrary(for {
    zid <- Gen.oneOf(DateTimeZone.getAvailableIDs().asScala.toSeq)
  } yield DateTimeZone.forID(zid))
}
