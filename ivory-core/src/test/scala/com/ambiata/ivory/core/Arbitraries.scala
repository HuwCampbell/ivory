package com.ambiata.ivory.core

import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.data.Arbitraries._
import com.ambiata.ivory.data._

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
      r = Stream.tabulate(4)(i => Date.create(y.toShort, m.toByte, (d - i).toByte)).dropWhile(!_.isDefined).headOption.flatten
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
    FeatureId("fruit", "apple") -> FeatureMeta(LongEncoding, Some(ContinuousType), "Reds and Greens", "?" :: Nil)
  , FeatureId("fruit", "orange") -> FeatureMeta(StringEncoding, Some(CategoricalType), "Oranges", "?" :: Nil)
  , FeatureId("vegetables", "potatoe") -> FeatureMeta(DoubleEncoding, Some(ContinuousType), "Browns", "?" :: Nil)
  , FeatureId("vegetables", "yams") -> FeatureMeta(BooleanEncoding, Some(CategoricalType), "Sweets", "?" :: Nil)
  , FeatureId("vegetables", "peas") -> FeatureMeta(IntEncoding, Some(ContinuousType), "Greens", "?" :: Nil)
  ))

  case class FeatureNamespace(namespace: String)
  implicit def FeatureNamespaceArbitrary: Arbitrary[FeatureNamespace] =
    Arbitrary(Gen.oneOf(TestDictionary.meta.map(_._1.namespace).toSeq).map(FeatureNamespace.apply))

  def testEntities(n: Int): List[String] =
    (1 to n).toList.map(i => "T+%05d".format(i))

  implicit def DictionaryArbitrary: Arbitrary[Dictionary] =
    Arbitrary(Gen.mapOf[FeatureId, FeatureMeta](for {
      ns    <- arbitrary[DictId].map(_.s)
      name  <- arbitrary[DictId].map(_.s)
      enc   <- arbitrary[Encoding]
      ty    <- arbitrary[Option[Type]]
      desc  <- arbitrary[DictDesc].map(_.s)
      tombs <- Gen.listOf(arbitrary[DictTomb].map(_.s))
    } yield FeatureId(ns, name) -> FeatureMeta(enc, ty, desc, tombs)).map(Dictionary))

  implicit def EncodingArbitrary: Arbitrary[Encoding] =
    Arbitrary(Gen.oneOf(arbitrary[SubEncoding], arbitrary[ListEncoding]))

  implicit def SubEncodingArbitrary: Arbitrary[SubEncoding] =
    Arbitrary(Gen.oneOf(arbitrary[PrimitiveEncoding], arbitrary[StructEncoding]))

  implicit def PrimitiveEncodingArbitrary: Arbitrary[PrimitiveEncoding] =
    Arbitrary(Gen.oneOf(BooleanEncoding, IntEncoding, LongEncoding, DoubleEncoding, StringEncoding))

  implicit def StructEncodingArbitrary: Arbitrary[StructEncoding] =
    Arbitrary(Gen.mapOf[String, StructEncodedValue](for {
      name <- arbitrary[DictId].map(_.s)
      enc <- arbitrary[PrimitiveEncoding]
      optional <- arbitrary[Boolean]
    } yield name -> StructEncodedValue(enc, optional)).map(StructEncoding))

  implicit def ListEncodingArbitrary: Arbitrary[ListEncoding] =
    Arbitrary(arbitrary[SubEncoding].map(ListEncoding))

  implicit def TypeArbitrary: Arbitrary[Type] =
    Arbitrary(Gen.oneOf(NumericalType, ContinuousType, CategoricalType, BinaryType))

  def valueOf(encoding: Encoding): Gen[Value] = encoding match {
    case p: PrimitiveEncoding => valueOfPrim(p)
    case StructEncoding(s)    =>
      Gen.sequence[Seq, Option[(String, PrimitiveValue)]](s.map { case (k, v) =>
        for {
          p <- valueOfPrim(v.encoding).map(k ->)
          // _Sometimes_ generate a value for optional fields :)
          b <- if (v.optional) arbitrary[Boolean] else Gen.const(true)
        } yield if (b) Some(p) else None
      }).map(_.flatten.toMap).map(StructValue)
    case l: ListEncoding      => sys.error("List encoding not supported yet") // TODO
  }

  def valueOfPrim(encoding: PrimitiveEncoding): Gen[PrimitiveValue] = encoding match {
    case BooleanEncoding =>
      arbitrary[Boolean].map(BooleanValue)
    case IntEncoding =>
      arbitrary[Int].map(IntValue)
    case LongEncoding =>
      arbitrary[Long].map(LongValue)
    case DoubleEncoding =>
      arbitrary[Double].retryUntil(Value.validDouble).map(DoubleValue)
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

  case class DictId(s: String)
  case class DictDesc(s: String)
  case class DictTomb(s: String)

  implicit def DictIdArbitrary: Arbitrary[DictId] = Arbitrary(
    Gen.frequency(
      1 -> Gen.const("_"),
      99 -> Gen.listOf(Gen.alphaNumChar).map(_.mkString)
    ).map(DictId)
  )

  implicit def DictTombArbitrary: Arbitrary[DictTomb] =
    Arbitrary(arbitrary[DictDesc].map(_.s).retryUntil(s => !s.contains(",") && !s.contains("\"")).map(DictTomb))

  implicit def DictDescArbitrary: Arbitrary[DictDesc] =
    Arbitrary(arbitrary[String].map(_.trim).retryUntil(s => !s.contains("|") && !s.contains("\uFFFF") && s.forall(_ > 31)).map(DictDesc))

  implicit def FeatureStoreIdArbitrary: Arbitrary[FeatureStoreId] =
    Arbitrary(arbitrary[OldIdentifier].map(FeatureStoreId.apply))

  case class SmallFeatureStoreIdList(ids: List[FeatureStoreId])
  implicit def SmallFeatureStoreIdListArbitrary: Arbitrary[SmallFeatureStoreIdList] =
    Arbitrary(arbitrary[SmallOldIdentifierList].map(ids => SmallFeatureStoreIdList(ids.ids.map(FeatureStoreId.apply))))

  implicit def FeatureStoreArbitrary: Arbitrary[FeatureStore] = Arbitrary(
    arbitrary[OldIdentifierList].map(ids =>
      FeatureStore(ids.ids.zipWithIndex.map({ case (id, i) =>
         PrioritizedFactset(FactsetId(id.render), Priority.unsafe((i + 1).toShort))
      }).toList)
    ))

  implicit def SnapshotIdArbitrary: Arbitrary[SnapshotId] =
    Arbitrary(arbitrary[Identifier].map(SnapshotId.apply))

  case class SmallSnapshotIdList(ids: List[SnapshotId])
  implicit def SmallSnapshotIdListArbitrary: Arbitrary[SmallSnapshotIdList] =
    Arbitrary(arbitrary[SmallIdentifierList].map(ids => SmallSnapshotIdList(ids.ids.map(SnapshotId.apply))))

  case class EncodingAndValue(enc: Encoding, value: Value)

  implicit def EncodingAndValueArbitrary: Arbitrary[EncodingAndValue] = Arbitrary(for {
    enc   <- arbitrary[Encoding]
    value <- valueOf(enc)
  } yield EncodingAndValue(enc, value))

  implicit def FactsetArbitrary: Arbitrary[FactsetId] =
    Arbitrary(arbitrary[OldIdentifier].map(id => FactsetId(id.render)))

  implicit def PartitionArbitrary: Arbitrary[Partition] = Arbitrary(for {
    factset <- arbitrary[FactsetId]
    ns      <- arbitrary[FeatureNamespace]
    date    <- arbitrary[Date]
  } yield Partition(factset, ns.namespace, date, None))

  case class SingleFactsetPartitions(partitions: List[Partition])
  implicit def SingleFactsetPartitionsArbitrary: Arbitrary[SingleFactsetPartitions] =
    Arbitrary(for {
      factset <- arbitrary[FactsetId]
      dates   <- arbitrary[List[(Date, FeatureNamespace)]]
    } yield SingleFactsetPartitions(dates.distinct.map({ case (d, FeatureNamespace(ns)) => Partition(factset, ns, d, None) })))
}
