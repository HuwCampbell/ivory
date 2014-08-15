package com.ambiata.ivory.core

import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.data.Arbitraries._
import com.ambiata.ivory.data._

import org.joda.time.DateTimeZone
import scala.collection.JavaConverters._
import Gen._

import scalaz._, Scalaz._, scalacheck.ScalaCheckBinding._

object Arbitraries {
  case class Entity(value: String)
  implicit def EntityArbitrary: Arbitrary[Entity] =
    Arbitrary(Gen.identifier map Entity.apply)

  implicit def ValueArbitrary: Arbitrary[Value] =
    Arbitrary(Gen.frequency(
      1 -> (Gen.identifier map StringValue.apply)
    , 2 -> (arbitrary[Int] map IntValue.apply)
    , 2 -> (arbitrary[Long] map LongValue.apply)
    , 2 -> (arbitrary[Double] map DoubleValue.apply)
    , 2 -> (arbitrary[Boolean] map BooleanValue.apply)
    ))

  implicit def PriorityArbitrary: Arbitrary[Priority] =
    Arbitrary(Gen.choose(Priority.Min.toShort, Priority.Max.toShort).map(Priority.unsafe))

  def genDate(from: Date, to: Date): Gen[Date] = for {
    y <- Gen.choose(from.year, to.year)
    m <- Gen.choose(from.month, to.month)
    d <- Gen.choose(from.day, to.day)
    r = Stream.tabulate(4)(i => Date.create(y.toShort, m.toByte, (d - i).toByte)).dropWhile(!_.isDefined).headOption.flatten
    if r.isDefined
  } yield r.get

  implicit def DateArbitrary: Arbitrary[Date] =
    Arbitrary(genDate(Date(1970, 1, 1), Date(3000, 12, 31)))

  case class TwoDifferentDates(earlier: Date, later: Date)
  implicit def TwoDifferentDatesArbitrary: Arbitrary[TwoDifferentDates] =
    Arbitrary(for {
      d1     <- genDate(Date(1970, 1, 1), Date(2100, 12, 31))
      offset <- Gen.choose(1, 100)
      d2 = Date.fromLocalDate(d1.localDate.plusDays(offset))
      dd = TwoDifferentDates(d1, d2)
    } yield dd)

  /* Generate a distinct list of Dates up to size n */
  def genDates(nDates: Gen[Int]): Gen[List[Date]] =
    nDates.flatMap(n => Gen.listOfN(n, arbitrary[Date])).map(_.distinct)

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

  case class FeatureNamespace(namespace: Name)
  def genFeatureNamespace: Gen[FeatureNamespace] =
    NameArbitrary.arbitrary.map(FeatureNamespace.apply)

  implicit def FeatureNamespaceArbitrary: Arbitrary[FeatureNamespace] =
    Arbitrary(genFeatureNamespace)

  /* Generate a distinct list of FeatureNamespaces up to size n */
  def genFeatureNamespaces(n: Gen[Int]): Gen[List[FeatureNamespace]] =
    n.flatMap(n => Gen.listOfN(n, genFeatureNamespace).map(_.distinct))

  def testEntityId(i: Int): String =
    "T+%05d".format(i)

  def testEntities(n: Int): List[String] =
    (1 to n).toList.map(testEntityId)

  implicit def FeatureIdArbitrary: Arbitrary[FeatureId] =
    Arbitrary(for {
      ns    <- arbitrary[Name]
      name  <- arbitrary[DictId].map(_.s)
    } yield FeatureId(ns, name))

  def featureMetaGen(genc: Gen[Encoding]): Gen[FeatureMeta] =
    for {
      enc   <- genc
      ty    <- arbitrary[Option[Type]]
      desc  <- arbitrary[DictDesc].map(_.s)
      tombs <- Gen.listOf(arbitrary[DictTomb].map(_.s))
    } yield FeatureMeta(enc, ty, desc, tombs)

  implicit def FeatureMetaArbitrary: Arbitrary[FeatureMeta] =
    Arbitrary(featureMetaGen(arbitrary[Encoding]))

  implicit def DictionaryArbitrary: Arbitrary[Dictionary] =
    Arbitrary(Gen.mapOf[FeatureId, FeatureMeta](arbitrary[(FeatureId, FeatureMeta)]).map(Dictionary))

  implicit def EncodingArbitrary: Arbitrary[Encoding] =
    Arbitrary(Gen.oneOf(arbitrary[SubEncoding], arbitrary[ListEncoding]))

  implicit def SubEncodingArbitrary: Arbitrary[SubEncoding] =
    Arbitrary(Gen.oneOf(arbitrary[PrimitiveEncoding], arbitrary[StructEncoding]))

  implicit def PrimitiveEncodingArbitrary: Arbitrary[PrimitiveEncoding] =
    Arbitrary(Gen.oneOf(BooleanEncoding, IntEncoding, LongEncoding, DoubleEncoding, StringEncoding))

  // TODO needs review
  implicit def StructEncodingArbitrary: Arbitrary[StructEncoding] =
    Arbitrary(Gen.choose(1, 5).flatMap(n => Gen.mapOfN[String, StructEncodedValue](n, for {
      name     <- arbitrary[DictId].map(_.s)
      enc      <- arbitrary[PrimitiveEncoding]
      optional <- arbitrary[Boolean]
    } yield name -> StructEncodedValue(enc, optional)).map(StructEncoding)))

  implicit def ListEncodingArbitrary: Arbitrary[ListEncoding] =
    Arbitrary(arbitrary[SubEncoding].map(ListEncoding))

  implicit def TypeArbitrary: Arbitrary[Type] =
    Arbitrary(Gen.oneOf(NumericalType, ContinuousType, CategoricalType, BinaryType))

  def valueOf(encoding: Encoding, tombstones: List[String]): Gen[Value] = encoding match {
    case sub: SubEncoding  => valueOfSub(sub, tombstones)
    case ListEncoding(sub) => Gen.listOf(valueOfSub(sub, tombstones)).map(ListValue)
  }

  def valueOfSub(encoding: SubEncoding, tombstones: List[String]): Gen[SubValue] = encoding match {
    case p: PrimitiveEncoding => valueOfPrim(p, tombstones)
    case StructEncoding(s)    =>
      Gen.sequence[Seq, Option[(String, PrimitiveValue)]](s.map { case (k, v) =>
        for {
          p <- valueOfPrim(v.encoding, tombstones).map(k ->)
          // _Sometimes_ generate a value for optional fields :)
          b <- if (v.optional) arbitrary[Boolean] else Gen.const(true)
        } yield if (b) Some(p) else None
      }).map(_.flatten.toMap).map(StructValue)
  }

  def valueOfPrim(encoding: PrimitiveEncoding, tombstones: List[String]): Gen[PrimitiveValue] = (encoding match {
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
   }).flatMap { v =>
    if (Value.toStringPrimitive(v).exists(tombstones.contains))
      if (tombstones.nonEmpty) Gen.const(TombstoneValue())
      // There's really nothing we can do here - need to try again
      else valueOfPrim(encoding, tombstones)
    else Gen.const(v)
  }

  def factWithZoneGen(entity: Gen[String], mgen: Gen[FeatureMeta]): Gen[(FeatureMeta, Fact, DateTimeZone)] = for {
    e      <- entity
    f      <- arbitrary[FeatureId]
    m      <- mgen
    dtz    <- arbitrary[DateTimeWithZone]
    // Don't generate a Tombstone if it's not possible
    v      <- Gen.frequency((if (m.tombstoneValue.nonEmpty) 1 else 0) -> Gen.const(TombstoneValue()), 99 -> valueOf(m.encoding, m.tombstoneValue))
  } yield (m, Fact.newFact(e, f.namespace.name, f.name, dtz.datetime.date, dtz.datetime.time, v), dtz.zone)

  /** All generated SparseEntities will have a large range of possible entity id's */
  case class SparseEntities(meta: FeatureMeta, fact: Fact, zone: DateTimeZone)

  /**
   * Create an arbitrary fact and timezone such that the time in the fact is valid given the timezone
   */
  implicit def SparseEntitiesArbitrary: Arbitrary[SparseEntities] =
   Arbitrary(factWithZoneGen(Gen.choose(0, 1000).map(testEntityId), arbitrary[FeatureMeta]).map(SparseEntities.tupled))

  implicit def FactArbitrary: Arbitrary[Fact] =
    Arbitrary(arbitrary[SparseEntities].map(_.fact))

  implicit def DateTimeZoneArbitrary: Arbitrary[DateTimeZone] = Arbitrary(for {
    zid <- Gen.oneOf(DateTimeZone.getAvailableIDs().asScala.toSeq)
  } yield DateTimeZone.forID(zid))

  case class DictId(s: String)
  case class DictDesc(s: String)
  case class DictTomb(s: String)

  case class GoodNameString(name: String)
  case class BadNameString(name: String)
  case class RandomNameString(name: String)

  implicit def NameArbitrary: Arbitrary[Name] = Arbitrary(
    for {
      firstCharacter  <- frequency(4 -> const('-'), 96 -> alphaNumChar)
      otherCharacters <- nonEmptyListOf(frequency(2 -> const('_'), 2  -> const('-'), 96 -> alphaNumChar))
    } yield Name.reviewed((firstCharacter +: otherCharacters).mkString)
  )

  implicit def BadNameStringArbitrary: Arbitrary[BadNameString] = Arbitrary {
    oneOf("", "_name", "name1/name2", "name㭊").map(BadNameString)
  }

  implicit def GoodNameStringArbitrary: Arbitrary[GoodNameString] = Arbitrary {
    NameArbitrary.arbitrary.map(n => GoodNameString(n.name))
  }

  implicit def RandomNameStringArbitrary: Arbitrary[RandomNameString] = Arbitrary {
    frequency((50, BadNameStringArbitrary.arbitrary.map(_.name)), (50, GoodNameStringArbitrary.arbitrary.map(_.name)))
      .map(RandomNameString)
  }

  implicit def DictIdArbitrary: Arbitrary[DictId] = Arbitrary(
    Gen.nonEmptyListOf(Gen.frequency(
      1 -> Gen.const("_"),
      99 -> Gen.alphaNumChar
    )).map(_.mkString).map(DictId)
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

  implicit def FeatureStoreArbitrary: Arbitrary[FeatureStore] = Arbitrary(for {
    storeId    <- arbitrary[FeatureStoreId]
    factsets   <- genFactsetList(Gen.choose(1, 3))
  } yield FeatureStore.fromList(storeId, factsets).get)

  implicit def SnapshotIdArbitrary: Arbitrary[SnapshotId] =
    Arbitrary(arbitrary[Identifier].map(SnapshotId.apply))

  case class SmallSnapshotIdList(ids: List[SnapshotId])
  implicit def SmallSnapshotIdListArbitrary: Arbitrary[SmallSnapshotIdList] =
    Arbitrary(arbitrary[SmallIdentifierList].map(ids => SmallSnapshotIdList(ids.ids.map(SnapshotId.apply))))

  case class EncodingAndValue(enc: Encoding, value: Value)

  implicit def EncodingAndValueArbitrary: Arbitrary[EncodingAndValue] = Arbitrary(for {
    enc   <- arbitrary[Encoding]
    value <- valueOf(enc, List())
  } yield EncodingAndValue(enc, value))

  implicit def FactsetIdArbitrary: Arbitrary[FactsetId] =
    Arbitrary(arbitrary[OldIdentifier].map(id => FactsetId(id)))

  def genFactsetIds(ids: Gen[Int]): Gen[List[FactsetId]] =
    ids.map(createOldIdentifiers).map(ids => ids.map(FactsetId.apply))

  /* List of FactsetIds with a size between 0 and 10 */
  case class FactsetIdList(ids: List[FactsetId])
  implicit def FactsetIdListArbitrary: Arbitrary[FactsetIdList] =
    Arbitrary(genFactsetIds(Gen.choose(0, 10)).map(FactsetIdList.apply))

  /* Generate a Factset with number of partitions up to n namespaces x n dates */
  def genFactset(factsetId: FactsetId, nNamespaces: Gen[Int], nDates: Gen[Int]): Gen[Factset] =
    genPartitions(nNamespaces, nDates).map(ps => Factset(factsetId, ps))

  /* Factset with up to 3 x 3 partitions */
  implicit def FactsetArbitrary: Arbitrary[Factset] =
    Arbitrary(for {
      id <- arbitrary[FactsetId]
      fs <- genFactset(id, Gen.choose(1, 3), Gen.choose(1, 3))
    } yield fs)

  /* Generate a list of Factset's, each with up to 3 x 3 partitions */
  def genFactsetList(size: Gen[Int]): Gen[List[Factset]] = for {
    ids <- genFactsetIds(size)
    fs  <- ids.traverse(id => genFactset(id, Gen.choose(1, 3), Gen.choose(1, 3)))
  } yield fs

  /* List of Factsets with size between 0 and 3. Each Factset has up to 3 x 3 partitions */
  case class FactsetList(factsets: List[Factset])
  implicit def FactsetListArbitrary: Arbitrary[FactsetList] =
    Arbitrary(genFactsetList(Gen.choose(0, 3)).map(FactsetList.apply))

  implicit def PartitionArbitrary: Arbitrary[Partition] = Arbitrary(for {
    ns      <- arbitrary[FeatureNamespace]
    date    <- arbitrary[Date]
  } yield Partition(ns.namespace, date))

  /* Generate a list of Partitions with the size up to n namespaces x n dates */
  def genPartitions(nNamespaces: Gen[Int], nDates: Gen[Int]): Gen[Partitions] = for {
    namespaces <- genFeatureNamespaces(nNamespaces)
    partitions <- namespaces.traverse(ns => genDates(nDates).map(_.map(d => Partition(ns.namespace, d))))
  } yield Partitions(partitions.flatten)

  /* Partitions with size up to 3 x 5 */
  implicit def PartitionsArbitrary: Arbitrary[Partitions] =
    Arbitrary(genPartitions(Gen.choose(1, 3), Gen.choose(1, 5)))
}
