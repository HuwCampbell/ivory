package com.ambiata.ivory.core

import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.data.Arbitraries._
import com.ambiata.ivory.data._

import org.joda.time.{DateTimeZone, Days => JodaDays}
import scala.collection.JavaConverters._
import Gen._

import scalaz._, Scalaz._, scalacheck.ScalaCheckBinding._

object Arbitraries extends arbitraries.ArbitrariesDictionary {
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
    , 2 -> (arbitrary[Date] map DateValue.apply)
    ))

  implicit def PriorityArbitrary: Arbitrary[Priority] =
    Arbitrary(Gen.choose(Priority.Min.toShort, Priority.Max.toShort).map(Priority.unsafe))

  def genDate(from: Date, to: Date): Gen[Date] =
     Gen.choose(0, JodaDays.daysBetween(from.localDate, to.localDate).getDays)
       .map(y => Date.fromLocalDate(from.localDate.plusDays(y)))

  implicit def DateArbitrary: Arbitrary[Date] =
    Arbitrary(genDate(Date(1970, 1, 1), Date(3000, 12, 31)))

  case class UniqueDates(earlier: Date, now: Date, later: Date)
  implicit def TwoDifferentDatesArbitrary: Arbitrary[UniqueDates] =
    Arbitrary(for {
      d1 <- genDate(Date(1970, 1, 1), Date(2100, 12, 31))
      d2 <- Gen.frequency(5 -> Date.minValue, 95 -> Gen.choose(1, 100).map(o => Date.fromLocalDate(d1.localDate.minusDays(o))))
      d3 <- Gen.frequency(5 -> Date.maxValue, 95 -> Gen.choose(1, 100).map(o => Date.fromLocalDate(d1.localDate.plusDays(o))))
    } yield UniqueDates(d2, d1, d3))

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

  def featureMetaGen(genc: Gen[Encoding]): Gen[ConcreteDefinition] =
    for {
      enc   <- genc
      ty    <- arbitrary[Option[Type]]
      desc  <- arbitrary[DictDesc].map(_.s)
      tombs <- Gen.listOf(arbitrary[DictTomb].map(_.s))
    } yield ConcreteDefinition(enc, ty, desc, tombs)


  case class DefinitionWithQuery(cd: ConcreteDefinition, expression: Expression, filter: FilterEncoded)
  case class FactsWithQuery(filter: DefinitionWithQuery, facts: List[Fact], other: List[Fact])

  implicit def DefinitionWithFilterArb: Arbitrary[DefinitionWithQuery] = Arbitrary(for {
    cd <- featureMetaGen(Gen.oneOf(arbitrary[PrimitiveEncoding], arbitrary[StructEncoding]))
    e  <- expressionArbitrary(cd)
    f  <- arbitraryFilter(cd)
  } yield DefinitionWithQuery(cd, e, f.get))

  implicit def FactsWithFilterArb: Arbitrary[FactsWithQuery] = Arbitrary(for {
    d <- arbitrary[DefinitionWithQuery]
    n <- Gen.choose(0, 10)
    entities = Gen.choose(0, 1000).map(testEntityId)
    f <- Gen.listOfN(n, for {
      f <- factWithZoneGen(entities, Gen.const(d.cd))
      // This is probably going to get pretty hairy when we add more filter operations
      v  = d.filter.fold({
        case FilterEquals(ev) => ev
      })(identity, (k, v) => StructValue(Map(k -> v))) {
        case (op, h :: t) => op.fold(t.foldLeft(h) {
          case (StructValue(m1), StructValue(m2)) => StructValue(m1 ++ m2)
          case (a, _) => a
        }, h)
        case (_, Nil)     => StringValue("")
      }
    } yield f._2.withValue(v))
    o  <- Gen.choose(0, 10).flatMap(n => Gen.listOfN(n, factWithZoneGen(entities, Gen.const(d.cd)).map(_._2)).map(_.filterNot {
      // Filter out facts that would otherwise match
      fact => FilterTester.eval(d.filter, fact)
    }))
  } yield FactsWithQuery(d, f, o))

  def expressionArbitrary(cd: ConcreteDefinition): Gen[Expression] = {
    val fallback = Gen.frequency(
      15 -> Gen.oneOf(Count, Latest, DaysSinceLatest, DaysSinceEarliest, MeanInDays, MaximumInDays, MinimumInDays,
        MeanInWeeks, MaximumInWeeks, MinimumInWeeks, CountDays),
      1 -> (for {
        q <- Gen.choose(10, 100)
        k <- Gen.choose(1, q)
        e <- Gen.oneOf(QuantileInDays(k, q), QuantileInWeeks(k, q))
      } yield e)
    )
    Gen.oneOf(fallback, cd.encoding match {
      case StructEncoding(values) =>
        val subexpGen = Gen.oneOf(values.toList).flatMap {
          case (name, sve) => subExpressionArbitrary(sve.encoding).map(se => StructExpression(name, se))
        }
        // SumBy is a little more complicated
        (for {
          se <- values.find(_._2.encoding == StringEncoding)
          ie <- values.find(v => List(IntEncoding, LongEncoding, DoubleEncoding).contains(v._2.encoding))
        } yield SumBy(se._1, ie._1)).cata(sumBy => Gen.frequency(5 -> Gen.const(sumBy), 5 -> subexpGen), subexpGen)
      case p: PrimitiveEncoding   => subExpressionArbitrary(p).map(BasicExpression)
      case l: ListEncoding        => fallback
    })
  }

  def subExpressionArbitrary(pe: PrimitiveEncoding): Gen[SubExpression] = {
    val all = Gen.const(NumFlips)
    val numeric = Gen.oneOf(Sum, Mean, Gradient, StandardDeviation)
    pe match {
      case IntEncoding =>
        Gen.oneOf(Gen.const(CountBy), numeric, all)
      case LongEncoding | DoubleEncoding =>
        Gen.oneOf(numeric, all)
      case StringEncoding => Gen.oneOf(
        Gen.oneOf(DaysSinceLatestBy, DaysSinceEarliestBy, CountBy, CountUnique),
        Gen.identifier.map(Proportion.apply),
        all
      )
      case BooleanEncoding => Gen.oneOf(all, arbitrary[Boolean].map(b => Proportion(b.toString)))
      case DateEncoding => all
    }
  }

  /** You can't generate a filter without first knowing what fields exist for this feature */
  def arbitraryFilter(cd: ConcreteDefinition): Gen[Option[FilterEncoded]] = {

    def arbitraryFilterExpression(encoding: PrimitiveEncoding): Gen[FilterExpression] =
      valueOfPrim(encoding).flatMap {
        case StringValue(s) => Gen.identifier.map(StringValue.apply) // Just for now keep this _really_ simple
        case v              => Gen.const(v)
      }.map(FilterEquals.apply)

    cd.encoding match {
      case se: StructEncoding =>
        def sub(maxChildren: Int, left: Map[String, StructEncodedValue]): Gen[FilterStructOp] =
          for {
            // Be careful in this section - ScalaCheck will discard values if asking for move than is contained
            // in a list, which can break some of the MR specs that have a low minTestsOk value (eg SquashSpec).
            op     <- Gen.oneOf(FilterOpAnd, FilterOpOr)
            // Make sure we have a at least one value
            sev    <- Gen.choose(1, left.size).flatMap(i => Gen.pick(i, left))
            fields <- Gen.sequence[Seq, (String, FilterExpression)](sev.map {
              case (name, StructEncodedValue(enc, _)) => arbitraryFilterExpression(enc).map(name ->)
            }).map(_.toList)
            // For 'and' we can only see each key once
            cn     <- op.fold(Gen.const(1), Gen.choose(0, maxChildren))
            keys    = op.fold(sev.map(_._1), Nil)
            subvs   = left -- keys
            chlds  <- Gen.listOfN(Math.min(subvs.size, cn), sub(maxChildren - 1, subvs))
          } yield FilterStructOp(op, fields, chlds)
        sub(2, se.values).map(FilterStruct).map(some)
      case pe: PrimitiveEncoding =>
        def sub(maxChildren: Int): Gen[FilterValuesOp] =
          for {
            op     <- Gen.oneOf(FilterOpAnd, FilterOpOr)
            // Make sure we have at least one value
            // For non-struct values it's impossible to equal more than one value
            n      <- Gen.choose(1, op.fold(1, 3))
            fields <- Gen.listOfN(n, arbitraryFilterExpression(pe))
            cn     <- op.fold(Gen.const(0), Gen.choose(0, maxChildren))
            chlds  <- Gen.listOfN(cn, sub(maxChildren - 1))
          } yield FilterValuesOp(op, fields, chlds)
        sub(2).map(FilterValues).map(some)
      // We don't support list encoding at the moment
      case _: ListEncoding => Gen.const(none)
    }
  }

  implicit def WindowArbitrary: Arbitrary[Window] = Arbitrary(for {
    length <- GenPlus.posNum[Int]
    unit   <- Gen.oneOf(Days, Weeks, Months, Years)
  } yield Window(length, unit))

  def virtualDefGen(gen: (FeatureId, ConcreteDefinition)): Gen[(FeatureId, VirtualDefinition)] = for {
    fid        <- arbitrary[FeatureId]
    exp        <- expressionArbitrary(gen._2)
    filter     <- arbitraryFilter(gen._2)
    window     <- arbitrary[Option[Window]]
    query       = Query(exp, filter.map(FilterTextV0.asString).map(_.render).map(Filter.apply))
  } yield (fid, VirtualDefinition(gen._1, query, window))

  implicit def FeatureMetaArbitrary: Arbitrary[ConcreteDefinition] =
    Arbitrary(featureMetaGen(arbitrary[Encoding]))

  implicit def DictionaryArbitrary: Arbitrary[Dictionary] =
    Arbitrary(for {
      n <- Gen.choose(10, 20)
      i <- Gen.listOfN(n, arbitrary[FeatureId]).map(_.distinct)
      c <- Gen.listOfN(i.length, arbitrary[ConcreteDefinition]).map(cds => i.zip(cds))
      // For every concrete definition there is a chance we may have a virtual feature
      v <- c.traverse(x => Gen.frequency(
        70 -> Gen.const(None),
        30 -> virtualDefGen(x).map(some).map(_.filterNot(vd => i.contains(vd._1))))
      ).map(_.flatten)
    } yield Dictionary(c.map({ case (f, d) => d.toDefinition(f) }) ++ v.map({ case (f, d) => d.toDefinition(f) })))

  implicit def EncodingArbitrary: Arbitrary[Encoding] =
    Arbitrary(Gen.oneOf(arbitrary[SubEncoding], arbitrary[ListEncoding]))

  implicit def SubEncodingArbitrary: Arbitrary[SubEncoding] =
    Arbitrary(Gen.oneOf(arbitrary[PrimitiveEncoding], arbitrary[StructEncoding]))

  implicit def PrimitiveEncodingArbitrary: Arbitrary[PrimitiveEncoding] =
    Arbitrary(Gen.oneOf(BooleanEncoding, IntEncoding, LongEncoding, DoubleEncoding, StringEncoding, DateEncoding))

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
    case p: PrimitiveEncoding => valueOfPrimOrTomb(p, tombstones)
    case sub: SubEncoding  => valueOfSub(sub)
    case ListEncoding(sub) => Gen.listOf(valueOfSub(sub)).map(ListValue)
  }

  def valueOfSub(encoding: SubEncoding): Gen[SubValue] = encoding match {
    case p: PrimitiveEncoding => valueOfPrim(p)
    case StructEncoding(s)    =>
      Gen.sequence[Seq, Option[(String, PrimitiveValue)]](s.map { case (k, v) =>
        for {
          p <- valueOfPrim(v.encoding).map(k ->)
          // _Sometimes_ generate a value for optional fields :)
          b <- if (v.optional) arbitrary[Boolean] else Gen.const(true)
        } yield if (b) Some(p) else None
      }).map(_.flatten.toMap).map(StructValue)
  }

  def valueOfPrimOrTomb(encoding: PrimitiveEncoding, tombstones: List[String]): Gen[Value] =
    valueOfPrim(encoding).flatMap { v =>
      if (tombstones.contains(Value.toStringPrimitive(v)))
        if (tombstones.nonEmpty) Gen.const(TombstoneValue)
        // There's really nothing we can do here - need to try again
        else valueOfPrimOrTomb(encoding, tombstones)
      else Gen.const(v)
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
      // We shouldn't be stripping these out but we need to encode our output first...
      // https://github.com/ambiata/ivory/issues/353
      arbitrary[String].map(_.filter(c => c > 31 && c != '|')).map(StringValue)
    case DateEncoding =>
      arbitrary[Date].map(DateValue)
   }

  def factWithZoneGen(entity: Gen[String], mgen: Gen[ConcreteDefinition]): Gen[(ConcreteDefinition, Fact, DateTimeZone)] = for {
    f      <- arbitrary[FeatureId]
    m      <- mgen
    dtz    <- arbitrary[DateTimeWithZone]
    f      <- factGen(entity, f, m, dtz.datetime)
  } yield (m, f, dtz.zone)

  def factGen(entity: Gen[String], f: FeatureId, m: ConcreteDefinition, dt: DateTime): Gen[Fact] = for {
    e      <- entity
    // Don't generate a Tombstone if it's not possible
    v      <- Gen.frequency((if (m.tombstoneValue.nonEmpty) 1 else 0) -> Gen.const(TombstoneValue), 99 -> valueOf(m.encoding, m.tombstoneValue))
  } yield Fact.newFact(e, f.namespace.name, f.name, dt.date, dt.time, v)

  /** All generated SparseEntities will have a large range of possible entity id's */
  case class SparseEntities(meta: ConcreteDefinition, fact: Fact, zone: DateTimeZone)

  /** Facts for a _single_ [[ConcreteDefinition]] (feel free to generate a [[List]] of them if you need more) */
  case class FactsWithDictionary(cg: ConcreteGroupFeature, facts: List[Fact]) {
    def dictionary: Dictionary = cg.dictionary
  }

  /**
   * Create an arbitrary fact and timezone such that the time in the fact is valid given the timezone
   */
  implicit def SparseEntitiesArbitrary: Arbitrary[SparseEntities] =
   Arbitrary(factWithZoneGen(Gen.choose(0, 1000).map(testEntityId), arbitrary[ConcreteDefinition]).map(SparseEntities.tupled))

  implicit def FactsWithDictionaryArbitrary: Arbitrary[FactsWithDictionary] = Arbitrary(for {
    cg    <- arbitrary[ConcreteGroupFeature]
    n     <- Gen.choose(2, 10)
    dt    <- arbitrary[DateTime]
    facts <- Gen.listOfN(n, factGen(Gen.choose(0, 1000).map(testEntityId), cg.fid, cg.cg.definition, dt))
  } yield FactsWithDictionary(cg, facts))

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
      otherCharacters <- GenPlus.nonEmptyListOf(frequency(2 -> const('_'), 2  -> const('-'), 96 -> alphaNumChar))
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
    GenPlus.nonEmptyListOf(Gen.frequency(
      1 -> Gen.const("_"),
      99 -> Gen.alphaNumChar
    )).map(_.mkString).map(DictId)
  )

  implicit def DictTombArbitrary: Arbitrary[DictTomb] =
    Arbitrary(arbitrary[DictDesc].map(_.s).retryUntil(s => !s.contains(",") && !s.contains("\"")).map(DictTomb))

  implicit def DictDescArbitrary: Arbitrary[DictDesc] =
    Arbitrary(Gen.identifier.map(_.trim).retryUntil(s => !s.contains("|") && !s.contains("\uFFFF") && s.forall(_ > 31)).map(DictDesc))

  implicit def FeatureStoreIdArbitrary: Arbitrary[FeatureStoreId] =
    Arbitrary(arbitrary[OldIdentifier].map(FeatureStoreId.apply))

  case class SmallFeatureStoreIdList(ids: List[FeatureStoreId])
  implicit def SmallFeatureStoreIdListArbitrary: Arbitrary[SmallFeatureStoreIdList] =
    Arbitrary(arbitrary[SmallOldIdentifierList].map(ids => SmallFeatureStoreIdList(ids.ids.map(FeatureStoreId.apply))))

  case class SmallCommitIdList(ids: List[CommitId])
  implicit def SmallCommitIdListArbitrary: Arbitrary[SmallCommitIdList] =
    Arbitrary(arbitrary[SmallIdentifierList].map(ids => SmallCommitIdList(ids.ids.map(CommitId.apply))))

  implicit def FeatureStoreArbitrary: Arbitrary[FeatureStore] = Arbitrary(for {
    storeId    <- arbitrary[FeatureStoreId]
    factsets   <- genFactsetList(Gen.choose(1, 3))
  } yield FeatureStore.fromList(storeId, factsets).get)

  implicit def DictionaryIdArbitrary: Arbitrary[DictionaryId] =
    Arbitrary(arbitrary[Identifier].map(DictionaryId.apply))

  implicit def CommitIdArbitrary: Arbitrary[CommitId] = Arbitrary(for {
    x <- arbitrary[Identifier]
  } yield CommitId(x))

  implicit def CommitArbitrary: Arbitrary[Commit] = Arbitrary(for {
    dictId     <- arbitrary[DictionaryId]
    fsid       <- arbitrary[FeatureStoreId]
  } yield Commit(dictId, fsid))

  implicit def SnapshotIdArbitrary: Arbitrary[SnapshotId] =
    Arbitrary(arbitrary[Identifier].map(SnapshotId.apply))

  case class SmallSnapshotIdList(ids: List[SnapshotId])
  implicit def SmallSnapshotIdListArbitrary: Arbitrary[SmallSnapshotIdList] =
    Arbitrary(arbitrary[SmallIdentifierList].map(ids => SmallSnapshotIdList(ids.ids.map(SnapshotId.apply))))

  case class EncodingAndValue(enc: Encoding, value: Value)

  implicit def EncodingAndValueArbitrary: Arbitrary[EncodingAndValue] = Arbitrary(for {
    enc   <- arbitrary[Encoding]
    value <- Gen.frequency(19 -> valueOf(enc, List()), 1 -> Gen.const(TombstoneValue))
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

  case class FactAndPriority(f: Fact, p: Priority)

  implicit def ArbitraryFactAndPriority: Arbitrary[FactAndPriority] = Arbitrary(for {
    f <- Arbitrary.arbitrary[Fact]
    p <- Arbitrary.arbitrary[Priority]
  } yield new FactAndPriority(f, p))
}
