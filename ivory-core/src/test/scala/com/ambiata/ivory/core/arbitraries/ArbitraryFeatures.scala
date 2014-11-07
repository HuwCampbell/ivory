package com.ambiata.ivory.core
package arbitraries

import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}

import scalaz.Scalaz._
import ArbitraryValues._
import ArbitraryDictionaries._
import ArbitraryEncodings._
import ArbitraryMetadata._

/**
 * Arbitraries for generating features
 */
trait ArbitraryFeatures {

  implicit def FeatureNamespaceArbitrary: Arbitrary[FeatureNamespace] =
    Arbitrary(genFeatureNamespace)

  implicit def FeatureIdArbitrary: Arbitrary[FeatureId] =
    Arbitrary(for {
      ns <- arbitrary[Name]
      name <- arbitrary[DictId].map(_.s)
    } yield FeatureId(ns, name))

  implicit def ConcreteGroupFeatureArbitrary: Arbitrary[ConcreteGroupFeature] = Arbitrary(for {
    fid <- arbitrary[FeatureId]
    cd  <- arbitrary[ConcreteDefinition]
    vi <- Gen.choose(0, 3)
    vd <- Gen.listOfN(vi, virtualDefGen(fid -> cd))
  } yield ConcreteGroupFeature(fid, ConcreteGroup(cd, vd.toMap.mapValues(_.copy(source = fid)).toList)))

  implicit def DefinitionWithFilterArb: Arbitrary[DefinitionWithQuery] = Arbitrary(for {
    cd <- concreteDefinitionGen(Gen.oneOf(arbitrary[PrimitiveEncoding], arbitrary[StructEncoding]))
    e <- expressionArbitrary(cd)
    f <- arbitraryFilter(cd)
  } yield DefinitionWithQuery(cd, e, f.get))

  implicit def WindowArbitrary: Arbitrary[Window] = Arbitrary(for {
    length <- GenPlus.posNum[Int]
    unit <- Gen.oneOf(Days, Weeks, Months, Years)
  } yield Window(length, unit))

  implicit def ConcreteDefinitionArbitrary: Arbitrary[ConcreteDefinition] =
    Arbitrary(concreteDefinitionGen(arbitrary[Encoding]))

  def genFeatureNamespace: Gen[FeatureNamespace] =
    NameArbitrary.arbitrary.map(FeatureNamespace.apply)

  /* Generate a distinct list of FeatureNamespaces up to size n */
  def genFeatureNamespaces(n: Gen[Int]): Gen[List[FeatureNamespace]] =
    n.flatMap(n => Gen.listOfN(n, genFeatureNamespace).map(_.distinct))

  def testEntityId(i: Int): String =
    "T+%05d".format(i)

  def testEntities(n: Int): List[String] =
    (1 to n).toList.map(testEntityId)

  def concreteDefinitionGen(genc: Gen[Encoding]): Gen[ConcreteDefinition] =
    for {
      enc <- genc
      mode <- arbitrary[Mode]
      ty <- arbitrary[Option[Type]]
      desc <- arbitrary[DictDesc].map(_.s)
      tombs <- Gen.listOf(arbitrary[DictTomb].map(_.s))
    } yield ConcreteDefinition(enc, mode, ty, desc, tombs)

  def expressionArbitrary(cd: ConcreteDefinition): Gen[Expression] = {
    val fallback = Gen.frequency(
      15 -> Gen.oneOf(Count, DaysSinceLatest, DaysSinceEarliest, MeanInDays, MaximumInDays, MinimumInDays,
        MeanInWeeks, MaximumInWeeks, MinimumInWeeks, CountDays, Interval(Min), Interval(Mean), Interval(Max), Interval(Gradient), Interval(StandardDeviation)),
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
        // SumBy and CountBySecondary are a little more complicated
        (for {
          se <- values.find(_._2.encoding == StringEncoding).map(_._1)
          ie <- values.find(v => v._1 != se && List(StringEncoding).contains(v._2.encoding)).map(ie => CountBySecondary(se, ie._1)) orElse
            values.find(v => List(IntEncoding, LongEncoding, DoubleEncoding).contains(v._2.encoding)).map(ie => SumBy(se, ie._1))
        } yield ie).cata(v => Gen.frequency(5 -> Gen.const(v), 5 -> subexpGen), subexpGen)
      case p: PrimitiveEncoding   => subExpressionArbitrary(p).map(BasicExpression)
      case l: ListEncoding        => fallback
    })
  }

  def subExpressionArbitrary(pe: PrimitiveEncoding): Gen[SubExpression] = {
    val all = Gen.oneOf(Latest, NumFlips)
    val numeric = Gen.oneOf(Sum, Min, Mean, Max, Gradient, StandardDeviation)
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
      case DateEncoding => Gen.oneOf(all, Gen.const(DaysSince))
    }
  }


  /** You can't generate a filter without first knowing what fields exist for this feature */
  def arbitraryFilter(cd: ConcreteDefinition): Gen[Option[FilterEncoded]] = {

    def arbitraryFilterExpression(encoding: PrimitiveEncoding): Gen[FilterExpression] =
      valueOfPrim(encoding).flatMap {
        case StringValue(s) => Gen.identifier.map(StringValue.apply) // Just for now keep this _really_ simple
        case v              => Gen.const(v)
      }.flatMap { x => if (x match {
        // It's a little tricky to do anything else
        case BooleanValue(v) => false
        // Make sure we don't make the < or > impossible
        case DoubleValue(v)  => Double.MinValue <  v && v < Double.MaxValue
        case IntValue(v)     => Int.MinValue < v && v < Int.MaxValue
        case LongValue(v)    => Long.MinValue < v && v < Long.MinValue
        case DateValue(v)    => Date.minValue < v && v < Date.maxValue
        case StringValue(v)  => v != ""
      }) Gen.oneOf(
        FilterEquals(x), FilterLessThan(x), FilterLessThanOrEqual(x), FilterGreaterThan(x), FilterGreaterThanOrEqual(x)
      ) else Gen.const(FilterEquals(x))}

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

  def virtualDefGen(gen: (FeatureId, ConcreteDefinition)): Gen[(FeatureId, VirtualDefinition)] = for {
    fid <- arbitrary[FeatureId]
    exp <- expressionArbitrary(gen._2)
    filter <- arbitraryFilter(gen._2)
    window <- arbitrary[Option[Window]]
    query = Query(exp, filter.map(FilterTextV0.asString).map(_.render).map(Filter.apply))
  } yield (fid, VirtualDefinition(gen._1, query, window))
}

object ArbitraryFeatures extends ArbitraryFeatures

/** namespace for features */
case class FeatureNamespace(namespace: Name)
/** namespace for features */
case class DefinitionWithQuery(cd: ConcreteDefinition, expression: Expression, filter: FilterEncoded)
/** Helpful wrapper around [[ConcreteGroup]] */
case class ConcreteGroupFeature(fid: FeatureId, cg: ConcreteGroup) {

  def withExpression(expression: Expression): ConcreteGroupFeature =
    copy(cg = cg.copy(virtual = cg.virtual.map(vd => vd._1 -> vd._2.copy(query = vd._2.query.copy(expression = expression)))))

  def dictionary: Dictionary =
    Dictionary(cg.definition.toDefinition(fid) :: cg.virtual.map(vd => vd._2.toDefinition(vd._1)))
}

