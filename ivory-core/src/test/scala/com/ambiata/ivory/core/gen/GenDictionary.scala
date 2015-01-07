package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._

import org.scalacheck._, Arbitrary.arbitrary

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

object GenDictionary {
  def mode: Gen[Mode] =
    Gen.oneOf(Mode.State, Mode.Set)

  def type_ : Gen[Type] =
    Gen.oneOf(NumericalType, ContinuousType, CategoricalType, BinaryType)

  def encoding: Gen[Encoding] =
    Gen.oneOf(subEncoding, listEncoding)

  def subEncoding: Gen[SubEncoding] =
    Gen.oneOf(primitiveEncoding, structEncoding)

  def listEncoding: Gen[ListEncoding] =
    subEncoding.map(ListEncoding)

  def primitiveEncoding: Gen[PrimitiveEncoding] =
    Gen.oneOf(BooleanEncoding, IntEncoding, LongEncoding, DoubleEncoding, StringEncoding, DateEncoding)

  def structEncoding: Gen[StructEncoding] =
    Gen.choose(1, 5).flatMap(n => Gen.mapOfN[String, StructEncodedValue](n, for {
      name <- GenString.namespace.map(_.name)
      enc <- primitiveEncoding
      optional <- arbitrary[Boolean]
    } yield name -> StructEncodedValue(enc, optional)).map(StructEncoding))

  def concrete: Gen[ConcreteDefinition] =
    concreteWith(encoding)

  def concreteWith(genc: Gen[Encoding]): Gen[ConcreteDefinition] = for {
    e <- genc
    m <- mode
    t <- Gen.option(type_)
    d <- GenString.sentence
    x <- GenString.words
  } yield ConcreteDefinition(e, m, t, d, x)

  def window: Gen[Window] = for {
    length <- GenPlus.posNum[Int]
    unit <- Gen.oneOf(Days, Weeks, Months, Years)
  } yield Window(length, unit)

  def featureWindow: Gen[FeatureWindow] = for {
    id <- GenIdentifier.feature
    w <- Gen.listOf(window)
  } yield FeatureWindow(id, w)

  def featureWindows: Gen[FeatureWindows] =
    Gen.nonEmptyListOf(featureWindow).map(w => FeatureWindows(w.distinct))

  def range[A: Arbitrary]: Gen[Range[A]] = for {
    id <- arbitrary[A]
    r <- rangeOf(id)
  } yield r

  def rangeOf[A](id: A): Gen[Range[A]] = for {
    dates <- Gen.nonEmptyListOf(GenDate.date)
    to = dates.max
  } yield Range(id, dates.filter(_ /== to), to)

  def ranges[A: Arbitrary: Equal]: Gen[Ranges[A]] = for {
    n <- Gen.sized(s => Gen.choose(1, math.min(s, 20)))
    r <- Gen.listOfN(n, range[A])
  } yield Ranges(distinctBy(r)(_.id))

  def identified: Gen[Identified[DictionaryId, Dictionary]] =
    GenIdentifier.identified(Arbitrary(GenIdentifier.dictionary), Arbitrary(GenDictionary.dictionary))

  def dictionary: Gen[Dictionary] = for {
    n <- Gen.sized(s => Gen.choose(3, math.min(s, 20)))
    i <- Gen.listOfN(n, GenIdentifier.feature).map(_.distinct)
    c <- Gen.listOfN(i.length, concrete).map(cds => i.zip(cds))
    // For every concrete definition there is a chance we may have a virtual feature
    v <- c.traverse(x => Gen.frequency(
      70 -> Gen.const(None)
    , 30 -> virtual(x, 0).map(some)
    )).map(_.flatten)
  } yield Dictionary(c.map({ case (f, d) => d.toDefinition(f) }) ++ v.map({ case (f, d) => d.toDefinition(f) }))

  // FIX ARB Could do with some polish.
  def expression(cd: ConcreteDefinition): Gen[Expression] = {
    val numericSubs = Gen.oneOf(Sum, Min, Mean, Max, Gradient, StandardDeviation, NumFlips)
    val fallback = Gen.frequency(
      15 -> Gen.oneOf(Count, DaysSinceLatest, DaysSinceEarliest, MeanInDays, MaximumInDays, MinimumInDays,
        Inverse(Count), Inverse(DaysSinceLatest), Inverse(DaysSinceEarliest), Inverse(Interval(Mean)),
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
          case (name, sve) => Gen.oneOf(subExpression(sve.encoding).map(se => StructExpression(name, se)),
            if (Encoding.isNumeric(sve.encoding))
              numericSubs.map(se => Inverse(StructExpression(name, se)))
            else Gen.const(NumFlips).map(se => Inverse(StructExpression(name, se)))
          )
        }
        // SumBy and CountBySecondary are a little more complicated
        (for {
          se <- values.find(_._2.encoding == StringEncoding).map(_._1)
          ie <- values.find(v => v._1 != se && List(StringEncoding).contains(v._2.encoding)).map(ie => CountBySecondary(se, ie._1)) orElse
            values.find(v => List(IntEncoding, LongEncoding, DoubleEncoding).contains(v._2.encoding)).map(ie => SumBy(se, ie._1))
        } yield ie).cata(v => Gen.frequency(5 -> Gen.const(v), 5 -> subexpGen), subexpGen)
      case p: PrimitiveEncoding   => Gen.oneOf(subExpression(p).map(BasicExpression),
                  p match {
                    case IntEncoding | LongEncoding | DoubleEncoding => numericSubs.map(x => Inverse(BasicExpression(x)))
                    case _ => Gen.const(NumFlips).map( x=> Inverse(BasicExpression(x)))
                  })
      case l: ListEncoding        => fallback
    })
  }

  def subExpression(pe: PrimitiveEncoding): Gen[SubExpression] = {
    val all = Gen.oneOf(Gen.const(Latest), Gen.choose(2,10).map(LatestN), Gen.const(NumFlips))
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
  def filter(cd: ConcreteDefinition): Gen[Option[FilterEncoded]] = {

    def filterExpression(encoding: PrimitiveEncoding): Gen[FilterExpression] =
      GenValue.valueOfPrim(encoding).flatMap {
        case StringValue(s) => Gen.identifier.map(StringValue.apply) // Just for now keep this _really_ simple
        case v              => Gen.const(v)
      }.flatMap { x => if (x match {
        // It's a little tricky to do anything else
        case BooleanValue(v) => false
        // Make sure we don't make the < or > impossible
        case DoubleValue(v)  => Double.MinValue < v && v < Double.MaxValue
        case IntValue(v)     => Int.MinValue < v && v < Int.MaxValue
        case LongValue(v)    => Long.MinValue < v && v < Long.MinValue
        case DateValue(v)    => Date.minValue < v && v < Date.maxValue
        case StringValue(v)  => v != ""
      }) Gen.oneOf(
        FilterEquals(x), FilterLessThan(x), FilterLessThanOrEqual(x), FilterGreaterThan(x), FilterGreaterThanOrEqual(x)
      ) else Gen.oneOf(FilterEquals(x), FilterNotEquals(x))}

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
              case (name, StructEncodedValue(enc, _)) => filterExpression(enc).map(name ->)
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
            fields <- Gen.listOfN(n, filterExpression(pe))
            cn     <- op.fold(Gen.const(0), Gen.choose(0, maxChildren))
            chlds  <- Gen.listOfN(cn, sub(maxChildren - 1))
          } yield FilterValuesOp(op, fields, chlds)
        sub(2).map(FilterValues).map(some)
      // We don't support list encoding at the moment
      case _: ListEncoding => Gen.const(none)
    }
  }

  /** Require an index for this definition to guarantee uniqueness of feature ids across the dictionary */
  def virtual(gen: (FeatureId, ConcreteDefinition), featureIdOffset: Int): Gen[(FeatureId, VirtualDefinition)] = for {
    exp <- expression(gen._2)
    // Also give the virtual feature a different namespace
    fid  = FeatureId(Namespace.reviewed(gen._1.namespace.name + "_v"), gen._1.name + "_" + featureIdOffset)
    filter <- filter(gen._2)
    window <- Gen.option(GenDictionary.window)
    query = Query(exp, filter.map(FilterTextV0.asString).map(_.render).map(Filter.apply))
  } yield (fid, VirtualDefinition(gen._1, query, window))

  class DistinctKey[A, B](val key: A, val value: B) {
    override def hashCode: Int =
      key.hashCode
    override def equals(o: Any): Boolean =
      o.isInstanceOf[DistinctKey[A, B]] &&
        value.equals(o.asInstanceOf[DistinctKey[A, B]].value)
  }

  def distinctBy[A, B: Equal](l: List[A])(f: A => B): List[A] =
    l.map(v => new DistinctKey(f(v), v)).distinct.map(_.value)
}
