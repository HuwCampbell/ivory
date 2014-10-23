package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.storage.metadata.DictionaryTextStorageV2

import scala.PartialFunction.condOpt

/**
 * Map-reduce reductions that represent a single feature gen.
 *
 * NOTE: For performance reasons only these are stateful, please design with care!!!
 */
trait Reduction {

  /** Reset the state of this reduction for the next series of facts */
  def clear(): Unit

  /** Update the state based on a single [[Fact]] */
  def update(f: Fact): Unit

  /** Return a final thrift value based on the current state, or null to filter out */
  def save: ThriftFactValue
}

object Reduction {

  def compile(fr: FeatureReduction, end: Date, profile: Reduction => Reduction): Option[Reduction] = for {
    exp      <- Expression.parse(fr.getExpression).toOption
    encoding <- DictionaryTextStorageV2.parseEncoding(fr.getEncoding).toOption
    filter    = if (fr.isSetFilter) Some(Filter(fr.getFilter)) else None
    dates     = DateOffsets.calculateLazyCompact(Date.unsafeFromInt(fr.date), end)
    reduction<- Reduction.fromExpression(exp, encoding, dates)
    // We want to profile _after_ the filter has been applied
    profiled  = profile(reduction)
  } yield filter.flatMap(FilterReducer.compile(_, encoding, profiled).toOption).getOrElse(profiled)

  def fromExpression(exp: Expression, encoding: Encoding, dates: DateOffsetsLazy): Option[Reduction] = exp match {
    case Count                        => Some(new CountReducer())
    case Latest                       => Some(new LatestReducer())
    case DaysSinceLatest              => Some(new DaysSinceLatestReducer(dates.dates))
    case MeanInDays                   => Some(new DateReduction(dates.dates, new MeanInDaysReducer, ReductionValueDouble))
    case MeanInWeeks                  => Some(new DateReduction(dates.dates, new MeanInWeeksReducer, ReductionValueDouble))
    case MinimumInDays                => Some(new DateReduction(dates.dates, new MinimumInDaysReducer, ReductionValueInt))
    case MinimumInWeeks               => Some(new DateReduction(dates.dates, new MinimumInWeeksReducer, ReductionValueInt))
    case MaximumInDays                => Some(new DateReduction(dates.dates, new MaximumInDaysReducer, ReductionValueInt))
    case MaximumInWeeks               => Some(new DateReduction(dates.dates, new MaximumInWeeksReducer, ReductionValueInt))
    case CountDays                    => Some(new DateReduction(dates.dates, new CountDaysReducer, ReductionValueInt))
    case QuantileInDays(k, q)         => Some(new DateReduction(dates.dates, new QuantileInDaysReducer(dates.dates, k, q), ReductionValueDouble))
    case QuantileInWeeks(k, q)        => Some(new DateReduction(dates.dates, new QuantileInWeeksReducer(dates.dates, k, q), ReductionValueDouble))
    case ProportionByTime(s, e)       => Some(new ProportionByTimeReducer(s, e))
    case DaysSinceEarliest            => Some(new DaysSinceEarliestReducer(dates.dates))
    case SumBy(k, f)                  => encoding match {
      case StructEncoding(values) => values.get(f).map(_.encoding).flatMap {
        case IntEncoding    => Some(new ReductionFold2StructWrapper(k, f, new SumByReducer[Int], ReductionValueString, ReductionValueInt, ReductionValueStruct[String, Int](ReductionValueInt)))
        case LongEncoding   => Some(new ReductionFold2StructWrapper(k, f, new SumByReducer[Long], ReductionValueString, ReductionValueLong, ReductionValueStruct[String, Long](ReductionValueLong)))
        case DoubleEncoding => Some(new ReductionFold2StructWrapper(k, f, new SumByReducer[Double], ReductionValueString, ReductionValueDouble, ReductionValueStruct[String, Double](ReductionValueDouble)))
        case _              => None
      }
      case _ => None
    }
    case BasicExpression(sexp)        => encoding match {
      case pe: PrimitiveEncoding      => fromSubExpression(sexp, pe, dates, new EncodedReduction {
        def s[A, B](f: ReductionFoldWithDate[A, String, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldWrapper(f, ReductionValueString, to)
        def b[A, B](f: ReductionFoldWithDate[A, Boolean, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldWrapper(f, ReductionValueBoolean, to)
        def i[A, B](f: ReductionFoldWithDate[A, Int, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldWrapper(f, ReductionValueInt, to)
        def l[A, B](f: ReductionFoldWithDate[A, Long, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldWrapper(f, ReductionValueLong, to)
        def d[A, B](f: ReductionFoldWithDate[A, Double, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldWrapper(f, ReductionValueDouble, to)
        def date[A, B](f: ReductionFoldWithDate[A, Int, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldWrapper(f, ReductionValueDate, to)
      })
      case _                          => None
    }
    case StructExpression(name, sexp) => encoding match {
      case StructEncoding(values) => values.get(name).flatMap(sev => fromSubExpression(sexp, sev.encoding, dates, new EncodedReduction {
        def s[A, B](f: ReductionFoldWithDate[A, String, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldStructWrapper(name, f, ReductionValueString, to)
        def b[A, B](f: ReductionFoldWithDate[A, Boolean, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldStructWrapper(name, f, ReductionValueBoolean, to)
        def i[A, B](f: ReductionFoldWithDate[A, Int, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldStructWrapper(name, f, ReductionValueInt, to)
        def l[A, B](f: ReductionFoldWithDate[A, Long, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldStructWrapper(name, f, ReductionValueLong, to)
        def d[A, B](f: ReductionFoldWithDate[A, Double, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldStructWrapper(name, f, ReductionValueDouble, to)
        def date[A, B](f: ReductionFoldWithDate[A, Int, B], to: ReductionValueTo[B]): Reduction =
          new ReductionFoldStructWrapper(name, f, ReductionValueDate, to)
      }))
      case _  => None
    }
  }

  def fromSubExpression(exp: SubExpression, encoding: PrimitiveEncoding, dates: DateOffsetsLazy, f: EncodedReduction): Option[Reduction] = exp match {
    case Sum => condOpt(encoding) {
      case IntEncoding    => f.i(new ReductionFoldIntToLong(new SumReducer[Long]), ReductionValueLong)
      case LongEncoding   => f.l(new SumReducer[Long], ReductionValueLong)
      case DoubleEncoding => f.d(new SumReducer[Double], ReductionValueDouble)
    }
    case Mean => condOpt(encoding) {
      case IntEncoding    => f.i(new ReductionFoldIntToLong(new MeanReducer[Long]), ReductionValueDouble)
      case LongEncoding   => f.l(new MeanReducer[Long], ReductionValueDouble)
      case DoubleEncoding => f.d(new MeanReducer[Double], ReductionValueDouble)
    }
    case Gradient => condOpt(encoding) {
      case IntEncoding    => f.i(new ReductionFoldIntToLong(new GradientReducer[Long](dates.dates)), ReductionValueDouble)
      case LongEncoding   => f.l(new GradientReducer[Long](dates.dates), ReductionValueDouble)
      case DoubleEncoding => f.d(new GradientReducer[Double](dates.dates), ReductionValueDouble)
    }
    case StandardDeviation => condOpt(encoding) {
      case IntEncoding    => f.i(new ReductionFoldIntToLong(new StandardDeviationReducer[Long]), ReductionValueDouble)
      case LongEncoding   => f.l(new StandardDeviationReducer[Long], ReductionValueDouble)
      case DoubleEncoding => f.d(new StandardDeviationReducer[Double], ReductionValueDouble)
    }
    case CountUnique => condOpt(encoding) {
      case StringEncoding  => f.s(new CountUniqueReducer[String], ReductionValueLong)
    }
    case NumFlips => encoding match {
      case StringEncoding  => Some(f.s(new NumFlipsReducer[String](""), ReductionValueLong))
      case BooleanEncoding => Some(f.b(new NumFlipsReducer[Boolean](false), ReductionValueLong))
      case IntEncoding     => Some(f.i(new NumFlipsReducer[Int](0), ReductionValueLong))
      case LongEncoding    => Some(f.l(new NumFlipsReducer[Long](0), ReductionValueLong))
      case DoubleEncoding  => Some(f.d(new NumFlipsReducer[Double](0), ReductionValueLong))
      case DateEncoding    => Some(f.date(new NumFlipsReducer[Int](0), ReductionValueLong))
    }
    case CountBy => condOpt(encoding) {
      case StringEncoding  => f.s(new CountByReducer, ReductionValueStruct[String, Long](ReductionValueLong))
      case IntEncoding     => f.i(new CountByReducer, ReductionValueStruct[Int, Long](ReductionValueLong))
    }
    case DaysSince => condOpt(encoding) {
      case DateEncoding    => new DaysSinceReducer(dates.dates)
    }
    case DaysSinceEarliestBy => condOpt(encoding) {
      case StringEncoding  => f.s(new DaysSinceEarliestByReducer(dates.dates), ReductionValueStruct[String, Int](ReductionValueInt))
    }
    case DaysSinceLatestBy => condOpt(encoding) {
      case StringEncoding  => f.s(new DaysSinceLatestByReducer(dates.dates), ReductionValueStruct[String, Int](ReductionValueInt))
    }
    case Proportion(value) => Value.parsePrimitive(encoding, value).toOption.map {
      case StringValue(v)  => f.s(new ProportionReducer[String](v), ReductionValueDouble)
      case BooleanValue(v) => f.b(new ProportionReducer[Boolean](v), ReductionValueDouble)
      case IntValue(v)     => f.i(new ProportionReducer[Int](v), ReductionValueDouble)
      case LongValue(v)    => f.l(new ProportionReducer[Long](v), ReductionValueDouble)
      case DoubleValue(v)  => f.d(new ProportionReducer[Double](v), ReductionValueDouble)
      case DateValue(v)    => f.date(new ProportionReducer[Int](v.int), ReductionValueDouble)
    }
  }
}

trait EncodedReduction {
  def s[A, B](f: ReductionFoldWithDate[A, String, B],  to: ReductionValueTo[B]): Reduction
  def b[A, B](f: ReductionFoldWithDate[A, Boolean, B], to: ReductionValueTo[B]): Reduction
  def i[A, B](f: ReductionFoldWithDate[A, Int, B],     to: ReductionValueTo[B]): Reduction
  def l[A, B](f: ReductionFoldWithDate[A, Long, B],    to: ReductionValueTo[B]): Reduction
  def d[A, B](f: ReductionFoldWithDate[A, Double, B],  to: ReductionValueTo[B]): Reduction
  def date[A, B](f: ReductionFoldWithDate[A, Int, B],  to: ReductionValueTo[B]): Reduction
}

/**
 * Pure version of [[Reduction]], specialized for the [[B]] value being folded over.
 *
 * Unfortunately [[C]] can't be specialized without running into compiler errors. :(
 */
trait ReductionFoldWithDate[A, @specialized(Int, Float, Double, Boolean) B, C] {
  def initial: A
  def foldWithDate(a: A, b: B, d: Date): A
  def tombstoneWithDate(a: A, d: Date): A
  def aggregate(a: A): C
}

trait ReductionFold[A, @specialized(Int, Float, Double, Boolean) B, C] extends ReductionFoldWithDate[A, B, C] {
  def fold(a: A, b: B): A
  def tombstone(a: A): A
  // Oh god I'm sorry :(
  def foldWithDate(a: A, b: B, d: Date): A = fold(a, b)
  def tombstoneWithDate(a: A, B: Date): A = tombstone(a)
}

/** Currently only for `sum_by` which requires _two_ struct fields */
trait ReductionFold2[A, @specialized(Int, Float, Double, Boolean) B1, @specialized(Int, Float, Double, Boolean) B2, C] {
  def initial: A
  def fold(a: A, b1: B1, b2: B2, d: Date): A
  def tombstone(a: A, B: Date): A
  def aggregate(a: A): C
}

/** For wrapping any [[Int]] based [[ReductionFoldWithDate]] that accumulates a sum and we want to avoid overflow */
class ReductionFoldIntToLong[A, B](f: ReductionFoldWithDate[A, Long, B]) extends ReductionFoldWithDate[A, Int, B] {
  def initial: A = f.initial
  def foldWithDate(a: A, b: Int, d: Date): A = f.foldWithDate(a, b.toLong, d)
  def tombstoneWithDate(a: A, d: Date): A = f.tombstoneWithDate(a, d)
  def aggregate(a: A): B = f.aggregate(a)
}

class ReductionFoldWrapper[A, @specialized(Int, Float, Double, Boolean) B, C](f: ReductionFoldWithDate[A, B, C], from: ReductionValueFrom[B], v: ReductionValueTo[C]) extends Reduction {
  val value = new ThriftFactValue
  var a: A = f.initial

  def clear(): Unit =
    a = f.initial

  def update(fact: Fact): Unit =
    a = if (fact.isTombstone)  f.tombstoneWithDate(a, fact.date) else foldSpecialized(fact, f, from)

  // DO NOT INLINE - this is required to force scala to specialise on B
  def foldSpecialized(fact: Fact, f: ReductionFoldWithDate[A, B, C], from: ReductionValueFrom[B]): A =
    f.foldWithDate(a, from.from(fact.toThrift.getValue), fact.date)

  def save: ThriftFactValue = {
    v.to(f.aggregate(a), value)
    value
  }
}

class ReductionFoldStructWrapper[A, @specialized(Int, Float, Double, Boolean) B, C](field: String, f: ReductionFoldWithDate[A, B, C], from: ReductionValueFromPrim[B], v: ReductionValueTo[C]) extends Reduction {
  val value = new ThriftFactValue
  var a: A = f.initial
  def clear(): Unit =
    a = f.initial
  def update(fact: Fact): Unit =
    if (!fact.isTombstone) {
      val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
      if (value != null) a = foldSpecialized(fact, value, f, from)
    } else a = f.tombstoneWithDate(a, fact.date)
  // DO NOT INLINE - this is required to force scala to specialise on B
  def foldSpecialized(fact: Fact, value: ThriftFactPrimitiveValue, f: ReductionFoldWithDate[A, B, C], from: ReductionValueFromPrim[B]): A =
    f.foldWithDate(a, from.fromPrim(value), fact.date)
  def save: ThriftFactValue = {
    v.to(f.aggregate(a), value)
    value
  }
}

class ReductionFold2StructWrapper[A, @specialized(Int, Float, Double, Boolean) B1, @specialized(Int, Float, Double, Boolean) B2, C]
(field1: String, field2: String, f: ReductionFold2[A, B1, B2, C], from1: ReductionValueFromPrim[B1], from2: ReductionValueFromPrim[B2], v: ReductionValueTo[C]) extends Reduction {
  val value = new ThriftFactValue
  var a: A = f.initial
  def clear(): Unit =
    a = f.initial
  def update(fact: Fact): Unit =
    if (!fact.isTombstone) {
      val value1 = fact.toThrift.getValue.getStructSparse.getV.get(field1)
      val value2 = fact.toThrift.getValue.getStructSparse.getV.get(field2)
      if (value1 != null && value2 != null) a = foldSpecialized(fact, value1, value2, f, from1, from2)
    } else a = f.tombstone(a, fact.date)
  // DO NOT INLINE - this is required to force scala to specialise on B
  def foldSpecialized(fact: Fact, v1: ThriftFactPrimitiveValue, v2: ThriftFactPrimitiveValue,
                      f: ReductionFold2[A, B1, B2, C], from1: ReductionValueFromPrim[B1], from2: ReductionValueFromPrim[B2]): A =
    f.fold(a, from1.fromPrim(v1), from2.fromPrim(v2), fact.date)
  def save: ThriftFactValue = {
    v.to(f.aggregate(a), value)
    value
  }
}
