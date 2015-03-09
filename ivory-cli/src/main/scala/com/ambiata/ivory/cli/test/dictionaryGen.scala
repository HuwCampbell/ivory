package com.ambiata.ivory.cli.test

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata.DictionaryTextStorageV2

/**
 * This is a fairly horrible application that generates a complete dictionary to ensure that the Ivory metadata version
 * is _always_ increased for non forwards compatible changes (which is almost anything).
 *
 * Remember, this is not about generating _every_ permutation, just at least one of everything.
 *
 * Unfortunately it's basically impossible to tell whether that is true for ADTs. Instead, please try to pattern match
 * in the function that they're being generated to ensure when the structure changes this class will no longer compile.
 */
object dictionaryGen {

  def main(args: Array[String]): Unit =
    println(DictionaryTextStorageV2.delimitedString(dictionary))

  def dictionary: Dictionary =
    Dictionary(concrete.zipWithIndex.flatMap({
      case (c, i) =>
        val f = FeatureId(Namespace.unsafe("ns_" + i), "concrete")
        Concrete(f, c) :: virtual(f, c.encoding).zipWithIndex.map({
          case (v, j) =>
            Virtual(FeatureId(f.namespace, "virtual_" + j), v)
        })
    }))

  def concrete: List[ConcreteDefinition] = for {
    m <- mode
    e <- m.fold(encoding, encoding, _ => List(encodingStruct.toEncoding))
    t <- None :: typ.map(Some.apply).take(1)
  } yield ConcreteDefinition(e, m, t, "", List("na"))

  def virtual(feature: FeatureId, encoding: Encoding): List[VirtualDefinition] = for {
    q <- query(encoding)
    w <- None :: window.map(Some.apply)
  } yield VirtualDefinition(feature, q, w)

  def window: List[Window] =
    List(Days, Weeks, Months, Years).map({
      case Days => Days
      case Weeks => Months
      case Months => Months
      case Years => Years
    }).map(w => Window(1, w))

  def query(encoding: Encoding): List[Query] = for {
    e <- expression(encoding)
    f <- None :: filter(encoding).map(FilterTextV0.asString).map(Some.apply)
  } yield Query(e, f)

  def filter(encoding: Encoding): List[FilterEncoded] =
    encoding.fold(
      e => FilterValues(FilterValuesOp(FilterOpAnd, filterExpression(e), Nil)) :: Nil,
      _ => FilterStruct(FilterStructOp(FilterOpAnd, filterExpression(StringEncoding).map("string" -> _), Nil)) :: Nil,
      _ => Nil
    )

  def expression(encoding: Encoding): List[Expression] =
    (encoding.fold(
      e => expressionSub(e).map(BasicExpression),
      s =>
        expressionSub(StringEncoding).map(StructExpression("string", _)) ++
        expressionSub(IntEncoding).map(StructExpression("int", _)) ++
        List(LatestBy("keyed_set"), SumBy("string", "int"), CountBySecondary("string", "string")),
      _ => Nil
    ) ++ List(
      Count, DaysSinceLatest, DaysSinceEarliest, MeanInDays, MeanInWeeks, MaximumInDays, MaximumInWeeks, MinimumInDays,
      MinimumInWeeks, CountDays, QuantileInDays(2, 10), QuantileInWeeks(2, 10), Interval(Min), Inverse(Count)
    )).map({
      case Count => Count
      case DaysSinceLatest => DaysSinceLatest
      case DaysSinceEarliest => DaysSinceEarliest
      case MeanInDays => MeanInDays
      case MeanInWeeks => MeanInWeeks
      case MaximumInDays => MaximumInDays
      case MaximumInWeeks => MaximumInWeeks
      case MinimumInDays => MinimumInDays
      case MinimumInWeeks => MinimumInWeeks
      case CountDays => CountDays
      case e@LatestBy(_) => e
      case e@QuantileInDays(_, _) => e
      case e@QuantileInWeeks(_, _) => e
      case e@ProportionByTime(_, _) => e
      case e@Interval(_) => e
      case e@Inverse(_) => e
      case e@SumBy(_, _) => e
      case e@CountBySecondary(_, _) => e
      case e@BasicExpression(_) => e
      case e@StructExpression(_, _) => e
    })

  def expressionSub(encoding: PrimitiveEncoding): List[SubExpression] = {
    val extra =
      if (Encoding.isNumericPrim(encoding)) List(Sum, Min, Max, Mean, Gradient, StandardDeviation, Proportion("0"))
      else if (encoding == DateEncoding) List(DaysSince)
      else if (encoding == StringEncoding) List(CountBy, CountUnique, DaysSinceLatestBy, DaysSinceEarliestBy, Proportion(""))
      else Nil
    val all = List(Latest, NumFlips, LatestN(1))
    (extra ++ all).map({
      case Latest => Latest
      case Sum => Sum
      case Min => Min
      case Max => Max
      case Mean => Max
      case Gradient => Gradient
      case StandardDeviation => StandardDeviation
      case CountUnique => CountUnique
      case NumFlips => NumFlips
      case CountBy => CountBy
      case DaysSince => DaysSince
      case DaysSinceLatestBy => DaysSinceLatestBy
      case DaysSinceEarliestBy => DaysSinceEarliestBy
      case e@Proportion(_) => e
      case e@LatestN(_) => e
    })
  }

  def filterExpression(e: PrimitiveEncoding): List[FilterExpression] = {
    val v = e match {
      case StringEncoding => StringValue("v")
      case BooleanEncoding => BooleanValue(true)
      case IntEncoding => IntValue(0)
      case LongEncoding => LongValue(0)
      case DoubleEncoding => DoubleValue(0)
      case DateEncoding => DateValue(Date.maxValue)
    }
    List(FilterEquals(v), FilterNotEquals(v), FilterLessThan(v), FilterLessThanOrEqual(v), FilterGreaterThan(v), FilterGreaterThanOrEqual(v))
  }

  def mode: List[Mode] =
    List(Mode.State, Mode.State, Mode.KeyedSet(List("keyed_set"))).map({
      case Mode.State => Mode.State
      case Mode.Set => Mode.Set
      case s@Mode.KeyedSet(_) => s
    })

  def typ: List[Type] =
    List(NumericalType, ContinuousType, CategoricalType, BinaryType).map({
      case NumericalType => NumericalType
      case ContinuousType => ContinuousType
      case CategoricalType => CategoricalType
      case BinaryType => BinaryType
    })

  def encoding: List[Encoding] =
    encodingPrim.map(_.toEncoding) ++
      List(
        ListEncoding(SubPrim(StringEncoding)).toEncoding,
        encodingStruct.toEncoding
      ).map({
        case s@EncodingPrim(_) => s
        case s@EncodingStruct(_) => s
        case s@EncodingList(_) => s
      })

  def encodingStruct: StructEncoding =
    StructEncoding(Map(
      "keyed_set" -> StructEncodedValue.mandatory(StringEncoding),
      "string" -> StructEncodedValue.optional(StringEncoding),
      "int" -> StructEncodedValue.optional(IntEncoding)
    ))

  def encodingPrim: List[PrimitiveEncoding] =
    List(StringEncoding, IntEncoding, LongEncoding, DoubleEncoding, BooleanEncoding, DateEncoding).map({
      case StringEncoding => StringEncoding
      case IntEncoding => IntEncoding
      case LongEncoding => LongEncoding
      case DoubleEncoding => DoubleEncoding
      case BooleanEncoding => BooleanEncoding
      case DateEncoding => DateEncoding
    })
}
