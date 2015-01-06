package com.ambiata.ivory.core

import scalaz._, Scalaz._

sealed trait Expression

object Expression {

  /**
   * For now we want to be able to represent a single expression as a simple string.
   * In future this will be far more complex, but that will take some time.
   */
  def asString(exp: Expression): String = {
    def asSubString(se: SubExpression): List[String] = se match {
      case Latest                       => List("latest")
      case LatestN(n)                   => List("latestN", n.toString)
      case Sum                          => List("sum")
      case Min                          => List("min")
      case Max                          => List("max")
      case Mean                         => List("mean")
      case Gradient                     => List("gradient")
      case StandardDeviation            => List("std_dev")
      case CountUnique                  => List("count_unique")
      case NumFlips                     => List("num_flips")
      case CountBy                      => List("count_by")
      case DaysSince                    => List("days_since")
      case DaysSinceEarliestBy          => List("days_since_earliest_by")
      case DaysSinceLatestBy            => List("days_since_latest_by")
      case Proportion(value)            => List("proportion", value)
    }
    (exp match {
      case Count                        => List("count")
      case Interval(other)              => List("interval") ++ asSubString(other)
      case Inverse(other)               => List("inverse") ++ asString(other).pure[List]
      case DaysSinceLatest              => List("days_since_latest")
      case DaysSinceEarliest            => List("days_since_earliest")
      case MeanInDays                   => List("mean_in_days")
      case MeanInWeeks                  => List("mean_in_weeks")
      case MaximumInDays                => List("maximum_in_days")
      case MaximumInWeeks               => List("maximum_in_weeks")
      case MinimumInDays                => List("minimum_in_days")
      case MinimumInWeeks               => List("minimum_in_weeks")
      case CountDays                    => List("count_days")
      case QuantileInDays(k, q)         => List("quantile_in_days", k, q)
      case QuantileInWeeks(k, q)        => List("quantile_in_weeks", k, q)
      case ProportionByTime(s, e)       => List("proportion_by_time", s, e)
      case SumBy(key, field)            => List("sum_by", key, field)
      case CountBySecondary(key, field) => List("count_by", key, field)
      case BasicExpression(sexp)        => asSubString(sexp)
      case StructExpression(name, sexp) => asSubString(sexp) ++ List(name)
    }).mkString(",")
  }

  def parse(exp: String): String \/ Expression = {
    def parseInt(s: String): String \/ Int = s.parseInt.disjunction.leftMap(_.getMessage)
    def parseSub(sexp: SubExpression, args: List[String]): Expression = args match {
      case Nil    => BasicExpression(sexp)
      case h :: t => StructExpression(h, sexp)
    }
    val args = exp.split(",", -1).toList
    PartialFunction.condOpt(args) {
      case "count" :: Nil                   => Count
      case "mean_in_days" :: Nil            => MeanInDays
      case "mean_in_weeks" :: Nil           => MeanInWeeks
      case "days_since_latest" :: Nil       => DaysSinceLatest
      case "days_since_earliest" :: Nil     => DaysSinceEarliest
      case "maximum_in_days" :: Nil         => MaximumInDays
      case "maximum_in_weeks" :: Nil        => MaximumInWeeks
      case "minimum_in_days" :: Nil         => MinimumInDays
      case "minimum_in_weeks" :: Nil        => MinimumInWeeks
      case "count_days" :: Nil              => CountDays
      case "sum_by" :: key :: sumBy :: Nil  => SumBy(key, sumBy)
      case "count_by" :: key :: fld :: Nil  => CountBySecondary(key, fld)
      // Subexpressions
      case "latest" :: tail                 => parseSub(Latest, tail)
      case "sum" :: tail                    => parseSub(Sum, tail)
      case "min" :: tail                    => parseSub(Min, tail)
      case "max" :: tail                    => parseSub(Max, tail)
      case "mean" :: tail                   => parseSub(Mean, tail)
      case "gradient" :: tail               => parseSub(Gradient, tail)
      case "std_dev" :: tail                => parseSub(StandardDeviation, tail)
      case "count_unique" :: tail           => parseSub(CountUnique, tail)
      case "num_flips" :: tail              => parseSub(NumFlips, tail)
      case "count_by" :: tail               => parseSub(CountBy, tail)
      case "proportion" :: value :: tail    => parseSub(Proportion(value), tail)
      case "days_since" :: tail             => parseSub(DaysSince, tail)
      case "days_since_latest_by" :: tail   => parseSub(DaysSinceLatestBy, tail)
      case "days_since_earliest_by" :: tail => parseSub(DaysSinceEarliestBy, tail)
    }.map(_.right).getOrElse {
      args match {
      case "quantile_in_days" :: k :: q :: Nil  => (parseInt(k) |@| parseInt(q))(QuantileInDays.apply)
      case "quantile_in_weeks" :: k :: q :: Nil => (parseInt(k) |@| parseInt(q))(QuantileInWeeks.apply)
      case "proportion_by_time" :: s :: e :: Nil =>
        def hour(s: String): String \/ Time = parseInt(s).map(3600 *).flatMap(i => Time.create(i).toRightDisjunction(s"Invalid time: $i"))
        (hour(s) |@| hour(e))(ProportionByTime)
      case "interval" :: other :: Nil       => parse(other) match {
        case \/-(BasicExpression(e)) => Interval(e).right
        case -\/(m)                  => s"Error parsing interval expression '$exp' internal with message '$m'".left
        case _                       => s"Bad interval expression '$exp'".left
      }
      case "inverse" :: others       => parse(others.mkString(",")) match {
        case \/-(e)                  => Inverse(e).right
        case -\/(m)                  => s"Error parsing inverse expression '$exp' internal with message '$m'".left
      }
      case "latestN" :: num :: tail => parseInt(num).map(n => parseSub(LatestN(n), tail))
      case _ => s"Unrecognised expression '$exp'".left
    }}
  }

  def validate(exp: Expression, encoding: Encoding): String \/ Unit = {
    val ok = ().right
    def validateSub(sexp: SubExpression, subenc: PrimitiveEncoding): String \/ Unit = sexp match {
      case Latest           => ok
      case LatestN(i)       => if (i > 0) ok else "Only positive values supported for latestN".left
      case (Sum | Min | Max | Mean | Gradient | StandardDeviation) =>
        if (Encoding.isNumeric(subenc)) ok else"Non-numeric encoding not supported".left
      case DaysSince => subenc match {
        case DateEncoding   => ok
        case _              => "Non-date encoding not supported".left
      }
      case (CountUnique | DaysSinceLatestBy | DaysSinceEarliestBy) => subenc match {
        case StringEncoding => ok
        case _              => "Non-string encoding not supported".left
      }
      case CountBy => subenc match {
        case StringEncoding => ok
        case IntEncoding    => ok
        case _              => "Non-string encoding not supported".left
      }
      case NumFlips         => ok
      case Proportion(v)    => Value.parsePrimitive(subenc, v).disjunction.void
    }
    (exp match {
      case Count                         => ok
      case DaysSinceLatest               => ok
      case DaysSinceEarliest             => ok
      case MeanInDays                    => ok
      case MeanInWeeks                   => ok
      case MaximumInDays                 => ok
      case MaximumInWeeks                => ok
      case MinimumInDays                 => ok
      case MinimumInWeeks                => ok
      case CountDays                     => ok
      case QuantileInDays(_, _)          => ok
      case QuantileInWeeks(_, _)         => ok
      case ProportionByTime(_, _)        => ok
      case Interval(other)               => other match {
        case (Min | Max | Mean | Gradient | StandardDeviation) => ok
        case _  => "Non-supported interval sub expression".left
      }
      case Inverse(other)                =>
        if (Encoding.isNumeric(expressionEncoding(other, encoding))) ok else "Non numeric encoding for inverse".left
      case SumBy(key, field)             => encoding match {
        case StructEncoding(values) => for {
           k <- values.get(key).map(_.encoding).toRightDisjunction(s"Struct field not found '$key'")
           f <- values.get(field).map(_.encoding).toRightDisjunction(s"Struct field not found '$field'")
           _ <- (k, f) match {
             case (StringEncoding, fieldEncoding) =>
               if (Encoding.isNumeric(fieldEncoding)) ok else "sum_by field is required to be numerical".left
             case _ => "sum_by key is required to be a string".left
           }
        } yield ()
        case _                           => "sum_by requires struct encoding".left
      }
      case CountBySecondary(key, field)             => encoding match {
        case StructEncoding(values) => for {
          k <- values.get(key).map(_.encoding).toRightDisjunction(s"Struct field not found '$key'")
          f <- values.get(field).map(_.encoding).toRightDisjunction(s"Struct field not found '$field'")
          _ <- (k, f) match {
            case (StringEncoding, StringEncoding) => ok
            case _                                => "count_by fields are required to be strings".left
          }
        } yield ()
        case _                           => "count_by requires struct encoding".left
      }
      case BasicExpression(sexp)         => encoding match {
        case pe: PrimitiveEncoding       => validateSub(sexp, pe)
        case se: StructEncoding          => sexp match {
          // These two operations work on structs
          case Latest     => ok
          case LatestN(i) => if (i > 0) ok else "Only positive values supported for latestN".left
          case _          => "Struct encoding not supported for the given expression".left
        }
        case se: ListEncoding          => sexp match {
          // Latest works on everything
          case Latest     => ok
          case _          => "List encoding not supported for the given expression".left
        }
      }
      case StructExpression(field, sexp) => encoding match {
        case StructEncoding(values) =>
          values.get(field).toRightDisjunction(s"Struct field not found '$field'")
            .flatMap(sev => validateSub(sexp, sev.encoding))
        case _                      => "Expression with a field not supported".left
      }
    }).leftMap(_ + " " + asString(exp))
  }

  /**
   * Return the expected encoding for an expression.
   * NOTE: We don't currently have a way to really expression what the key/value encoding
   * will look like and they are currently represented below as [[StructEncoding]], but
   * the keys are not known until later.
   */
  def expressionEncoding(expression: Expression, source: Encoding): Encoding = {
    def getExpressionEncoding(exp: SubExpression, enc: Encoding): Encoding = exp match {
      case Latest              => enc
      case LatestN(_)          => enc match {
                                    case sub: SubEncoding  => ListEncoding(sub)
                                    // Nesting is forbidden, this is just to make the match exhaustive
                                    case _  : ListEncoding => enc
                                  }
      case Sum                 => enc
      case CountUnique         => LongEncoding
      case Min                 => enc
      case Max                 => enc
      case Mean                => DoubleEncoding
      case Gradient            => DoubleEncoding
      case StandardDeviation   => DoubleEncoding
      case NumFlips            => LongEncoding
      case DaysSince           => IntEncoding
      case CountBy             => StructEncoding(Map())
      case DaysSinceEarliestBy => StructEncoding(Map())
      case DaysSinceLatestBy   => StructEncoding(Map())
      case Proportion(_)       => DoubleEncoding
    }
    expression match {
      // A short term hack for supporting feature gen based on known functions
      case Count                        => LongEncoding
      case Interval(sexp)               => getExpressionEncoding(sexp, LongEncoding)
      case Inverse(sexp)                => DoubleEncoding
      case DaysSinceLatest              => IntEncoding
      case DaysSinceEarliest            => IntEncoding
      case MeanInDays                   => DoubleEncoding
      case MeanInWeeks                  => DoubleEncoding
      case MaximumInDays                => IntEncoding
      case MaximumInWeeks               => IntEncoding
      case MinimumInDays                => IntEncoding
      case MinimumInWeeks               => IntEncoding
      case CountDays                    => IntEncoding
      case QuantileInDays(k, q)         => DoubleEncoding
      case QuantileInWeeks(k, q)        => DoubleEncoding
      case ProportionByTime(s, e)       => DoubleEncoding
      case SumBy(_, _)                  => StructEncoding(Map())
      case CountBySecondary(_, _)       => StructEncoding(Map())
      case BasicExpression(sexp)        => getExpressionEncoding(sexp, source)
      case StructExpression(name, sexp) => source match {
        case StructEncoding(values) => values.get(name).map {
          sve => getExpressionEncoding(sexp, sve.encoding)
        }.getOrElse(source)
        case _                          => source
      }
    }
  }
}

// Expressions that can only be done on the top-level
case object Count extends Expression
case object DaysSinceLatest extends Expression
case object DaysSinceEarliest extends Expression
case object MeanInDays extends Expression
case object MeanInWeeks extends Expression
case object MaximumInDays extends Expression
case object MaximumInWeeks extends Expression
case object MinimumInDays extends Expression
case object MinimumInWeeks extends Expression
case object CountDays extends Expression
case class QuantileInDays(k: Int, q: Int) extends Expression
case class QuantileInWeeks(k: Int, q: Int) extends Expression
case class ProportionByTime(start: Time, end: Time) extends Expression

/** These are "special" in that they requires _two_ struct fields */
case class SumBy(key: String, field: String) extends Expression
case class CountBySecondary(key: String, field: String) extends Expression

case class BasicExpression(exp: SubExpression) extends Expression
case class StructExpression(field: String, exp: SubExpression) extends Expression
case class Interval(exp: SubExpression) extends Expression
case class Inverse(exp: Expression) extends Expression

/** Represents an expression that can be done on values, which may be a specific field of a struct */
trait SubExpression
case object Latest extends SubExpression
case object Sum extends SubExpression
case object Min extends SubExpression
case object Max extends SubExpression
case object Mean extends SubExpression
case object Gradient extends SubExpression
case object StandardDeviation extends SubExpression
case object CountUnique extends SubExpression
case object NumFlips extends SubExpression
case object CountBy extends SubExpression
case object DaysSince extends SubExpression
case object DaysSinceLatestBy extends SubExpression
case object DaysSinceEarliestBy extends SubExpression
case class Proportion(value: String) extends SubExpression
case class LatestN(n: Int) extends SubExpression
