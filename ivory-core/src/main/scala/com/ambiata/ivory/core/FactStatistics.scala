package com.ambiata.ivory.core

import argonaut._, Argonaut._

import scalaz._, Scalaz._

case class FactStatistics(stats: List[FactStatisticsType]) {
  def numerical: List[NumericalFactStatistics] =
    stats.flatMap(_.toNumerical)

  def categorical: List[CategoricalFactStatistics] =
    stats.flatMap(_.toCategorical)

  def +++(other: FactStatistics): FactStatistics =
    FactStatistics(stats ++ other.stats)
}

object FactStatistics {

  def fromNumerical(numerical: List[NumericalFactStatistics]): FactStatistics =
    FactStatistics(numerical.map(NumericalFactStatisticsType.apply))

  def fromCategorical(categorical: List[CategoricalFactStatistics]): FactStatistics =
    FactStatistics(categorical.map(CategoricalFactStatisticsType.apply))

  def valueToCategory(fact: Fact): String = fact.value match {
    case IntValue(i)      => i.toString
    case LongValue(l)     => l.toString
    case DoubleValue(d)   => d.toString
    case TombstoneValue   => "☠"
    case StringValue(s)   => s
    case BooleanValue(b)  => b.toString
    case DateValue(r)     => r.hyphenated
    case ListValue(v)     => "List entries"
    case StructValue(m)   => "Struct entries"
  }
}

object FactStasticsCodecs {

  implicit def FactStatisticsCodecJson: CodecJson[FactStatistics] =
    casecodec1(FactStatistics.apply, FactStatistics.unapply)("stats")

  implicit def FactStatisticsTypeCodecJson: CodecJson[FactStatisticsType] =
    CodecJson(_ match {
      case NumericalFactStatisticsType(n)   => jSingleObject("Numerical", NumericalCodecJson.encode(n))
      case CategoricalFactStatisticsType(c) => jSingleObject("Categorical", CategoricalCodecJson.encode(c))
    }, a => {
      val en = (a --\ "Numerical").success
      val ec = (a --\ "Categorical").success
      (en, ec) match {
        case (Some(c), None) => NumericalCodecJson.decode(c) map (NumericalFactStatisticsType(_))
        case (None, Some(c)) => CategoricalCodecJson.decode(c) map (CategoricalFactStatisticsType(_))
        case _ => DecodeResult.fail("Expecting either 'Numerical' or 'Categorical'", a.history)
      }
    })

  implicit def NumericalCodecJson: CodecJson[NumericalFactStatistics] =
    casecodec5(NumericalFactStatistics.apply, NumericalFactStatistics.unapply)("featureId", "date", "count", "sum", "sqsum")

  implicit def CategoricalCodecJson: CodecJson[CategoricalFactStatistics] =
    casecodec3(CategoricalFactStatistics.apply, CategoricalFactStatistics.unapply)("featureId", "date", "histogram")
}

sealed trait FactStatisticsType {
  def fold[X](
    numerical: NumericalFactStatistics => X
  , categorical: CategoricalFactStatistics => X
  ): X = this match {
    case NumericalFactStatisticsType(n) => numerical(n)
    case CategoricalFactStatisticsType(c) => categorical(c)
  }

  def toNumerical: Option[NumericalFactStatistics] =
    fold(_.some, _ => none)

  def toCategorical: Option[CategoricalFactStatistics] =
    fold(_ => none, _.some)

  def isNumerical: Boolean =
    fold(_ => true, _ => false)

  def isCategorical: Boolean =
    fold(_ => false, _ => true)
}
case class NumericalFactStatisticsType(stats: NumericalFactStatistics) extends FactStatisticsType
case class CategoricalFactStatisticsType(stats: CategoricalFactStatistics) extends FactStatisticsType

case class NumericalFactStatistics(featureId: FeatureId, date: Date, count: Long, mean: Double, sqsum: Double)
case class CategoricalFactStatistics(featureId: FeatureId, date: Date, histogram: Map[String, Long])
