package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._

import com.ambiata.mundane.io._
import com.ambiata.mundane.data.{Lists => L}
import com.ambiata.poacher.hdfs._

import org.apache.hadoop.fs.Path

object DictionaryOutput {

  /** Turn a dictionary into indexed lines ready to store externally */
  def indexedDictionaryLines(dictionary: Dictionary, tombstone: String, delim: Char): List[String] = {
    import com.ambiata.ivory.storage.metadata.DictionaryTextStorage
    dictionary.byFeatureIndex.toList.sortBy(_._1).map({
      case (i, d) => i.toString + delim + DictionaryTextStorage.delimitedLineWithDelim(d.featureId -> (d match {
        case Concrete(_, m) =>
          m.copy(tombstoneValue = List(tombstone))
        case Virtual(_, vd) =>
          val (source, mode) = dictionary.byFeatureId.get(vd.source).flatMap {
            case Concrete(_, cd) => Some(cd.encoding -> cd.mode)
            case Virtual(_, _)   => None
          }.getOrElse(StringEncoding -> Mode.State)
          ConcreteDefinition(expressionEncoding(vd.query.expression, source), mode, None, "", List(tombstone))
      }), delim.toString)
    })
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

  def writeToHdfs(output: Path, dictionary: Dictionary, missing: String, delimiter: Char): Hdfs[Unit] =
    Hdfs.writeWith(new Path(output, ".dictionary"), os =>
      Streams.write(os, L.prepareForFile(indexedDictionaryLines(dictionary, missing, delimiter))))
}
