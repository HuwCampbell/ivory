package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries._, Arbitraries._
import FactStasticsCodecs._

import org.specs2._
import scalaz._, Scalaz._
import argonaut._, Argonaut._

class FactStatisticsSpec extends Specification with ScalaCheck { def is = s2"""

Json
----

  Symmetric JSON parsing                                     $symmerticJson

Combinators

   Can get all numerical statistics                          $numericalStats
   Can get all categorical statistics                        $categoricalStats
   isNumerical works                                         $isNumerical
   isCategorical works                                       $isCategorical
   append works                                              $append
   symmetric from/to numerical statistics                    $symmetricNumerical

"""

  def symmerticJson = prop((stats: FactStatistics) =>
    Parse.decodeEither[FactStatistics](stats.asJson.nospaces) ==== stats.right[String])

  def numericalStats = prop((stats: FactStatistics) =>
    stats.numerical ==== stats.stats.collect({ case NumericalFactStatisticsType(n) => n }))

  def categoricalStats = prop((stats: FactStatistics) =>
    stats.categorical ==== stats.stats.collect({ case CategoricalFactStatisticsType(c) => c }))

  def isNumerical = prop((stats: FactStatistics) =>
    stats.stats.filter(_.isNumerical).flatMap(_.toNumerical) ==== stats.numerical)

  def isCategorical = prop((stats: FactStatistics) =>
    stats.stats.filter(_.isCategorical).flatMap(_.toCategorical) ==== stats.categorical)

  def append = prop((s1: FactStatistics, s2: FactStatistics) =>
    (s1 +++ s2).stats ==== s1.stats ++ s2.stats)

  def symmetricNumerical = prop((stats: FactStatistics) =>
    FactStatistics.fromNumerical(stats.numerical).numerical ==== stats.numerical)

  def symmerticCategorical = prop((stats: FactStatistics) =>
    FactStatistics.fromCategorical(stats.categorical).categorical ==== stats.categorical)
}
