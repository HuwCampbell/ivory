package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._
import scalaz._
import scalaz.scalacheck.ScalazArbitrary._

class SumByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the sum of a list of arbitrary ints                       $sumBy
  Sum by laws                                                    $sumByLaws
"""

  def sumBy = prop((xs: Map[String, NonEmptyList[Int]]) => {
    val xss = xs.toList.flatMap(x => x._2.list.map(x._1 ->))
    ReducerUtil.run2(new SumByReducer[Int], xss).map.asScala.toMap ==== xs.mapValues(_.list.sum)
  })

  def sumByLaws =
    ReducerUtil.reductionFold2Laws(new SumByReducer[Int])
}
