package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class MinMaxReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the min and max of an arbitrary number of doubles            $minDouble
  Take the min and max of an arbitrary number of doubles            $maxDouble
  Take the min and max of an arbitrary number of ints               $minInt
  Take the min and max of an arbitrary number of ints               $maxInt
  Take the min and max of an arbitrary number of longs              $minLong
  Take the min and max of an arbitrary number of longs              $maxLong
  Min reducer laws                                                  $minLaws
  Max reducer laws                                                  $maxLaws
"""

  def minDouble = prop((xs: List[Double]) =>
    ReducerUtil.run(new MinReducer[Double](), xs) ====
      (if (xs.length < 1) 0.0 else xs.min)
  )

  def maxDouble = prop((xs: List[Double]) =>
    ReducerUtil.run(new MaxReducer[Double](), xs) ====
      (if (xs.length < 1) 0.0 else xs.max)
  )

  def minInt = prop((xs: List[Int]) =>
    ReducerUtil.run(new MinReducer[Int](), xs) ====
      (if (xs.length < 1) 0 else xs.min)
  )

  def maxInt = prop((xs: List[Int]) =>
    ReducerUtil.run(new MaxReducer[Int](), xs) ====
      (if (xs.length < 1) 0 else xs.max)
  )

  def minLong = prop((xs: List[Long]) =>
    ReducerUtil.run(new MinReducer[Long](), xs) ====
      (if (xs.length < 1) 0L else xs.min)
  )

  def maxLong = prop((xs: List[Long]) =>
    ReducerUtil.run(new MaxReducer[Long](), xs) ====
      (if (xs.length < 1) 0L else xs.max)
  )

  def minLaws =
    ReducerUtil.reductionFoldLaws(new MinReducer[Long])

  def maxLaws =
    ReducerUtil.reductionFoldLaws(new MaxReducer[Long])
}
