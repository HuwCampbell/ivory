package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class SumReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the sum of an arbitrary number of doubles            $sumDouble
  Take the sum of an arbitrary number of longs              $sumLong
"""

  def sumDouble = prop((xs: List[Double]) =>
    ReducerUtil.run(new SumReducer[Double](), xs) ==== xs.sum
  )

  def sumLong = prop((xs: List[Long]) =>
    ReducerUtil.run(new SumReducer[Long](), xs) ==== xs.sum
  )
}
