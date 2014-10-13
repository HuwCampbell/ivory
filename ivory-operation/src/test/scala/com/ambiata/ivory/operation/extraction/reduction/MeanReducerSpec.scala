package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class MeanReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the mean of an arbitrary number of doubles            $meanDouble
  Take the mean of an arbitrary number of ints               $meanInt
"""

  def meanDouble = prop((xs: List[Double]) =>
    ReducerUtil.run(new MeanReducer[Double](), xs) ====
      (if (xs.length < 1) 0.0 else xs.foldLeft(0.0)(_ + _) / xs.length)
  )

  def meanInt = prop((xs: List[Int]) =>
    ReducerUtil.run(new MeanReducer[Int](), xs) ====
      (if (xs.length < 1) 0.0 else xs.foldLeft(0)(_ + _).toDouble / xs.length)
  )
}
