package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class MinMaxReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the min and max of an arbitrary number of doubles            $mmDouble
  Take the min and max of an arbitrary number of ints               $mmInt
  Take the min and max of an arbitrary number of longs              $mmLong
"""

  def mmDouble = prop((xs: List[Double]) => {
    ReducerUtil.run(new MinReducer[Double](), xs) ====
      (if (xs.length < 1) 0.0 else xs.min)
    ReducerUtil.run(new MaxReducer[Double](), xs) ====
      (if (xs.length < 1) 0.0 else xs.max)
  })

  def mmInt = prop((xs: List[Int]) =>{
    ReducerUtil.run(new MinReducer[Int](), xs) ====
      (if (xs.length < 1) 0 else xs.min)
    ReducerUtil.run(new MaxReducer[Int](), xs) ====
      (if (xs.length < 1) 0 else xs.max)
  })

  def mmLong = prop((xs: List[Long]) =>{
    ReducerUtil.run(new MinReducer[Long](), xs) ====
      (if (xs.length < 1) 0L else xs.min)
    ReducerUtil.run(new MaxReducer[Long](), xs) ====
      (if (xs.length < 1) 0L else xs.max)
  })
}
