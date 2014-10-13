package com.ambiata.ivory.operation.extraction.reduction
import org.specs2.{ScalaCheck, Specification}

/**
 * There are numerous ways in which the quantile of a set can be defined. In 
 * Sample Quantiles in Statistical packaged (Hyndman and Fan, 1996) the authors
 * describe 9 different algorithms, which differ in continuity, the location and
 * values at any discontinuities, and the characteristics of any interpolattion
 * between the values for continuous functions.
 * This algorithm is algorithm 7, which is the default in S and GNU R. It is 
 * a continuous function, where the value used when the quantile does not lie
 * directly on a value is the linearly weighted average between the two. This 
 * is necessarily a double. Algorithms which return ints and longs must be 
 * discontinuous, and a different function would have to be chosen.
 */


class QuantileInReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the quantiles of a sorted array      $quantileInDays
"""

  def quantileInDays = {
    val input = Array(1, 2, 3, 4, 5)
    val output = List((0, 4, 1.0)
        ,(1, 4, 2.0)
        ,(2, 4, 3.0)
        ,(3, 4, 4.0)
        ,(4, 4, 5.0)
    ).map { case (a, b, c) => QuantileInReducer.quantileOnSorted(a, b, input) must beCloseTo(c +/- 1E-9) }

    val inputLonger = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val outputLonger = List((0, 4, 1.0)
        ,(1, 4, 3.25)
        ,(2, 4, 5.5)
        ,(3, 4, 7.75)
        ,(4, 4, 10.0)
    ).map { case (a, b, c) => QuantileInReducer.quantileOnSorted(a, b, inputLonger) must beCloseTo(c +/- 1E-9) }

    val inputComplex = Array(1,2,7,8,14,17,21,35,36,37,49,55)
    val outputComplex =  List((0, 10, 1.0)
        ,(1, 10, 2.5)
        ,(2, 10, 7.2)
        ,(3, 10, 9.8)
        ,(4, 10, 15.2)
        ,(5, 10, 19.0)
        ,(6, 10, 29.4)
        ,(7, 10, 35.7)
        ,(8, 10, 36.8)
        ,(9, 10, 47.8)
        ,(10, 10, 55.0)
    ).map { case (a, b, c) => QuantileInReducer.quantileOnSorted(a, b, inputComplex) must beCloseTo(c +/- 1E-9) }

    output ++ outputLonger ++ outputComplex
  }
}
