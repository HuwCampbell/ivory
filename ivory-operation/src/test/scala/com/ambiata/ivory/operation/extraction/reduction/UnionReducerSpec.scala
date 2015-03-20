package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}
import org.scalacheck.{Gen, Arbitrary}
import com.ambiata.disorder._

class UnionReducerSpec extends Specification with ScalaCheck { def is = s2"""
  UnionReducer is providing the smallest possible list          $unionsSize
  UnionReducer reducer obtains the union of a list of lists     $unions
"""

  def unionsSize = prop((xs: List10[List10[Int]]) => {
    val xsl = xs.value.map(_.value)
    ReducerUtil.run(new UnionReducer[Int], xsl).length ====
      xsl.flatten.toSet.size
  })

  def unions = prop((xs: List10[List10[Int]]) => {
    val xsl = xs.value.map(_.value)
    ReducerUtil.run(new UnionReducer[Int], xsl).toSet ====
      xsl.flatten.toSet
  })

}
