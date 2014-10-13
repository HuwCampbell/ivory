package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class ProportionReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the proportion of an arbitrary number of strings            $proportion
"""

  def proportion = prop((a: String, b: List[String], s: Short) => {
    val ps = Math.abs(s)
    val l = util.Random.shuffle(List.fill(Math.abs(ps))(a) ++ b.filter(_ != a))
    ReducerUtil.run(new ProportionReducer[String](a), l) ==== (if (l.isEmpty) 0 else ps / l.length)
  })
}
