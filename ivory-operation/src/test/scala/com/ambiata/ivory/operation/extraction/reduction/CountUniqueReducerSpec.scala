package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class CountUniqueReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the unique count of an arbitrary number of ints               $countUnique
"""

  def countUnique = prop((l: List[String], s: Short) =>
    ReducerUtil.run(new CountUniqueReducer[String], List.fill(Math.abs(s) + 1)(l).flatten) ==== l.distinct.length
  )
}
