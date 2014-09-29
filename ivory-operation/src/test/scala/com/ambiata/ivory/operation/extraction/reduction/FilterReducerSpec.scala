package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core.thrift.ThriftFactValue
import org.specs2.{ScalaCheck, Specification}

class FilterReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Count arbitrary number of filtered facts    $count
  Eval all facts                              $eval
"""

  def count = prop((f: FactsWithFilter) => {
    val counter = new CountReducer
    val r = FilterReducer.compileEncoded(f.filter.filter, counter)
    (f.facts ++ f.other).foreach(r.update)
    r.save ==== ThriftFactValue.l(f.facts.size)
  })

  def eval = prop((f: FactsWithFilter) => {
    val r = FilterReducer.compileExpression(f.filter.filter)
    f.facts.forall(r.eval) && !f.other.exists(r.eval)
  })
}
