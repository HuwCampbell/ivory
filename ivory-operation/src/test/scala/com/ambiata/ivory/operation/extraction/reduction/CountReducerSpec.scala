package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.thrift.ThriftFactValue
import org.specs2.{ScalaCheck, Specification}

class CountReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Count arbitrary number of facts    $count
  Clear resets the count             $clear
"""

  def count = prop((facts: List[Fact]) => {
    val r = new CountReducer()
    facts.foreach(r.update)
    r.save ==== ThriftFactValue.l(facts.count(!_.isTombstone))
  })

  def clear = prop((facts: List[Fact]) => {
    val r = new CountReducer()
    facts.foreach(r.update)
    r.clear()
    r.save ==== ThriftFactValue.l(0)
  })
}
