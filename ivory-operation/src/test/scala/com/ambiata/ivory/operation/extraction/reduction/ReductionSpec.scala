package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.arbitraries._, ArbitraryFeatures._
import com.ambiata.ivory.core.Date
import com.ambiata.ivory.core.thrift.ThriftFactValue
import org.specs2.{ScalaCheck, Specification}

class ReductionSpec extends Specification with ScalaCheck { def is = s2"""

  Can always compile valid expression                     $compile
"""

  def compile = prop((d: DefinitionWithQuery) => {
    val dates = DateOffsets.calculateLazyCompact(Date.minValue, Date.minValue)
    Reduction.fromExpression(d.expression, d.cd.encoding, dates) must beSome
  })
}
