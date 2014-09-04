package com.ambiata.ivory.core

import Arbitraries._
import org.specs2._

class ExpressionSpec extends Specification with ScalaCheck { def is = s2"""

  Can serialise to and from a string                       $parse
"""

  def parse = prop((e: Expression) =>
    Expression.parse(Expression.asString(e)) ==== Some(e)
  )
}
