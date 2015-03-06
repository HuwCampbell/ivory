package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries
import org.specs2._
import scalaz.scalacheck.ScalazProperties

class FactSpec extends Specification with ScalaCheck { def is = s2"""

Laws
====

  Order
    $order

"""

  def order =
    ScalazProperties.order.laws[Fact](Fact.orderEntityDateTime, Arbitraries.FactArbitrary)
}
