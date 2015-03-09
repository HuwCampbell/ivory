package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._
import scalaz.scalacheck.ScalazProperties

class FactSpec extends Specification with ScalaCheck { def is = s2"""

Laws
====

  Order
    $order

Properties
==========

  Can set the date time
    $withDateTime

  fromThriftPrimitive and toThriftPrimitive are symmetrical
    $thriftPrimitive

"""

  def order =
    ScalazProperties.order.laws[Fact](Fact.orderEntityDateTime, Arbitraries.FactArbitrary)

  def withDateTime = prop((f: Fact, dt: DateTime) =>
    f.withDateTime(dt).datetime ==== dt
  )

  def thriftPrimitive = prop((v: PrimitiveValue) =>
    Value.fromThriftPrimitive(Value.toThriftPrimitive(v)) ==== v
  )
}
