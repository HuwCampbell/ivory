package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.ArgonautProperties._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class BytesSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                        ${equal.laws[Bytes]}
  Order                                        ${order.laws[Bytes]}
  Monoid                                       ${monoid.laws[Bytes]}
  Encode/Decode Json                           ${encodedecode[Bytes]}

Combinators
-----------

  '+' is just a convenience:

    prop((a: Byte, b: Byte) => a + b ==== Byte(a.toLong + b.toLong))

  '-' is just a convenience:

    prop((a: Byte, b: Byte) => a - b ==== Byte(a.toLong - b.toLong))

"""
}
