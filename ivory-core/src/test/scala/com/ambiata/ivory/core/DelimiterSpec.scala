package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class DelimiterSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                        ${equal.laws[Delimiter]}


Combinators
-----------

  fromString/render symmetry:

     ${ prop((d: Delimiter) => Delimiter.fromString(d.render) ==== d.some) }

"""
}
