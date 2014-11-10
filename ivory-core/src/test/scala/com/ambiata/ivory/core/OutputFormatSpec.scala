package com.ambiata.ivory.core

import com.ambiata.ivory.core.ArgonautProperties._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._


class OutputFormatSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Encode/Decode Json                           ${encodedecode[OutputFormat]}
  Equal                                        ${equal.laws[OutputFormat]}


Combinators
-----------

  fromString/render symmetry:

     ${ prop((o: OutputFormat) => OutputFormat.fromString(o.render) ==== o.some) }

"""
}
