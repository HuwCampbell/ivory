package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class TextEscapingSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                                    ${equal.laws[TextEscaping]}

Combinators
-----------

  fromString/render symmetry:

    ${ prop((e: TextEscaping) => TextEscaping.fromString(e.render) ==== e.some) }

Escaping
--------

  Escape and parsing should be symmetric                     $escapeAndSplit
"""

  def escapeAndSplit = prop {(s: List[String], c: Char) => (s != List("") && c != TextEscaping.escapeChar) ==> {
    TextEscaping.split(c, TextEscaping.mkString(c, s)) ==== s
  }}
}
