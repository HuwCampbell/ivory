package com.ambiata.ivory.core

import org.specs2._

class TextEscapingSpec extends Specification with ScalaCheck { def is = s2"""

  Escape and parsing should be symmetric                     $escapeAndSplit
"""

  def escapeAndSplit = prop {(s: List[String], c: Char) => s != List("") ==> {
    TextEscaping.split(c, TextEscaping.mkString(c, s)) ==== s
  }}
}
