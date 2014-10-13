package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.matcher._
import org.specs2.matcher.MustMatchers._
import spire.math._
import spire.implicits._

case class SignificantFigures(i: Int)

object SignificantFigures {

  implicit class SignificantFiguresSyntax(value: Int) {
    def significantfigures = SignificantFigures(value)
  }

  def beCloseTo[A: Numeric](target: A, i: SignificantFigures): Matcher[A] = {a: A=>
    if (target == 0)
      (a == 0, s"Target 0 doesn't have significant figures, but $a is not matching anyway.")
    else {
      val border = Math.pow(10, Math.log10(a.toDouble().abs).floor - (i.i - 1))
      ((a - target).abs().toDouble < border, s"$a is not within ${i.i} significant figures of $target")
    }
  }
}
