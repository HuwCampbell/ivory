package com.ambiata.ivory.core

import org.specs2._
import scalaz._, Scalaz._

class OptionTPlusSpec extends Specification with ScalaCheck { def is = s2"""
  Create OptionT from an Option                 $fromOption
  When true create a Some value                 $whenTrue
  When false ignore input and return None       $whenFalse

"""

  def fromOption = prop((o: Option[String]) =>
    OptionTPlus.fromOption[Id, String](o).run ==== o
  )

  def whenTrue = prop((s: String) =>
    OptionTPlus.when[Id, String](true, s).run must beSome(s)
  )

  def whenFalse =
    OptionTPlus.when[Id, String](false, sys.error("")).run must beNone
}
