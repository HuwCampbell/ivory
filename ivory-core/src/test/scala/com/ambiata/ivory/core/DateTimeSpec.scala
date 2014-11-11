package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._
import scalaz.scalacheck.ScalazProperties

class DateTimeSpec extends Specification with ScalaCheck { def is = s2"""

  Order laws                                             ${ScalazProperties.order.laws[DateTime]}
  Can read and write to json                             ${ArgonautProperties.encodedecode[DateTime]}
"""
}
