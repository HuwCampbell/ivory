package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._

import scalaz.scalacheck.ScalazProperties

class IvoryVersionSpec extends Specification with ScalaCheck { def is = s2"""

  Version is taken from the BuildInfo                        $version
  Equal laws                                                 ${ScalazProperties.equal.laws[IvoryVersion]}
  Encode and decode as JSON                                  ${ArgonautProperties.encodedecode[IvoryVersion]}
"""

  def version =
    IvoryVersion.get.version ==== BuildInfo.version
}
