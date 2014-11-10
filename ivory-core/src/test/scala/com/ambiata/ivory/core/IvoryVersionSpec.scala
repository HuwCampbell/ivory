package com.ambiata.ivory.core

import argonaut._, Argonaut._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._

class IvoryVersionSpec extends Specification with ScalaCheck { def is = s2"""

  Version is taken from the BuildInfo                        $version
  Can read and write to json                                 $json
"""

  def version =
    IvoryVersion.get.version ==== BuildInfo.version

  def json = prop { (v: IvoryVersion) =>
    Parse.decodeEither[IvoryVersion](v.asJson.spaces2).toEither must beRight(v)
  }
}
