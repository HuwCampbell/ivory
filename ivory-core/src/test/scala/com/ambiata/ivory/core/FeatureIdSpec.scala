package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._
import scalaz.scalacheck.ScalazProperties

class FeatureIdSpec extends Specification with ScalaCheck { def is = s2"""

  Parse a valid FeatureId                                 $parseOk
  Fail on trying to parse invalid FeatureId               $parseFail
  Equal laws                                              ${ScalazProperties.equal.laws[FeatureId]}
  Encode and decode as JSON                               ${ArgonautProperties.encodedecode[FeatureId]}
"""

  def parseOk = prop((fid: FeatureId) =>
    FeatureId.parse(fid.toString).toOption must beSome(fid)
  )

  def parseFail =
    seqToResult(List("", "a").map(FeatureId.parse(_).toEither must beLeft))
}
