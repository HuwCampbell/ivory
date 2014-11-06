package com.ambiata.ivory.core

import com.ambiata.ivory.core.Arbitraries._
import org.specs2._

class FeatureIdSpec extends Specification with ScalaCheck { def is = s2"""

  Parse a valid FeatureId                                 $parseOk
  Fail on trying to parse invalid FeatureId               $parseFail
"""

  def parseOk = prop((fid: FeatureId) =>
    FeatureId.parse(fid.toString).toOption must beSome(fid)
  )

  def parseFail =
    seqToResult(List("", "a").map(FeatureId.parse(_).toEither must beLeft))
}
