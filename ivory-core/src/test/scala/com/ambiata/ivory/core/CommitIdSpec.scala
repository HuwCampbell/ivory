package com.ambiata.ivory.core

import org.specs2._
import org.scalacheck._, Arbitrary._, Arbitraries._

import scalaz._, Scalaz._
import argonaut._, Argonaut._

class CommitIdSpec extends Specification with ScalaCheck { def is = s2"""

Commit Id Properties
--------------------

  Encode/Decode Json is symmetric               $encodeDecodeJson

"""

  def encodeDecodeJson = prop((id: CommitId) =>
    Parse.decodeEither[CommitId](id.asJson.nospaces) must_== id.right)

}
