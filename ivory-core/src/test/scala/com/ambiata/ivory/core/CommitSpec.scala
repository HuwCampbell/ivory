package com.ambiata.ivory.core

import org.specs2._
import org.scalacheck._, Arbitrary._
import arbitraries.Arbitraries._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class CommitSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                        ${equal.laws[Commit]}

"""

}
