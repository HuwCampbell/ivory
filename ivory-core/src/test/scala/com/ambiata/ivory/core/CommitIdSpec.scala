package com.ambiata.ivory.core

import arbitraries.Arbitraries._
import com.ambiata.ivory.core.ArgonautProperties._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._


class CommitIdSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                        ${equal.laws[CommitId]}
  Order                                        ${order.laws[CommitId]}
  Encode/Decode Json                           ${encodedecode[CommitId]}

"""
}
