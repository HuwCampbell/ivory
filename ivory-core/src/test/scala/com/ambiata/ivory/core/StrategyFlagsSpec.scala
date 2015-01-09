package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._


class StrategyFlagSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                        ${equal.laws[StrategyFlag]}

Properties
----------

   Symmetric parsing:

     ${ prop((f: StrategyFlag) => StrategyFlag.fromString(f.render) ==== f.some) }

"""
}
