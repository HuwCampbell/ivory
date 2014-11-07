package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._

import org.scalacheck._, Arbitrary._
import Arbitraries._


// FIX ARB Get rid of this, it isn't doing anything. But it is used _lots_, so
//         I want to make sure no one is particularly attached to it first.
case class FactAndPriority(f: Fact, p: Priority)

object FactAndPriority {
  implicit def ArbitraryFactAndPriority: Arbitrary[FactAndPriority] =
    Arbitrary(for {
      f <- Arbitrary.arbitrary[Fact]
      p <- Arbitrary.arbitrary[Priority]
    } yield new FactAndPriority(f, p))
}
