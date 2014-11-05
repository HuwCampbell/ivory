package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.mr._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import com.ambiata.ivory.mr.Counter
import com.ambiata.ivory.operation.extraction.reduction.CountReducer
import org.scalacheck.Arbitrary
import org.specs2.{ScalaCheck, Specification}
import scalaz._
import scalaz.scalacheck.ScalazProperties.monoid

class SquashProfileSpec extends Specification with ScalaCheck { def is = s2"""

  Profile facts                                                   $profile
  SquashCounts monoid laws                                        ${monoid.laws[SquashCounts]}

"""

  case class PCounter(var counter: Int) extends Counter {
    def count(i: Int): Unit =
      counter += 1
  }

  def profile = propNoShrink((factss: Short, s: Short, fact: Fact) => {
    val facts = Math.abs(factss)
    val c1 = MemoryCounter()
    val c2 = MemoryCounter()
    val p1 = PCounter(0)
    val p2 = PCounter(0)
    val mod = Math.abs(s) + 1
    val reducer = new SquashProfileReduction(mod, new CountReducer, c1, c2, p1, p2)
    val f = fact.withValue(StringValue(""))
    (0 until facts).map(_ => f).foreach(reducer.update)
    val result = reducer.save
    result.getL ==== facts and
      (c1.counter ==== facts) and
      (c2.counter ==== 1) and (
        if (facts == 0) p1.counter ==== 0 and p2.counter ==== 0
        else p1.counter must beGreaterThan(0) and p2.counter ==== 1
      )
  })

  implicit def SquashCountsArb: Arbitrary[SquashCounts] =
    Arbitrary(for {
      a <- Arbitrary.arbitrary[Long]
      b <- Arbitrary.arbitrary[Long]
      c <- Arbitrary.arbitrary[Long]
      d <- Arbitrary.arbitrary[Long]
    } yield SquashCounts(a, b, c, d))

  implicit def SquashCountsEquals: Equal[SquashCounts] =
    Equal.equalA[SquashCounts]
}
