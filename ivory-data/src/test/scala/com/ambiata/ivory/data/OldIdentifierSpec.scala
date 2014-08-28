package com.ambiata.ivory.data

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.data.Arbitraries._

import scalaz._, Scalaz._

class OldIdentifierSpec extends Specification with ScalaCheck { def is = s2"""

Old Identifier Properties
---------------------

  Can't overflow                                  $overflow
  Can't parse ints greater then 99999             $invalid
  Can parse all ints below 100000                 $valid
  Render/parse is symmetric                       $symmetric
  Initial starts with zero                        $initial
  If next succeeds identifier is always larger    $next
  It can be created as a literal                  $literal

"""
  def overflow = {
    val max = OldIdentifier.parse(OldIdentifier.max.render)
    (max must beSome) and (max.flatMap(_.next) must beNone)
  }

  case class GreaterThenMax(id: String)
  implicit def GreaterThenMaxArbitrary: Arbitrary[GreaterThenMax] =
    Arbitrary(Gen.choose(OldIdentifier.max.toInt + 1, Long.MaxValue).map(l => GreaterThenMax(l.toString)))

  def invalid = prop((i: GreaterThenMax) =>
    OldIdentifier.parse(i.id) must beNone)

  case class LessThenMax(id: String)
  implicit def LessThenMaxArbitrary: Arbitrary[LessThenMax] =
    Arbitrary(Gen.choose(0, OldIdentifier.max.toInt).map(i => LessThenMax(i.toString)))

  def valid = prop((i: LessThenMax) =>
    OldIdentifier.parse(String.format("%05d", java.lang.Integer.valueOf(i.id))) must beSome)

  def symmetric = prop((i: OldIdentifier) =>
    OldIdentifier.parse(i.render) must_== Some(i))

  def initial =
    Some(OldIdentifier.initial) must_== OldIdentifier.parse("00000")

  def next = prop((i: OldIdentifier) =>
    i.next.forall(_ > i))

  def literal =
    Some(OldIdentifier("1234")) must_== OldIdentifier.parse("1234")
}
