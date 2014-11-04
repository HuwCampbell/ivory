package com.ambiata.ivory.core

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._

import scalaz._, Scalaz._
import argonaut._, Argonaut._

class OldIdentifierSpec extends Specification with ScalaCheck { def is = s2"""

Old Identifier Properties
---------------------

  Can't overflow                                  $overflow
  Can't parse ints greater then 99999             $invalid
  Can parse all ints below 100000                 $valid
  Render/parse is symmetric                       $symmetric
  Encode/Decode Json is symmetric                 $encodeDecodeJson
  Can't Decode ints greater than 99999            $decodeIntFail
  Initial starts with zero                        $initial
  If next succeeds identifier is always larger    $next
  It can be created as a literal                  $literal

"""
  def overflow = {
    val max = OldIdentifier.parse(OldIdentifier.max.render)
    (max must beSome) and (max.flatMap(_.next) must beNone)
  }

  case class GreaterThanMax(id: String)
  implicit def GreaterThanMaxArbitrary: Arbitrary[GreaterThanMax] =
    Arbitrary(Gen.choose(OldIdentifier.max.toInt + 1, Long.MaxValue).map(l => GreaterThanMax(l.toString)))

  def invalid = prop((i: GreaterThanMax) =>
    OldIdentifier.parse(i.id) must beNone)

  case class LessThanMax(id: String)
  implicit def LessThanMaxArbitrary: Arbitrary[LessThanMax] =
    Arbitrary(Gen.choose(0, OldIdentifier.max.toInt).map(i => LessThanMax(i.toString)))

  def valid = prop((i: LessThanMax) =>
    OldIdentifier.parse(String.format("%05d", java.lang.Integer.valueOf(i.id))) must beSome)

  def symmetric = prop((i: OldIdentifier) =>
    OldIdentifier.parse(i.render) must_== Some(i))

  def encodeDecodeJson = prop((i: OldIdentifier) =>
    Parse.decodeEither[OldIdentifier](i.asJson.nospaces) must_== i.right)

  def decodeIntFail = prop((n: GreaterThanMax) =>
    Parse.decodeEither[OldIdentifier](n.id.asJson.nospaces) must_== "OldIdentifier: []".left)

  def decodeIntPass = prop((n: LessThanMax) =>
    Parse.decodeEither[OldIdentifier](n.id.asJson.nospaces) must_== n.right)

  def initial =
    Some(OldIdentifier.initial) must_== OldIdentifier.parse("00000")

  def next = prop((i: OldIdentifier) =>
    i.next.forall(_ > i))

  def literal =
    Some(OldIdentifier("1234")) must_== OldIdentifier.parse("1234")
}
