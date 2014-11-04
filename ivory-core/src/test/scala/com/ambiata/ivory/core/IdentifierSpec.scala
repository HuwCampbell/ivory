package com.ambiata.ivory.core

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._

import scalaz._, Scalaz._
import argonaut._, Argonaut._

class IdentifierSpec extends Specification with ScalaCheck { def is = s2"""

Identifier Properties
---------------------

  Can't overflow                                  $overflow
  Can't parse longs                               $longs
  Can parse all ints                              $ints
  Can parse all v1 ints                           $intsV1
  Render length                                   $render
  Render/parse is symmetric                       $symmetric
  Encode/Decode Json is symmetric                 $encodeDecodeJson
  Can't Decode some long values                   $decodeLong
  Initial starts with zero                        $initial
  If next succeeds identifier is always larger    $next
  Next on any v1 is a v2 identifier               $nextV1
  Literals work                                   $literals
  All v1 identifiers are less than v2             $v1v2order

"""
  def overflow = {
    val max = Identifier.parse("ffffffff")
    (max must beSome) and (max.flatMap(_.next) must beNone)
  }

  def longs =
    Identifier.parse("fffffffff") must beNone

  def ints = prop((n: Int) =>
    Identifier.parse(String.format("%08x", java.lang.Integer.valueOf(n))) must beSome)

  def intsV1 = prop((n: IV1) =>
    Identifier.parse(String.format("%05d", java.lang.Integer.valueOf(n.id.toInt))) must beSome)

  def render = prop((i: Identifier) =>
    i.render.length must_== (if (i.tag == IdentifierV1) 5 else 8))

  def symmetric = prop((i: Identifier) =>
    Identifier.parse(i.render) must_== Some(i))

  def encodeDecodeJson = prop((i: Identifier) =>
    Parse.decodeEither[Identifier](i.asJson.nospaces) must_== i.right)

  def decodeLong = prop((n: Int) =>
    Parse.decodeEither[Identifier]((n.toLong + 0xffffffffL).asJson.nospaces) must_== "Identifier: []".left)

  def initial =
    Identifier.initial must_== Identifier("00000000")

  def next = prop((i: Identifier) =>
    i.next.forall(_ > i))

  def nextV1 = prop((i: IV1) =>
    i.id.next must beSome(Identifier.initial))

  def v1v2order = prop((i: IV1, j: IV2) =>
    i.id < j.id must beTrue)

  def literals =
    Some(Identifier("01234")) must_== Identifier.parse("01234")

  case class IV1(id: Identifier)
  implicit def IV1Arbitrary: Arbitrary[IV1] =
    Arbitrary(Gen.choose(1, 99999).map(x => IV1(Identifier.unsafeV1(x))))

  case class IV2(id: Identifier)
  implicit def IV2Arbitrary: Arbitrary[IV2] =
    Arbitrary(Gen.choose(1l, 0xffffffffl).map(x => IV2(Identifier.unsafe(x.toInt))))

}
