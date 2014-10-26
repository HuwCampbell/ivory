package com.ambiata.ivory.data

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.data.Arbitraries._

import scalaz._, Scalaz._
import argonaut._, Argonaut._

class IdentiferSpec extends Specification with ScalaCheck { def is = s2"""

Identifier Properties
---------------------

  Can't overflow                                  $overflow
  Can't parse longs                               $longs
  Can parse all ints                              $ints
  Render/parse is symmetric                       $symmetric
  Encode/Decode Json is symmetric                 $encodeDecodeJson
  Can't Decode some long values                   $decodeLong
  Initial starts with zero                        $initial
  If next succeeds identifier is always larger    $next
  Literals work                                   $literals

"""
  def overflow = {
    val max = Identifier.parse("ffffffff")
    (max must beSome) and (max.flatMap(_.next) must beNone)
  }

  def longs =
    Identifier.parse("fffffffff") must beNone

  def ints = prop((n: Int) =>
    Identifier.parse(String.format("%08x", java.lang.Integer.valueOf(n))) must beSome)

  def symmetric = prop((i: Identifier) =>
    Identifier.parse(i.render) must_== Some(i))

  def encodeDecodeJson = prop((i: Identifier) =>
    Parse.decodeEither[Identifier](i.asJson.nospaces) must_== i.right)

  def decodeLong = prop((n: Int) =>
    Parse.decodeEither[Identifier]((n.toLong + 0xffffffffL).asJson.nospaces) must_== "Identifier: []".left)

  def initial =
    Some(Identifier.initial) must_== Identifier.parse("0")

  def next = prop((i: Identifier) =>
    i.next.forall(_ > i))

  def literals =
    Some(Identifier("1234")) must_== Identifier.parse("1234")
}
