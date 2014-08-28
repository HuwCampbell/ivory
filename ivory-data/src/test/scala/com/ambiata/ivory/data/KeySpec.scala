package com.ambiata.ivory.data

import org.specs2._
import org.scalacheck._, Arbitrary._

class KeySpec extends Specification with ScalaCheck { def is = s2"""

Key Properties
--------------

  Render/create is symmetric                       $symmetric
  Literals work                                    $literals

"""
  def symmetric = prop((k: Key) =>
    Key.create(k.render) must_== Some(k))

  def literals =
    Some(Key("hello")) must_== Key.create("hello")

  implicit def KeyArbitrary: Arbitrary[Key] =
    Arbitrary(Gen.identifier map (s => Key.create(s).get))
}
