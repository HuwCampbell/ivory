package com.ambiata.ivory.core

import com.ambiata.ivory.core.gen._
import com.ambiata.ivory.core.arbitraries._, Arbitraries._
import org.specs2._
import org.scalacheck._, Arbitrary._
import scalaz.scalacheck.ScalazProperties

class FactSpec extends Specification with ScalaCheck { def is = s2"""

Laws
====

  Order
    $order

Properties
==========

  Can set the date time
    $withDateTime

  fromThriftPrimitive and toThriftPrimitive are symmetrical
    $thriftPrimitive

  Facts with entity ids larger then 256 are invalid
    $invalidEntityIdFact
"""

  def order =
    ScalazProperties.order.laws[Fact](Fact.orderEntityDateTime, FactArbitrary)

  def withDateTime = prop((f: Fact, dt: DateTime) =>
    f.withDateTime(dt).datetime ==== dt
  )

  def thriftPrimitive = prop((v: PrimitiveValue) =>
    Value.fromThriftPrimitive(Value.toThriftPrimitive(v)) ==== v
  )

  def invalidEntityIdFact = prop((fact: BadEntityFact) =>
    Fact.validate(fact.fact, fact.dictionary).toEither must beLeft)

  case class BadEntityFact(fact: Fact, dictionary: Dictionary)
  implicit def BadEntityArbitrary: Arbitrary[BadEntityFact] = Arbitrary(for {
    n     <- Gen.choose(257, 1000)
    chars <- Gen.listOfN(n, arbitrary[Char])
    cg    <- arbitrary[ConcreteGroupFeature]
    dt    <- arbitrary[DateTime]
    fact  <- GenFact.factWith(chars.mkString, cg.fid, cg.cg.definition, dt)
  } yield BadEntityFact(fact, cg.dictionary))
}
