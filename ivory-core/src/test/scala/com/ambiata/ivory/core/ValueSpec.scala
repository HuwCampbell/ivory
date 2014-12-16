package com.ambiata.ivory.core

import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.arbitraries._, Arbitraries._
import com.ambiata.ivory.core.gen._
import org.specs2.{ScalaCheck, Specification}

class ValueSpec extends Specification with ScalaCheck { def is = s2"""

  Can validate with correct encoding                     $valid
  Can validate with incorrect encoding                   $invalid
  Can convert to and from thrift                         $thrift
  Facts with entity ids larger then 256 are invalid      $invalidEntityIdFact
"""

  def valid = prop((e: EncodingAndValue) =>
    Value.validateEncoding(e.value, e.enc).toEither must beRight
  )

  def invalid = prop((e: EncodingAndValue, e2: Encoding) => (e.enc != e2 && e.value != TombstoneValue && !isCompatible(e, e2)) ==> {
    Value.validateEncoding(e.value, e2).toEither must beLeft
  })

  def thrift = prop { (v: Value) =>
    Value.fromThrift(Value.toThrift(v)) ==== v
  }

  def invalidEntityIdFact = prop((fact: BadEntityFact) =>
    Value.validateFact(fact.fact, fact.dictionary).toEither must beLeft)

  // A small subset of  encoded values are valid for different optional/empty Structs/Lists
  private def isCompatible(e1: EncodingAndValue, e2: Encoding): Boolean =
    (e1, e2) match {
      case (EncodingAndValue(_, StructValue(m)), StructEncoding(v)) => m.isEmpty && v.forall(_._2.optional)
      case (EncodingAndValue(_, ListValue(l)), ListEncoding(e))     => l.forall(v => isCompatible(EncodingAndValue(e, v), e))
      case _ => false
    }

  case class BadEntityFact(fact: Fact, dictionary: Dictionary)
  implicit def BadEntityArbitrary: Arbitrary[BadEntityFact] = Arbitrary(for {
    n     <- Gen.choose(257, 1000)
    chars <- Gen.listOfN(n, arbitrary[Char])
    cg    <- arbitrary[ConcreteGroupFeature]
    dt    <- arbitrary[DateTime]
    fact  <- GenFact.factWith(chars.mkString, cg.fid, cg.cg.definition, dt)
  } yield BadEntityFact(fact, cg.dictionary))
}
