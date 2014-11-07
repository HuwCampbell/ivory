package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._
import org.joda.time.DateTimeZone
import Arbitraries._


// FIX ARB Name? looks like this has moved on...
case class PrimitiveSparseEntities(meta: ConcreteDefinition, fact: Fact, zone: DateTimeZone)

object PrimitiveSparseEntities {
  implicit def PrimitiveSparseEntitiesArbitrary: Arbitrary[PrimitiveSparseEntities] =
    Arbitrary(GenFact.factWithZone(GenEntity.entity, GenDictionary.concreteWith(arbitrary[PrimitiveEncoding]))
      .map((PrimitiveSparseEntities.apply _).tupled))
}
