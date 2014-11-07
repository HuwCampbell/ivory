package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._
import org.joda.time.DateTimeZone

import Arbitraries._


// FIX ARB Name? looks like this has moved on...
case class SparseEntities(meta: ConcreteDefinition, fact: Fact, zone: DateTimeZone)

object SparseEntities {
  implicit def SparseEntitiesArbitrary: Arbitrary[SparseEntities] =
    Arbitrary(GenFact.factWithZone(GenEntity.entity, arbitrary[ConcreteDefinition]).map((SparseEntities.apply _).tupled))
}
