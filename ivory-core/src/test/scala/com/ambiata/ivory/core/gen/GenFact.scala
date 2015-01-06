package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._

import org.joda.time.DateTimeZone
import org.scalacheck._


object GenFact {
  def fact: Gen[Fact] =
    factWithZone(GenEntity.entity, GenDictionary.concrete).map(_._2)

  // FIX ARB Gens as args are a smell.
  def factWithZone(entity: Gen[String], mgen: Gen[ConcreteDefinition]): Gen[(ConcreteDefinition, Fact, DateTimeZone)] = for {
    f <- GenIdentifier.feature
    m <- mgen
    (dt, z) <- GenDate.dateTimeWithZone
    f <- factWith(entity, f, m, Gen.const(dt))
  } yield (m, f, z)

  // FIX ARB Gens as args are a smell.
  def factWith(entity: Gen[String], f: FeatureId, m: ConcreteDefinition, dtGen: Gen[DateTime]): Gen[Fact] = for {
    e <- entity
    dt <- dtGen
    // Don't generate a Tombstone if it's not possible
    v <- Gen.frequency((if (m.tombstoneValue.nonEmpty) 1 else 0) -> Gen.const(TombstoneValue), 99 -> GenValue.valueOf(m.encoding, m.tombstoneValue))
  } yield Fact.newFact(e, f.namespace.name, f.name, dt.date, dt.time, v)
}
