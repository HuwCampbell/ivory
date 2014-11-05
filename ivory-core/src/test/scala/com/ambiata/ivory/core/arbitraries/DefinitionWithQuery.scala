package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._
import Arbitraries._


/** namespace for features */
case class DefinitionWithQuery(cd: ConcreteDefinition, expression: Expression, filter: FilterEncoded)

object DefinitionWithQuery {
  implicit def DefinitionWithQueryArbitrary: Arbitrary[DefinitionWithQuery] =
    Arbitrary(for {
      cd <- GenDictionary.concreteWith(Gen.oneOf(
          arbitrary[PrimitiveEncoding]
        , arbitrary[StructEncoding]
        ))
      e <- GenDictionary.expression(cd)
      f <- GenDictionary.filter(cd)
    } yield DefinitionWithQuery(cd, e, f.get))
}
