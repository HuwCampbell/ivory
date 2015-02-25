package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._


/** namespace for features */
case class DefinitionWithQuery(cd: ConcreteDefinition, expression: Expression, filter: FilterEncoded)

object DefinitionWithQuery {
  implicit def DefinitionWithQueryArbitrary: Arbitrary[DefinitionWithQuery] =
    Arbitrary(for {
      m <- GenDictionary.mode
      cd <- GenDictionary.concreteWith(m, GenDictionary.subEncodingWithMode(m).map(_.toEncoding))
      e <- GenDictionary.expression(cd)
      f <- GenDictionary.filter(cd)
    } yield DefinitionWithQuery(cd, e, f.get))
}
