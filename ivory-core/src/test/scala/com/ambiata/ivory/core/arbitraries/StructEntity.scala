package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class StructEntity(k: String, e: StructEncoding, v: StructValue)

object StructEntity {
   implicit def StructEntityArbitrary: Arbitrary[StructEntity] =
      Arbitrary(genStructEntity)

   def genStructEntity: Gen[StructEntity] = for {
      k <- GenString.sensible
      e <- GenDictionary.structEncodingWithMode(Mode.keyedSet(k))
      v <- GenValue.valueOfStruct(e)
   } yield StructEntity(k, e, v)
}
