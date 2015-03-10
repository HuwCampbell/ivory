package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core.gen._
import com.ambiata.ivory.core.{PrimitiveEncoding, PrimitiveValue}
import org.scalacheck.{Arbitrary, Gen}

case class PrimitiveValuePair(e: PrimitiveEncoding, v1: PrimitiveValue, v2: PrimitiveValue)

object PrimitiveValuePair {

  implicit def PrimitiveValuePairArbitrary: Arbitrary[PrimitiveValuePair] =
    Arbitrary(genPrimitiveValuePair)

  def genPrimitiveValuePair: Gen[PrimitiveValuePair] = for {
    e <- GenDictionary.primitiveEncoding
    a <- GenValue.valueOfPrim(e)
    b <- GenValue.valueOfPrim(e)
  } yield PrimitiveValuePair(e, a, b)
}
