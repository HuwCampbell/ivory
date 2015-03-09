package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class StructEntity(k: List[(String, PrimitiveEncoding, PrimitiveValue)], e: StructEncoding, v: StructValue) {

   def keys: List[String] =
      k.map(_._1)

   def encoding: StructEncoding =
      StructEncoding(e.values ++ k.map(x => x._1 -> StructEncodedValue.mandatory(x._2)).toMap)

   def value: StructValue =
      StructValue(v.values ++ k.map(x => x._1 -> x._3))
}

object StructEntity {
   implicit def StructEntityArbitrary: Arbitrary[StructEntity] =
      Arbitrary(genStructEntity)

   def genStructEntity: Gen[StructEntity] = for {
      k <- GenPlus.listOfSizedWithIndex(1, 3, i => for {
         k <- GenString.sensible
         e <- GenDictionary.primitiveEncoding
         v <- GenValue.valueOfPrim(e)
      } yield (s"k_${k}_${i}", e, v))
      e <- GenDictionary.structEncodingWithMode(Mode.State)
      v <- GenValue.valueOfStruct(e)
   } yield StructEntity(k, e, v)
}
