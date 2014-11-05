package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._
import Arbitraries._


/** value with its corresponding encoding */
case class EncodingAndValue(enc: Encoding, value: Value)

object EncodingAndValue {
  implicit def EncodingAndValueArbitrary: Arbitrary[EncodingAndValue] =
    Arbitrary(for {
      enc <- arbitrary[Encoding]
      value <- Gen.frequency(
          19 -> GenValue.valueOf(enc, List())
        , 1 -> Gen.const(TombstoneValue)
        )
    } yield EncodingAndValue(enc, value))
}
