package com.ambiata.ivory.core

import argonaut._
import org.scalacheck._
import scalaz._

object ArgonautProperties {

  // This should probably live in a separate argonaut module at some point
  def encodedecode[A: EncodeJson: DecodeJson: Arbitrary: Equal]: Prop =
    Prop.forAll(CodecJson.derived[A].codecLaw.encodedecode _)
}
