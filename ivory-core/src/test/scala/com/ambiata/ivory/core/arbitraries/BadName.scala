package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class BadName(name: String)

object BadName {
  implicit def BadNameArbitrary: Arbitrary[BadName] =
    Arbitrary(GenString.badName.map(BadName.apply))
}
