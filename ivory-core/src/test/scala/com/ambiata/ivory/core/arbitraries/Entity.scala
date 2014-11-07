package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core.gen._

import org.scalacheck._


case class Entity(value: String)

object Entity {
  implicit def EntityArbitrary: Arbitrary[Entity] =
    Arbitrary(GenEntity.entity map Entity.apply)
}
