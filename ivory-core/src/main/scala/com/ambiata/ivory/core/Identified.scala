package com.ambiata.ivory.core

import scalaz._, Scalaz._

/** A slightly less cumbersome pair, for purpose of attaching Id's to metadata. */
case class Identified[A, B](id: A, value: B)

object Identified {
  implicit def IdentifiedEqual[A: Equal, B: Equal]: Equal[Identified[A, B]] =
    Equal.equal((a, b) =>  a.id === b.id && a.value === b.value)
}
