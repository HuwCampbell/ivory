package com.ambiata.ivory.core

import scalaz._, Scalaz._

/* A date range of interest for a specified id, `from` is _exclusive_, `to` is _inclusive_. */
case class Range[A](id: A, froms: List[Date], to: Date) {
  def from: Option[Date] =
    froms.sorted.headOption

  def fromOrMin: Date =
    from.getOrElse(Date.minValue)

  def fromOrMax: Date =
    from.getOrElse(Date.maxValue)
}

object Range {
  implicit def RangeEqual[A: Equal]: Equal[Range[A]] =
    Equal.equal((a, b) => a.id === b.id && a.froms === b.froms && a.to === b.to)
}
