package com.ambiata.ivory.core

/* A date range of interest for a specified namespace, from is _exclusive_, to is _inclusive_. */
case class NamespaceRange(id: Name, froms: List[Date], to: Date) {
  def from: Date =
    froms.sorted.headOption.getOrElse(Date.minValue)
}
