package com.ambiata.ivory.core

case class NamespaceRanges(namespaces: List[NamespaceRange]) {
  /** Determine if this set of namespaces is fully contained by the
      specified set of ranges, i.e. for each of the namespaces
      does it include data _at or before_ the required date for that
      namespace.*/
  def containedBy(ranges: NamespaceRanges): Boolean ={
    val indexed = ranges.namespaces.groupBy(_.id)
    namespaces.forall(required => indexed.get(required.id).exists(_.map(_.from).headOption.getOrElse(Date.minValue) <= required.from)) }
  }
