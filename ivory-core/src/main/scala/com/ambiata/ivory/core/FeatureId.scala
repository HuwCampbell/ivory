package com.ambiata.ivory.core

import scala.math.{Ordering => SOrdering}

case class FeatureId(namespace: Name, name: String) {
  override def toString =
    toString(":")

  def toString(delim: String): String =
    s"${namespace.name}${delim}${name}"
}

object FeatureId {
  implicit val FeatureIdSOrdering: SOrdering[FeatureId] =
    SOrdering.by(f => (f.namespace.name, f.name))
}
