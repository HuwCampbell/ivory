package com.ambiata.ivory.core

import com.ambiata.mundane.parse.Delimited
import com.ambiata.mundane.parse.ListParser._

import scala.math.{Ordering => SOrdering}
import scalaz._, Scalaz._

case class FeatureId(namespace: Name, name: String) {
  override def toString =
    toString(":")

  def toString(delim: String): String =
    s"${namespace.name}${delim}${name}"
}

object FeatureId {
  implicit val FeatureIdSOrdering: SOrdering[FeatureId] =
    SOrdering.by(f => (f.namespace.name, f.name))

  def parse(featureId: String): String \/ FeatureId =
    (string tuple string).run(Delimited.parseRow(featureId, ':')).disjunction.flatMap {
      case (ns, n) => Name.nameFromStringDisjunction(ns).map(FeatureId(_, n))
    }
}
