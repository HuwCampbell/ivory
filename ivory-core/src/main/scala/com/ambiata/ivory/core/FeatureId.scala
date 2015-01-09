package com.ambiata.ivory.core

import argonaut._, Argonaut._
import com.ambiata.mundane.parse.Delimited
import com.ambiata.mundane.parse.ListParser._

import scala.math.{Ordering => SOrdering}
import scalaz._, Scalaz._

case class FeatureId(namespace: Namespace, name: String) {
  override def toString =
    toString(":")

  def toString(delim: String): String =
    s"${namespace.name}${delim}${name}"
}

object FeatureId {
  implicit val FeatureIdSOrdering: SOrdering[FeatureId] =
    SOrdering.by(f => (f.namespace.name, f.name))

  implicit def FeatureIdEqual: Equal[FeatureId] =
    Equal.equalA[FeatureId]

  def parse(featureId: String): String \/ FeatureId =
    (string tuple string).run(Delimited.parseRow(featureId, ':')).disjunction.flatMap {
      case (ns, n) => Namespace.nameFromStringDisjunction(ns).map(FeatureId(_, n))
    }

  implicit def FeatureIdCodecJson: CodecJson[FeatureId] = CodecJson.derived(
    EncodeJson(_.toString.asJson),
    DecodeJson.optionDecoder(_.string.flatMap(parse(_).toOption), "FeatureId")
  )
}
