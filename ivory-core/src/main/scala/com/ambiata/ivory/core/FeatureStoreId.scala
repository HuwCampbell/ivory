package com.ambiata.ivory.core

import com.ambiata.ivory.data.OldIdentifier
import com.ambiata.mundane.parse.ListParser

import scalaz._, Scalaz._
import argonaut._, Argonaut._

case class FeatureStoreId(id: OldIdentifier) {
  def render = id.render
  def asKeyName = id.asKeyName
  def next = id.next.map(FeatureStoreId.apply)
  def order(other: FeatureStoreId): Ordering =
    id ?|? other.id
}

object FeatureStoreId {
  implicit def FeatureStoreIdOrder: Order[FeatureStoreId] =
    Order.order(_ order _)

  implicit def FeatureStoreIdOrdering =
    FeatureStoreIdOrder.toScalaOrdering

  implicit def FeatureStoreIdCodecJson: CodecJson[FeatureStoreId] = CodecJson(
    (_.id.asJson),
    (_.as[OldIdentifier].map(FeatureStoreId.apply))
    )

  def initial: FeatureStoreId =
    FeatureStoreId(OldIdentifier.initial)

  def listParser: ListParser[FeatureStoreId] =
    OldIdentifier.listParser.map(FeatureStoreId.apply)

  def parse(strId: String): Option[FeatureStoreId] =
    OldIdentifier.parse(strId).map(FeatureStoreId.apply)
}
