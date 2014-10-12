package com.ambiata.ivory.core

import com.ambiata.ivory.data.Identifier

import scalaz._, Scalaz._
import argonaut._, Argonaut._

case class SnapshotId(id: Identifier) {
  def render = id.render
  def asKeyName = id.asKeyName
  def next = id.next.map(SnapshotId.apply)
  def order(other: SnapshotId): Ordering =
    id ?|? other.id
}

object SnapshotId {
  implicit def SnapshotIdJSONCodec: CodecJson[SnapshotId] =
    CodecJson(
      ((sid: SnapshotId) =>
        sid.id.asJson),
      (c => c.as[Identifier].map(SnapshotId(_))))

  implicit def SnapshotIdOrder: Order[SnapshotId] =
    Order.order(_ order _)

  implicit def SnapshotIdOrdering =
    SnapshotIdOrder.toScalaOrdering

  def initial: SnapshotId =
    SnapshotId(Identifier.initial)

  def parse(strId: String): Option[SnapshotId] =
    Identifier.parse(strId).map(SnapshotId.apply)
}
