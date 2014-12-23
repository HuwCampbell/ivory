package com.ambiata.ivory.core

import argonaut._
import com.ambiata.notion.core._
import scalaz._, Scalaz._


case class CommitId(id: Identifier) {
  def render: String =
    id.render

  def asKeyName: KeyName =
    id.asKeyName

  def next: Option[CommitId] =
    id.next.map(CommitId.apply)

  def order(other: CommitId): Ordering =
    id ?|? other.id
}

object CommitId {
  def initial: CommitId =
    CommitId(Identifier.initial)

  def parse(strId: String): Option[CommitId] =
    Identifier.parse(strId).map(CommitId.apply)

  implicit def CommitIdOrder: Order[CommitId] =
    Order.order(_ order _)

  implicit def CommitIdOrdering: scala.Ordering[CommitId] =
    CommitIdOrder.toScalaOrdering

  implicit def CommitIdCodecJson: CodecJson[CommitId] =
    CodecJson.derived[Identifier].xmap(CommitId.apply)(_.id)
}
