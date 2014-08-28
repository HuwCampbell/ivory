package com.ambiata.ivory.core

import com.ambiata.ivory.data.Identifier

import scalaz._, Scalaz._

case class CommitId(id: Identifier) {
  def render = id.render
  def next = id.next.map(CommitId.apply)
  def order(other: CommitId): Ordering =
    id ?|? other.id
}

object CommitId {
  implicit def CommitIdOrder: Order[CommitId] =
    Order.order(_ order _)

  implicit def CommitIdOrdering =
    CommitIdOrder.toScalaOrdering

  def initial: CommitId =
    CommitId(Identifier.initial)

  def parse(strId: String): Option[CommitId] =
    Identifier.parse(strId).map(CommitId.apply)
}
