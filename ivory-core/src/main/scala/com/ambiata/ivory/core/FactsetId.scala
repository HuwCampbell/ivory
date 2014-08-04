package com.ambiata.ivory.core

import com.ambiata.ivory.data.OldIdentifier
import com.ambiata.mundane.parse.ListParser

import scalaz._, Scalaz._

case class FactsetId(id: OldIdentifier) {
  def render = id.render
  def next = id.next.map(FactsetId.apply)
  def order(other: FactsetId): Ordering =
    id ?|? other.id
}

object FactsetId {
  implicit def FactsetIdOrder: Order[FactsetId] =
    Order.order(_ order _)

  implicit def FactsetIdOrdering: scala.Ordering[FactsetId] =
    FactsetIdOrder.toScalaOrdering

  def initial: FactsetId =
    FactsetId(OldIdentifier.initial)

  val max = FactsetId(OldIdentifier.max)

  def listParser: ListParser[FactsetId] =
    OldIdentifier.listParser.map(FactsetId.apply)

  def parse(strId: String): Option[FactsetId] =
    OldIdentifier.parse(strId).map(FactsetId.apply)
}
