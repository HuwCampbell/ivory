package com.ambiata.ivory.core

import com.ambiata.mundane.io.FilePath
import com.ambiata.mundane.parse.ListParser

import scalaz._, Scalaz._

case class FactsetId(id: Identifier) {
  def render = id.render
  def asKeyName = id.asKeyName
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
    FactsetId(Identifier.initial)

  def listParser: ListParser[FactsetId] =
    Identifier.listParser.map(FactsetId.apply)

  def parse(strId: String): Option[FactsetId] =
    Identifier.parse(strId).map(FactsetId.apply)
}
