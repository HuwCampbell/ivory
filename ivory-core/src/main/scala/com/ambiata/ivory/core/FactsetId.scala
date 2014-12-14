package com.ambiata.ivory.core

import argonaut._

import com.ambiata.notion.core._
import com.ambiata.mundane.io.FilePath
import com.ambiata.mundane.parse.ListParser

import scalaz._, Scalaz._


case class FactsetId(id: Identifier) {
  def render: String =
    id.render

  def asKeyName: KeyName =
    id.asKeyName

  def next: Option[FactsetId] =
    id.next.map(FactsetId.apply)

  def order(other: FactsetId): Ordering =
    id ?|? other.id
}

object FactsetId {
  def initial: FactsetId =
    FactsetId(Identifier.initial)

  def listParser: ListParser[FactsetId] =
    Identifier.listParser.map(FactsetId.apply)

  def parse(strId: String): Option[FactsetId] =
    Identifier.parse(strId).map(FactsetId.apply)

  implicit def FactsetIdOrder: Order[FactsetId] =
    Order.order(_ order _)

  implicit def FactsetIdOrdering: scala.Ordering[FactsetId] =
    FactsetIdOrder.toScalaOrdering

  implicit def FactsetIdCodecJson: CodecJson[FactsetId] =
    CodecJson.derived[Identifier].xmap(FactsetId.apply)(_.id)
}
