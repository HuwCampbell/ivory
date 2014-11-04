package com.ambiata.ivory.core

import com.ambiata.mundane.io.FileName
import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}

case class DictionaryId(id: Identifier) {
  def render = id.render
  def asKeyName = id.asKeyName
  def next = id.next.map(DictionaryId.apply)
  def order(other: DictionaryId): Ordering =
    id ?|? other.id
}

object DictionaryId {
  implicit def DictionaryIdOrder: Order[DictionaryId] =
    Order.order(_ order _)

  implicit def DictionaryIdOrdering: SOrdering[DictionaryId] =
    DictionaryIdOrder.toScalaOrdering

  def initial: DictionaryId =
    DictionaryId(Identifier.initial)

  def parse(strId: String): Option[DictionaryId] =
    Identifier.parse(strId).map(DictionaryId.apply)
}
