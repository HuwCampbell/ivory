package com.ambiata.ivory.core

import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}

case class DictionaryId(id: String) {
  def render = id
  def order(other: DictionaryId): Ordering =
    id ?|? other.id
}

object DictionaryId {
  implicit def DictionaryIdOrder: Order[DictionaryId] =
    Order.order(_ order _)

  implicit def DictionaryIdOrdering =
    DictionaryIdOrder.toScalaOrdering
}
