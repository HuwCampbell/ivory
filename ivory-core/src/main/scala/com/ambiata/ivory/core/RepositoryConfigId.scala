package com.ambiata.ivory.core

import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}

case class RepositoryConfigId(id: Identifier) {
  def render = id.render
  def asKeyName = id.asKeyName
  def next = id.next.map(RepositoryConfigId.apply)
  def order(other: RepositoryConfigId): Ordering =
    id ?|? other.id
}

object RepositoryConfigId {
  implicit def RepositoryConfigOrder: Order[RepositoryConfigId] =
    Order.order(_ order _)

  implicit def RepositoryConfigOrdering: SOrdering[RepositoryConfigId] =
    RepositoryConfigOrder.toScalaOrdering

  def initial: RepositoryConfigId =
    RepositoryConfigId(Identifier.initial)

  def parse(strId: String): Option[RepositoryConfigId] =
    Identifier.parse(strId).map(RepositoryConfigId.apply)
}
