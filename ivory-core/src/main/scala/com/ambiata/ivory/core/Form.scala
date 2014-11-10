package com.ambiata.ivory.core

import scalaz._, Scalaz._

sealed trait Form {
  import Form._

  def render: String = this match {
    case Sparse =>
      "sparse"
    case Dense =>
      "dense"
  }
}

object Form {
  case object Sparse extends Form
  case object Dense extends Form

  def unapply(s: String): Option[Form] =
    fromString(s)

  def fromString(s: String): Option[Form] = s match {
    case "sparse" =>
      Sparse.some
    case "dense" =>
      Dense.some
    case _ =>
      none
  }
}
