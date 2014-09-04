package com.ambiata.ivory.core

import scalaz._, Scalaz._

sealed trait Expression

object Expression {

  /**
   * For now we want to be able to represent a single expression as a simple string.
   * In future this will be far more complex, but that will take some time.
   */
  def asString(e: Expression): String = e match {
    case Count => "count"
    case Latest => "latest"
  }

  def parse(exp: String): Option[Expression] = exp match {
    case "count"  => some(Count)
    case "latest" => some(Latest)
    case _        => none
  }
}

case object Count extends Expression
case object Latest extends Expression
