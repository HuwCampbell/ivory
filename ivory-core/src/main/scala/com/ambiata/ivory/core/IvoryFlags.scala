package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class IvoryFlags(
  plan: StrategyFlag
)

object IvoryFlags {
  implicit def IvoryFlagsEqual: Equal[IvoryFlags] =
    Equal.equalA

  def default: IvoryFlags =
    IvoryFlags(PessimisticStrategyFlag)
}

sealed trait StrategyFlag {
  def render: String = this match {
    case PessimisticStrategyFlag =>
      "pessimistic"
    case OptimisticStrategyFlag =>
      "optimistic"
    case ConservativeStrategyFlag =>
      "conservative"
  }
}

case object PessimisticStrategyFlag extends StrategyFlag
case object OptimisticStrategyFlag extends StrategyFlag
case object ConservativeStrategyFlag extends StrategyFlag

object StrategyFlag {
  implicit def StrategyFlagEqual: Equal[StrategyFlag] =
    Equal.equalA

  def fromString(s: String): Option[StrategyFlag] = s match {
    case "pessimistic" =>
      PessimisticStrategyFlag.some
    case "optimistic" =>
      OptimisticStrategyFlag.some
    case "conservative" =>
      ConservativeStrategyFlag.some
    case _ =>
      none
  }
}
