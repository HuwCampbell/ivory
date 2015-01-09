package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._
import org.scalacheck._

object GenFlags {
  def plan: Gen[StrategyFlag] =
    Gen.oneOf(PessimisticStrategyFlag, OptimisticStrategyFlag, ConservativeStrategyFlag)

  def flags: Gen[IvoryFlags] =
    plan.map(IvoryFlags.apply)
}
