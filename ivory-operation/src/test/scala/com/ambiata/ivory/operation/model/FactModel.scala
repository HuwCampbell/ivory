package com.ambiata.ivory.operation.model

import com.ambiata.ivory.core._

object FactModel {

  /** Flattens an ordered list of in-memory factsets to a list of prioritized facts */
  def factsPriority(facts: List[List[Fact]]): List[Prioritized[Fact]] =
    // Remember - a low number is "high" priority, so the oldest factsets have the "lowest" priority
    facts.reverse.zipWithIndex.flatMap(pf => pf._1.map(f => Prioritized(Priority.unsafe(pf._2.toShort), f)))
}
