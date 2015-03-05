package com.ambiata.ivory.core.arbitraries

import com.ambiata.disorder._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.scalacheck.Arbitrary, Arbitrary._

case class FactsWithStatePriority(values: Map[DateTime, NaturalIntSmall]) {

  def factsets(template: Fact): List[List[Fact]] = {
    val max = values.values.map(_.value).max
    (0 until max)
      .map(i => values.filter(_._2.value > i).map(x => x._1 -> i).toList).toList
      .map(_.map(x => {
        val value = Value.unique(template.value, x._2)
        Fact.newFactWithNamespace(template.entity, template.namespace, template.feature, x._1.date, x._1.time, value)
      }))
      .reverse
  }
}

object FactsWithStatePriority {

  implicit def FactsWithStatePriorityArbitrary: Arbitrary[FactsWithStatePriority] =
    Arbitrary(GenPlus.listOfSized(2, 10, arbitrary[(DateTime, NaturalIntSmall)]).map(l =>  FactsWithStatePriority(l.toMap)))
}

