package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class LatestByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  LatestBy reducer works with arbitrary facts               $latestBy
"""

  def latestBy = prop((fact: Fact, keyname: String, keyVals: List[(String, List[(String, PrimitiveValue)])]) => {
    val r = new LatestByReducer(keyname)
    keyVals.foreach { case (v, ms) => {
      val asPrim: Map[String, PrimitiveValue] = Map(keyname -> StringValue(v))
      val newFact = fact.withValue(StructValue(ms.toMap ++ asPrim))
      r.update(newFact)
    }}

    Value.fromThrift(r.save) match {
      case ListValue(ls) => ls.toSet ==== keyVals.groupBy(_._1).map(_._2.last).map { case (v, ms) => {
          val asPrim: Map[String, PrimitiveValue] = Map(keyname -> StringValue(v))
          StructValue(ms.toMap ++ asPrim)
        }}.toSet
      case _ => ko
    }
  })
}
