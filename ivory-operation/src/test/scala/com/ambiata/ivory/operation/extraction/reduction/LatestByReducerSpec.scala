package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class LatestByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  LatestBy reducer works with arbitrary facts               $latestBy
"""

  def latestBy = prop((fact: Fact, keyname: String, keyVals: List[String], others: List[(String, PrimitiveValue)]) => {
    val r = new LatestByReducer(keyname)
    keyVals.foreach(keyVal => {
      val asPrim: (String, PrimitiveValue) = keyname -> StringValue(keyVal)
      val newFact = fact.withValue(StructValue((others ++ List(asPrim)).toMap))
      r.update(newFact)
    })
     
    Value.fromThrift(r.save) ==== ListValue(keyVals.distinct.map(keyVal => {
      val asPrim: (String, PrimitiveValue) = keyname -> StringValue(keyVal)
      StructValue((others ++ List(asPrim)).toMap)
    }))

    r.clear()
  })
}
