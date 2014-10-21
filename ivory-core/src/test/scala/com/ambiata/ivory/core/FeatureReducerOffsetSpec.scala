package com.ambiata.ivory.core

import org.specs2.{ScalaCheck, Specification}

class FeatureReducerOffsetSpec extends Specification with ScalaCheck { def is = s2"""

  Offset and count are encoded and decoded from an int            $intEncoding
"""

  def intEncoding = prop((i: Short, j: Short, entity: Int) => (j > Short.MinValue && j < Short.MaxValue) ==> {
    val offset = Math.abs(i).toShort
    val count = Math.abs(j) + 1
    FeatureReducerOffset.getReducer(FeatureReducerOffset(offset, count.toShort).toInt, entity) ==== ((entity & Int.MaxValue) % count + offset)
  })
}
