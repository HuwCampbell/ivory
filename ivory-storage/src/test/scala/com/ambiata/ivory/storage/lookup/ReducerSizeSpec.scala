package com.ambiata.ivory.storage.lookup

import com.ambiata.mundane.io.MemoryConversions._
import org.specs2.{ScalaCheck, Specification}

class ReducerSizeSpec extends Specification with ScalaCheck { def is = s2"""

   Reducers count is always positive                $positive
   Reducers calculation does the right thing        $sanityCheck
"""

  def positive = prop((s: Short, b: Short) => {
    ReducerSize.calculateFromSize(s.bytes, (Math.abs(b) + 1).bytes) must beGreaterThan(0)
  })

  def sanityCheck =
    ReducerSize.calculateFromSize(10.gb, 1.gb) ==== 10
}
