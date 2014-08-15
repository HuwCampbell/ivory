package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core.Fact
import org.specs2.Specification

class NoBoxingSpec extends Specification { def is = s2"""
  The Fact class must not create a boxed value when providing the namespace Name $fact
"""
  
  def fact = {
    classOf[Fact].getMethod("namespace").getReturnType.getName === "java.lang.String"
  }
  
}
