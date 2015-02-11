package com.ambiata.ivory

import com.ambiata.ivory.core.thrift.{NamespacedThriftFact, ThriftFact}

package object core {
  type NamespacedFact = NamespacedThriftFact with NamespacedThriftFactDerived

  def createNamespacedFact: NamespacedFact = {
    val fact = new NamespacedThriftFact with NamespacedThriftFactDerived
    fact.setFact(new ThriftFact)
    fact
  }
}
