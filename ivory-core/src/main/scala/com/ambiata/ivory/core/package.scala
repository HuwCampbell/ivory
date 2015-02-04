package com.ambiata.ivory

import com.ambiata.ivory.core.thrift.{NamespacedThriftFact, ThriftFact}

package object core {
  type MutableFact = NamespacedThriftFact with NamespacedThriftFactDerived

  def createMutableFact: MutableFact = {
    val fact = new NamespacedThriftFact with NamespacedThriftFactDerived
    fact.setFact(new ThriftFact)
    fact
  }
}
