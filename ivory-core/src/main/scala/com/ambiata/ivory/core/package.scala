package com.ambiata.ivory

import com.ambiata.ivory.core.thrift._

package object core {
  type NamespacedFactV1 = NamespacedThriftFactV1 with NamespacedThriftFactDerivedV1
  type NamespacedFactV2 = NamespacedThriftFactV2 with NamespacedThriftFactDerivedV2
  type NamespacedFact = NamespacedFactV1

  def createNamespacedFact: NamespacedFact = {
    val fact = new NamespacedThriftFactV1 with NamespacedThriftFactDerivedV1
    fact.setFact(new ThriftFactV1)
    fact
  }
}
