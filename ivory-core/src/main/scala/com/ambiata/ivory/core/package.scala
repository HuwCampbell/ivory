package com.ambiata.ivory

import com.ambiata.ivory.core.thrift.NamespacedThriftFact

package object core {
  type MutableFact = NamespacedThriftFact with NamespacedThriftFactDerived

  def createMutableFact: MutableFact =
    new NamespacedThriftFact with NamespacedThriftFactDerived
}
