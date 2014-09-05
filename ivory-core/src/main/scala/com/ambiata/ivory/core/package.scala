package com.ambiata.ivory

import com.ambiata.ivory.core.thrift.NamespacedThriftFact
import com.ambiata.mundane.control._

package object core {
  type MutableFact = NamespacedThriftFact with NamespacedThriftFactDerived
  def createMutableFact: MutableFact =
    new NamespacedThriftFact with NamespacedThriftFactDerived
  type ReferenceIO = Reference[ResultTIO]
  type ReferenceResultT[F[+_]] = Reference[({ type l[a] = ResultT[F, a] })#l]
}
