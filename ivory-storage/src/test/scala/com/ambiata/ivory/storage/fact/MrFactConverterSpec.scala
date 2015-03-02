package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.poacher.mr.Writables
import org.apache.hadoop.io._
import org.scalacheck._
import org.scalacheck.Prop._
import org.specs2._

class MrFactConverterSpec extends Specification with ScalaCheck { def is = s2"""

Laws
====

  PartitionFact
    ${laws(PartitionFactWriter(), NullWritable.get(), Writables.bytesWritable(4096))}

  MutableFact
    ${laws(MutableFactWriter(), NullWritable.get(), Writables.bytesWritable(4096))}

  NamespaceDateFact
    ${laws(NamespaceDateFactWriter(), new IntWritable, Writables.bytesWritable(4096))}
"""

  def laws[K <: Writable, V <: Writable](mr: MrFactWriter[K, V], k: K, v: V): Prop = new Properties("MrFactConverter laws") {
    property("symmetric") = forAll((f1: Fact, f2: Fact) => {
      mr.write(f1, k, v).convert(f2.toNamespacedThrift, k, v)
      f2 ?= f1
    })
    property("consistent") = forAll((f1: Fact, f2: Fact, f3: Fact) => {
      mr.write(f1, k, v).convert(f3.toNamespacedThrift, k, v)
      mr.write(f2, k, v).convert(f3.toNamespacedThrift, k, v)
      f2 ?= f3
    })
  }
}
