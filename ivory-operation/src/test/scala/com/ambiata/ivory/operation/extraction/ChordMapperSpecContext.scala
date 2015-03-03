package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.mr._
import com.ambiata.ivory.lookup._
import com.ambiata.poacher.mr._

import org.apache.hadoop.io.BytesWritable

import org.scalacheck._, Arbitrary.arbitrary

case class ChordMapperSpecContext(facts: List[Fact]) {
  val all = facts
  val serializer = ThriftSerialiser()
  val ok = MemoryCounter()
  val lookup = new FeatureIdLookup(new java.util.HashMap[String, Integer])
  val emitter = TestEmitter[BytesWritable, BytesWritable, (BytesWritable, BytesWritable)]((k, v) => (k, v))
  facts.zipWithIndex.foreach({ case (f, i) =>
    lookup.putToIds(f.featureId.toString, i)
  })
}

object ChordMapperSpecContext {
  implicit def ChordMapperSpecContextArbitrary: Arbitrary[ChordMapperSpecContext] = Arbitrary(Gen.resize(10, for {
    facts   <- arbitrary[List[Fact]].map(fs => fs.zipWithIndex.map({ case (f, i) => f.withEntity(f.entity + i) }))
  } yield ChordMapperSpecContext(facts)))
}
