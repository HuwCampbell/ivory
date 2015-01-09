package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.mr._
import com.ambiata.ivory.lookup._
import com.ambiata.ivory.storage.entities._
import com.ambiata.poacher.mr._
import org.scalacheck._, Arbitrary.arbitrary

case class ChordMapperSpecContext(facts: List[Fact], skipped: List[Fact], dropped: List[Fact]) {
  val all = facts ++ skipped ++ dropped
  val serializer = ThriftSerialiser()
  val ok = MemoryCounter()
  val skip = MemoryCounter()
  val drop = MemoryCounter()
  val lookup = new FeatureIdLookup(new java.util.HashMap[String, Integer])
  val emitter = TestEmitter()
  val entities = Entities(new java.util.HashMap)
  facts.zipWithIndex.foreach({ case (f, i) =>
    lookup.putToIds(f.featureId.toString, i)
    entities.entities.put(f.entity, Array(f.date.int + 1))

  })
  skipped.zipWithIndex.foreach({ case (f, i) =>
    lookup.putToIds(f.featureId.toString, i)
    entities.entities.put(f.entity, Array(Date.minValue.int - 1))
  })
}

object ChordMapperSpecContext {
  implicit def ChordMapperSpecContextArbitrary: Arbitrary[ChordMapperSpecContext] = Arbitrary(Gen.resize(10, for {
    facts   <- arbitrary[List[Fact]].map(fs => fs.zipWithIndex.map({ case (f, i) => f.withEntity(f.entity + i) }))
    skipped <- arbitrary[List[Fact]].map(fs => fs.zipWithIndex.map({ case (f, i) => f.withEntity(f.entity + i + facts.size) }))
    dropped <- arbitrary[List[Fact]].map(fs => fs.zipWithIndex.map({ case (f, i) => f.withEntity(f.entity + i + facts.size + skipped.size) }))
  } yield ChordMapperSpecContext(
      facts
    , skipped.filter(f => !facts.exists(_.featureId == f.featureId))
    , dropped.filter(f => !(facts ++ skipped).exists(_.featureId == f.featureId)))))
}
