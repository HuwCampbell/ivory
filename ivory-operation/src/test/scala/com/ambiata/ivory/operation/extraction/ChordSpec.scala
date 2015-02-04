package com.ambiata.ivory.operation.extraction

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import com.ambiata.ivory.operation.extraction.squash.SquashConfig
import com.ambiata.ivory.storage.legacy.SnapshotLoader
import com.ambiata.ivory.storage.repository._
import org.specs2._
import scalaz.effect.IO

class ChordSpec extends Specification with ScalaCheck { def is = s2"""

ChordSpec
---------

  Can extract expected facts (state)                  $normal         ${tag("mr")}
  Can extract expected facts with windowing (state)   $windowingState ${tag("mr")}
  Can extract expected facts with windowing (set)     $windowingSet   ${tag("mr")}
"""

  def normal = prop((local: LocalTemporary, cf: ChordFacts) => cf.expected.nonEmpty ==> {
    val facts = cf.copy(window = None).withMode(Mode.State)
    run(local, facts.copy(window = None), facts.dictionary) must beOkLike(_ must containTheSameElementsAs(facts.expected))
  }).set(minTestsOk = 1)

  def windowingState = prop((local: LocalTemporary, cf: ChordFacts, window: Window) => cf.expected.nonEmpty ==> {
    val facts = cf.copy(window = Some(window)).withMode(Mode.State)
    run(local, facts, facts.dictionaryWithCount) must beOkLike(_ must containTheSameElementsAs(facts.expectedSquashState))
  }).set(minTestsOk = 1)

  def windowingSet = prop((local: LocalTemporary, cf: ChordFacts, window: Window) => cf.expected.nonEmpty ==> {
    // FIX Ignore "latest" facts as they are non-deterministic until sets are fully implemented
    // This is because the ordering of facts in squash ignores (and can't know about) priority
    // When this is fixed we probably don't need both window tests any more
    // https://github.com/ambiata/ivory/issues/376
    def filterCount(f: Fact): Boolean = f.value match { case LongValue(_) => true case _ => false }
    val facts = cf.copy(window = Some(window)).withMode(Mode.Set)
    run(local, facts, facts.dictionaryWithCount) must
      beOkLike(_.filter(filterCount) must containTheSameElementsAs(facts.expectedSquashSet.filter(filterCount)))
  }).set(minTestsOk = 1)

  def run(local: LocalTemporary, facts: ChordFacts, dictionary: Dictionary): RIO[List[Fact]] = for {
    c <- ClusterTemporary().cluster
    d <- local.directory
    r <- RepositoryBuilder.using { repo =>
      val entities = facts.ces.flatMap(ce => ce.dates.map(ce.entity + "|" + _._1.hyphenated))
      for {
        _                <- RepositoryBuilder.createRepo(repo, dictionary, facts.allFacts)
        entitiesLocation =  IvoryLocation.fromDirPath(d </> "entities")
        _                <- IvoryLocation.writeUtf8Lines(entitiesLocation, entities)
        out              <- Repository.tmpDir("chord-spec").map(repo.toIvoryLocation)
        output           = OutputDataset(out.location)
        outPath          <- Chord.createChordWithSquash(repo, IvoryFlags.default, entitiesLocation, facts.takeSnapshot, SquashConfig.testing, c)
        facts            <- RIO.safe[List[Fact]](SnapshotLoader.readV1(outPath._1.hdfsPath, repo.scoobiConfiguration))
      } yield facts
    }
  } yield r
}
