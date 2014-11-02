package com.ambiata.ivory.operation.extraction

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.repository._
import com.nicta.scoobi.Scoobi._
import org.specs2._
import scalaz.effect.IO

class ChordSpec extends Specification with ScalaCheck { def is = s2"""

ChordSpec
---------

  Can extract expected facts                          $normal    ${tag("mr")}
  Can extract expected facts with windowing           $windowing ${tag("mr")}
"""

  def normal = prop((facts: ChordFacts) => facts.expected.nonEmpty ==> (
    run(facts, windowing = false) must beOkLike(_ must containTheSameElementsAs(facts.expected))
  )).set(minTestsOk = 1)

  def windowing = prop((facts: ChordFacts) => facts.expected.nonEmpty ==> (
    run(facts, windowing = true) must beOkLike(_ must containTheSameElementsAs(facts.expectedWindow))
  )).set(minTestsOk = 1)

  def run(facts: ChordFacts, windowing: Boolean) =
    TemporaryDirPath.withDirPath { directory =>
      RepositoryBuilder.using { repo =>
        val entities = facts.ces.flatMap(ce => ce.dates.map(ce.entity + "|" + _._1.hyphenated))
        implicit val sc = repo.scoobiConfiguration
        for {
          _                <- RepositoryBuilder.createRepo(repo, facts.dictionary, facts.allFacts)
          entitiesLocation =  IvoryLocation.fromDirPath(directory </> "entities")
          _                <- IvoryLocation.writeUtf8Lines(entitiesLocation, entities)
          outPath          <- Chord.createChord(repo, entitiesLocation, takeSnapshot = facts.takeSnapshot, windowing)
          facts            <- ResultT.safe[IO, List[Fact]](valueFromSequenceFile[Fact](repo.toIvoryLocation(outPath._1).toHdfs).run.toList)
        } yield facts
      }
    }
}
