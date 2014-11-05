package com.ambiata.ivory.operation.extraction

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._, Arbitraries._
import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import com.ambiata.ivory.operation.extraction.squash.SquashConfig
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

  def normal = prop((cf: ChordFacts) => cf.expected.nonEmpty ==> {
    val facts = cf.copy(window = None)
    run(facts.copy(window = None), facts.dictionary) must beOkLike(_ must containTheSameElementsAs(facts.expected))
  }).set(minTestsOk = 1)

  def windowing = prop((cf: ChordFacts, window: Window) => cf.expected.nonEmpty ==> {
    val facts = cf.copy(window = Some(window))
    run(facts, facts.dictionaryWithCount) must beOkLike(_ must containTheSameElementsAs(facts.expectedSquash))
  }).set(minTestsOk = 1)

  def run(facts: ChordFacts, dictionary: Dictionary): ResultTIO[List[Fact]] =
    TemporaryDirPath.withDirPath { directory =>
      RepositoryBuilder.using { repo =>
        val entities = facts.ces.flatMap(ce => ce.dates.map(ce.entity + "|" + _._1.hyphenated))
        implicit val sc = repo.scoobiConfiguration
        for {
          _                <- RepositoryBuilder.createRepo(repo, dictionary, facts.allFacts)
          entitiesLocation =  IvoryLocation.fromDirPath(directory </> "entities")
          _                <- IvoryLocation.writeUtf8Lines(entitiesLocation, entities)
          out              <- Repository.tmpDir(repo).map(repo.toIvoryLocation)
          facts            <- Chord.createChordWithSquash(repo, entitiesLocation, takeSnapshot = facts.takeSnapshot,
            SquashConfig.testing, List(out))((outPath, _) => ResultT.safe[IO, List[Fact]](
            valueFromSequenceFile[Fact](repo.toIvoryLocation(outPath).toHdfs).run.toList
          ))
        } yield facts
      }
    }
}
