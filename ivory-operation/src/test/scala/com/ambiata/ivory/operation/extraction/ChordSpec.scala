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

  Can extract expected facts     $e1 ${tag("mr")}
"""

  def e1 = prop((facts: ChordFacts) => facts.expected.nonEmpty ==> (TemporaryDirPath.withDirPath { directory =>
    RepositoryBuilder.using { repo =>
      val entities = facts.ces.flatMap(ce => ce.dates.map(ce.entity + "|" + _._1.hyphenated))
      implicit val sc = repo.scoobiConfiguration
      for {
        _                <- RepositoryBuilder.createRepo(repo, facts.dictionary, facts.facts)
        entitiesLocation =  IvoryLocation.fromDirPath(directory </> "entities")
        _                <- IvoryLocation.writeUtf8Lines(entitiesLocation, entities)
        outPath          <- Chord.createChord(repo, entitiesLocation, takeSnapshot = facts.takeSnapshot)
        facts            <- ResultT.safe[IO, List[Fact]](valueFromSequenceFile[Fact](repo.toIvoryLocation(outPath).toHdfs).run.toList)
      } yield facts
    }
  } must beOkLike(_ must containTheSameElementsAs(facts.expected))
  )).set(minTestsOk = 1)
}
