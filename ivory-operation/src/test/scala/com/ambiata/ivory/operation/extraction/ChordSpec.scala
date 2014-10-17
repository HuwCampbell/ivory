package com.ambiata.ivory.operation.extraction

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.nicta.scoobi.Scoobi._
import org.specs2._
import scalaz.effect.IO


class ChordSpec extends Specification with SampleFacts { def is = s2"""

ChordSpec
---------

  Can extract expected facts     $e1

"""

  def e1 = TemporaryDirPath.withDirPath { directory =>
    RepositoryBuilder.using { repo =>
      val entities = List("eid1|2012-09-15", "eid2|2012-12-01", "eid1|2012-11-01")
      implicit val sc = repo.scoobiConfiguration
      for {
        _                <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
        entitiesLocation =  IvoryLocation.fromDirPath(directory </> "entities")
        _                <- IvoryLocation.writeUtf8Lines(entitiesLocation, entities)
        outPath          <- Chord.createChord(repo, entitiesLocation, takeSnapshot = true)
        facts            <- ResultT.safe[IO, List[Fact]](valueFromSequenceFile[Fact](repo.toIvoryLocation(outPath).toHdfs).run.toList)
      } yield facts
    }
  } must beOkLike {
    case facts =>
      facts must containTheSameElementsAs(List(
        StringFact("eid1:2012-09-15", FeatureId(Name("ns1"), "fid1"), Date(2012, 9, 1), Time(0), "ghi"),
        StringFact("eid1:2012-11-01", FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
        IntFact("eid2:2012-12-01",    FeatureId(Name("ns1"), "fid2"), Date(2012, 11, 1), Time(0), 11)
      ))
  }
}
