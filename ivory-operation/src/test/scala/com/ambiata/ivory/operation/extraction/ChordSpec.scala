package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.control.ResultTIO
import org.specs2._
import org.specs2.matcher._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._, Metadata._
import com.ambiata.ivory.storage.store._
import com.nicta.scoobi.Scoobi._
import org.specs2._
import org.specs2.matcher.ThrownExpectations
import scalaz.effect.IO

class ChordSpec extends Specification with SampleFacts with ThrownExpectations { def is = s2"""

ChordSpec
---------

  Can extract expected facts     $e1

"""

  def e1 = Temporary.using { directory =>
    RepositoryBuilder.using { repo =>
      val outPath = directory </> "out"
      val entities = List("eid1|2012-09-15", "eid2|2012-12-01", "eid1|2012-11-01")
      implicit val sc = repo.scoobiConfiguration
      for {
        _           <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
        outRef      <- Reference.fromUriResultTIO(outPath.path, sc)
        entitiesRef <- Reference.fromUriResultTIO((directory </> "entities").path, sc)
        _           <- entitiesRef.run(s => s.linesUtf8.write(_, entities))
        tmpRef      <- Reference.fromUriResultTIO((directory </> "tmp").path, sc)

        _           <- Chord.onStore(repo, entitiesRef, outRef, tmpRef, true)

        dictRef     <- Reference.fromUriResultTIO((outPath </> ".dictionary").path, sc)
        dict        <- DictionaryTextStorageV2.fromStore(dictRef)
        repoDict    <- dictionaryFromIvory(repo)
        facts       <- ResultT.safe[IO, List[Fact]](valueFromSequenceFile[Fact](outPath.path).run.toList)
      } yield (dict, repoDict, facts)
    }
  } must beOkLike {
    case (dict, repoDict, facts) =>
      dict ==== repoDict
      facts must containTheSameElementsAs(List(
      StringFact("eid1:2012-09-15", FeatureId(Name("ns1"), "fid1"), Date(2012, 9, 1), Time(0), "ghi"),
      StringFact("eid1:2012-11-01", FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
      IntFact("eid2:2012-12-01",    FeatureId(Name("ns1"), "fid2"), Date(2012, 11, 1), Time(0), 11)
    ))
  }

  def createChord(repository: HdfsRepository, directory: FilePath, output: FilePath, configuration: RepositoryConfiguration): ResultTIO[Unit] =
    for {
      outRef      <- Reference.fromUriFilePathResultTIO(output, configuration)
      entitiesRef <- Reference.fromUriFilePathResultTIO(directory </> "entities", configuration)
      tmpRef      <- Reference.fromUriFilePathResultTIO(directory </> "tmp", configuration)
      _           <- Chord.onStore(repository, entitiesRef, outRef, tmpRef, takeSnapshot = true)
    } yield ()

  def readDictionary(outPath: FilePath, configuration: RepositoryConfiguration): ResultTIO[Dictionary] =
    for {
      dictRef <- Reference.fromUriFilePathResultTIO(outPath </> ".dictionary", configuration)
      dict    <- DictionaryTextStorageV2.fromStore(dictRef)
    } yield dict
}
