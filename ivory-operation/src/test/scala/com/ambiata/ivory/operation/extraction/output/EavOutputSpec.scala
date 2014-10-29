package com.ambiata.ivory.operation.extraction.output

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.operation.extraction.Snapshot
import com.ambiata.ivory.operation.extraction.squash.SquashConfig
import org.specs2.matcher.ThrownExpectations
import org.specs2._

class EavOutputSpec extends Specification with SampleFacts with ThrownExpectations with ScalaCheck { def is = s2"""

 A Sequence file containing feature values can be
   extracted as EAV                                      $eav       ${tag("mr")}

 An EAV file matches the dictionary output               $matchDict ${tag("mr")}

"""

  def eav =
    RepositoryBuilder.using(extractEav(sampleFacts, sampleDictionary)) must beOkValue(
      List("eid1|ns1|fid1|abc"
         , "eid2|ns1|fid2|11"
         , "eid3|ns2|fid3|true").sorted.mkString("\n") -> expectedDictionary
    )

  def matchDict = prop { (facts: FactsWithDictionary) =>
    RepositoryBuilder.using(extractEav(List(facts.facts), facts.dictionary)) must beOkLike {
      case (out, dict) =>
        val namespaces = dict.map(_.split("\\|", -1) match { case l => l(1) -> l(2)})
        seqToResult(out.lines.toList.map(_.split("\\|", -1) match {
          case l => (l(1) -> l(2)) must beOneOf(namespaces: _*)
        }))
    }
  }.set(minTestsOk = 1)

  def expectedDictionary = List(
    "0|ns1|fid1|string|categorical|desc|NA",
    "1|ns1|fid2|int|numerical|desc|NA",
    "2|ns2|fid3|boolean|categorical|desc|NA"
  )

  def extractEav(facts: List[List[Fact]], dictionary: Dictionary)(repo: HdfsRepository): ResultTIO[(String, List[String])] =
    TemporaryDirPath.withDirPath { dir =>
      for {
        _               <- RepositoryBuilder.createRepo(repo, dictionary, facts)
        eav             <- IvoryLocation.fromUri((dir </> "eav").path, IvoryConfiguration.Empty)
        res             <- Snapshot.takeSnapshot(repo, Date.maxValue, incremental = false)
        meta            = res.meta
        input           = repo.toIvoryLocation(Repository.snapshot(meta.snapshotId))
        _               <- EavOutput.extractFromSnapshot(repo, eav, '|', "NA", meta, SquashConfig.testing)
        dictLocation    <- IvoryLocation.fromUri((dir </> "eav" </> ".dictionary").path, IvoryConfiguration.Empty)
        dictionaryLines <- IvoryLocation.readLines(dictLocation)
        eavLines        <- IvoryLocation.readLines(eav).map(_.sorted)
      } yield (eavLines.mkString("\n").trim, dictionaryLines)
    }
}
