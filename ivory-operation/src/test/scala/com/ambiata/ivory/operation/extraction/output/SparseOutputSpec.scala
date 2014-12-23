package com.ambiata.ivory.operation.extraction.output

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.notion.core._

import org.specs2.matcher.ThrownExpectations
import org.specs2._

class SparseOutputSpec extends Specification with SampleFacts with ThrownExpectations with ScalaCheck { def is = s2"""

 A Sequence file containing feature values can be
   extracted as EAV                                      $eav       ${tag("mr")}
   extracted as EAV (escaped)                            $escaped   ${tag("mr")}

 An EAV file matches the dictionary output               $matchDict ${tag("mr")}

"""

  def eav =
    RepositoryBuilder.using(extractSparse(sampleFacts, sampleDictionary, TextEscaping.Delimited)) must beOkValue(
      List("eid1|ns1|fid1|abc"
         , "eid2|ns1|fid2|11"
         , "eid3|ns2|fid3|true").sorted.mkString("\n") -> expectedDictionary
    )

  def escaped = prop { (facts: FactsWithDictionary) =>
    RepositoryBuilder.using(extractSparse(List(facts.facts), facts.dictionary, TextEscaping.Escaped)).map(_._1) must beOkLike(
      (text: String) => seqToResult(text.split("\n").map(TextEscaping.split('|', _).length ==== 4))
    )
  }.set(minTestsOk = 1)

  def matchDict = prop { (facts: FactsWithDictionary) =>
    RepositoryBuilder.using(extractSparse(List(facts.facts), facts.dictionary, TextEscaping.Delimited)) must beOkLike {
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

  def extractSparse(facts: List[List[Fact]], dictionary: Dictionary, escaping: TextEscaping)(repo: HdfsRepository): RIO[(String, List[String])] =
    TemporaryDirPath.withDirPath { dir =>
      TemporaryIvoryConfiguration.withConf(conf =>
        for {
          _               <- RepositoryBuilder.createRepo(repo, dictionary, facts)
          eav             = dir </> DirPath.unsafe("eav")
          res             <- Snapshots.takeSnapshot(repo, Date.maxValue)
          meta            = res.meta
          input           = ShadowOutputDataset.fromIvoryLocation(repo.toIvoryLocation(Repository.snapshot(meta.id)))
          _               <- SparseOutput.extractWithDictionary(repo, input, ShadowOutputDataset(HdfsLocation(eav.path)), dictionary, Delimiter.Psv, "NA", escaping)
          dictLocation    <- IvoryLocation.fromUri((dir </> "eav" </> ".dictionary").path, conf)
          dictionaryLines <- IvoryLocation.readLines(dictLocation)
          loc             <- IvoryLocation.fromUri(eav.path, conf)
          eavLines        <- IvoryLocation.readLines(loc).map(_.sorted)
        } yield (eavLines.mkString("\n").trim, dictionaryLines.toList)
      )
    }

}
