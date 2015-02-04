package com.ambiata.ivory.operation.extraction.output

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.notion.core._

import org.specs2._

class SparseOutputSpec extends Specification with ScalaCheck { def is = s2"""

 A Sequence file containing feature values can be
   extracted as EAV                                      $eav       ${tag("mr")}
   extracted as EAV (escaped)                            $escaped   ${tag("mr")}

 An EAV file matches the dictionary output               $matchDict ${tag("mr")}

"""

  def eav = prop { (facts: FactsWithDictionary) =>
    // Remove duplicates - we're not interested in testing snapshot logic here
    val factsUnique = facts.facts.groupBy(_.entity).values.flatMap(_.find(!_.isTombstone)).toList
    RepositoryBuilder.using(extractSparse(factsUnique, facts.dictionary, TextEscaping.Delimited)).map(_._1) must beOkLike(
      (text: String) => text.split("\n").toList.size ==== factsUnique.length
    )
  }.set(minTestsOk = 3)

  def escaped = prop { (facts: FactsWithDictionary) =>
    RepositoryBuilder.using(extractSparse(facts.facts, facts.dictionary, TextEscaping.Escaped)).map(_._1) must beOkLike(
      (text: String) => seqToResult(text.split("\n").map(TextEscaping.split('|', _).length ==== 4))
    )
  }.set(minTestsOk = 1)

  def matchDict = prop { (facts: FactsWithDictionary) =>
    RepositoryBuilder.using(extractSparse(facts.facts, facts.dictionary, TextEscaping.Delimited)) must beOkLike {
      case (out, dict) =>
        val namespaces = dict.map(_.split("\\|", -1) match { case l => l(1) -> l(2)})
        seqToResult(out.lines.toList.map(_.split("\\|", -1) match {
          case l => (l(1) -> l(2)) must beOneOf(namespaces: _*)
        }))
    }
  }.set(minTestsOk = 1)

  def extractSparse(facts: List[Fact], dictionary: Dictionary, escaping: TextEscaping)(repo: HdfsRepository): RIO[(String, List[String])] = for {
    dir             <- LocalTemporary.random.directory
    conf            <- IvoryConfigurationTemporary.random.conf
    _               <- RepositoryBuilder.createDictionary(repo, dictionary)
    eav             = dir </> DirPath.unsafe("eav")
    squash          <- RepositoryBuilder.createSquash(repo, facts)
    input           <- squash.asHdfsIvoryLocation.map(ShadowOutputDataset.fromIvoryLocation)
    _               <- SparseOutput.extractWithDictionary(repo, input, ShadowOutputDataset(HdfsLocation(eav.path)), dictionary, Delimiter.Psv, "NA", escaping)
    dictLocation    <- IvoryLocation.fromUri((dir </> "eav" </> ".dictionary").path, conf)
    dictionaryLines <- IvoryLocation.readLines(dictLocation)
    loc             <- IvoryLocation.fromUri(eav.path, conf)
    eavLines        <- IvoryLocation.readLines(loc).map(_.sorted)
  } yield (eavLines.mkString("\n").trim, dictionaryLines.toList)

}
