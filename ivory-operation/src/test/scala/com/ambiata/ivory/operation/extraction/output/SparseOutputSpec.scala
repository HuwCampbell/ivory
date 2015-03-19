package com.ambiata.ivory.operation.extraction.output

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.parse.EavtParsers
import com.ambiata.ivory.storage.repository._
import com.ambiata.notion.core._

import org.specs2._
import scalaz.{Value => _, _}, Scalaz._

class SparseOutputSpec extends Specification with ScalaCheck { def is = s2"""

 A Sequence file containing feature values can be
   extracted as EAV                                      $eav       ${tag("mr")}
   extracted as EAV (escaped)                            $escaped   ${tag("mr")}
   extracted as EAV (deprecated)                         $deprecated ${tag("mr")}

 An EAV file matches the dictionary output               $matchDict ${tag("mr")}

"""

  def eav = prop { (facts: FactsWithDictionary) =>
    RepositoryBuilder.using(extractSparse(facts.facts, facts.dictionary, TextEscaping.Delimited, TextFormat.Json))
      .map(_._1).map(l => parse(facts.dictionary, l.map(EavtParsers.splitLine('|', _)))) must beOkValue(expectedFacts(facts.facts).right)
  }.set(minTestsOk = 3)

  def escaped = prop { (facts: FactsWithDictionary) =>
    RepositoryBuilder.using(extractSparse(facts.facts, facts.dictionary, TextEscaping.Escaped, TextFormat.Json))
      .map(_._1).map(l => parse(facts.dictionary, l.map(TextEscaping.split('|', _)))) must beOkValue(expectedFacts(facts.facts).right)
  }.set(minTestsOk = 3)

  def deprecated = prop { (facts: FactsWithDictionary, escaping: TextEscaping) =>
    RepositoryBuilder.using(extractSparse(facts.facts, facts.dictionary, escaping, TextFormat.Deprecated)).map(_._1) must beOkLike(
      // Things are so much easier when they're symmetrical :(
      lines => seqToResult(lines.map(TextEscaping.split('|', _).length ==== 4))
    )
  }.set(minTestsOk = 3)

  def matchDict = prop { (facts: FactsWithDictionary) =>
    RepositoryBuilder.using(extractSparse(facts.facts, facts.dictionary, TextEscaping.Delimited, TextFormat.Json)) must beOkLike {
      case (out, dict) =>
        val namespaces = dict.map(_.split("\\|", -1) match { case l => l(1) -> l(2)})
        seqToResult(out.map(_.split("\\|", -1) match {
          case l => (l(1) -> l(2)) must beOneOf(namespaces: _*)
        }))
    }
  }.set(minTestsOk = 1)

  def extractSparse(facts: List[Fact], dictionary: Dictionary, escaping: TextEscaping, format: TextFormat)(repo: HdfsRepository): RIO[(List[String], List[String])] = for {
    dir             <- LocalTemporary.random.directory
    conf            <- IvoryConfigurationTemporary.random.conf
    _               <- RepositoryBuilder.createDictionary(repo, dictionary)
    eav             = dir </> DirPath.unsafe("eav")
    squash          <- RepositoryBuilder.createSquash(repo, facts)
    input           <- squash.asHdfsIvoryLocation.map(ShadowOutputDataset.fromIvoryLocation)
    _               <- SparseOutput.extractWithDictionary(repo, input, ShadowOutputDataset(HdfsLocation(eav.path)),
      dictionary, Delimiter.Psv, "NA", escaping, format)
    dictLocation    <- IvoryLocation.fromUri((dir </> "eav" </> ".dictionary").path, conf)
    dictionaryLines <- IvoryLocation.readLines(dictLocation)
    loc             <- IvoryLocation.fromUri(eav.path, conf)
    eavLines        <- IvoryLocation.readLines(loc).map(_.sorted)
  } yield (eavLines, dictionaryLines.toList)

  def parse(dict: Dictionary, lines: List[List[String]]): String \/ Map[String, Value] = {
    val cdict = dict.byConcrete.sources
    lines.traverseU({
      case List(e, n, a, v) =>
        cdict.get(FeatureId(Namespace.unsafe(n), a))
          .toRightDisjunction(s"Missing feature '$n:$a'")
          .flatMap(cd => EavtParsers.valueFromString(cd.definition, v).disjunction).map(e -> _)
      case l =>
        s"Invalid line: $l".left
    }).map(_.toMap)
  }

  def expectedFacts(facts: List[Fact]): Map[String, Value] =
    RepositoryBuilder.uniqueFacts(facts).map(f => f.entity -> f.value).toMap
}
