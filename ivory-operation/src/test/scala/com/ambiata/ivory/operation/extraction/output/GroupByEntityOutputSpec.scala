package com.ambiata.ivory.operation.extraction.output

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.IvoryConfigurationTemporary._
import com.ambiata.ivory.operation.ingestion.thrift.{ThriftFactDense, ThriftFactSparse}
import com.ambiata.ivory.storage.parse.EavtParsers
import com.ambiata.ivory.storage.repository._
import com.ambiata.notion.core._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.thrift._
import org.specs2.matcher.ThrownExpectations
import org.specs2._
import scala.collection.JavaConverters._
import scalaz.{Value => _, _}, Scalaz._

class GroupByEntityOutputSpec extends Specification with ThrownExpectations with ScalaCheck { def is = s2"""

 A Sequence file containing feature values can be
   pivoted as a row-oriented text file delimited       $textDelimited  ${tag("mr")}
   pivoted as a row-oriented text file escaped         $textEscaped    ${tag("mr")}
   pivoted as a row-oriented text file deprecated      $textDeprecated ${tag("mr")}
   pivoted as a row-oriented dense thrift file         $thriftList     ${tag("mr")}
   pivoted as a row-oriented sparse thrift file        $thriftMap      ${tag("mr")}

 A dense file must must the dictionary output          $matchDict      ${tag("mr")}

"""

  def matchDict = prop {(facts: FactsWithDictionary) =>
    RepositoryBuilder.using(createDenseText(facts.facts, facts.dictionary)) must beOkLike {
      case (out, dict) => seqToResult(out.lines.toList.map(_.split("\\|", -1).size - 1 ==== dict.size))
    }
  }.set(minTestsOk = 5)

  def textDelimited = prop { (fwd: FactsWithDictionaryMulti) =>
    RepositoryBuilder.using(createDense(fwd.facts, fwd.dictionary,
      GroupByEntityFormat.DenseText(Delimiter.Psv, "NA", TextEscaping.Delimited, TextFormat.Json)) {
      (_, file) =>
        IvoryLocation.readLines(file).map(l => parse(fwd.dictionary, "NA", l.map(EavtParsers.splitLine('|', _))))
    }) must beOkValue(expectedFacts(fwd.dictionary, fwd.facts).right)
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def textEscaped = prop { (fwd: FactsWithDictionaryMulti) =>
    RepositoryBuilder.using(createDense(fwd.facts, fwd.dictionary,
      GroupByEntityFormat.DenseText(Delimiter.Psv, "NA", TextEscaping.Escaped, TextFormat.Json)) {
      (_, file) =>
        IvoryLocation.readLines(file).map(l => parse(fwd.dictionary, "NA", l.map(TextEscaping.split('|', _))))
    }) must beOkValue(expectedFacts(fwd.dictionary, fwd.facts).right)
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def textDeprecated = prop { (fwd: FactsWithDictionaryMulti) =>
    RepositoryBuilder.using(createDense(fwd.facts, fwd.dictionary,
      GroupByEntityFormat.DenseText(Delimiter.Psv, "NA", TextEscaping.Escaped, TextFormat.Deprecated)) {
      (_, file) =>
        IvoryLocation.readLines(file).map(_.map(TextEscaping.split('|', _).length))
    }) must beOkValue(expectedFacts(fwd.dictionary, fwd.facts).toList.as(fwd.dictionary.size + 1))
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def thriftList = prop { (facts: FactsWithDictionaryMulti) =>
    RepositoryBuilder.using(createDense(facts.facts, facts.dictionary, GroupByEntityFormat.DenseThrift) {
      (repo, file) => RIO.io(valueFromSequenceFile[ThriftFactDense](file.show).run(repo.scoobiConfiguration).toList)
    }) must beOkLike {
      denseFacts =>
        // We're not actually checking the contexts of 'value' here
        (denseFacts.map(_.getEntity).sorted, denseFacts.map(_.getValue.size()).max) ====
        (facts.facts.filter(!_.isTombstone).groupBy(_.entity).keySet.toList.sorted -> facts.dictionary.size)
    }
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def thriftMap = prop { (facts: FactsWithDictionaryMulti) =>
    RepositoryBuilder.using(createDense(facts.facts, facts.dictionary, GroupByEntityFormat.SparseThrift) {
      (repo, file) => RIO.io(valueFromSequenceFile[ThriftFactSparse](file.show).run(repo.scoobiConfiguration).toList)
    }) must beOkLike {
      denseFacts =>
        // We're not actually checking the contexts of 'value' here
        denseFacts.map(f => f.getEntity -> f.getValue.keySet.asScala.toSet).toMap ====
          facts.facts.groupBy(_.entity).mapValues(_.filter(!_.isTombstone).map(_.featureId.toString).toSet)
    }
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def createDense[A](facts: List[Fact], dictionary: Dictionary, format: GroupByEntityFormat)(f: (HdfsRepository, IvoryLocation) => RIO[A])(repo: HdfsRepository): RIO[A] = for {
    dir     <- LocalTemporary.random.directory
    _       <- RepositoryBuilder.createDictionary(repo, dictionary)
    dense   <- withConf(conf => IvoryLocation.fromUri((dir </> "dense").path, conf))
    s       <- RepositoryBuilder.createSquash(repo, facts)
    inputS  <- s.asHdfsIvoryLocation.map(ShadowOutputDataset.fromIvoryLocation)
    denseS  = ShadowOutputDataset(HdfsLocation(dense.show))
    _       <- GroupByEntityOutput.createWithDictionary(repo, inputS, denseS, dictionary, format)
    out     <- f(repo, dense)
  } yield out

  def createDenseText(facts: List[Fact], dictionary: Dictionary)(repo: HdfsRepository): RIO[(String, List[String])] =
    createDense(facts, dictionary, GroupByEntityFormat.DenseText(Delimiter.Psv, "NA", TextEscaping.Delimited, TextFormat.Json))((_, dense) => for {
        dictionaryLines  <- IvoryLocation.readLines(dense </> ".dictionary")
        denseLines       <- IvoryLocation.readLines(dense)
      } yield (denseLines.mkString("\n").trim, dictionaryLines)
    )(repo)

  def parse(dict: Dictionary, missing: String, lines: List[List[String]]): String \/ Map[String, List[Value]] = {
    val cdict = dict.byConcrete
    lines.traverseU({
      case e :: l =>
        l.zipWithIndex.traverseU({
          case (v, i) =>
            dict.byFeatureIndex(i).fold(
              (_, cd) =>
                if (v == missing) TombstoneValue.right
                else EavtParsers.valueFromString(cd, v).disjunction,
              (_, vd) =>
                // We won't have any virtual features
                TombstoneValue.right
            )
        }).map(e -> _)
      case l =>
        s"Invalid empty line: $l".left
    }).map(_.toMap)
  }

  def expectedFacts(dict: Dictionary, facts: List[Fact]): Map[String, List[Value]] =
    RepositoryBuilder.uniqueFacts(facts).groupBy(_.entity).mapValues(f => {
      dict.sortedByFeatureId.map(_.featureId).map({
        fid => f.find(_.featureId == fid).map(_.value).getOrElse(TombstoneValue)
      })
    }).toList.toMap
}
