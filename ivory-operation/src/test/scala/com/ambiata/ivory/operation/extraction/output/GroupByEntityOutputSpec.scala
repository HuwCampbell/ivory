package com.ambiata.ivory.operation.extraction.output

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.IvoryConfigurationTemporary._
import com.ambiata.ivory.operation.ingestion.thrift.{ThriftFactDense, ThriftFactSparse}
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.notion.core._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.thrift._
import org.specs2.matcher.ThrownExpectations
import org.specs2._
import scala.collection.JavaConverters._

class GroupByEntityOutputSpec extends Specification with ThrownExpectations with ScalaCheck { def is = s2"""

 A Sequence file containing feature values can be
   pivoted as a row-oriented text file delimited       $uniqueFacts ${tag("mr")}
   pivoted as a row-oriented text file escaped         $textEscaped ${tag("mr")}
   pivoted as a row-oriented dense thrift file         $thriftList  ${tag("mr")}
   pivoted as a row-oriented sparse thrift file        $thriftMap   ${tag("mr")}

 A dense file must must the dictionary output          $matchDict   ${tag("mr")}

"""

  def matchDict = prop {(facts: FactsWithDictionary) =>
    RepositoryBuilder.using(createDenseText(List(facts.facts), facts.dictionary)) must beOkLike {
      case (out, dict) => seqToResult(out.lines.toList.map(_.split("\\|", -1).size - 1 ==== dict.size))
    }
  }.set(minTestsOk = 5)

  def uniqueFacts = prop { (fwd: FactsWithDictionary) =>
    // We don't care about the snapshot logic here
    val facts = fwd.facts.groupBy(_.entity).values.flatMap(_.find(!_.isTombstone)).toList
    RepositoryBuilder.using(createDense(List(facts), fwd.dictionary, GroupByEntityFormat.DenseText(Delimiter.Psv, "NA", TextEscaping.Delimited)) {
      (_, file) => IvoryLocation.readLines(file).map(_.length)
    }) must beOkValue(facts.length)
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def textEscaped = prop { (facts: FactsWithDictionaryMulti) =>
    RepositoryBuilder.using(createDense(List(facts.facts), facts.dictionary, GroupByEntityFormat.DenseText(Delimiter.Psv, "NA", TextEscaping.Escaped)) {
      (_, file) => IvoryLocation.readLines(file)
    }) must beOkLike {
      lines => seqToResult(lines.map(TextEscaping.split('|', _).length ==== facts.dictionary.size + 1))
    }
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def thriftList = prop { (facts: FactsWithDictionaryMulti) =>
    RepositoryBuilder.using(createDense(List(facts.facts), facts.dictionary, GroupByEntityFormat.DenseThrift) {
      (repo, file) => RIO.io(valueFromSequenceFile[ThriftFactDense](file.show).run(repo.scoobiConfiguration).toList)
    }) must beOkLike {
      denseFacts =>
        // We're not actually checking the contexts of 'value' here
        (denseFacts.map(_.getEntity).sorted, denseFacts.map(_.getValue.size()).max) ====
        (facts.facts.filter(!_.isTombstone).groupBy(_.entity).keySet.toList.sorted -> facts.dictionary.size)
    }
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def thriftMap = prop { (facts: FactsWithDictionaryMulti) =>
    RepositoryBuilder.using(createDense(List(facts.facts), facts.dictionary, GroupByEntityFormat.SparseThrift) {
      (repo, file) => RIO.io(valueFromSequenceFile[ThriftFactSparse](file.show).run(repo.scoobiConfiguration).toList)
    }) must beOkLike {
      denseFacts =>
        // We're not actually checking the contexts of 'value' here
        denseFacts.map(f => f.getEntity -> f.getValue.keySet.asScala.toSet).toMap ====
          facts.facts.groupBy(_.entity).mapValues(_.filter(!_.isTombstone).map(_.featureId.toString).toSet)
    }
  }.set(minTestsOk = 5, maxDiscardRatio = 10)

  def createDense[A](facts: List[List[Fact]], dictionary: Dictionary, format: GroupByEntityFormat)(f: (HdfsRepository, IvoryLocation) => RIO[A])(repo: HdfsRepository): RIO[A] = for {
    // Filter out tombstones to simplify the assertions - we're not interested in the snapshot logic here
    dir    <- LocalTemporary.random.directory
    _      <- RepositoryBuilder.createRepo(repo, dictionary, facts.map(_.filter(!_.isTombstone)))
    dense  <- withConf(conf => IvoryLocation.fromUri((dir </> "dense").path, conf))
    s      <- Snapshots.takeSnapshot(repo, IvoryFlags.default, Date.maxValue)
    input  = repo.toIvoryLocation(Repository.snapshot(s.id))
    inputS = ShadowOutputDataset(HdfsLocation(input.show))
    denseS = ShadowOutputDataset(HdfsLocation(dense.show))
    _      <- GroupByEntityOutput.createWithDictionary(repo, inputS, denseS, dictionary, format)
    out    <- f(repo, dense)
  } yield out

  def createDenseText(facts: List[List[Fact]], dictionary: Dictionary)(repo: HdfsRepository): RIO[(String, List[String])] =
    createDense(facts, dictionary, GroupByEntityFormat.DenseText(Delimiter.Psv, "NA", TextEscaping.Delimited))((_, dense) => for {
        dictionaryLines  <- IvoryLocation.readLines(dense </> ".dictionary")
        denseLines       <- IvoryLocation.readLines(dense)
      } yield (denseLines.mkString("\n").trim, dictionaryLines)
    )(repo)
}
