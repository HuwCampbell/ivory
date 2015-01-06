package com.ambiata.ivory.operation.extraction.output

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.operation.ingestion.thrift.{ThriftFactDense, ThriftFactSparse}
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.notion.core._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.thrift._
import org.specs2.matcher.ThrownExpectations
import org.specs2._
import scala.collection.JavaConverters._

class GroupByEntityOutputSpec extends Specification with SampleFacts with ThrownExpectations with ScalaCheck { def is = s2"""

 A Sequence file containing feature values can be
   pivoted as a row-oriented file, example 1           $dense       ${tag("mr")}
   pivoted as a row-oriented file, example 2           $dense2      ${tag("mr")}
   pivoted as a row-oriented text file escaped         $textEscaped ${tag("mr")}
   pivoted as a row-oriented dense thrift file         $thriftList  ${tag("mr")}
   pivoted as a row-oriented sparse thrift file        $thriftMap   ${tag("mr")}

 A dense file must must the dictionary output          $matchDict   ${tag("mr")}

"""
  def dense =
    RepositoryBuilder.using(createDenseText(sampleFacts, sampleDictionary)) must beOkValue(
      """|eid1|abc|NA|NA
         |eid2|NA|11|NA
         |eid3|NA|NA|true
      """.stripMargin.trim -> expectedDictionary
    )

  def dense2 = {
    val facts = List(
      IntFact(      "eid1", FeatureId(Namespace("ns1"), "fid2"), Date(2012, 10,  1), Time(0), 10)
      , IntFact(    "eid3", FeatureId(Namespace("ns1"), "fid2"), Date(2012, 10,  1), Time(0), 10)
      , StringFact( "eid1", FeatureId(Namespace("ns1"), "fid1"), Date(2012,  9,  1), Time(0), "abc")
      , StringFact( "eid1", FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10,  1), Time(0), "ghi")
      , StringFact( "eid1", FeatureId(Namespace("ns1"), "fid1"), Date(2012,  7,  2), Time(0), "def")
      , IntFact(    "eid2", FeatureId(Namespace("ns1"), "fid2"), Date(2012, 10,  1), Time(0), 10)
      , IntFact(    "eid2", FeatureId(Namespace("ns1"), "fid2"), Date(2012, 11,  1), Time(0), 11)
      , BooleanFact("eid3", FeatureId(Namespace("ns2"), "fid3"), Date(2012,  3, 20), Time(0), true)
    )
    RepositoryBuilder.using(createDenseText(List(facts), sampleDictionary)) must beOkValue(
      """|eid1|ghi|10|NA
         |eid2|NA|11|NA
         |eid3|NA|10|true
      """.stripMargin.trim -> expectedDictionary
    )
  }

  def matchDict = prop {(facts: FactsWithDictionary) =>
    RepositoryBuilder.using(createDenseText(List(facts.facts), facts.dictionary)) must beOkLike {
      case (out, dict) => seqToResult(out.lines.toList.map(_.split("\\|", -1).size - 1 ==== dict.size))
    }
  }.set(minTestsOk = 5)

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

  def expectedDictionary = List(
    "0|ns1|fid1|string|categorical|desc|NA",
    "1|ns1|fid2|int|numerical|desc|NA",
    "2|ns2|fid3|boolean|categorical|desc|NA"
  )

  def createDense[A](facts: List[List[Fact]], dictionary: Dictionary, format: GroupByEntityFormat)(f: (HdfsRepository, IvoryLocation) => RIO[A])(repo: HdfsRepository): RIO[A] =
    TemporaryDirPath.withDirPath { dir =>
      for {
        // Filter out tombstones to simplify the assertions - we're not interested in the snapshot logic here
        _      <- RepositoryBuilder.createRepo(repo, dictionary, facts.map(_.filter(!_.isTombstone)))
        dense  <- TemporaryIvoryConfiguration.withConf(conf => IvoryLocation.fromUri((dir </> "dense").path, conf))
        res    <- Snapshots.takeSnapshot(repo, Date.maxValue)
        input  = repo.toIvoryLocation(Repository.snapshot(res.snapshot.id))
        inputS = ShadowOutputDataset(HdfsLocation(input.show))
        denseS = ShadowOutputDataset(HdfsLocation(dense.show))
        _      <- GroupByEntityOutput.createWithDictionary(repo, inputS, denseS, dictionary, format)
        out    <- f(repo, dense)
      } yield out
    }

  def createDenseText(facts: List[List[Fact]], dictionary: Dictionary)(repo: HdfsRepository): RIO[(String, List[String])] =
    createDense(facts, dictionary, GroupByEntityFormat.DenseText(Delimiter.Psv, "NA", TextEscaping.Delimited))((_, dense) => for {
        dictionaryLines  <- IvoryLocation.readLines(dense </> ".dictionary")
        denseLines       <- IvoryLocation.readLines(dense)
      } yield (denseLines.mkString("\n").trim, dictionaryLines)
    )(repo)
}
