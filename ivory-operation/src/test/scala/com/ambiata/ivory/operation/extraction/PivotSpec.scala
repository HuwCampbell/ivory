package com.ambiata.ivory.operation.extraction

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.nicta.scoobi.core.ScoobiConfiguration
import org.joda.time.LocalDate
import org.specs2.matcher.ThrownExpectations
import org.specs2._

class PivotSpec extends Specification with SampleFacts with ThrownExpectations { def is = s2"""

 A Sequence file containing feature values can be
   pivoted as a row-oriented file                        $pivot       ${tag("mr")}
   pivoted as a row-oriented file (another example)      $pivot2      ${tag("mr")}

"""
  def pivot =
    RepositoryBuilder.using(createPivot(sampleFacts)) must beOkValue(
      """|eid1|abc|NA|NA
         |eid2|NA|11|NA
         |eid3|NA|NA|true
      """.stripMargin.trim -> expectedDictionary
    )

  def pivot2 = {
    val facts = List(
        IntFact(    "eid1", FeatureId(Name("ns1"), "fid2"), Date(2012, 10,  1), Time(0), 10)
      , IntFact(    "eid3", FeatureId(Name("ns1"), "fid2"), Date(2012, 10,  1), Time(0), 10)
      , StringFact( "eid1", FeatureId(Name("ns1"), "fid1"), Date(2012,  9,  1), Time(0), "abc")
      , StringFact( "eid1", FeatureId(Name("ns1"), "fid1"), Date(2012, 10,  1), Time(0), "ghi")
      , StringFact( "eid1", FeatureId(Name("ns1"), "fid1"), Date(2012,  7,  2), Time(0), "def")
      , IntFact(    "eid2", FeatureId(Name("ns1"), "fid2"), Date(2012, 10,  1), Time(0), 10)
      , IntFact(    "eid2", FeatureId(Name("ns1"), "fid2"), Date(2012, 11,  1), Time(0), 11)
      , BooleanFact("eid3", FeatureId(Name("ns2"), "fid3"), Date(2012,  3, 20), Time(0), true)
    )
    RepositoryBuilder.using(createPivot(List(facts))) must beOkValue(
      """|eid1|ghi|10|NA
         |eid2|NA|11|NA
         |eid3|NA|10|true
      """.stripMargin.trim -> expectedDictionary
    )
  }

  def expectedDictionary = List(
    "0|ns1|fid1|string|categorical|desc|NA",
    "1|ns1|fid2|int|numerical|desc|NA",
    "2|ns2|fid3|boolean|categorical|desc|NA"
  )

  def createPivot(facts: List[List[Fact]])(repo: HdfsRepository): ResultTIO[(String, List[String])] = {
    implicit val sc: ScoobiConfiguration = repo.scoobiConfiguration
    Temporary.using { dir =>
      for {
        _     <- RepositoryBuilder.createRepo(repo, sampleDictionary, facts)
        pivot <- Reference.fromUriResultTIO((dir </> "pivot").path, repo.configuration)
        meta  <- Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), incremental = false)
        input     = repo.toReference(Repository.snapshot(meta.snapshotId))
        _                <- Pivot.createPivot(repo, input, pivot, '|', "NA")
        dictRef          <- Reference.fromUriResultTIO((dir </> "pivot" </> ".dictionary").path, repo.configuration)
        dictionaryLines  <- dictRef.run(_.linesUtf8.read)
        pivotLines       <- pivot.readLines
      } yield (pivotLines.mkString("\n").trim, dictionaryLines)
    }
  }
}
