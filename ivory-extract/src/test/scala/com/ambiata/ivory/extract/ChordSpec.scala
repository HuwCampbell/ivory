package com.ambiata.ivory.extract

import org.specs2._
import org.specs2.matcher.{MustThrownMatchers, FileMatchers}
import scalaz.{DList => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.mutable._
import com.nicta.scoobi.testing.SimpleJobs
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import java.io.File
import java.net.URI
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._
import IvoryStorage._

class ChordSpec extends Specification with FileMatchers with SampleFacts { def is = s2"""

ChordSpec
-----------

  Can extract expected facts  $e1

"""
  def e1 = {
    implicit val sc: ScoobiConfiguration = scoobiConfiguration

    val directory = path(TempFiles.createTempDir("chord").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    Hdfs.mkdir(repo.snapshots.toHdfs).run(sc) must beOk

    val outPath = new Path(directory+"/out")
    Chord.onHdfs(repo.root.toHdfs, new Path(directory+"/entities"), outPath, new Path(directory+"/tmp"), true, None).run(sc) must beOk

    valueFromSequenceFile[Fact](outPath.toString).run.toList must containTheSameElementsAs(List(
      StringFact("eid1:2012-09-15", FeatureId("ns1", "fid1"), Date(2012, 9, 1), Time(0), "def"),
      StringFact("eid1:2012-11-01", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
      IntFact("eid2:2012-12-01", FeatureId("ns1", "fid2"), Date(2012, 11, 1), Time(0), 11)))
  }
}
