package com.ambiata.ivory.tools

import com.ambiata.ivory.storage.fact.{FactsetVersionOne, FactsetVersion}
import org.specs2._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi.TestConfigurations
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.extract._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.testing.ResultMatcher.{beOk => beOkResult}
import com.ambiata.mundane.io._
import scalaz.effect.IO

class PrintFactsSpec extends Specification with SampleFacts { def is = s2"""

 A sequence file containing facts can be read and printed on the console $a1

"""

  def a1 = {
    implicit val sc: ScoobiConfiguration = TestConfigurations.scoobiConfiguration

    val directory = path(TempFiles.createTempDir("print-facts").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val testDir = "target/"+getClass.getSimpleName+"/"
    val snapshot1 = Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), false, None)
    snapshot1 must beOk

    val buffer = new StringBuffer
    val stringBufferLogging = (s: String) => IO { buffer.append(s+"\n"); ()}

    PrintFacts.print(
      List(new Path(repo.snapshots.toHdfs.toString + "/00000000")),
      sc.configuration,
      delim = "|",
      tombstone = "NA",
      version = FactsetVersion.latest).execute(stringBufferLogging).unsafePerformIO must beOkResult

    buffer.toString must_==
      """|eid1|ns1|fid1|abc|2012-10-01|00:00:00
         |eid2|ns1|fid2|11|2012-11-01|00:00:00
         |eid3|ns2|fid3|true|2012-03-20|00:00:00
         |""".stripMargin
  }
}
