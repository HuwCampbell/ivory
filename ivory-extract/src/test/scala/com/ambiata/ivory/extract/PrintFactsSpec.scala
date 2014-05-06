package com.ambiata.ivory.extract

import com.nicta.scoobi.core.ScoobiConfiguration
import com.nicta.scoobi.testing.{TempFiles, HadoopSpecification}
import com.nicta.scoobi.testing.TestFiles._
import com.ambiata.ivory.storage.repository.Repository
import org.apache.hadoop.fs.Path
import org.joda.time.{LocalDate, DateTimeZone}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.io._
import scalaz.effect.IO

class PrintFactsSpec extends HadoopSpecification with SampleFacts { def is = s2"""

 A sequence file containing facts can be read and printed on the console $a1

"""

  def a1 = { implicit sc: ScoobiConfiguration =>
    val directory = path(TempFiles.createTempDir("snapshot").getPath)
    val repo = Repository.fromHdfsPath(new Path(directory + "/repo"))

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val testDir = "target/"+getClass.getSimpleName+"/"
    val snapshot1 = HdfsSnapshot.takeSnapshot(repo.path, new Path(s"$testDir/out"), new Path(s"testDir/errors"), LocalDate.now, None)
    snapshot1.run(sc) must beOk

    val buffer = new StringBuffer
    val stringBufferLogging = (s: String) => IO { buffer.append(s+"\n"); ()}

    PrintFacts.print(s"$testDir/out/", "**/out*", delimiter = "|", tombstone = "NA").execute(stringBufferLogging).unsafePerformIO

    buffer.toString must_==
      """|ns1|eid1|abc|2012-10-01|0:0:0
         |ns1|eid2|11|2012-11-01|0:0:0
         |ns2|eid3|true|2012-03-20|0:0:0
         |""".stripMargin
  }

  override def isCluster = false
}
