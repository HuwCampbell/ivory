package com.ambiata.ivory.operation.display

import com.ambiata.ivory.storage.fact._
import org.specs2._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.ivory.operation.extraction._
import com.ambiata.mundane.control.ResultT
import org.joda.time.LocalDate
import com.ambiata.mundane.testing.ResultTIOMatcher._
import scalaz.effect.IO

class PrintFactsSpec extends Specification with SampleFacts { def is = s2"""

 A sequence file containing facts can be read and printed on the console $a1

"""

  def a1 =
    RepositoryBuilder.using { repo => for {
      _         <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
      snapshot1 <- Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), false)
      buffer     = new StringBuffer
      stringBufferLogging = (s: String) => IO { buffer.append(s+"\n"); ()}
      _         <- ResultT.fromIO(PrintFacts.print(
        List(repo.toFilePath(Repository.snapshot(snapshot1.snapshotId)).toHdfs),
        repo.configuration,
        delim = "|",
        tombstone = "NA",
        version = FactsetVersion.latest).execute(stringBufferLogging)
      )
    } yield buffer.toString
    } must beOkValue(

      """|eid1|ns1|fid1|abc|2012-10-01|00:00:00
         |eid2|ns1|fid2|11|2012-11-01|00:00:00
         |eid3|ns2|fid3|true|2012-03-20|00:00:00
         |""".stripMargin
    )
}
