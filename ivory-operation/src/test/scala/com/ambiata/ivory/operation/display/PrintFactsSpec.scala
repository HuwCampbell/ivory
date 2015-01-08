package com.ambiata.ivory.operation.display

import org.specs2._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.ivory.operation.extraction._
import com.ambiata.mundane.control.RIO
import com.ambiata.mundane.testing.RIOMatcher._
import org.joda.time.LocalDate
import org.apache.hadoop.fs.Path
import scalaz.effect.IO

class PrintFactsSpec extends Specification with SampleFacts { def is = s2"""

 A sequence file containing facts can be read and printed on the console $a1

"""

  def a1 =
    RepositoryBuilder.using { repo => for {
      _         <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
      snapshot  <- Snapshots.takeSnapshot(repo, IvoryFlags.default, Date.fromLocalDate(LocalDate.now))
      buffer     = new StringBuffer
      stringBufferLogging = (p: Path, f: Fact) => IO { buffer.append(PrintFacts.renderFact('|', "NA", p, f)+"\n"); ()}
      _ <- Print.printPathsWith(
        List(repo.toIvoryLocation(Repository.snapshot(snapshot.id)).toHdfsPath),
        repo.configuration,
        createMutableFact,
        stringBufferLogging)
    } yield buffer.toString
    } must beOkValue(

      """|eid1|ns1|fid1|abc|2012-10-01|00:00:00
         |eid2|ns1|fid2|11|2012-11-01|00:00:00
         |eid3|ns2|fid3|true|2012-03-20|00:00:00
         |""".stripMargin
    )
}
