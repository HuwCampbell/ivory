package com.ambiata.ivory.operation.extraction

import com.nicta.scoobi.core.ScoobiConfiguration
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.ivory.core._
import org.apache.hadoop.fs.{Path}
import org.joda.time.LocalDate
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.TestConfigurations
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._, Metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import scalaz.{Store => _, _}, Scalaz._
import org.specs2._

class PivotSpec extends Specification with SampleFacts { def is = s2"""

 A Sequence file containing feature values can be pivoted as a row-oriented file with a new dictionary $e1

"""

  // TODO This whole test needs to be redone into something more robust
  def e1 = {
    val conf = RepositoryConfiguration(TestConfigurations.scoobiConfiguration)
    implicit val sc: ScoobiConfiguration = conf.scoobiConfiguration

    val directory = path(TempFiles.createTempDir("pivot").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createDictionary(repo)
    createFacts(repo)

    ((for {
      pivot <- Reference.fromUriResultTIO(directory+"/pivot", conf)
      snap  <- Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), false)
      (_, snapId) = snap
      input = repo.toReference(Repository.snapshot(snapId))
      dict  <- dictionaryFromIvory(repo)
      _     <- Pivot.withDictionary(repo, input, pivot, dict, '|', "NA")
      lines <- readLines(pivot)
    } yield lines) must beOkLike { lines =>
      lines.mkString("\n").trim must_==
      """|eid1|abc|NA|NA
         |eid2|NA|11|NA
         |eid3|NA|NA|true
      """.stripMargin.trim
    }) and
    ((for {
      dictRef <- Reference.fromUriResultTIO(directory+"/pivot/.dictionary", conf)
      lines   <- dictRef.run(store => store.linesUtf8.read)
    } yield lines) must beOkLike { lines =>
      lines must_== List("0|ns1|fid1|string|categorical|desc|NA",
                         "1|ns1|fid2|int|numerical|desc|NA",
                         "2|ns2|fid3|boolean|categorical|desc|NA")
    })
  }

  def readLines(ref: ReferenceIO): ResultTIO[List[String]] =
    ref.run(store => path => {
      for {
        paths <- store.filter(path, p => { val sp = (FilePath.root </> p).relativeTo(path); !sp.path.startsWith("_") && !sp.path.startsWith(".") })
        all   <- paths.traverseU(store.linesUtf8.read).map(_.flatten)
      } yield all
    })
}
