package com.ambiata.ivory.operation.extraction

import com.nicta.scoobi.core.ScoobiConfiguration
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

 A Sequence file containing feature values can be
   pivoted as a row-oriented file $pivot
   with a new dictionary          $dictionary

"""
  def pivot =
    RepositoryBuilder.using(createPivot) must beOkLike { lines: List[String] =>
      lines.mkString("\n").trim must_==
        """|eid1|abc|NA|NA
          |eid2|NA|11|NA
          |eid3|NA|NA|true
        """.stripMargin.trim
    }


  def dictionary =
    RepositoryBuilder.using { repo =>
      createPivot(repo) >>
      Temporary.using { dir =>
        (for {
          dictRef <- Reference.fromUriResultTIO((dir </> "pivot" </> ".dictionary").path, repo.configuration)
          lines   <- dictRef.run(_.linesUtf8.read)
        } yield lines)
      }
    } must beOkLike { lines =>
      lines must_== List("0|ns1|fid1|string|categorical|desc|NA",
                         "1|ns1|fid2|int|numerical|desc|NA",
                         "2|ns2|fid3|boolean|categorical|desc|NA")
    }

  def createPivot(repo: HdfsRepository) = {
    implicit val sc: ScoobiConfiguration = repo.scoobiConfiguration
    Temporary.using { dir =>
      for {
        _     <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
        pivot <- Reference.fromUriResultTIO((dir </> "pivot").path, repo.configuration)
        snap  <- Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), false)
        (_, snapId) = snap
        input = repo.toReference(Repository.snapshot(snapId))
        dict  <- dictionaryFromIvory(repo)
        _     <- Pivot.withDictionary(repo, input, pivot, dict, '|', "NA")
        lines <- readLines(pivot)
      } yield lines
    }
  }

  def readLines(ref: ReferenceIO): ResultTIO[List[String]] =
    ref.run(store => path => {
      for {
        paths <- store.filter(path, p => { val sp = (FilePath.root </> p).relativeTo(path); !sp.path.startsWith("_") && !sp.path.startsWith(".") })
        all   <- paths.traverseU(store.linesUtf8.read).map(_.flatten)
      } yield all
    })
}
