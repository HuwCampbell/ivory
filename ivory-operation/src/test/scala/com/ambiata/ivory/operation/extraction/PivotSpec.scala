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
import scalaz.{Store => _, _}, Scalaz._
import org.specs2._

class PivotSpec extends Specification with SampleFacts with ThrownExpectations { def is = s2"""

 A Sequence file containing feature values can be
   pivoted as a row-oriented file $pivot
   with a new dictionary          $dictionary

"""
  def pivot =
    RepositoryBuilder.using(createPivot) must beOkLike { case (lines, _) =>
      lines.mkString("\n").trim must_==
        """|eid1|abc|NA|NA
           |eid2|NA|11|NA
           |eid3|NA|NA|true
        """.stripMargin.trim
    }


  def dictionary =
    RepositoryBuilder.using(createPivot) must beOkLike { case (_, lines) =>
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
        meta  <- Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), incremental = false)
        input     = repo.toReference(Repository.snapshot(meta.snapshotId))
        _                <- Pivot.createPivot(repo, input, pivot, '|', "NA")
        dictRef          <- Reference.fromUriResultTIO((dir </> "pivot" </> ".dictionary").path, repo.configuration)
        dictionaryLines  <- dictRef.run(_.linesUtf8.read)
        pivotLines       <- readLines(pivot)
      } yield (pivotLines, dictionaryLines)
    }
  }

  def extractPivot(repository: HdfsRepository, outPath: FilePath): ResultTIO[List[String]] =
    for {
      pivot <- Reference.fromUriFilePathResultTIO(outPath, repository.repositoryConfiguration)
      meta  <- Snapshot.takeSnapshot(repository, Date.fromLocalDate(LocalDate.now), incremental = false)
      input =  repository.toReference(Repository.snapshot(meta.snapshotId))
      _     <- Pivot.createPivot(repository, input, pivot, '|', "NA")
      lines <- readLines(pivot)
    } yield lines

  def readLines(ref: ReferenceIO): ResultTIO[List[String]] =
    ref.run(store => path => {
      for {
        paths <- store.filter(path, p => { val sp = (FilePath.root </> p).relativeTo(path); !sp.path.startsWith("_") && !sp.path.startsWith(".") })
        all   <- paths.traverseU(store.linesUtf8.read).map(_.flatten)
      } yield all
    })

  def readDictionary(outPath: FilePath, configuration: RepositoryConfiguration): ResultTIO[List[String]] =
    for {
      dictRef <- Reference.fromUriResultTIO((outPath </> ".dictionary").path, configuration)
      lines   <- dictRef.run(_.linesUtf8.read)
    } yield lines

}
