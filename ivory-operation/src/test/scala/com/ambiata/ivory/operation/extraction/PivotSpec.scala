package com.ambiata.ivory.operation.extraction

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import org.joda.time.LocalDate
import org.specs2._
import scalaz.{Store => _, _}, Scalaz._

class PivotSpec extends Specification with SampleFacts { def is = s2"""

 A Sequence file containing feature values can be pivoted as a row-oriented file with a new dictionary $e1

"""

  // TODO This whole test needs to be redone into something more robust
  def e1 = Temporary.using { directory =>
    RepositoryBuilder.using { repo =>
      val conf = repo.scoobiConfiguration
      for {
        _     <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
        pivot <- Reference.fromUriResultTIO((directory </> "pivot").path, conf)
        snap  <- Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), false)
        (_, snapId) = snap
        input = repo.toReference(Repository.snapshot(snapId))

        _     <- Pivot.withDictionary(repo, input, pivot, sampleDictionary, '|', "NA")

        lines <- readLines(pivot)
        dictRef   <- Reference.fromUriResultTIO((directory </> "pivot" </> ".dictionary").path, conf)
        dictLines <- dictRef.run(store => store.linesUtf8.read)
      } yield (lines, dictLines)
    }
  } must beOkValue((List(
    "eid1|abc|NA|NA",
    "eid2|NA|11|NA",
    "eid3|NA|NA|true"
  ), List(
    "0|ns1|fid1|string|categorical|desc|NA",
    "1|ns1|fid2|int|numerical|desc|NA",
    "2|ns2|fid3|boolean|categorical|desc|NA"
  )))

  def readLines(ref: ReferenceIO): ResultTIO[List[String]] =
    ref.run(store => path => {
      for {
        paths <- store.filter(path, p => { val sp = (FilePath.root </> p).relativeTo(path); !sp.path.startsWith("_") && !sp.path.startsWith(".") })
        all   <- paths.traverseU(store.linesUtf8.read).map(_.flatten)
      } yield all
    })
}
