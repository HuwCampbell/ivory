package com.ambiata.ivory.operation.extraction

import org.specs2._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.TestConfigurations
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.legacy._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import com.nicta.scoobi.core.ScoobiConfiguration

class SnapshotSpec extends Specification with SampleFacts { def is = s2"""

  A snapshot of the features can be extracted as a sequence file $e1

"""

  def e1 = {
    implicit val sc: ScoobiConfiguration = TestConfigurations.scoobiConfiguration

    val directory = path(TempFiles.createTempDir("snapshot").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val testDir = "target/"+getClass.getSimpleName+"/"
    Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), false) must beOk
  }
}
