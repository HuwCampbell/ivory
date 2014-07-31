package com.ambiata.ivory.storage.legacy.fatrepo

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._
import com.ambiata.poacher.hdfs._

import org.specs2._
import org.scalacheck._, Arbitraries._
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.mundane.testing.ResultTIOMatcher._

import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import com.ambiata.mundane.io._

import scalaz._, Scalaz._

object ImportWorkflowSpec extends Specification with ScalaCheck { def is = s2"""

  Can find next factset on hdfs                  $e1
  Next name increments by one                    $e2
  Can list all stores on Hdfs                    $e3
  Can find latest name                           $e4
                                                 """

  def e1 = prop((ids: SmallOldIdentifiers) => {
    implicit val sc: ScoobiConfiguration = scoobiConfiguration

    val base = FilePath(TempFiles.createTempDir("ImportWorkflowSpec.e1").getPath)
    val repo = Repository.fromHdfsPath(base </> "repo", sc)

    seqToResult(ids.ids.map(id => {
      val path = repo.factsets </> FilePath(id) </> FilePath(".allocated")
      Hdfs.writeWith(path.toHdfs, os => Streams.write(os, "")).run(sc.configuration) must beOk
    })) and (ImportWorkflow.nextFactset(repo) must beOkLike(_ must_== FactsetId(ImportWorkflow.nextName(ids.ids))))
  }).set(minTestsOk = 5) // Not too many runs are it will take a long time

  // TODO fix after factset id migrated to Identifier
  def e2 = prop((i: Short) => i >= 0 ==> {
    val names = (0 to i).map(ImportWorkflow.zeroPad).toList
    ImportWorkflow.nextName(names) must_== ImportWorkflow.zeroPad(i.toInt + 1)
  })

  def e3 = prop((ids: SmallOldIdentifiers) => {
    implicit val sc: ScoobiConfiguration = scoobiConfiguration

    val base = FilePath(TempFiles.createTempDir("ImportWorkflowSpec.e2").getPath)
    val repo = Repository.fromHdfsPath(base </> "repo", sc)

    seqToResult(ids.ids.map(id => {
      val path = repo.stores </> FilePath(id)
      Hdfs.writeWith(path.toHdfs, os => Streams.write(os, "")).run(sc.configuration) must beOk
    })) and (ImportWorkflow.listStores(repo) must beOkLike(_ must_== ids.ids))
  }).set(minTestsOk = 5)

  // TODO fix after factset id migrated to Identifier
  def e4 = prop((i: Short) => i >= 0 ==> {
    val names = (0 to i).map(ImportWorkflow.zeroPad).toList
    ImportWorkflow.latestName(names) must_== Some(ImportWorkflow.zeroPad(i))
  }) and
  (ImportWorkflow.latestName(Nil) must_== None)
}
