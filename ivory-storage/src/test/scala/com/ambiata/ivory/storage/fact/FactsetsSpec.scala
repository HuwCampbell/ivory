package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._
import com.ambiata.poacher.hdfs._

import org.specs2._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.mundane.testing.ResultTIOMatcher._

import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import com.ambiata.mundane.io._

import scalaz._, Scalaz._

object FactsetsSpec extends Specification with ScalaCheck { def is = s2"""

  Can get latest factset id                      $latest
  Can allocate a new factset id                  $allocate
                                                 """

  def latest = prop((ids: SmallFactsetIdList) => {
    implicit val sc: ScoobiConfiguration = scoobiConfiguration

    val base = FilePath(TempFiles.createTempDir("Factsets.latest").getPath)
    val repo = Repository.fromHdfsPath(base </> "repo", sc)

    (for {
      _ <- ids.ids.traverseU(id => {
             val path = repo.factsets </> FilePath(id.render) </> FilePath(".allocated")
             Hdfs.writeWith(path.toHdfs, os => Streams.write(os, "")).run(sc.configuration)
           })
      l <- Factsets.latestId(repo)
    } yield l) must beOkLike(_ must_== ids.ids.sorted.lastOption)
  })

  def allocate = prop((factsetId: FactsetId) => {
    implicit val sc: ScoobiConfiguration = scoobiConfiguration

    val base = FilePath(TempFiles.createTempDir("Factsets.allocate").getPath)
    val repo = Repository.fromHdfsPath(base </> "repo", sc)

    def allocatedPath(id: FactsetId): FilePath =
      repo.factsets </> FilePath(id.render) </> FilePath(".allocated")

    val path = allocatedPath(factsetId)
    val expected = factsetId.next.map((true, _))

    val res = for {
      _ <- Hdfs.writeWith(path.toHdfs, os => Streams.write(os, "")).run(sc.configuration)
      n <- Factsets.allocateId(repo)
      e <- Hdfs.exists(allocatedPath(factsetId.next.get).toHdfs).run(sc.configuration)
    } yield (e, n)

    expected.map(e => res must beOkLike(_ must_== e)).getOrElse(res.run.unsafePerformIO.toOption must beNone)
  })
}
