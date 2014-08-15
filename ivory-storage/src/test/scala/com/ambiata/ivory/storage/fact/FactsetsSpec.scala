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
  Can list all factsets                          $factsets
  Can read a single factset                      $factset
                                                 """

  def latest = prop((ids: FactsetIdList) => {
    val repo = setup("latest")

    (for {
      _ <- ids.ids.traverseU(id => allocatePath(repo.factset(id)).run(repo.conf))
      l <- Factsets.latestId(repo)
    } yield l) must beOkLike(_ must_== ids.ids.sorted.lastOption)
  })

  def allocate = prop((factsetId: FactsetId) => {
    val repo = setup("allocate")

    val expected = factsetId.next.map((true, _))

    val res = for {
      _ <- allocatePath(repo.factset(factsetId)).run(repo.conf)
      n <- Factsets.allocateId(repo)
      e <- Hdfs.exists(repo.factset(factsetId.next.get).toHdfs).run(repo.conf)
    } yield (e, n)

    expected.map(e => res must beOkLike(_ must_== e)).getOrElse(res.run.unsafePerformIO.toOption must beNone)
  })

  def factsets = prop((factsets: FactsetList) => {
    val repo = setup("factsets")

    val expected = factsets.factsets.map(fs => fs.copy(partitions = fs.partitions.sorted)).sortBy(_.id)

    (factsets.factsets.traverseU(fs =>
      fs.partitions.partitions.traverseU(p => writeDataFile(repo.factset(fs.id) </> p.path)).run(repo.conf)
    ) must beOk) and
    (Factsets.factsets(repo) must beOkLike(_ must containTheSameElementsAs(expected)))
  })

  def factset = prop((factset: Factset) => {
    val repo = setup("factset")

    val expected = factset.copy(partitions = factset.partitions.sorted)

    (factset.partitions.partitions.traverseU(p => writeDataFile(repo.factset(factset.id) </> p.path)).run(repo.conf) must beOk) and
    (Factsets.factset(repo, factset.id) must beOkValue(expected))
  })

  def setup(name: String): HdfsRepository = {
    implicit val sc: ScoobiConfiguration = scoobiConfiguration

    val base = FilePath(TempFiles.createTempDir(s"Factsets.${name}").getPath)
    Repository.fromHdfsPath(base </> "repo", sc)
  }

  def allocatePath(path: FilePath): Hdfs[Unit] =
    writeEmptyFile(path </> FilePath(".allocated"))

  def writeDataFile(path: FilePath): Hdfs[Unit] =
    writeEmptyFile(path </> FilePath("data"))

  def writeEmptyFile(file: FilePath): Hdfs[Unit] =
    Hdfs.writeWith((file).toHdfs, os => Streams.write(os, ""))
}
