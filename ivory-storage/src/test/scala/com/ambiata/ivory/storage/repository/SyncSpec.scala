package com.ambiata.ivory.storage.repository

import java.util.UUID

import com.ambiata.ivory.core.{TemporaryReferences => T, _}
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.storage.plan.{Datasets, FactsetDataset}
import com.ambiata.ivory.storage.Arbitraries._
import com.ambiata.mundane.control.{ResultT, ResultTIO}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.specs2.{ScalaCheck, Specification}

import scalaz._, Scalaz._

class SyncSpec extends Specification with ScalaCheck { def is = s2"""

Sync operations from local file system to cluster
=================================================
 syncing single file to ShadowInputDataset        $singleFileToCluster
 syncing relative file to ShadowInputDataset      $relativeFileToCluster
 syncing directory to ShadowInputDataset          $directoryToCluster
 syncing repository to ShadowRepository           $repositoryToCluster


Sync operations from cluster to local file system
=================================================
 syncing repository from ShadowRepository         $repositoryFromCluster
 syncing single file from ShadowRepository        $fileFromCluster
 syncing directory from ShadowRepository          $directoryFromCluster


 checkPaths         $checkPaths

"""

  val conf = IvoryConfiguration(ScoobiConfiguration())

  def singleFileToCluster = {
    T.withCluster(cluster => {
      T.withLocationFile(T.Posix)(location => {
        val dataset = InputDataset(location)
        for {
          _      <- T.createLocationFile(dataset.location)
          shadow <- Sync.syncInputDataSet(dataset, cluster)
          exists <- Hdfs.exists(shadow.toHdfsPath).run(shadow.configuration)
        } yield exists})
    }) must beOkValue(true)
  }

  def relativeFileToCluster = {
    T.withCluster(cluster => {
      T.runWithLocationFile(LocalLocation("../foo"))(location => {
        val dataset = InputDataset(location)
        for {
          _      <- T.createLocationFile(location)
          shadow <- Sync.syncInputDataSet(dataset, cluster)
          exists <- Hdfs.exists(shadow.toHdfsPath).run(shadow.configuration)
        } yield exists})
    }) must beOkValue(true)
  }

  def directoryToCluster = {
    T.withCluster(cluster => {
      T.withLocationDir(T.Posix)(location => {
        val dataset = InputDataset(location)
        for {
          _      <- createFile(location, "foo")
          _      <- createFile(location, "foos/bar")
          shadow <- Sync.syncInputDataSet(dataset, cluster)
          exists <- Hdfs.exists(shadow.toHdfsPath).run(shadow.configuration)
          foo    <- Hdfs.exists((shadow.toFilePath </> "foo").toHdfs).run(shadow.configuration)
          bar    <- Hdfs.exists((shadow.toFilePath </> "foos" </> "bar").toHdfs).run(shadow.configuration)
        } yield exists && foo && bar})
    }) must beOkValue(true)
  }

  def createFile(location: Location, file: String): ResultTIO[Unit] = location match {
    case LocalLocation(s) => T.createLocationFile(LocalLocation(s + "/" + file))
    case S3Location(b, k) => T.createLocationFile(S3Location(b, k + "/" + file))
    case HdfsLocation(s)  => T.createLocationFile(HdfsLocation(s + "/" + file))
  }

  def checkPaths = prop((dataset: FactsetDataset) => {
    val datasets = Datasets(List(Prioritized(Priority.Min, dataset)))
    Sync.getPaths(datasets).length must be_==(dataset.partitions.length)
  })

  def repositoryToCluster = prop((one: FactsetDataset, two: FactsetDataset) => {
    T.withRepository(T.Posix)(repo => {
      T.withCluster(cluster => {
        val datasets = Datasets(List(Prioritized(Priority.Min, one), Prioritized(Priority.Min, two)))
        for {
          _ <- one.partitions.map(repo.factset(one.factset) </> _.path </> "file").traverseU(Files.write(_, ""))
          _ <- two.partitions.map(repo.factset(two.factset) </> _.path </> "file").traverseU(Files.write(_, ""))
          s <- Sync.syncToCluster(datasets, repo, cluster)
          o <- one.partitions.traverseU(p => Hdfs.exists((repo.factset(one.factset) </> p.path </> "file").toHdfs).run(s.configuration))
          t <- two.partitions.traverseU(p => Hdfs.exists((repo.factset(two.factset) </> p.path </> "file").toHdfs).run(s.configuration))
        } yield o ++ t })
    }) must beOkLike(_ must contain(true).forall)
  }).set(minTestsOk = 10)

  def repositoryFromCluster = prop((dataset: FactsetDataset) => {
    T.withRepository(T.Posix)(repo => {
      T.withCluster(cluster => {
        val datasets = Datasets(List(Prioritized(Priority.Min, dataset)))
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        for {
          _ <- dataset.partitions.map(shadowRepository.root </> "factsets" </> dataset.factset.render </> _.path
            </> "file").traverseU(p => Hdfs.writeWith(p.toHdfs, Streams.write(_, "")).run(cluster.hdfsConfiguration))
          s <- Sync.syncToRepository(datasets, cluster, repo)
          o <- dataset.partitions.traverseU(p => Files.exists(repo.factset(dataset.factset) </> p.path </> "file"))
        } yield o })
    }) must beOkLike(_ must contain(true).forall)
  }).set(minTestsOk = 10)

  def fileFromCluster = {
    T.withLocationFile(T.Posix)( location => {
      T.withCluster(cluster => {
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        val relativePath = s"shadowOutputDataset/${UUID.randomUUID()}" </> "file"
        val path = shadowRepository.root </> relativePath
        for {
          _ <- Hdfs.writeWith(path.toHdfs, Streams.write(_, "")).run(cluster.hdfsConfiguration)
          _ <- Sync.syncOutputDataSet(ShadowOutputDataset(path, shadowRepository.configuration), cluster, OutputDataset(location))
          o <- Files.exists(location.path </> relativePath)
        } yield o })
    }) must beOkValue(true)
  }

  def directoryFromCluster = {
    T.withCluster(cluster => {
      T.withLocationDir(T.Posix)(location => {
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        val relativePath = s"shadowOutputDataset/${UUID.randomUUID()}" </> "file"
        val path = shadowRepository.root </> relativePath
        for {
          _   <- Hdfs.writeWith((path </> "foo").toHdfs, Streams.write(_, "")).run(cluster.hdfsConfiguration)
          _   <- Hdfs.writeWith((path </> "foos" </> "bar").toHdfs, Streams.write(_, "")).run(cluster.hdfsConfiguration)
          _   <- Sync.syncOutputDataSet(ShadowOutputDataset(path, shadowRepository.configuration), cluster, OutputDataset(location))
          foo <- Files.exists(location.path </> relativePath </> "foo")
          bar <- Files.exists(location.path </> relativePath </> "foos" </> "bar")
        } yield foo && bar
      })
    }) must beOkValue(true)
  }
}