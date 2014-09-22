package com.ambiata.ivory.storage.sync

import java.util.UUID

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core.{TemporaryReferences => T, _}
import com.ambiata.ivory.storage.Arbitraries._
import com.ambiata.ivory.storage.plan.{Datasets, FactsetDataset}
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.KeyName
import com.ambiata.mundane.testing.ResultTIOMatcher._
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


Helper functions
================
 checkPaths                                       $checkPaths

"""

  val conf = IvoryConfiguration.fromScoobiConfiguration(ScoobiConfiguration())

  def singleFileToCluster = {
    T.withCluster(cluster => {
      T.withLocationFile(T.Posix)(location => {
        val dataset = InputDataset(location)
        for {
          _      <- T.createLocationDir(dataset.location)
          shadow <- SyncIngest.inputDataSet(dataset, cluster)
          exists <- Hdfs.exists(shadow.toHdfsPath).run(shadow.configuration)
        } yield exists})
    }) must beOkValue(true)
  }

  def relativeFileToCluster = {
    T.withCluster(cluster => {
      T.runWithLocationFile(LocalLocation(cluster.root </> "foo"))(location => {
        val dataset = InputDataset(location)
        for {
          _      <- T.createLocationDir(location)
          shadow <- SyncIngest.inputDataSet(dataset, cluster)
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
          shadow <- SyncIngest.inputDataSet(dataset, cluster)
          exists <- Hdfs.exists(shadow.toHdfsPath).run(shadow.configuration)
          foo    <- Hdfs.exists((shadow.path </> "foo").toHdfs).run(shadow.configuration)
          bar    <- Hdfs.exists((shadow.path </> "foos" </> "bar").toHdfs).run(shadow.configuration)
        } yield exists && foo && bar})
    }) must beOkValue(true)
  }

  def createFile(location: Location, file: String): ResultTIO[Unit] = location match {
    case LocalLocation(s) => T.createLocationFile(LocalLocation(s </> DirPath.unsafe(file)))
    case S3Location(s)    => T.createLocationFile(S3Location   (s </> DirPath.unsafe(file)))
    case HdfsLocation(s)  => T.createLocationFile(HdfsLocation (s </> DirPath.unsafe(file)))
  }

  def repositoryToCluster = prop((one: FactsetDataset, two: FactsetDataset) => {
    T.withRepository(T.Posix)(repo => {
      T.withCluster(cluster => {
        val datasets = Datasets(List(Prioritized(Priority.Min, one), Prioritized(Priority.Min, two)))
        for {
          _ <- one.partitions.map(Repository.factset(one.factset) / _.key / "file").traverseU(key => Files.write(repo.toFilePath(key), ""))
          _ <- two.partitions.map(Repository.factset(two.factset) / _.key / "file").traverseU(key => Files.write(repo.toFilePath(key), ""))
          s <- SyncIngest.toCluster(datasets, repo, cluster)
          o <- one.partitions.traverseU(p => Hdfs.exists(repo.toFilePath(Repository.factset(one.factset) / p.key / "file").toHdfs).run(s.configuration))
          t <- two.partitions.traverseU(p => Hdfs.exists(repo.toFilePath(Repository.factset(two.factset) / p.key / "file").toHdfs).run(s.configuration))
        } yield o ++ t })
    }) must beOkLike(_ must contain(true).forall)
  }).set(minTestsOk = 10)

  def repositoryFromCluster = prop((dataset: FactsetDataset) => {
    T.withRepository(T.Posix)(repo => {
      T.withCluster(cluster => {
        val datasets = Datasets(List(Prioritized(Priority.Min, dataset)))
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        for {
          _ <- dataset.partitions.map(Repository.factset(dataset.factset) / _.key / "file").traverseU(p => Hdfs.writeWith(shadowRepository.toFilePath(p).toHdfs, Streams.write(_, "")).run(cluster.hdfsConfiguration))
          s <- SyncExtract.toRepository(datasets, cluster, repo)
          o <- dataset.partitions.traverseU(p => Files.exists(repo.toFilePath(Repository.factset(dataset.factset) / p.key / "file")))
        } yield o })
    }) must beOkLike(_ must contain(true).forall)
  }).set(minTestsOk = 10)

  def fileFromCluster = {
    T.withLocationFile(T.Posix)( location => {
      T.withCluster(cluster => {
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        val relativePath: FilePath = DirPath("shadowOutputDataset") </> DirPath(UUID.randomUUID) <|> "file"
        val path: FilePath = shadowRepository.root </> relativePath
        for {
          _ <- Hdfs.writeWith(path.toHdfs, Streams.write(_, "")).run(cluster.hdfsConfiguration)
          _ <- SyncExtract.outputDataSet(ShadowOutputDataset(shadowRepository.root, shadowRepository.configuration), cluster, OutputDataset(location))
          o <- Files.exists(location.path </> relativePath)
        } yield o })
    }) must beOkValue(true)
  }

  def directoryFromCluster = {
    T.withCluster(cluster => {
      T.withLocationDir(T.Posix)(location => {
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        val relativePath: DirPath = DirPath("shadowOutputDataset") </> DirPath(UUID.randomUUID)
        val path = shadowRepository.root </> relativePath
        for {
          _   <- Hdfs.writeWith((path <|> "foo").toHdfs, Streams.write(_, "")).run(cluster.hdfsConfiguration)
          _   <- Hdfs.writeWith((path </> "foos" <|> "bar").toHdfs, Streams.write(_, "")).run(cluster.hdfsConfiguration)
          _   <- SyncExtract.outputDataSet(ShadowOutputDataset(shadowRepository.root, shadowRepository.configuration), cluster, OutputDataset(location))
          foo <- Files.exists(location.path </> relativePath <|> "foo")
          bar <- Files.exists(location.path </> relativePath </> "foos" <|> "bar")
        } yield foo -> bar
      })
    }) must beOkValue(true -> true)
  }

  def checkPaths = prop((dataset: FactsetDataset) => {
    val datasets = Datasets(List(Prioritized(Priority.Min, dataset)))
    Sync.getKeys(datasets).length must be_==(dataset.partitions.length)
  })

}