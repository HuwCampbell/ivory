package com.ambiata.ivory.storage.sync

import java.util.UUID
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.TemporaryLocations._
import com.ambiata.ivory.core.TemporaryRepositories._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import com.ambiata.mundane.testing.ResultTIOMatcher._
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
    withCluster(cluster => {
      withIvoryLocationFile(Posix)(location => {
        val dataset = InputDataset(location)
        for {
          _      <- createLocationDir(dataset.location)
          shadow <- SyncIngest.inputDataSet(dataset, cluster)
          exists <- IvoryLocation.exists(shadow.location)
        } yield exists})
    }) must beOkValue(true)
  }

  def relativeFileToCluster = {
    withCluster(cluster => {
      runWithIvoryLocationFile(cluster.root </> "foo")(location => {
        val dataset = InputDataset(location)
        for {
          _      <- createLocationDir(location)
          shadow <- SyncIngest.inputDataSet(dataset, cluster)
          exists <- IvoryLocation.exists(shadow.location)
        } yield exists})
    }) must beOkValue(true)
  }

  def directoryToCluster = {
    withCluster(cluster => {
      withIvoryLocationDir(Posix)(location => {
        val dataset = InputDataset(location)
        for {
          _      <- createLocationFile(location </> "foo")
          _      <- createLocationFile(location </> "foos" </> "bar")
          shadow <- SyncIngest.inputDataSet(dataset, cluster)
          exists <- IvoryLocation.exists(shadow.location)
          foo    <- IvoryLocation.exists(shadow.location </> "foo")
          bar    <- IvoryLocation.exists(shadow.location </> "foos" </> "bar")
        } yield exists && foo && bar})
    }) must beOkValue(true)
  }

  def repositoryToCluster = prop((one: Factset, two: Factset) => {
    withRepository(Posix)(repo => {
      withCluster(cluster => {
        val datasets = Datasets(List(Prioritized(Priority.Min, FactsetDataset(one)), Prioritized(Priority.Min, FactsetDataset(two))))
        for {
          _ <- one.partitions.map(Repository.factset(one.id) / _.key / "file").traverseU(key => IvoryLocation.writeUtf8(repo.toIvoryLocation(key), ""))
          _ <- two.partitions.map(Repository.factset(two.id) / _.key / "file").traverseU(key => IvoryLocation.writeUtf8(repo.toIvoryLocation(key), ""))
          s <- SyncIngest.toCluster(datasets, repo, cluster)
          o <- one.partitions.traverseU(p => IvoryLocation.exists(repo.toIvoryLocation(Repository.factset(one.id) / p.key / "file")))
          t <- two.partitions.traverseU(p => IvoryLocation.exists(repo.toIvoryLocation(Repository.factset(two.id) / p.key / "file")))
        } yield o ++ t })
    }) must beOkLike(_ must contain(true).forall)
  }).set(minTestsOk = 10)

  def repositoryFromCluster = prop((factset: Factset) => {
    withRepository(Posix)(repo => {
      withCluster(cluster => {
        val datasets = Datasets(List(Prioritized(Priority.Min, FactsetDataset(factset))))
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        for {
          _ <- factset.partitions.map(Repository.factset(factset.id) / _.key / "file")
                 .traverseU(key => IvoryLocation.writeUtf8(shadowRepository.root </> FilePath.unsafe(key.name), ""))
          s <- SyncExtract.toRepository(datasets, cluster, repo)
          o <- factset.partitions.traverseU(p => IvoryLocation.exists(repo.toIvoryLocation(Repository.factset(factset.id) / p.key / "file")))
        } yield o })
    }) must beOkLike(_ must contain(true).forall)
  }).set(minTestsOk = 10)

  def fileFromCluster = {
    withIvoryLocationFile(Posix)(location => {
      withCluster(cluster => {
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        val relativePath: FilePath = DirPath("shadowOutputDataset") </> DirPath(UUID.randomUUID) <|> "file"
        val shadowFile = shadowRepository.root </> relativePath
        for {
          _ <- IvoryLocation.writeUtf8(shadowFile, "")
          _ <- SyncExtract.outputDataSet(ShadowOutputDataset(shadowRepository.root), cluster, OutputDataset(location))
          o <- IvoryLocation.exists(location </> relativePath)
        } yield o })
    }) must beOkValue(true)
  }

  def directoryFromCluster = {
    withCluster(cluster => {
      withIvoryLocationDir(Posix)(location => {
        val shadowRepository = ShadowRepository.fromCluster(cluster)
        val relativePath: DirPath = DirPath("shadowOutputDataset") </> DirPath(UUID.randomUUID)
        val path = shadowRepository.root </> relativePath
        for {
          _   <- IvoryLocation.writeUtf8(path </> "foo", "")
          _   <- IvoryLocation.writeUtf8(path </> "foos" </> "bar", "")
          _   <- SyncExtract.outputDataSet(ShadowOutputDataset(shadowRepository.root), cluster, OutputDataset(location))
          foo <- IvoryLocation.exists(location </> relativePath </> "foo")
          bar <- IvoryLocation.exists(location </> relativePath </> "foos" </> "bar")
        } yield foo -> bar
      })
    }) must beOkValue(true -> true)
  }

  def checkPaths = prop((factset: Factset) => {
    val datasets = Datasets(List(Prioritized(Priority.Min, FactsetDataset(factset))))
    Sync.getKeys(datasets).length must be_==(factset.partitions.length)
  })

}
