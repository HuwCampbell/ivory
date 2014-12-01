package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.TemporaryLocations._
import com.ambiata.ivory.core.TemporaryRepositories._
import com.ambiata.ivory.storage.arbitraries.Arbitraries._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.S3

import com.nicta.scoobi.impl.ScoobiConfiguration

import org.apache.hadoop.fs.Path

import org.specs2.{ScalaCheck, Specification}

import scalaz._, Scalaz._, effect.IO

class SyncIngestSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Sync operations to cluster
==========================
 syncing single file to ShadowInputDataset              $file
 syncing folder/prefix to ShadowInputDataset            $prefix
 syncing datasets to ShadowRepository                   $dataset

"""
  def file = propNoShrink((data: String, locationType: TemporaryType) => {
    withCluster(cluster => {
      withIvoryLocationFile(locationType)(location => {
        val dataset = InputDataset(location.location)
        for {
          _      <- saveLocationFile(location, data)
          shadow <- SyncIngest.inputDataset(dataset, cluster)
          p      = new Path(shadow.location.path)
          dir    <- Hdfs.isDirectory(p).run(cluster.hdfsConfiguration)
          exists <- Hdfs.exists(p).run(cluster.hdfsConfiguration)
          d      <- Hdfs.readContentAsString(p).run(cluster.hdfsConfiguration)
        } yield (dir, exists, d)
      })
    }) must beOkValue((false, true, data))
  }).set(minTestsOk = 10)

  def prefix = prop((data: String, locationType: TemporaryType) => {
    withCluster(cluster => {
      withIvoryLocationDir(locationType)(dir => {
        val dataset = InputDataset(dir.location)
        for {
          _      <- saveLocationFile(dir </> FilePath.unsafe("foo"), data)
          shadow <- SyncIngest.inputDataset(dataset, cluster)
          p      = new Path(shadow.location.path + "/foo")
          pe     <- Hdfs.exists(new Path(shadow.location.path)).run(cluster.hdfsConfiguration)
          exists <- Hdfs.exists(p).run(cluster.hdfsConfiguration)
          d      <- Hdfs.readContentAsString(p).run(cluster.hdfsConfiguration)
        } yield (pe, exists, d)
      })
    }) must beOkValue((true, true, data))
  }).set(minTestsOk = 10)

  def dataset = prop((data: String, tt: TemporaryType, one: Factset, two: Factset) => one.id != two.id ==> {
    withRepository(Posix)(repo => {
      withCluster(cluster => {
        val datasets = Datasets(List(Prioritized(Priority.Min, FactsetDataset(one)), Prioritized(Priority.Min, FactsetDataset(two))))
        for {
          _ <- one.partitions.map(Repository.factset(one.id) / _.key / Key("file")).traverseU(repo.store.utf8.write(_, data))
          _ <- two.partitions.map(Repository.factset(two.id) / _.key / Key("file")).traverseU(repo.store.utf8.write(_, data))
          _ <- repo.store.utf8.write(Key("foo"), data)
          _ <- repo.store.utf8.write(Key("foos") / Key("bar"), data)
          s <- SyncIngest.toCluster(datasets, repo, cluster)
          o <- one.partitions.traverseU(p => Hdfs.exists(new Path(s.root.toString + "/" + (Repository.factset(one.id) / p.key / Key("file")).name)).run(s.configuration))
          t <- two.partitions.traverseU(p => Hdfs.exists(new Path(s.root.toString + "/" + (Repository.factset(two.id) / p.key / Key("file")).name)).run(s.configuration))
          f <- Hdfs.exists(new Path(cluster.root.toString + "/foo")).run(cluster.hdfsConfiguration).map(!_)
          b <- Hdfs.exists(new Path(cluster.root.toString + "foos/bar")).run(cluster.hdfsConfiguration).map(!_)
        } yield b :: f :: o ++ t
      })
    }) must beOkLike(_ must contain(true).forall)
  }).set(minTestsOk = 3)

}
