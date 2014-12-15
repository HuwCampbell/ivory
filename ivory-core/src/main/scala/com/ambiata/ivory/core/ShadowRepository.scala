package com.ambiata.ivory.core

import com.ambiata.notion.core._
import com.ambiata.notion.distcopy.DistCopyConfiguration
import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import org.apache.hadoop.fs.Path

case class ShadowRepository(root: Path, ivory: IvoryConfiguration, source: Repository) {
  def configuration       = ivory.configuration

  def toShadowOutputDataset(key: Key): ShadowOutputDataset = // Unsafe, test
    ShadowOutputDataset(HdfsLocation(new Path(root, key.name).toString))

  def tmpDir: ShadowOutputDataset =
    ShadowOutputDataset(HdfsLocation(new Path(root, s"/tmp/${java.util.UUID.randomUUID}").toString))

}

object ShadowRepository {
  def fromCluster(cluster: Cluster, source: Repository): ShadowRepository =
    fromDistCopyConfiguration(cluster.root, cluster.conf, source)

  def fromDistCopyConfiguration(path: Path, conf: DistCopyConfiguration, source: Repository): ShadowRepository =
    ShadowRepository(
        path
      , IvoryConfiguration(
          List()
          , conf.client
          , () => conf.hdfs
          , () => ScoobiConfiguration(conf.hdfs)
          , () => None)
      , source
    )

  def toRepository(shadow: ShadowRepository): Repository =
    shadow.source

  def fromRepository(repo: HdfsRepository, conf: IvoryConfiguration): ShadowRepository =
    ShadowRepository(repo.root.toHdfsPath, conf, repo)

}
