package com.ambiata.ivory.core

import com.ambiata.notion.distcopy.DistCopyConfiguration
import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import org.apache.hadoop.fs.Path

case class ShadowRepository(root: Path, ivory: IvoryConfiguration, source: Repository) {
  def configuration       = ivory.configuration
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

  def fromRepository(repo: Repository, conf: IvoryConfiguration): ShadowRepository = repo match {
    case HdfsRepository(r) =>
      ShadowRepository(r.toHdfsPath, conf, repo)
    case _ =>
      Crash.error(Crash.CodeGeneration, "Only HdfsRepository's are supported at this time") // Until plan is implemented
  }
}
