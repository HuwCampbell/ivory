package com.ambiata.ivory.core

import com.ambiata.notion.core._
import com.ambiata.notion.distcopy.DistCopyConfiguration
import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import org.apache.hadoop.fs.Path

case class ShadowRepository(root: Path, ivory: IvoryConfiguration) {
  def configuration       = ivory.configuration
}

object ShadowRepository {
  def fromCluster(cluster: Cluster): ShadowRepository =
    fromDistCopyConfiguration(cluster.root, cluster.conf)

  def fromDistCopyConfiguration(path: Path, conf: DistCopyConfiguration): ShadowRepository =
    ShadowRepository(
        path
      , IvoryConfiguration(
          List()
          , conf.client
          , () => conf.hdfs
          , () => ScoobiConfiguration(conf.hdfs)
          , () => None)
    )

  def toRepository(shadow: ShadowRepository): Repository =
    HdfsRepository(HdfsIvoryLocation(HdfsLocation(shadow.root.toString), shadow.ivory))

  def fromRepository(repo: Repository, conf: IvoryConfiguration): ShadowRepository = repo match {
    case HdfsRepository(r) =>
      ShadowRepository(r.toHdfsPath, conf)
    case _ =>
      Crash.error(Crash.CodeGeneration, "Only HdfsRepository's are supported at this time")
  }
}
