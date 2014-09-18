package com.ambiata.ivory.core

import com.ambiata.mundane.io._

case class ShadowRepository(root: FilePath, ivory: IvoryConfiguration) {
  def configuration       = ivory.configuration
  def scoobiConfiguration = ivory.scoobiConfiguration
  def codec               = ivory.codec
}


object ShadowRepository {
  def fromHdfsPath(path: FilePath, configuration: IvoryConfiguration): ShadowRepository =
    ShadowRepository(path, configuration)

  def fromCluster(cluster: Cluster): ShadowRepository = ShadowRepository(cluster.root, cluster.ivory)

  def toRepository(shadow: ShadowRepository): Repository = HdfsRepository(shadow.root, shadow.ivory)
}
