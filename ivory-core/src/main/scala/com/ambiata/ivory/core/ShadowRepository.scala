package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.mundane.store.Key

case class ShadowRepository(root: DirPath, ivory: IvoryConfiguration) {
  def configuration       = ivory.configuration
  def scoobiConfiguration = ivory.scoobiConfiguration
  def codec               = ivory.codec

  def toFilePath(key: Key): FilePath =
    root </> FilePath.unsafe(key.name)
}


object ShadowRepository {
  def fromHdfsPath(path: DirPath, configuration: IvoryConfiguration): ShadowRepository =
    ShadowRepository(path, configuration)

  def fromCluster(cluster: Cluster): ShadowRepository = ShadowRepository(cluster.root, cluster.ivory)

  def toRepository(shadow: ShadowRepository): Repository = HdfsRepository(shadow.root, shadow.ivory)
}
