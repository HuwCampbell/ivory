package com.ambiata.ivory.core

case class ShadowRepository(root: HdfsIvoryLocation) {
  def configuration       = root.configuration
  def scoobiConfiguration = root.scoobiConfiguration
  def codec               = root.codec
}


object ShadowRepository {

  def fromCluster(cluster: Cluster): ShadowRepository = ShadowRepository(cluster.root)

  def toRepository(shadow: ShadowRepository): Repository = HdfsRepository(shadow.root)
}
