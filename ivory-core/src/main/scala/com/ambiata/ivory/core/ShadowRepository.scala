package com.ambiata.ivory.core

case class ShadowRepository(root: IvoryLocation) {
  def ivory: IvoryConfiguration = root.ivory
  def configuration       = ivory.configuration
  def scoobiConfiguration = ivory.scoobiConfiguration
  def codec               = ivory.codec
}


object ShadowRepository {
  def fromLocation(location: IvoryLocation): ShadowRepository =
    ShadowRepository(location)

  def fromCluster(cluster: Cluster): ShadowRepository = ShadowRepository(cluster.root)

  def toRepository(shadow: ShadowRepository): Repository = HdfsRepository(shadow.root.location, shadow.ivory)
}
