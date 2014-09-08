package com.ambiata.ivory.core

import com.ambiata.mundane.io._

case class ShadowRepository(root: FilePath, repositoryConfiguration: IvoryConfiguration) {
  def configuration       = repositoryConfiguration.configuration
  def scoobiConfiguration = repositoryConfiguration.scoobiConfiguration
  def codec               = repositoryConfiguration.codec
}


object ShadowRepository {
  def fromHdfsPath(path: FilePath, configuration: IvoryConfiguration): ShadowRepository =
    ShadowRepository(path, configuration)

  def fromRepoisitory(repo: Repository): ShadowRepository = repo match {
    case HdfsRepository(root, conf) => ShadowRepository(root, conf)
    case _                          => Crash.error(Crash.CodeGeneration, "Only HDFS ShadowRepositories are currently supported")
  }

  def fromCluster(cluster: Cluster): ShadowRepository = ShadowRepository(cluster.root, cluster.configuration)

  def toRepository(shadow: ShadowRepository): Repository = HdfsRepository(shadow.root, shadow.repositoryConfiguration)
}
