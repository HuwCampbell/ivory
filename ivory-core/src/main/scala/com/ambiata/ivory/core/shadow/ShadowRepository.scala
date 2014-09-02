package com.ambiata.ivory.core.shadow

import com.ambiata.mundane.io._

sealed trait ShadowRepository

case class ShadowRepositoryHdfs(root: FilePath, repositoryConfiguration: RepositoryConfiguration) extends ShadowRepository{
  def configuration       = repositoryConfiguration.configuration
  def scoobiConfiguration = repositoryConfiguration.scoobiConfiguration
  def codec               = repositoryConfiguration.codec
}


object ShadowRepository {
  def fromHdfsPath(path: FilePath, configuration: RepositoryConfiguration): ShadowRepository =
    ShadowRepositoryHdfs(path, configuration)
}
