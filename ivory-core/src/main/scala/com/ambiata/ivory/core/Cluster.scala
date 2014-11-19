package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.notion.distcopy.DistCopyConfiguration
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class Cluster(root: Path, conf: DistCopyConfiguration) {
  def hdfsConfiguration: Configuration = conf.hdfs
  def s3Client: AmazonS3Client = conf.client
  def rootDirPath: DirPath = DirPath.unsafe(root.toString)
}

object Cluster {
  def fromIvoryConfiguration(root: Path, ivory: IvoryConfiguration, mappers: Int): Cluster = {
    val conf = DistCopyConfiguration(
        ivory.configuration
      , ivory.s3Client
      , mappers
      , DistCopyConfiguration.Default.retryCount
      , DistCopyConfiguration.Default.partSize
      , DistCopyConfiguration.Default.readLimit
      , DistCopyConfiguration.Default.multipartUploadThreshold
    )
    Cluster(root, conf)
  }

}
