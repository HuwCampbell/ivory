package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.notion.core.Location
import com.ambiata.notion.distcopy.DistCopyConfiguration
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.notion.io.LocationIO

import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.fs.Path


case class Cluster(root: Path, conf: DistCopyConfiguration, codec: Option[CompressionCodec]) {
  def hdfsConfiguration: Configuration = conf.hdfs
  def s3Client: AmazonS3Client = conf.client
  def rootDirPath: DirPath = DirPath.unsafe(root.toString)

  def io: LocationIO =
    LocationIO(hdfsConfiguration, s3Client)

  /** A very short term convenience method - we need to remove this soon */
  def toIvoryLocation(l: Location): IvoryLocation =
    IvoryLocation.fromLocation(l, Cluster.ivoryConfiguration(this))
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
    Cluster(root, conf, ivory.codec)
  }

  def ivoryConfiguration(cluster: Cluster): IvoryConfiguration =
    new IvoryConfiguration(
      List()
      , cluster.s3Client
      , () => cluster.hdfsConfiguration
      , () => ScoobiConfiguration(cluster.hdfsConfiguration)
      , () => cluster.codec
    )

}
