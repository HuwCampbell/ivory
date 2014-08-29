package com.ambiata.ivory.core

import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.io._
import com.ambiata.saws.core.Clients
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec

case class IvoryConfiguration(
    arguments: List[String],
    s3Client: AmazonS3Client,
    hdfs: () => Configuration,
    scoobi: () => ScoobiConfiguration,
    compressionCodec: () => Option[CompressionCodec]) {
  val s3TmpDirectory: DirPath = IvoryConfiguration.defaultS3TmpDirectory

  lazy val configuration: Configuration             = hdfs()
  lazy val scoobiConfiguration: ScoobiConfiguration = scoobi()
  lazy val codec: Option[CompressionCodec]          = compressionCodec()
}

object IvoryConfiguration {
  def fromConfiguration(configuration: Configuration): IvoryConfiguration =
    new IvoryConfiguration(
      arguments = List(),
      s3Client = Clients.s3,
      hdfs = () => configuration,
      scoobi = () => ScoobiConfiguration(configuration),
      compressionCodec = () => None)

  def fromScoobiConfiguration(sc: ScoobiConfiguration): IvoryConfiguration =
    new IvoryConfiguration(
      arguments = List(),
      s3Client = Clients.s3,
      hdfs = () => sc.configuration,
      scoobi = () => sc,
      compressionCodec = () => None)

  val defaultS3TmpDirectory: DirPath = DirPath(".s3repository")
}