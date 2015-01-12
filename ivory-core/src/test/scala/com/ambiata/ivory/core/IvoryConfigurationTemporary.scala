package com.ambiata.ivory.core

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.core._
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.Scoobi._
import scalaz._, Scalaz._, effect.IO

case class IvoryConfigurationTemporary(dir: String) {
  def conf: RIO[IvoryConfiguration] = for {
    d <- ConfigurationTemporary(dir).conf
    c = new IvoryConfiguration(
      arguments = List(),
      s3Client = Clients.s3,
      hdfs = () => d,
      scoobi = () => ScoobiConfiguration(d),
      compressionCodec = () => None)
  } yield c
}

object IvoryConfigurationTemporary {
  def random: IvoryConfigurationTemporary =
    IvoryConfigurationTemporary(java.util.UUID.randomUUID().toString)

  /** Deprecated callbacks. Use `IvoryConfigurationTemporary.conf` */
  def withConf[A](f: IvoryConfiguration => RIO[A]): RIO[A] = for {
    d <- LocalTemporary.random.directory
    r <- runWithConf(d, f)
  } yield r

  def runWithConf[A](dir: DirPath, f: IvoryConfiguration => RIO[A]): RIO[A] =
    IvoryConfigurationTemporary(dir.path).conf >>= (f(_))

  def withConfX[A](f: IvoryConfiguration => A): RIO[A] =
    IvoryConfigurationTemporary(java.util.UUID.randomUUID.toString).conf >>= (c => RIO.ok(f(c)))
}
