package com.ambiata.ivory.core

import com.ambiata.mundane.control._
import com.ambiata.notion.core.{TemporaryType => T, _}
import com.ambiata.notion.core.Arbitraries._
import org.apache.hadoop.conf.Configuration
import org.scalacheck._, Arbitrary._

case class IvoryLocationTemporary(t: T, path: String, conf: IvoryConfiguration) {
  override def toString: String =
    s"IvoryLocationTemporary($t, $path)"

  def file: RIO[IvoryLocation] =
    directory

  def directory: RIO[IvoryLocation] =
    LocationTemporary(t, path, conf.s3Client, conf.configuration).location.map(locationToIvoryLocation(_))

  def locationToIvoryLocation(location: Location): IvoryLocation = location match {
    case l @ LocalLocation(_) =>
      LocalIvoryLocation(l)
    case s @ S3Location(_, _) =>
      S3IvoryLocation(s, conf)
    case h @ HdfsLocation(_) =>
      HdfsIvoryLocation(h, conf)
  }
}

object IvoryLocationTemporary {
  implicit def IvoryLocationTemporaryArbitrary: Arbitrary[IvoryLocationTemporary] =
    Arbitrary(arbitrary[T].map(IvoryLocationTemporary(_, java.util.UUID.randomUUID().toString,
      IvoryConfiguration.fromConfiguration(new Configuration))))

  /** Deprecated callbacks. Use `IvoryLocationTemporary.file` or `IvoryLocationTemporary.directory` */
  def withIvoryLocationDir[A](t: T)(f: IvoryLocation => RIO[A]): RIO[A] = for {
    c <- IvoryConfigurationTemporary.random.conf
    p = java.util.UUID.randomUUID().toString
    d <- IvoryLocationTemporary(t, p, c).directory
    r <- f(d)
  } yield r

  def withIvoryLocationFile[A](t: T)(f: IvoryLocation => RIO[A]): RIO[A] = for {
    c <- IvoryConfigurationTemporary.random.conf
    p = java.util.UUID.randomUUID().toString
    d <- IvoryLocationTemporary(t, p, c).file
    r <- f(d)
  } yield r
}
