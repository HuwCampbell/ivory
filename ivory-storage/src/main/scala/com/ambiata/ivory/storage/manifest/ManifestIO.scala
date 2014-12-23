package com.ambiata.ivory.storage.manifest

import argonaut._, Argonaut._
import com.ambiata.ivory.core._
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._
import scalaz._, Scalaz._

case class ManifestIO[A](base: IvoryLocation) {
  def location: IvoryLocation =
    base </> FileName.unsafe(".manifest.json")

  def write(manifest: A)(implicit A: EncodeJson[A]): RIO[Unit] =
    IvoryLocation.writeUtf8(location, manifest.asJson.spaces2)

  def read(implicit A: DecodeJson[A]): RIO[Option[A]] = for {
    e <- exists
    m <- if (e) readOrFail.map(_.some) else none[A].pure[RIO]
  } yield m

  def readOrFail(implicit A: DecodeJson[A]): RIO[A] =
    IvoryLocation.readUtf8(location).flatMap(s => ResultT.fromDisjunctionString(s.decodeEither[A].leftMap(e => s"Failed to parse metadata at $base, with $e")))

  def exists: RIO[Boolean] =
    IvoryLocation.exists(location)

  def peek: RIO[Option[VersionManifest]] = for {
    e <- exists
    m <- if (e) peekOrFail.map(_.some) else none[VersionManifest].pure[RIO]
  } yield m

  def peekOrFail: RIO[VersionManifest] =
    IvoryLocation.readUtf8(location).flatMap(s => ResultT.fromDisjunctionString(s.decodeEither[VersionManifestPeek].map(_.version).leftMap(e => s"Failed to peek at metadata at $base, with $e")))

}
