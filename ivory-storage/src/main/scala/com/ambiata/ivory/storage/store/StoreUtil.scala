package com.ambiata.ivory.storage.store

import com.ambiata.ivory.alien.hdfs.HdfsStore
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.S3Store
import com.nicta.scoobi.Scoobi._

import scalaz.\/

object StoreUtil {

  val defaultS3TmpDirectory: FilePath =
    ".s3repository".toFilePath

  def fromUri(s: String, conf: ScoobiConfiguration): String \/ Store[ResultTIO] = {
    Location.fromUri(s).map {
      case HdfsLocation(path) => HdfsStore(conf, path.toFilePath)
      case LocalLocation(path) => PosixStore(path.toFilePath)
      case S3Location(bucket, path) => S3Store(bucket, path.toFilePath, Clients.s3, defaultS3TmpDirectory)
    }
  }
}
