package com.ambiata.ivory.storage.repository

import scalaz.{Store => _, _}, effect.IO, \&/.This
import org.apache.hadoop.conf.Configuration
import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{Location => _, S3Location => _, HdfsLocation => _, LocalLocation => _, _}
import com.ambiata.mundane.store._
import com.ambiata.saws.s3._
import com.ambiata.saws.core._
import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.store._
import com.nicta.scoobi.Scoobi._

sealed trait Repository {
  def toStore: Store[ResultTIO]
  def toReference(path: FilePath): ReferenceIO =
    Reference(toStore, path)

  def root: FilePath
  def errors: FilePath = root </> "errors"
  def factsets: FilePath = root </> "factsets"
  def snapshots: FilePath = root </> "snapshots"
  def metadata: FilePath = root </> "metadata"
  def dictionaries: FilePath = metadata </> "dictionaries"
  def stores: FilePath = metadata </> "stores"
  def dictionaryByName(name: String): FilePath =  dictionaries </> name
  def storeByName(name: String): FilePath =  stores </> name
  def factset(set: Factset): FilePath =  factsets </> set.name
  def namespace(set: Factset, namespace: String): FilePath =  factset(set) </> namespace
  def version(set: Factset): FilePath =  factset(set) </> ".version"
}

case class HdfsRepository(root: FilePath, conf: Configuration, run: ScoobiRun) extends Repository {
  def toStore = HdfsStore(conf, root)
}

case class LocalRepository(root: FilePath) extends Repository {
  def toStore = PosixStore(root)
}

/**
 * Repository on S3
 * all data is going to be stored on bucket/key
 * tmpDirectory is a transient directory (on Hdfl) that is used to import data and
 * convert them to the ivory format before pushing them to S3
 */
case class S3Repository(bucket: String, root: FilePath, tmp: FilePath, conf: ScoobiConfiguration, client: AmazonS3Client, run: S3Run) extends Repository {
  def toStore = S3Store(bucket, root, client, tmp)
  val hdfs = HdfsRepository(tmp, conf, run)
}

object Repository {
  def root: FilePath = FilePath.root
  def errors: FilePath = root </> "errors"
  def factsets: FilePath = root </> "factsets"
  def snapshots: FilePath = root </> "snapshots"
  def metadata: FilePath = root </> "metadata"
  def dictionaries: FilePath = metadata </> "dictionaries"
  def stores: FilePath = metadata </> "stores"
  def dictionaryByName(name: String): FilePath =  dictionaries </> name
  def storeByName(name: String): FilePath =  stores </> name
  def factset(set: Factset): FilePath =  factsets </> set.name
  def namespace(set: Factset, namespace: String): FilePath =  factset(set) </> namespace

  val defaultS3TmpDirectory = Reference.defaultS3TmpDirectory

  def fromUri(s: String, conf: ScoobiConfiguration): String \/ Repository =
    Reference.storeFromUri(s, conf).map {
      case HdfsStore(config, base) => fromHdfsPath(base, config)
      case PosixStore(root) => fromLocalPath(root)
      case S3Store(bucket, base, client, cache) => fromS3WithTemp(bucket, base, cache, conf).copy(client = client)
    }

  def fromUriResultTIO(s: String, conf: ScoobiConfiguration): ResultTIO[Repository] =
    ResultT.fromDisjunction[IO, Repository](fromUri(s, conf).leftMap(This.apply))

  def fromHdfsPath(path: FilePath, conf: ScoobiConfiguration): HdfsRepository =
    HdfsRepository(path, conf, ScoobiRun(conf))

  def fromLocalPath(path: FilePath): LocalRepository =
    LocalRepository(path)

  def fromS3(bucket: String, path: FilePath, conf: ScoobiConfiguration): S3Repository =
    fromS3WithTemp(bucket, path, Reference.defaultS3TmpDirectory, conf)

  /** use a specific temporary directory to store ivory files before they are saved on S3 */
  def fromS3WithTemp(bucket: String, path: FilePath, tmp: FilePath, conf: ScoobiConfiguration): S3Repository =
    S3Repository(bucket, path, tmp, conf, Clients.s3, S3Run(conf))
}
