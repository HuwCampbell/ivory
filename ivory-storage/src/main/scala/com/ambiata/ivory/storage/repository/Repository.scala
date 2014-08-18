package com.ambiata.ivory.storage.repository

import com.ambiata.saws.core.Clients

import scalaz.{Store => _, _}, effect.IO, \&/.This
import org.apache.hadoop.conf.Configuration
import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{Location => _, S3Location => _, HdfsLocation => _, LocalLocation => _, _}
import com.ambiata.mundane.store._
import com.ambiata.saws.s3._
import com.ambiata.ivory.core._
import com.ambiata.poacher.hdfs._
import com.ambiata.ivory.storage.store._
import com.nicta.scoobi.Scoobi._
import com.ambiata.poacher.hdfs._

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
  def storeById(id: FeatureStoreId): FilePath =  stores </> id.render
  def factset(id: FactsetId): FilePath =  factsets </> id.render
  def namespace(id: FactsetId, namespace: String): FilePath =  factset(id) </> namespace
  def snapshot(id: SnapshotId): FilePath = snapshots </> FilePath(id.render)
  def version(set: FactsetId): FilePath =  factset(set) </> ".version"
}

case class HdfsRepository(root: FilePath, repositoryConfiguration: RepositoryConfiguration) extends Repository {
  def configuration       = repositoryConfiguration.configuration
  def scoobiConfiguration = repositoryConfiguration.scoobiConfiguration

  def toStore = HdfsStore(configuration, root)
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
case class S3Repository(bucket: String, root: FilePath, repositoryConfiguration: RepositoryConfiguration) extends Repository {
  def toStore = S3Store(bucket, root, repositoryConfiguration.s3Client, repositoryConfiguration.s3TmpDirectory)
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
  def storeById(id: FeatureStoreId): FilePath =  stores </> id.render
  def factset(id: FactsetId): FilePath =  factsets </> id.render
  def namespace(set: FactsetId, namespace: String): FilePath =  factset(set) </> namespace
  def snapshot(id: SnapshotId): FilePath = snapshots </> FilePath(id.render)

  def fromUri(uri: String, repositoryConfiguration: RepositoryConfiguration): String \/ Repository =
    Reference.storeFromUri(uri, repositoryConfiguration).map {
      case HdfsStore(config, base)              => fromHdfsPath(base, repositoryConfiguration.scoobiConfiguration)
      case PosixStore(root)                     => fromLocalPath(root)
      case S3Store(bucket, base, client, cache) => fromS3(bucket, base, repositoryConfiguration)
    }

  def fromUriResultTIO(uri: String, repositoryConfiguration: RepositoryConfiguration): ResultTIO[Repository] =
    ResultT.fromDisjunction[IO, Repository](fromUri(uri, repositoryConfiguration).leftMap(This.apply))

  def fromHdfsPath(path: FilePath, sc: ScoobiConfiguration): HdfsRepository =
    HdfsRepository(path, RepositoryConfiguration(sc))

  def fromLocalPath(path: FilePath): LocalRepository =
    LocalRepository(path)

  def fromS3(bucket: String, path: FilePath, repositoryConfiguration: RepositoryConfiguration): S3Repository =
    S3Repository(bucket, path, repositoryConfiguration)
}

case class RepositoryConfiguration(s3: () => AmazonS3Client, hdfs: () => Configuration, scoobi: () => ScoobiConfiguration) {
  def s3TmpDirectory: FilePath                 = RepositoryConfiguration.defaultS3TmpDirectory
  def s3Client: AmazonS3Client                 = s3()
  def configuration: Configuration             = hdfs()
  def scoobiConfiguration: ScoobiConfiguration = scoobi()
}

object RepositoryConfiguration {
  def apply(configuration: Configuration): RepositoryConfiguration =
    new RepositoryConfiguration(s3 = () => Clients.s3, hdfs = () => configuration, scoobi = () => ScoobiConfiguration(configuration))

  def apply(sc: ScoobiConfiguration): RepositoryConfiguration =
    new RepositoryConfiguration(s3 = () => Clients.s3, hdfs = () => sc.configuration, scoobi = () => sc)

  val defaultS3TmpDirectory: FilePath = ".s3repository".toFilePath
}