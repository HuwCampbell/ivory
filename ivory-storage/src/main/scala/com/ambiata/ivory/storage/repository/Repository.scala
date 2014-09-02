package com.ambiata.ivory.storage.repository

import com.ambiata.saws.core.Clients
import org.apache.hadoop.io.compress.CompressionCodec

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
  def featureStores: FilePath = metadata </> "stores"
  def commits: FilePath = metadata </> "commits"
  def dictionaryById(id: DictionaryId): FilePath = dictionaries </> id.render
  def featureStoreById(id: FeatureStoreId): FilePath = featureStores </> id.render
  def commitById(id: CommitId): FilePath = commits </> id.render
  def factset(id: FactsetId): FilePath = factsets </> id.render
  def namespace(id: FactsetId, namespace: String): FilePath = factset(id) </> namespace
  def snapshot(id: SnapshotId): FilePath = snapshots </> FilePath(id.render)
  def version(set: FactsetId): FilePath = factset(set) </> ".version"
}

case class HdfsRepository(root: FilePath, @transient repositoryConfiguration: RepositoryConfiguration) extends Repository {
  def configuration       = repositoryConfiguration.configuration
  def scoobiConfiguration = repositoryConfiguration.scoobiConfiguration
  def codec               = repositoryConfiguration.codec

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
case class S3Repository(bucket: String, root: FilePath, @transient repositoryConfiguration: RepositoryConfiguration) extends Repository {
  def toStore = S3Store(bucket, root, repositoryConfiguration.s3Client, repositoryConfiguration.s3TmpDirectory)
}

object Repository {
  def root: FilePath = FilePath.root
  def errors: FilePath = root </> "errors"
  def factsets: FilePath = root </> "factsets"
  def snapshots: FilePath = root </> "snapshots"
  def metadata: FilePath = root </> "metadata"
  def dictionaries: FilePath = metadata </> "dictionaries"
  def featureStores: FilePath = metadata </> "stores"
  def commits: FilePath = metadata </> "repositories"
  def dictionaryById(id: DictionaryId): FilePath = dictionaries </> id.render
  def featureStoreById(id: FeatureStoreId): FilePath = featureStores </> id.render
  def commitById(id: CommitId): FilePath = commits </> id.render
  def factset(id: FactsetId): FilePath = factsets </> id.render
  def namespace(set: FactsetId, namespace: String): FilePath = factset(set) </> namespace
  def snapshot(id: SnapshotId): FilePath = snapshots </> FilePath(id.render)

  def fromUri(uri: String, repositoryConfiguration: RepositoryConfiguration): String \/ Repository =
    Reference.storeFromUri(uri, repositoryConfiguration).map(fromStore(_,repositoryConfiguration))

  def fromUriResultTIO(uri: String, repositoryConfiguration: RepositoryConfiguration): ResultTIO[Repository] =
    ResultT.fromDisjunction[IO, Repository](fromUri(uri, repositoryConfiguration).leftMap(This.apply))

  def fromHdfsPath(path: FilePath, configuration: RepositoryConfiguration): HdfsRepository =
    HdfsRepository(path, configuration)

  def fromHdfsPath(path: FilePath, configuration: Configuration): HdfsRepository =
    HdfsRepository(path, RepositoryConfiguration(configuration))

  def fromHdfsPath(path: FilePath, scoobiConfiguration: ScoobiConfiguration): HdfsRepository =
    HdfsRepository(path, RepositoryConfiguration(scoobiConfiguration))

  def fromLocalPath(path: FilePath): LocalRepository =
    LocalRepository(path)

  def fromS3(bucket: String, path: FilePath, repositoryConfiguration: RepositoryConfiguration): S3Repository =
    S3Repository(bucket, path, repositoryConfiguration)

  def fromStore(store: Store[ResultTIO], repositoryConfiguration: RepositoryConfiguration): Repository = store match {
      case HdfsStore(config, base)              => fromHdfsPath(base, repositoryConfiguration)
      case PosixStore(root)                     => fromLocalPath(root)
      case S3Store(bucket, base, client, cache) => fromS3(bucket, base, repositoryConfiguration)
    }
}

case class RepositoryConfiguration(
  arguments: List[String],
  s3Client: AmazonS3Client,
  hdfs: () => Configuration,
  scoobi: () => ScoobiConfiguration,
  compressionCodec: () => Option[CompressionCodec]) {
  val s3TmpDirectory: FilePath                 = RepositoryConfiguration.defaultS3TmpDirectory

  lazy val configuration: Configuration             = hdfs()
  lazy val scoobiConfiguration: ScoobiConfiguration = scoobi()
  lazy val codec: Option[CompressionCodec]          = compressionCodec()
  }

object RepositoryConfiguration {
  def apply(configuration: Configuration): RepositoryConfiguration =
    new RepositoryConfiguration(
      arguments = List(),
      s3Client = Clients.s3,
      hdfs = () => configuration,
      scoobi = () => ScoobiConfiguration(configuration),
      compressionCodec = () => None)

  def apply(sc: ScoobiConfiguration): RepositoryConfiguration =
    new RepositoryConfiguration(
      arguments = List(),
      s3Client = Clients.s3,
      hdfs = () => sc.configuration,
      scoobi = () => sc,
      compressionCodec = () => None)

  val defaultS3TmpDirectory: FilePath = ".s3repository".toFilePath
}