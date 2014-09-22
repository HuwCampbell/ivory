package com.ambiata.ivory.core

import scalaz.{Name=>_,Store => _, _}
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{HdfsLocation => _, LocalLocation => _, Location => _, S3Location => _, _}
import com.ambiata.mundane.store._
import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.saws.s3.S3Store
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration

import scalaz.\&/.This
import scalaz.effect.IO
import scalaz.{Store => _, _}

/**
 * Ivory repository
 *
 * It defines the layout of both metadata (dictionary, feature store) and data (factsets, snapshots)
 *
 * The repository encapsulate a Store[ResultTIO] in order to be able to build references (ReferenceIO)
 * to the data
 */
sealed trait Repository {
  def store: Store[ResultTIO]
  def rootPath: DirPath
  def toFilePath(key: Key): FilePath = rootPath </> FilePath.unsafe(key.name)
}

case class HdfsRepository(rootPath: DirPath, @transient ivory: IvoryConfiguration) extends Repository {
  def configuration       = ivory.configuration
  def scoobiConfiguration = ivory.scoobiConfiguration
  def codec               = ivory.codec

  def store = HdfsStore(configuration, rootPath)
}

case class LocalRepository(rootPath: DirPath) extends Repository {
  def store = PosixStore(rootPath)
}

/**
 * Repository on S3
 * all data is going to be stored on bucket/key
 * tmpDirectory is a transient directory (on Hdfs) that is used to import data and
 * convert them to the ivory format before pushing them to S3
 */
case class S3Repository(bucket: String, rootPath: DirPath, @transient ivory: IvoryConfiguration) extends Repository {
  def store = S3Store(bucket, rootPath, ivory.s3Client, ivory.s3TmpDirectory)
}

object Repository {

  /**
   * list of repository keys
   */

  def root: Key = Key.Root

  def errors: Key        = root / "errors"
  def factsets: Key      = root / "factsets"
  def snapshots: Key     = root / "snapshots"
  def metadata: Key      = root / "metadata"

  def dictionaries: Key  = metadata / "dictionaries"
  def featureStores: Key = metadata / "stores"
  def commits: Key       = metadata / "repositories"

  def dictionaryById(id: DictionaryId): Key           = dictionaries  / id.asKeyName
  def featureStoreById(id: FeatureStoreId): Key       = featureStores / id.asKeyName
  def commitById(id: CommitId): Key                   = commits       / id.asKeyName
  def factset(id: FactsetId): Key                     = factsets      / id.asKeyName
  def namespace(set: FactsetId, namespace: Name): Key = factset(set)  / namespace.asKeyName
  def snapshot(id: SnapshotId): Key                   = snapshots     / id.asKeyName
  def version(set: FactsetId): Key                    = factset(set)  / ".version"

  def fromUri(uri: String, repositoryConfiguration: IvoryConfiguration): String \/ Repository =
    Reference.storeFromUri(uri, repositoryConfiguration).map(fromStore(_, repositoryConfiguration))

  def fromUriResultTIO(uri: String, repositoryConfiguration: IvoryConfiguration): ResultTIO[Repository] =
    ResultT.fromDisjunction[IO, Repository](fromUri(uri, repositoryConfiguration).leftMap(This.apply))

  def fromHdfsPath(path: DirPath, configuration: IvoryConfiguration): HdfsRepository =
    HdfsRepository(path, configuration)

  def fromHdfsPath(path: DirPath, configuration: Configuration): HdfsRepository =
    HdfsRepository(path, IvoryConfiguration.fromConfiguration(configuration))

  def fromHdfsPath(path: DirPath, scoobiConfiguration: ScoobiConfiguration): HdfsRepository =
    HdfsRepository(path, IvoryConfiguration.fromScoobiConfiguration(scoobiConfiguration))

  def fromLocalPath(path: DirPath): LocalRepository =
    LocalRepository(path)

  def fromS3(bucket: String, path: DirPath, repositoryConfiguration: IvoryConfiguration): S3Repository =
    S3Repository(bucket, path, repositoryConfiguration)

  def fromStore(store: Store[ResultTIO], repositoryConfiguration: IvoryConfiguration): Repository = store match {
      case HdfsStore(config, base)              => fromHdfsPath(base, repositoryConfiguration)
      case PosixStore(root)                     => fromLocalPath(root)
      case S3Store(bucket, base, client, cache) => fromS3(bucket, base, repositoryConfiguration)
    }
}