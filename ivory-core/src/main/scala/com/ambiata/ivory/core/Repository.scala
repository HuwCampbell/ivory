package com.ambiata.ivory.core

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

case class HdfsRepository(root: FilePath, @transient ivory: IvoryConfiguration) extends Repository {
  def configuration       = ivory.configuration
  def scoobiConfiguration = ivory.scoobiConfiguration
  def codec               = ivory.codec

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
case class S3Repository(bucket: String, root: FilePath, @transient ivory: IvoryConfiguration) extends Repository {
  def toStore = S3Store(bucket, root, ivory.s3Client, ivory.s3TmpDirectory)
}

object Repository {
  def root: FilePath = FilePath.root
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
  def namespace(set: FactsetId, namespace: String): FilePath = factset(set) </> namespace
  def snapshot(id: SnapshotId): FilePath = snapshots </> FilePath(id.render)

  def fromUri(uri: String, repositoryConfiguration: IvoryConfiguration): String \/ Repository =
    Reference.storeFromUri(uri, repositoryConfiguration).map(fromStore(_,repositoryConfiguration))

  def fromUriResultTIO(uri: String, repositoryConfiguration: IvoryConfiguration): ResultTIO[Repository] =
    ResultT.fromDisjunction[IO, Repository](fromUri(uri, repositoryConfiguration).leftMap(This.apply))

  def fromHdfsPath(path: FilePath, configuration: IvoryConfiguration): HdfsRepository =
    HdfsRepository(path, configuration)

  def fromHdfsPath(path: FilePath, configuration: Configuration): HdfsRepository =
    HdfsRepository(path, IvoryConfiguration.fromConfiguration(configuration))

  def fromHdfsPath(path: FilePath, scoobiConfiguration: ScoobiConfiguration): HdfsRepository =
    HdfsRepository(path, IvoryConfiguration.fromScoobiConfiguration(scoobiConfiguration))

  def fromLocalPath(path: FilePath): LocalRepository =
    LocalRepository(path)

  def fromS3(bucket: String, path: FilePath, repositoryConfiguration: IvoryConfiguration): S3Repository =
    S3Repository(bucket, path, repositoryConfiguration)

  def fromStore(store: Store[ResultTIO], repositoryConfiguration: IvoryConfiguration): Repository = store match {
      case HdfsStore(config, base)              => fromHdfsPath(base, repositoryConfiguration)
      case PosixStore(root)                     => fromLocalPath(root)
      case S3Store(bucket, base, client, cache) => fromS3(bucket, base, repositoryConfiguration)
    }
}