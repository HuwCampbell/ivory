package com.ambiata.ivory.core

import scalaz.{Name=>_,Store => _, _}
import com.amazonaws.services.s3.AmazonS3Client
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
  def toStore: Store[ResultTIO]

  def toReference(path: DirPath): ReferenceIO = Reference(toStore, path)
  def toReference(path: FilePath): ReferenceIO = Reference(toStore, path)

  def root: ReferenceIO = toReference(DirPath.Empty)

  def errors: ReferenceIO        = toReference(Repository.errors)
  def factsets: ReferenceIO      = toReference(Repository.factsets)
  def snapshots: ReferenceIO     = toReference(Repository.snapshots)
  def metadata: ReferenceIO      = toReference(Repository.metadata)

  def dictionaries: ReferenceIO  = toReference(Repository.dictionaries)
  def featureStores: ReferenceIO = toReference(Repository.featureStores)
  def commits: ReferenceIO       = toReference(Repository.commits)

  def dictionaryById(id: DictionaryId): ReferenceIO           = toReference(Repository.dictionaryById(id))
  def featureStoreById(id: FeatureStoreId): ReferenceIO       = toReference(Repository.featureStoreById(id))
  def commitById(id: CommitId): ReferenceIO                   = toReference(Repository.commitById(id))
  def factset(id: FactsetId): ReferenceIO                     = toReference(Repository.factset(id))
  def namespace(id: FactsetId, namespace: Name): ReferenceIO  = toReference(Repository.namespace(id, namespace))
  def snapshot(id: SnapshotId): ReferenceIO                   = toReference(Repository.snapshot(id))
  def version(id: FactsetId): ReferenceIO                     = toReference(Repository.version(id))

}

case class HdfsRepository(rootPath: DirPath, @transient ivory: IvoryConfiguration) extends Repository {
  def configuration       = ivory.configuration
  def scoobiConfiguration = ivory.scoobiConfiguration
  def codec               = ivory.codec

  def toStore = HdfsStore(configuration, rootPath)
}

case class LocalRepository(rootPath: DirPath) extends Repository {
  def toStore = PosixStore(rootPath)
}

/**
 * Repository on S3
 * all data is going to be stored on bucket/key
 * tmpDirectory is a transient directory (on Hdfs) that is used to import data and
 * convert them to the ivory format before pushing them to S3
 */
case class S3Repository(bucket: String, rootPath: DirPath, @transient ivory: IvoryConfiguration) extends Repository {
  def toStore = S3Store(bucket, root, ivory.s3Client, ivory.s3TmpDirectory)
}

object Repository {

  /**
   * list of specific repository paths
   * They are all relative to the repository root
   */

  def errors: DirPath        = DirPath.Empty </> "errors"
  def factsets: DirPath      = DirPath.Empty </> "factsets"
  def snapshots: DirPath     = DirPath.Empty </> "snapshots"
  def metadata: DirPath      = DirPath.Empty </> "metadata"

  def dictionaries: DirPath  = metadata </> "dictionaries"
  def featureStores: DirPath = metadata </> "stores"
  def commits: DirPath       = metadata </> "repositories"

  def dictionaryById(id: DictionaryId): DirPath           = dictionaries  </> id.asFileName
  def featureStoreById(id: FeatureStoreId): DirPath       = featureStores </> id.asFileName
  def commitById(id: CommitId): DirPath                   = commits       </> id.asFileName
  def factset(id: FactsetId): DirPath                     = factsets      </> id.asFileName
  def namespace(set: FactsetId, namespace: Name): DirPath = factset(set)  </> namespace.asFileName
  def snapshot(id: SnapshotId): DirPath                   = snapshots     </> id.asFileName
  def version(set: FactsetId): DirPath                    = factset(set)  </> ".version"

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