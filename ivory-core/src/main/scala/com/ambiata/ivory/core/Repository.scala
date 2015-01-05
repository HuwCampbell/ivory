package com.ambiata.ivory.core

import com.ambiata.saws.s3.S3Prefix

import scalaz.{Name=>_,Store => _, _}
import com.ambiata.mundane.control.{RIO, ResultT}
import com.ambiata.mundane.io._
import com.ambiata.notion.core._

import scalaz.\&/.This
import scalaz.effect.IO
import scalaz.{Store => _, _}, Scalaz._

/**
 * Ivory repository
 *
 * It defines the layout of both metadata (dictionary, feature store) and data (factsets, snapshots)
 *
 * The repository encapsulate a Store[RIO] in order to be able to build references (ReferenceIO)
 * to the data
 */
sealed trait Repository {
  def store: Store[RIO]
  def root: IvoryLocation
  def toIvoryLocation(key: Key): IvoryLocation

  /** This is far from ok, but is acting as a magnet for broken code that depends on this
      nonsense casting. This will be removed with s3 changes. */
  def asHdfsRepository: RIO[HdfsRepository] =
    this match {
      case h @ HdfsRepository(_) =>
        h.pure[RIO]
      case _ =>
        RIO.fail[HdfsRepository]("This ivory operation currently only supports hdfs repositories.")
    }
}

case class HdfsRepository(root: HdfsIvoryLocation) extends Repository {
  def configuration       = root.configuration
  def scoobiConfiguration = root.scoobiConfiguration
  def codec               = root.codec

  val store = HdfsStore(configuration, root.location.dirPath)

  def toIvoryLocation(key: Key): HdfsIvoryLocation =
    root </> DirPath.unsafe(key.name)
}

object HdfsRepository {
  def create(dir: DirPath, ivory: IvoryConfiguration): HdfsRepository =
    apply(HdfsLocation(dir.path), ivory)

  def apply(location: HdfsLocation, ivory: IvoryConfiguration): HdfsRepository =
    new HdfsRepository(HdfsIvoryLocation(location, ivory.configuration, ivory.scoobiConfiguration, ivory.codec))

  def fromUri(uri: String, ivory: IvoryConfiguration): RIO[HdfsRepository] =
    Repository.fromUri(uri, ivory).flatMap {
      case h: HdfsRepository => RIO.ok[HdfsRepository](h)
      case r                 => RIO.fail[HdfsRepository](s"${r.root.show} is not an HDFS repository")
    }

}

case class LocalRepository(root: LocalIvoryLocation) extends Repository {
  def store = PosixStore(root.dirPath)

  def toIvoryLocation(key: Key): LocalIvoryLocation =
    root </> DirPath.unsafe(key.name)
}

object LocalRepository {
  def create(dir: DirPath): LocalRepository =
    apply(LocalLocation(dir.path))

  def apply(location: LocalLocation): LocalRepository =
    new LocalRepository(LocalIvoryLocation(location))
}

/**
 * Repository on S3
 * all data is going to be stored on bucket/key
 * tmpDirectory is a transient directory (on Hdfs) that is used to import data and
 * convert them to the ivory format before pushing them to S3
 */
case class S3Repository(root: S3IvoryLocation) extends Repository {
  def store = S3Store(S3Prefix(root.location.bucket, root.location.key), root.s3Client)

  def toIvoryLocation(key: Key): S3IvoryLocation =
    root </> DirPath.unsafe(key.name)
}

object S3Repository {
  def apply(location: S3Location, ivory: IvoryConfiguration): S3Repository =
    new S3Repository(S3IvoryLocation(location, ivory.s3Client))
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

  def configs: Key       = metadata / "configs"
  def dictionaries: Key  = metadata / "dictionaries"
  def featureStores: Key = metadata / "stores"
  def commits: Key       = metadata / "commits"

  def config(id: RepositoryConfigId): Key             = configs       / id.asKeyName
  def dictionaryById(id: DictionaryId): Key           = dictionaries  / id.asKeyName
  def featureStoreById(id: FeatureStoreId): Key       = featureStores / id.asKeyName
  def commitById(id: CommitId): Key                   = commits       / id.asKeyName
  def factset(id: FactsetId): Key                     = factsets      / id.asKeyName
  def namespace(set: FactsetId, namespace: Namespace): Key = factset(set)  / namespace.asKeyName
  def snapshot(id: SnapshotId): Key                   = snapshots     / id.asKeyName
  def version(set: FactsetId): Key                    = factset(set)  / ".version"
  def tmp(task: KeyName, context: KeyName): Key       = root          / task / context

  def parseUri(uri: String, repositoryConfiguration: IvoryConfiguration): String \/ Repository =
    IvoryLocation.parseUri(uri, repositoryConfiguration).map(fromIvoryLocation(_, repositoryConfiguration))

  def fromUri(uri: String, repositoryConfiguration: IvoryConfiguration): RIO[Repository] =
    RIO.fromDisjunction[Repository](parseUri(uri, repositoryConfiguration).leftMap(This.apply))

  def fromIvoryLocation(location: IvoryLocation, repositoryConfiguration: IvoryConfiguration): Repository = location match {
    case l: HdfsIvoryLocation  => HdfsRepository(l)
    case l: LocalIvoryLocation => LocalRepository(l)
    case l: S3IvoryLocation    => S3Repository(l)
  }

  /** Creates a unique [[Key]] that can be used as a temporary directory (but doesn't actually create it) */
  def tmpDir(task: KeyName): RIO[Key] =
    RIO.safe(Repository.tmp(task, KeyName.fromUUID(java.util.UUID.randomUUID)))

  def tmpLocation(repository: Repository, task: KeyName): RIO[IvoryLocation] =
    tmpDir(task).map(repository.toIvoryLocation)
}
