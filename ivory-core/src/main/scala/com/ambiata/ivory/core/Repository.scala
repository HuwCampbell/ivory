package com.ambiata.ivory.core

import scalaz.{Name=>_,Store => _, _}
import com.ambiata.mundane.control.{ResultTIO, ResultT}
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.saws.s3._

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
  def root: IvoryLocation
  def toIvoryLocation(key: Key): IvoryLocation
}

case class HdfsRepository(root: HdfsIvoryLocation) extends Repository {
  def configuration       = root.configuration
  def scoobiConfiguration = root.scoobiConfiguration
  def codec               = root.codec

  val store = HdfsStore(configuration, root.location.path)

  def toIvoryLocation(key: Key): HdfsIvoryLocation =
    root </> DirPath.unsafe(key.name)
}

object HdfsRepository {
  def apply(location: HdfsLocation, ivory: IvoryConfiguration): HdfsRepository =
    new HdfsRepository(HdfsIvoryLocation(location, ivory.configuration, ivory.scoobiConfiguration, ivory.codec))

  def fromUri(uri: String, ivory: IvoryConfiguration): ResultTIO[HdfsRepository] =
    Repository.fromUri(uri, ivory).flatMap {
      case h: HdfsRepository => ResultT.ok[IO, HdfsRepository](h)
      case r                 => ResultT.fail[IO, HdfsRepository](s"${r.root.show} is not an HDFS repository")
    }

}

case class LocalRepository(root: LocalIvoryLocation) extends Repository {
  def store = PosixStore(root.location.path)

  def toIvoryLocation(key: Key): LocalIvoryLocation =
    root </> DirPath.unsafe(key.name)
}

object LocalRepository {
  def apply(location: LocalLocation): LocalRepository =
    new LocalRepository(LocalIvoryLocation(location))
}

/**
 * Repository on S3
 * all data is going to be stored on bucket/key
 * tmpDirectory is a transient directory (on Hdfs) that is used to import data and
 * convert them to the ivory format before pushing them to S3
 */
case class S3Repository(root: S3IvoryLocation, s3TmpDirectory: DirPath) extends Repository {
  def store = S3Store(root.location.path.rootname.basename.name, root.location.path.fromRoot, root.s3Client, s3TmpDirectory)

  def toIvoryLocation(key: Key): S3IvoryLocation =
    root </> DirPath.unsafe(key.name)
}

object S3Repository {
  def apply(location: S3Location, ivory: IvoryConfiguration): S3Repository =
    new S3Repository(S3IvoryLocation(location, ivory.s3Client), ivory.s3TmpDirectory)
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
  def commits: Key       = metadata / "commits"

  def dictionaryById(id: DictionaryId): Key           = dictionaries  / id.asKeyName
  def featureStoreById(id: FeatureStoreId): Key       = featureStores / id.asKeyName
  def commitById(id: CommitId): Key                   = commits       / id.asKeyName
  def factset(id: FactsetId): Key                     = factsets      / id.asKeyName
  def namespace(set: FactsetId, namespace: Name): Key = factset(set)  / namespace.asKeyName
  def snapshot(id: SnapshotId): Key                   = snapshots     / id.asKeyName
  def version(set: FactsetId): Key                    = factset(set)  / ".version"

  def parseUri(uri: String, repositoryConfiguration: IvoryConfiguration): String \/ Repository =
    IvoryLocation.parseUri(uri, repositoryConfiguration).map(fromIvoryLocation(_, repositoryConfiguration))

  def fromUri(uri: String, repositoryConfiguration: IvoryConfiguration): ResultTIO[Repository] =
    ResultT.fromDisjunction[IO, Repository](parseUri(uri, repositoryConfiguration).leftMap(This.apply))

  def fromIvoryLocation(location: IvoryLocation, repositoryConfiguration: IvoryConfiguration): Repository = location match {
    case l: HdfsIvoryLocation  => HdfsRepository(l)
    case l: LocalIvoryLocation => LocalRepository(l)
    case l: S3IvoryLocation    => S3Repository(l, repositoryConfiguration.s3TmpDirectory)
  }

}