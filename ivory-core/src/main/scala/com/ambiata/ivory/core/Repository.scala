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
  def root: Location
  def toIvoryLocation(key: Key): IvoryLocation
}

case class HdfsRepository(root: Location, @transient ivory: IvoryConfiguration) extends Repository {
  def configuration       = ivory.configuration
  def scoobiConfiguration = ivory.scoobiConfiguration
  def codec               = ivory.codec

  val store = HdfsStore(configuration, root.path)

  def toIvoryLocation(key: Key): IvoryLocation =
    IvoryLocation(HdfsLocation(root.path </> DirPath.unsafe(key.name), root.uri), ivory)
}

case class LocalRepository(root: Location) extends Repository {
  def store = PosixStore(root.path)

  def toIvoryLocation(key: Key): IvoryLocation =
    IvoryLocation(LocalLocation(root.path </> DirPath.unsafe(key.name), root.uri), IvoryConfiguration.Empty)
}

/**
 * Repository on S3
 * all data is going to be stored on bucket/key
 * tmpDirectory is a transient directory (on Hdfs) that is used to import data and
 * convert them to the ivory format before pushing them to S3
 */
case class S3Repository(root: Location, s3TmpDirectory: DirPath, @transient ivory: IvoryConfiguration) extends Repository {
  def store = S3Store(root.path.rootname.basename.name, root.path.fromRoot, ivory.s3Client, s3TmpDirectory)

  def toIvoryLocation(key: Key): IvoryLocation =
    IvoryLocation(S3Location(root.path </> DirPath.unsafe(key.name), root.uri), ivory)
}

object S3Repository {
  def apply(location: Location, ivory: IvoryConfiguration): S3Repository =
    new S3Repository(location, ivory.s3TmpDirectory, ivory)

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

  def parseUri(uri: String, repositoryConfiguration: IvoryConfiguration): String \/ Repository =
    IvoryLocation.parseUri(uri, repositoryConfiguration).map(fromIvoryLocation(_, repositoryConfiguration))

  def fromUri(uri: String, repositoryConfiguration: IvoryConfiguration): ResultTIO[Repository] =
    ResultT.fromDisjunction[IO, Repository](parseUri(uri, repositoryConfiguration).leftMap(This.apply))

  def fromLocation(location: Location, repositoryConfiguration: IvoryConfiguration): Repository = location match {
    case l: HdfsLocation  => HdfsRepository(l, repositoryConfiguration)
    case l: LocalLocation => LocalRepository(l)
    case l: S3Location    => S3Repository(l, repositoryConfiguration)
  }

  def fromIvoryLocation(location: IvoryLocation, repositoryConfiguration: IvoryConfiguration): Repository =
    fromLocation(location.location, repositoryConfiguration)
}