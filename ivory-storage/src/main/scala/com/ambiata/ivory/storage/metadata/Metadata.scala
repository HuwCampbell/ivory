package com.ambiata.ivory.storage.metadata

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._

object Metadata {

  /* Store */
  def storeFromIvory(repo: HdfsRepository, name: String): Hdfs[FeatureStore] =
    FeatureStoreTextStorage.storeFromHdfs(repo.storeByName(name).toHdfs)

  def storeFromIvoryS3(repository: S3Repository, name: String): HdfsS3Action[FeatureStore] =
    FeatureStoreTextStorage.storeFromS3(repository.bucket, repository.storeByName(name).path, (repository.tmp </> name).toHdfs)

  def storeToIvory(repo: HdfsRepository, store: FeatureStore, name: String): Hdfs[Unit] =
    FeatureStoreTextStorage.storeToHdfs(repo.storeByName(name).toHdfs, store)

  def storeToIvoryS3(repository: S3Repository, store: FeatureStore, name: String): HdfsS3Action[Unit] = {
    val tmpPath = new Path(repository.tmp.path, repository.storeByName(name).path)
    FeatureStoreTextStorage.storeToS3(repository.bucket, repository.storeByName(name).path, store, tmpPath)
  }

  /* Dictionary */
  def dictionaryFromIvory(repo: Repository): ResultTIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): ResultTIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).map(_ => ())
}
