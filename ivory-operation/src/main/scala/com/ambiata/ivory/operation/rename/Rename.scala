package com.ambiata.ivory.operation.rename

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.control.RepositoryT._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.plan._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.BytesQuantity
import com.nicta.scoobi.impl.ScoobiConfiguration

import scalaz._, Scalaz._

object Rename {

  def rename(mapping: RenameMapping, reducerSize: BytesQuantity): RepositoryTIO[(FactsetId, Option[FeatureStoreId], RenameStats)] = for {
    commit       <- fromRIO(repository => CommitStorage.head(repository))
    plan         =  RenamePlan.inmemory(commit, mapping.oldFeatures)
    lookups      <- prepareLookups(commit, mapping, plan.datasets.factsets.map(_.id), reducerSize)
    renameResult <- renameWithFactsets(mapping, plan, lookups)
    (fsid, stats) = renameResult
    sid          <- Factsets.updateFeatureStore(fsid)
  } yield (fsid, sid, stats)

  def prepareLookups(commit: Commit, mapping: RenameMapping, factsets: List[FactsetId], reducerSize: BytesQuantity): RepositoryTIO[ReducerLookups] = for {
    hdfs       <- getHdfs
    subdict     = renameDictionary(mapping, commit.dictionary.value)
    namespaces  = subdict.byFeatureId.groupBy(_._1.namespace).keys.toList
    partitions <- fromRIO(_ => Namespaces.allNamespaceSizes(hdfs, namespaces, factsets).run(hdfs.configuration))
    _          <- fromRIO(_ => RIO.fromDisjunction[Unit](validate(mapping, commit.dictionary.value).leftMap(\&/.This.apply)))
    // Create a subset of the dictionary with only the featureIds that we care about
    lookup      = ReducerLookups.createLookups(subdict, partitions, reducerSize)
  } yield lookup

  def renameWithFactsets(mapping: RenameMapping, plan: RenamePlan, reducerLookups: ReducerLookups): RepositoryTIO[(FactsetId, RenameStats)] = for {
    factset    <- RepositoryT.fromRIO(repository => Factsets.allocateFactsetId(repository))
    hdfs       <- getHdfs
    output      = hdfs.toIvoryLocation(Repository.factset(factset)).toHdfsPath
    stats      <- fromRIO(_ => RenameJob.run(hdfs, mapping, plan, output, reducerLookups))
  } yield factset -> stats

  def getHdfs: RepositoryTIO[HdfsRepository] =
    fromRIO {
      case hr: HdfsRepository => RIO.ok[HdfsRepository](hr)
      case _                  => RIO.fail[HdfsRepository]("Must be HdfsRepository")
    }

  def validate(mapping: RenameMapping, dictionary: Dictionary): \/[String, Unit] = {
    val inputFeatures = mapping.mapping.map(_._1).toSet
    val missing = inputFeatures -- dictionary.byFeatureId.keySet
    if (missing.nonEmpty) s"""The following features do not exist: ${missing.mkString(",")}""".left else  ().right
  }

  /** Rename the old feature to the new mapping and remove everything else */
  def renameDictionary(mapping: RenameMapping, dictionary: Dictionary): Dictionary =
    Dictionary(dictionary.definitions.flatMap(d =>
      mapping.mapping.find(_._1 == d.featureId).map({ case (_, newFid) => d.featureId = newFid})
    ))
}

case class RenameStats(facts: Long)
