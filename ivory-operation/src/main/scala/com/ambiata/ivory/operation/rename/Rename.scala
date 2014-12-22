package com.ambiata.ivory.operation.rename

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.control.RepositoryT._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.legacy.IvoryStorage
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.BytesQuantity
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._, effect._

object Rename {

  def rename(mapping: RenameMapping, reducerSize: BytesQuantity): RepositoryTIO[(FactsetId, FeatureStoreId, RenameStats)] = for {
    globs        <- prepareGlobsFromLatestStore(mapping)
    lookups      <- prepareLookups(mapping, globs.map(_.value.factset), reducerSize)
    renameResult <- renameWithFactsets(mapping, globs, lookups)
    (fsid, stats) = renameResult
    sid          <- Metadata.incrementFeatureStore(List(fsid))
  } yield (fsid, sid, stats)

  def prepareLookups(mapping: RenameMapping, factsets: List[FactsetId], reducerSize: BytesQuantity): RepositoryTIO[ReducerLookups] = for {
    dictionary <- Metadata.latestDictionaryFromRepositoryT
    hdfs       <- getHdfs
    subdict     = renameDictionary(mapping, dictionary)
    namespaces  = subdict.byFeatureId.groupBy(_._1.namespace).keys.toList
    partitions <- fromResultT(_ => Namespaces.allNamespaceSizes(hdfs, namespaces, factsets).run(hdfs.configuration))
    _          <- fromResultT(_ => ResultT.fromDisjunction[IO, Unit](validate(mapping, dictionary).leftMap(\&/.This.apply)))
    // Create a subset of the dictionary with only the featureIds that we care about
    lookup      = ReducerLookups.createLookups(subdict, partitions, reducerSize)
  } yield lookup

  def prepareGlobsFromLatestStore(mapping: RenameMapping): RepositoryTIO[List[Prioritized[FactsetGlob]]] = for {
    repository <- getHdfs
    storeIdO   <- Metadata.latestFeatureStoreIdT
    storeId    <- fromResultT(_ => ResultT.fromOption[IO, FeatureStoreId](storeIdO, "Repository doesn't yet contain a store"))
    store      <- Metadata.featureStoreFromRepositoryT(storeId)
    inputs     <- fromResultT(FeatureStoreGlob.filter(_, store, p => mapping.mapping.exists(_._1.namespace == p.namespace)))
  } yield inputs.globs

  def renameWithFactsets(mapping: RenameMapping, inputs: List[Prioritized[FactsetGlob]], reducerLookups: ReducerLookups): RepositoryTIO[(FactsetId, RenameStats)] = for {
    factset    <- RepositoryT.fromRIO(repository => Factsets.allocateFactsetId(repository))
    hdfs       <- getHdfs
    output      = hdfs.toIvoryLocation(Repository.factset(factset)).toHdfsPath
    stats      <- fromResultT(_ => RenameJob.run(hdfs, mapping, inputs, output, reducerLookups, hdfs.codec).run(ScoobiConfiguration(hdfs.configuration)))
    _          <- IvoryStorage.writeFactsetVersionI(List(factset))
  } yield factset -> stats

  def getHdfs: RepositoryTIO[HdfsRepository] =
    fromResultT {
      case hr: HdfsRepository => ResultT.ok[IO, HdfsRepository](hr)
      case _                  => ResultT.fail[IO, HdfsRepository]("Must be HdfsRepository")
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
