package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact.Namespaces
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.metadata.{Metadata, FeatureIdMappingsStorage}
import com.ambiata.ivory.storage.sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core.KeyName
import com.ambiata.poacher.hdfs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.joda.time.DateTimeZone

import scalaz.{DList => _, _}, Scalaz._, effect.IO

object FactImporter {
  def importFacts(
    repository: Repository
  , cluster: Cluster
  , optimal: BytesQuantity
  , factsetId: FactsetId
  , inputs: List[(FileFormat, Option[Namespace], IvoryLocation)]
  , timezone: Option[DateTimeZone]
  ): IvoryTIO[Unit] = {

    IvoryT.read[RIO] >>= (read => IvoryT.fromRIO { for {
      hr            <- repository.asHdfsRepository
      dictionary    <- Metadata.latestDictionaryFromIvory(repository)
      mappings      <- FeatureIdMappingsStorage.fromDictionaryAndSave(repository, Repository.factset(factsetId), dictionary)
      config        <- Metadata.configuration.toIvoryT(repository).run(read)
      paths         <- inputs.traverseU { case (f, ns, input) =>
        SyncIngest.inputDataset(InputDataset(input.location), cluster).map(sid => (f, ns, new Path(sid.location.path)))
      }
      _             <- runJob(hr, optimal, dictionary, factsetId, paths, timezone, config)
    } yield () })
  }

  def runJob(hr: HdfsRepository, optimal: BytesQuantity, dictionary: Dictionary, factsetId: FactsetId,
             inputs: List[(FileFormat, Option[Namespace], Path)], timezone: Option[DateTimeZone],
             config: RepositoryConfig): RIO[Unit] = for {
    paths      <- inputs.traverseU((prepareInput _).tupled).run(hr.configuration)
    partitions  = Namespaces.sum(paths.map(_._5).flatten)
    _          <- Hdfs.fromDisjunction(validateNamespaces(dictionary, partitions.keys.toList)).run(hr.configuration)
    _          <-
      IngestJob.run(
        hr.configuration,
        dictionary,
        ReducerLookups.createLookups(dictionary, partitions.toList, optimal),
        config.timezone,
        timezone,
        paths.map { x => (x._1, x._2, x._3, x._4) },
        hr.toIvoryLocation(Repository.factset(factsetId)).toHdfsPath,
        hr.toIvoryLocation(Repository.errors / factsetId.asKeyName).toHdfsPath,
        hr.codec
      )
  } yield ()

  def prepareInput(format: FileFormat, namespace: Option[Namespace], inputPath: Path): Hdfs[(FileFormat, Option[Namespace], Path, List[Path], List[(Namespace, BytesQuantity)])] = for {
    partitions    <- namespace.fold(Namespaces.namespaceSizes(inputPath))(ns => Namespaces.namespaceSizesSingle(inputPath, ns).map(List(_)))
    paths         <- getAllInputPaths(namespace, inputPath, partitions.map(_._1))
  } yield (format, namespace, inputPath, paths, partitions)

  def getAllInputPaths(namespace: Option[Namespace], path: Path, namespaceNames: List[Namespace]): Hdfs[List[Path]] =
    if (namespace.isDefined) Hdfs.globFilesRecursively(path).filterHidden
    else                     namespaceNames.map(ns => Hdfs.globFilesRecursively(new Path(path, ns.name)).filterHidden).sequence.map(_.flatten)

  def validateNamespaces(dictionary: Dictionary, namespaces: List[Namespace]): String \/ Unit = {
    val unknown = namespaces.toSet diff dictionary.byFeatureId.keySet.map(_.namespace)
    if (unknown.isEmpty) ().right
    else                 ("Unknown namespaces: " + unknown.map(_.name).mkString(", ")).left
  }
}
