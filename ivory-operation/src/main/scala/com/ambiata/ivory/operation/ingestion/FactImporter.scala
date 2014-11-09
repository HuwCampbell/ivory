package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.storage.fact.Namespaces
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.joda.time.DateTimeZone
import scalaz.{Name => _, DList => _, _}, Scalaz._, effect.IO


object FactImporter {
  def importFacts(
    repository: Repository
  , namespace: Option[Name]
  , optimal: BytesQuantity
  , format: Format
  , factsetId: FactsetId
  , input: IvoryLocation
  , timezone: DateTimeZone
  ): IvoryTIO[Unit] = {
    val errorKey = Repository.errors / factsetId.asKeyName

    IvoryT.fromResultTIO { for {
      hr            <- repository.asHdfsRepository[IO]
      inputLocation <- input.asHdfsIvoryLocation[IO]
      dictionary    <- latestDictionaryFromIvory(repository)
      inputPath     =  inputLocation.toHdfsPath
      errorPath     =  hr.toIvoryLocation(errorKey).toHdfsPath
      partitions    <- namespace.fold(Namespaces.namespaceSizes(inputPath))(ns => Namespaces.namespaceSizesSingle(inputPath, ns).map(List(_))).run(hr.configuration)
      _             <- ResultT.fromDisjunction[IO, Unit](validateNamespaces(dictionary, partitions.map(_._1)).leftMap(\&/.This(_)))
      _             <- runJob(hr, namespace, optimal, dictionary, format, factsetId, inputPath, errorPath, partitions, timezone)
    } yield () }
  }

  def runJob(hr: HdfsRepository, namespace: Option[Name], optimal: BytesQuantity, dictionary: Dictionary, format: Format, factsetId: FactsetId, inputPath: Path, errorPath: Path, partitions: List[(Name, BytesQuantity)], timezone: DateTimeZone): ResultTIO[Unit] = for {
    paths      <- getAllInputPaths(namespace, inputPath, partitions.map(_._1))(hr.configuration)
    _          <- ResultT.safe[IO, Unit] {
      IngestJob.run(
        hr.configuration,
        dictionary,
        ReducerLookups.createLookups(dictionary, partitions, optimal),
        timezone,
        timezone,
        inputPath,
        namespace,
        paths,
        hr.toIvoryLocation(Repository.factset(factsetId)).toHdfsPath,
        errorPath,
        format,
        hr.codec
      )
    }
  } yield ()

  def getAllInputPaths(namespace: Option[Name], path: Path, namespaceNames: List[Name])(conf: Configuration): ResultTIO[List[Path]] =
    if (namespace.isDefined) Hdfs.globFilesRecursively(path).filterHidden.run(conf)
    else                     namespaceNames.map(ns => Hdfs.globFilesRecursively(new Path(path, ns.name)).filterHidden).sequence.map(_.flatten).run(conf)

  def validateNamespaces(dictionary: Dictionary, namespaces: List[Name]): String \/ Unit = {
    val unknown = namespaces.toSet diff dictionary.byFeatureId.keySet.map(_.namespace)
    if (unknown.isEmpty) ().right
    else                 ("Unknown namespaces: " + unknown.map(_.name).mkString(", ")).left
  }
}
