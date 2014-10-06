package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.storage.fact.Namespaces
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.joda.time.DateTimeZone
import EavtTextImporter._
import scalaz.{Name => _, DList => _, _}, Scalaz._, effect.IO
import IvorySyntax._

/**
 * Import a text file, formatted as an EAVT file, into ivory.
 *
 * There is first a preprocessing of the namespaces to import and their size on disk.
 * This data + the "optimal" size is passed to the IngestJob to optimise the import
 */
case class EavtTextImporter(repository: Repository,
                            namespace: Option[Name],
                            optimal: BytesQuantity,
                            format: Format) {

  val  importFacts = { (factsetId: FactsetId, input: IvoryLocation, timezone: DateTimeZone) =>
    val errorKey = Repository.errors / factsetId.asKeyName

    for {
      hr            <- downcast[Repository, HdfsRepository](repository, "Repository must be HDFS")
      inputLocation <- downcast[IvoryLocation, HdfsIvoryLocation](input, "The input must be HDFS")
      dictionary    <- latestDictionaryFromIvory(repository)
      inputPath     =  inputLocation.toHdfsPath
      errorPath     =  hr.toIvoryLocation(errorKey).toHdfsPath
      partitions    <- namespace.fold(Namespaces.namespaceSizes(inputPath))(ns => Namespaces.namespaceSizesSingle(inputPath, ns).map(List(_))).run(hr.configuration)
      _             <- ResultT.fromDisjunction[IO, Unit](validateNamespaces(dictionary, partitions.map(_._1)).leftMap(\&/.This(_)))
      _             <- runJob(hr, dictionary, factsetId, inputPath, errorPath, partitions, timezone)
    } yield ()
  }

  def runJob(hr: HdfsRepository, dictionary: Dictionary, factsetId: FactsetId, inputPath: Path, errorPath: Path, partitions: List[(Name, BytesQuantity)], timezone: DateTimeZone) = for {
    paths      <- getAllInputPaths(inputPath, partitions.map(_._1))(hr.configuration)
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

  private def getAllInputPaths(path: Path, namespaceNames: List[Name])(conf: Configuration): ResultTIO[List[Path]] =
    if (namespace.isDefined) Hdfs.globFilesRecursively(path).filterHidden.run(conf)
    else                     namespaceNames.map(ns => Hdfs.globFilesRecursively(new Path(path, ns.name)).filterHidden).sequence.map(_.flatten).run(conf)

}

object EavtTextImporter {

  def validateNamespaces(dictionary: Dictionary, namespaces: List[Name]): String \/ Unit = {
    val unknown = namespaces.toSet diff dictionary.byFeatureId.keySet.map(_.namespace)
    if (unknown.isEmpty) ().right
    else                 ("Unknown namespaces: " + unknown.map(_.name).mkString(", ")).left
  }
}
