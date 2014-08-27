package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.storage.fact.Namespaces
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.metadata.Metadata._
import org.apache.hadoop.fs.Path
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import scalaz.{Name => _, DList => _, _}, Scalaz._, effect.IO
import com.ambiata.poacher._
import hdfs._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.BytesQuantity
import org.joda.time.DateTimeZone
import org.apache.hadoop.conf.Configuration

// FIX move to com.ambiata.ivory.ingest.internal

/**
 * Import a text file, formatted as an EAVT file, into ivory.
 *
 * There is first a preprocessing of the namespaces to import and their size on disk.
 * This data + the "optimal" size is passed to the IngestJob to optimise the import
 */
case class EavtTextImporter(repository: Repository,
                            input: ReferenceIO,
                            namespace: Option[Name],
                            optimal: BytesQuantity,
                            format: Format) {

  val  importFacts = { (factsetId: FactsetId, input: ReferenceIO, namespace: Option[Name], timezone: DateTimeZone) =>
    val errorRef = repository.toReference(repository.errors </> factsetId.render)

    for {
      hr         <- downcast[Repository, HdfsRepository](repository, "Repository must be HDFS")
      dictionary <- dictionaryFromIvory(repository)
      inputPath  <- Reference.hdfsPath(input)
      errorPath  <- Reference.hdfsPath(errorRef)
      partitions <- namespace.fold(Namespaces.namespaceSizes(inputPath))(ns => Namespaces.namespaceSizesSingle(inputPath, ns).map(List(_))).run(hr.configuration)
      _          <- runJob(hr, dictionary, factsetId, inputPath, errorPath, partitions, timezone)
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
        repository.factset(factsetId).toHdfs,
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
