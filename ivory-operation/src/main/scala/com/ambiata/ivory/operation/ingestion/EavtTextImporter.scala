package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.storage.lookup.ReducerLookups
import org.apache.hadoop.fs.Path
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy.IvoryStorage._
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
 */
object EavtTextImporter {
  def onStore(
    repository: Repository,
    dictionary: Dictionary,
    factset: FactsetId,
    namespace: List[Name],
    singleNamespace: Option[Name],
    inputRef: ReferenceIO,
    errorRef: ReferenceIO,
    timezone: DateTimeZone,
    partitions: List[(Name, BytesQuantity)],
    optimal: BytesQuantity,
    format: Format
  ): ResultTIO[Unit] = for {
    hr <- repository match {
      case hr: HdfsRepository => ResultT.ok[IO, HdfsRepository](hr)
      case _                  => ResultT.fail[IO, HdfsRepository]("Repository must be HDFS")
    }
    path      <- Reference.hdfsPath(inputRef)
    errorPath <- Reference.hdfsPath(errorRef)
    _         <- writeFactsetVersion(repository, List(factset))
    paths     <- getAllInputPaths(path, partitions.map(_._1), singleNamespace.isDefined)(hr.configuration)
    _         <- ResultT.safe[IO, Unit] {
      IngestJob.run(
        hr.configuration,
        dictionary,
        ReducerLookups.createLookups(dictionary, partitions, optimal),
        timezone,
        timezone,
        path,
        singleNamespace,
        paths,
        repository.factset(factset).toHdfs,
        errorPath,
        format,
        hr.codec
      )
    }
  } yield ()


  private def getAllInputPaths(path: Path, namespaceNames: List[Name], isSingleNamespace: Boolean)(conf: Configuration): ResultTIO[List[Path]] =
    if (isSingleNamespace) Hdfs.globFilesRecursively(path).filterHidden.run(conf)
    else                   namespaceNames.map(ns => Hdfs.globFilesRecursively(new Path(path, ns.name)).filterHidden).sequence.map(_.flatten).run(conf)

}
