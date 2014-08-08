package com.ambiata.ivory
package ingest

import com.ambiata.ivory.lookup.{ReducerLookup, NamespaceLookup, FeatureIdLookup}
import com.ambiata.ivory.storage.lookup.ReducerLookups
import org.apache.hadoop.fs.Path
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy.IvoryStorage
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import scalaz.{DList => _, _}, Scalaz._, effect.IO
import com.ambiata.poacher._
import hdfs._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{BytesQuantity, FilePath}
import org.joda.time.DateTimeZone
import org.apache.hadoop.io.compress._
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
    namespace: List[String],
    singleNamespace: Option[String],
    inputRef: ReferenceIO,
    errorRef: ReferenceIO,
    timezone: DateTimeZone,
    partitions: List[(String, BytesQuantity)],
    optimal: BytesQuantity,
    format: Format,
    codec: Option[CompressionCodec]
  ): ResultTIO[Unit] = for {
    c <- repository match {
      case HdfsRepository(_, conf, _) => ResultT.ok[IO, Configuration](conf)
      case _                          => ResultT.fail[IO, Configuration]("Repository must be HDFS")
    }
    path      <- Reference.hdfsPath(inputRef)
    errorPath <- Reference.hdfsPath(errorRef)
    _         <- writeFactsetVersion(repository, List(factset))
    paths     <- getAllInputPaths(path, partitions.map(_._1), singleNamespace.isDefined)(c)
    _         <- ResultT.safe[IO, Unit] {
      IngestJob.run(
        c,
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
        codec
      )
    }
  } yield ()


  private def getAllInputPaths(path: Path, namespaceNames: List[String], isSingleNamespace: Boolean)(conf: Configuration): ResultTIO[List[Path]] =
    if (isSingleNamespace) Hdfs.globFilesRecursively(path).filterHidden.run(conf)
    else                   namespaceNames.map(ns => Hdfs.globFilesRecursively(new Path(path, ns)).filterHidden).sequence.map(_.flatten).run(conf)

}
