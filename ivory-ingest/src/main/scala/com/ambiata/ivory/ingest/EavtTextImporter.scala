package com.ambiata.ivory
package ingest

import com.ambiata.ivory.lookup.{ReducerLookup, NamespaceLookup, FeatureIdLookup}
import com.ambiata.ivory.storage.lookup.ReducerLookups
import org.apache.hadoop.fs.Path
import com.nicta.scoobi.Scoobi._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy.IvoryStorage
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.repository._
import scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect.IO
import alien.hdfs._
import ScoobiAction._
import WireFormats._, FactFormats._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{BytesQuantity, FilePath}
import com.ambiata.saws.emr._
import org.joda.time.DateTimeZone
import org.apache.hadoop.io.compress._
import org.apache.hadoop.conf.Configuration

// FIX move to com.ambiata.ivory.ingest.internal


/**
 * Import a text file, formatted as an EAVT file, into ivory.
 */
object EavtTextImporter {
  def onHdfs(
    repository: HdfsRepository,
    dictionary: Dictionary,
    factset: Factset,
    namespace: List[String],
    path: Path,
    errorPath: Path,
    timezone: DateTimeZone,
    partitions: List[(String, BytesQuantity)],
    optimal: BytesQuantity,
    codec: Option[CompressionCodec]
  ): ScoobiAction[Unit] = for {
    sc <- ScoobiAction.scoobiConfiguration
    _  <- ScoobiAction.fromHdfs(writeFactsetVersion(repository, List(factset)))
    _  <- ScoobiAction.safe {
      IngestJob.run(
        sc,
        dictionary,
        ReducerLookups.createLookups(dictionary, partitions, optimal),
        timezone,
        timezone,
        path,
        partitions.map(_._1).map(namespace => path.toString + "/" + namespace + "/*"),
        repository.factset(factset).toHdfs,
        errorPath,
        codec
      )
    }
  } yield ()

}
