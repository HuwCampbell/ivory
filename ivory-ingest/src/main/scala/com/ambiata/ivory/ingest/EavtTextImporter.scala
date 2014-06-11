package com.ambiata.ivory
package ingest

import com.ambiata.ivory.lookup.{ReducerLookup, NamespaceLookup, FeatureIdLookup}
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
    (reducers, allocations) = Skew.calculate(dictionary, partitions, optimal)
    (namespaces, features) = index(dictionary)
    indexed = allocateReducers(allocations, features)
    _  <- ScoobiAction.safe {
      IngestJob.run(
        sc,
        reducers,
        indexed,
        namespaces,
        features,
        dictionary,
        timezone,
        timezone,
        path,
        partitions.map(_._1).map(namespace => path.toString + "/" + namespace + "/*"),
        repository.factset(factset).toHdfs,
        errorPath,
        codec
      )
    }
    _  <- ScoobiAction.fromHdfs(writeFactsetVersion(repository, List(factset)))
  } yield ()

  def allocateReducers(allocations: List[(String, String, Int)], features: FeatureIdLookup): ReducerLookup = {
    val indexed = new ReducerLookup
    allocations.foreach({ case (n, f, r) =>
      indexed.putToReducers(features.ids.get(FeatureId(n, f).toString), r)
    })
    indexed
  }

  def index(dict: Dictionary): (NamespaceLookup, FeatureIdLookup) = {
    val namespaces = new NamespaceLookup
    val features = new FeatureIdLookup
    dict.meta.toList.zipWithIndex.foreach({ case ((fid, _), idx) =>
      namespaces.putToNamespaces(idx, fid.namespace)
      features.putToIds(fid.toString, idx)
    })
    (namespaces, features)
  }
}
