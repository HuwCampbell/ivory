package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core.Partition.Namespace
import com.ambiata.ivory.core.{Date, FeatureId, Skew, Dictionary}
import com.ambiata.ivory.lookup.{ReducerLookup, FeatureIdLookup, NamespaceLookup}
import com.ambiata.ivory.mr.ThriftCache
import com.ambiata.mundane.io.BytesQuantity
import org.apache.hadoop.io.LongWritable

/**
 * Create lookup tables for MR jobs that will ingest data or recreate factsets
 * based on a dictionary, input paths (and their sizes) and the optimal size per reducer
 */
object ReducerLookups {
  /**
   * create lookup tables where different things are just represented by an Int:
   *
   *  - namespaces
   *  - feature ids
   *
   *  Then those ids are assigned to reducers so that each reducer has the same amount of data to process
   *
   *
   * @param dictionary
   * @param paths
   * @param optimal
   * @return
   */
  def createLookups(dictionary: Dictionary, paths: List[(Namespace, BytesQuantity)], optimal: BytesQuantity): ReducerLookups = {
    val (reducersNb, allocations) = Skew.calculate(dictionary, paths, optimal)

    val (namespaces, features) = index(dictionary)
    val reducers = new ReducerLookup
    allocations.foreach { case (n, f, r) =>
      reducers.putToReducers(features.ids.get(FeatureId(n, f).toString), r)
    }
    ReducerLookups(reducersNb, reducers, namespaces, features)
  }

  def factsetPartitionFor(lookup: NamespaceLookup, key: LongWritable): String =
    "factset" + "/" + lookup.namespaces.get((key.get >>> 32).toInt) + "/" + Date.unsafeFromInt((key.get & 0xffffffff).toInt).slashed + "/part"

  /**
   * create a dictionary index, i.e. 2 lookup tables:
   *
   * NamespaceLookup assigns an int for each namespace
   * FeatureIdLookup assigns an int for each feature id
   */
  private def index(dict: Dictionary): (NamespaceLookup, FeatureIdLookup) = {
    val namespaces = new NamespaceLookup
    val features = new FeatureIdLookup

    dict.meta.zipWithIndex.foreach { case ((fid, _), idx) =>
      namespaces.putToNamespaces(idx, fid.namespace)
      features.putToIds(fid.toString, idx)
    }
    (namespaces, features)
  }

  object Keys {
    val NamespaceLookup = ThriftCache.Key("namespace-lookup")
    val FeatureIdLookup = ThriftCache.Key("feature-id-lookup")
    val ReducerLookup   = ThriftCache.Key("reducer-lookup")
    val Dictionary      = ThriftCache.Key("dictionary")
  }
}

case class ReducerLookups(reducersNb: Int,
                          reducers: ReducerLookup,
                          namespaces: NamespaceLookup,
                          features: FeatureIdLookup)
