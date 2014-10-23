package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core.{Name, Date, FeatureId, Skew, Dictionary, Definition}
import com.ambiata.ivory.lookup.{ReducerLookup, FeatureIdLookup, NamespaceLookup}
import com.ambiata.mundane.io.BytesQuantity
import com.ambiata.poacher.mr.ThriftCache
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
  def createLookups(dictionary: Dictionary, paths: List[(Name, BytesQuantity)], optimal: BytesQuantity): ReducerLookups = {
    val (reducersNb, allocations) = Skew.calculate(dictionary, paths, optimal)

    val (namespaces, features) = index(dictionary)
    val reducers = new ReducerLookup
    allocations.foreach { case (n, f, r) =>
      reducers.putToReducers(features.ids.get(FeatureId(n, f).toString), r)
    }
    ReducerLookups(reducersNb, reducers, namespaces, features)
  }

  def factsetPartitionFor(lookup: NamespaceLookup, key: LongWritable): String =
    factsetPartitionForInt(lookup, (key.get >>> 32).toInt, Date.unsafeFromInt((key.get & 0xffffffff).toInt))

  def factsetPartitionForInt(lookup: NamespaceLookup, featureId: Int, date: Date): String =
    "factset" + "/" + lookup.namespaces.get(featureId) + "/" + date.slashed + "/part"

  /**
   * create a dictionary index, i.e. 2 lookup tables:
   *
   * NamespaceLookup assigns an int for each namespace
   * FeatureIdLookup assigns an int for each feature id
   */
  def index(dictionary: Dictionary): (NamespaceLookup, FeatureIdLookup) =
    indexDefinitions(dictionary.definitions)

  def indexDefinitions(definitions: List[Definition]): (NamespaceLookup, FeatureIdLookup) = {
    val namespaces = new NamespaceLookup
    val features = new FeatureIdLookup

    definitions.zipWithIndex.foreach { case (d, idx) =>
      namespaces.putToNamespaces(idx, d.featureId.namespace.name)
      features.putToIds(d.featureId.toString, idx)
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
