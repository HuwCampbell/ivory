package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.ReducerLookup
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.plan.ChordPlan
import com.ambiata.poacher.mr.MrContext
import org.apache.hadoop.conf.{Configuration, Configurable}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.Partitioner
import org.joda.time.Days
import scala.collection.JavaConverters._
import scalaz._, Scalaz._

object SquashReducerLookup {

  def createFromChord(chord: ChordPlan, reducers: Int): ReducerLookup =
    // This is not optimal at the moment and falls back to the dictionary-only implementation
    // https://github.com/ambiata/ivory/issues/610
    createLookup(chord.commit.dictionary.value, lookupByWindowOnly(chord.commit.dictionary.value, reducers))

  def createFromSnapshot(snapshot: Snapshot, dictionary: Dictionary, reducers: Int): ReducerLookup =
    createLookup(dictionary, snapshot.bytes.fold(
      _ => lookupByWindowOnly(dictionary, reducers),
      s => lookupByNamespaceSizeAndWindow(dictionary, s, reducers)
    ))

  /**
   * To partition features across reducers we use the window "size" and assume it's proportional to the data size
   * (which isn't necessarily true).
   *
   * The number of (total) reducers is divided again by the proportion of window size.
   *
   * {{{
   *   Total no. of reducers = 5
   *   Feature -> No. of reducers
   *   fid0: no window -> 1
   *   fid1: no window -> 1
   *   fid2: no window -> 1
   *   ...
   *   --------------------
   *   fidX: 4 months  -> 2
   *   fidY: 8 months  -> 3
   * }}}
   *
   * This could lead to the following case, where a single windowed feature is divided/shared across all of the reducers.
   *
   * {{{
   *   Total no. of reducers = 5
   *   Feature -> No. of reducers
   *   fid0: no window -> 1
   *   fid1: no window -> 1
   *   fid2: no window -> 1
   *   ....
   *   --------------------
   *   fidX: 1 month   -> 5
   * }}}
   */
  def lookupByWindowOnly(dictionary: Dictionary, reducers: Int): Map[FeatureId, Int] = {
    val windowSizes = calcWindowSizes(dictionary)
    val totalDays = windowSizes.concatenate
    calculateReducers(windowSizes, _ => totalDays, _ => reducers)
  }

  /**
   * Improve the size calculation by partitioning the reducers by namespaces by their input data size first.
   *
   * {{{
   *   Total no. of reducers = 10
   *   ns1: 10gb
   *   ns2: 40gb
   *
   *   Feature -> No. of reducers
   *   ns1:fid0: 3 months -> 2
   *   ns1:fid1: 1 months -> 1
   *   ns2:fid2: 3 months -> 6
   *   ns2:fid3: 1 months -> 2
   *
   *   NOTE: We round-up so in this example ns1 features actually gets 1 more reducer than they are allocated.
   * }}}
   */
  def lookupByNamespaceSizeAndWindow(dictionary: Dictionary, sizes: List[Sized[Namespace]], reducers: Int): Map[FeatureId, Int] = {
    val windowSizes = calcWindowSizes(dictionary)
    val namespaceTotals = calculateNamespaceTotals(windowSizes)
    val reducersPerNamespace = proportionReducersPerNamespace(sizes, reducers)
    calculateReducers(windowSizes,
      f => namespaceTotals.getOrElse(f.namespace, 1),
      ns => reducersPerNamespace.getOrElse(ns, 1)
    )
  }

  def calculateReducers(sizes: Map[FeatureId, Int], totalDays: FeatureId => Int, reducers: Namespace => Int): Map[FeatureId, Int] =
    sizes.map {
      case (fid, days) =>
        // The number of reducers for this feature, which is proportional of the number of total days
        fid -> math.max(1, math.ceil(days.toDouble / totalDays(fid) * reducers(fid.namespace)).toInt)
    }.toMap

  def createLookup(dictionary: Dictionary, reducers: Map[FeatureId, Int]): ReducerLookup =
    // Create a sub-index for all of the features so they don't overlap on the reducers
    new ReducerLookup(reducers.zipWithIndex.map {
      case ((fid, count), i) =>
        val id = dictionary.byFeatureIndexReverse.getOrElse(fid, 0)
        Int.box(id) -> Int.box(FeatureReducerOffset(i.toShort, count.toShort).toInt)
    }.toMap.asJava)

  def calcWindowSizes(dictionary: Dictionary): Map[FeatureId, Int] = {
    // The actual date doesn't matter - we're just using it to calculate the rough size of the window
    val date = Date.maxValue
    dictionary.byConcrete.sources.mapValues { cg =>
      cg.virtual.flatMap(_._2.window).map(Window.startingDate(_, date)).sorted.headOption
        .map(d => Days.daysBetween(d.localDate, date.localDate).getDays).getOrElse(1)
    }
  }

  def proportionReducersPerNamespace(partitions: List[Sized[Namespace]], reducers: Int): Map[Namespace, Int] = {
    val totalSize = partitions.foldMap(_.bytes)
    partitions.map {
      // Always round-up - we want to make the most of those reducers!
      sn => sn.value -> math.ceil(sn.bytes.toLong * reducers / totalSize.toLong.toDouble).toInt
    }.toMap
  }

  def calculateNamespaceTotals(windowSizes: Map[FeatureId, Int]): Map[Namespace, Int] =
    windowSizes.groupBy(_._1.namespace).mapValues(_.values.sum)
}

class SquashPartitioner extends Partitioner[BytesWritable, BytesWritable] with Configurable {
  var _conf: Configuration = null
  var ctx: MrContext = null
  val lookup = new ReducerLookup

  def setConf(conf: Configuration): Unit = {
    _conf = conf
    ctx = MrContext.fromConfiguration(_conf)
    ctx.thriftCache.pop(conf, ReducerLookups.Keys.ReducerLookup, lookup)
  }

  def getConf: Configuration =
    _conf

  def getPartition(k: BytesWritable, v: BytesWritable, partitions: Int): Int =
    SquashPartitioner.getPartition(lookup, k, partitions)
}

object SquashPartitioner {
  def getPartition(lookup: ReducerLookup, k: BytesWritable, partitions: Int): Int = {
    val featureId = SquashWritable.GroupingByFeatureId.getFeatureId(k)
    val value = lookup.reducers.get(featureId)
    if (value == null) {
      // If we don't have a window do the normal thing and send the feature to one reducer
      featureId % partitions
    } else {
      // Otherwise hash the entity, pick a bucket in the number of reducers for this feature
      val entity = SquashWritable.GroupingByFeatureId.hashEntity(k)
      FeatureReducerOffset.getReducer(value, entity) % partitions
    }
  }
}
