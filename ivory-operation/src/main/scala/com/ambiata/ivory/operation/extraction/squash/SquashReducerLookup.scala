package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core.{DictionaryConcrete, Date}
import com.ambiata.ivory.lookup.{FeatureIdLookup, ReducerLookup}
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWindows
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.poacher.mr.MrContext
import org.apache.hadoop.conf.{Configuration, Configurable}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.Partitioner
import org.joda.time.Days
import scala.collection.JavaConverters._

object SquashReducerLookup {

  /**
   * Currently we don't have any size information for namespaces, so the best we can do is look at the window "size"
   * and assume it's proportional to the data size (which isn't necessarily true).
   *
   * For features without windows they continue to be partitioned evenly.
   * For features with windows the number of (total) reducers is divided again by the proportion of window size.
   *
   * The implications on the following is such:
   * - Features without windows will _always_ be `1`
   * - Features with windows will add up to the total number (regardless of the non-window features)
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
  def create(dictionary: DictionaryConcrete, lookup: FeatureIdLookup, reducers: Int): ReducerLookup = {
    // The actual date doesn't matter - we're just using it to calculate the rough size of the window
    val date = Date.maxValue
    val windowSizes = dictionary.sources.flatMap { case (fid, cg) =>
      cg.virtual.flatMap(_._2.window).map(SnapshotWindows.startingDate(_, date)).sorted.headOption
        .map(d => Days.daysBetween(d.localDate, date.localDate).getDays).map(fid ->)
    }
    val totalDays = windowSizes.values.sum
    // Create a sub-index for all of the window features so they don't overlap on the reducers
    new ReducerLookup(windowSizes.zipWithIndex.map {
      case ((fid, days), i) =>
        lookup.ids.get(fid.toString) -> Int.box(toLookup(i.toShort, Math.max(1, days / totalDays * reducers).toShort))
    }.asJava)
  }

  /**
   * Store the feature index (different from the featureId) and number of reducers.
   * The reason for a single [[Int]] is largely to avoid having to generate yet-another bespoke thrift lookup class.
   * We're assuming that the number of features or reducers is < [[Short.MaxValue]].
   */
  def toLookup(offset: Short, count: Short): Int =
    (offset.toShort << 16) | count

  /** Calculate the reducer for a given entity based on the encoded [[Int]] (see [[toLookup()]]) */
  def getReducer(lookup: Int, entity: Int): Int = {
    val offset = lookup >> 16
    val count = lookup & 0xffff
    (entity % count) + offset
  }
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
      val entity = SquashWritable.GroupingByFeatureId.hashEntity(k) & Int.MaxValue
      SquashReducerLookup.getReducer(value, entity) % partitions
    }
  }
}
