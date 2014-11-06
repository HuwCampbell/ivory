package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup._
import scala.collection.JavaConverters._

object FeatureLookups {
  def isSetTable(dictionary: Dictionary): FlagLookup = {
    val isSet = new FlagLookup
    dictionary.byFeatureIndex.foreach({
      case (n, Concrete(id, definition)) =>
        isSet.putToFlags(n, definition.mode.isSet)
      case (n, Virtual(id, definition)) =>
        ()
    })
    isSet
  }

  def featureIdTable(dictionary: Dictionary): FeatureIdLookup = {
    val features = new FeatureIdLookup()
    dictionary.byFeatureIndex.foreach({
      case (n, definition) =>
        features.putToIds(definition.featureId.toString, n)
    })
    features
  }

  def isSetLookupToArray(lookup: FlagLookup): Array[Boolean] =
    sparseMapToArray(lookup.getFlags.asScala.toList.map { case (i, b) => i.toInt -> b.booleanValue()}, false)

  def maxConcreteWindows(dictionary: Dictionary): Map[Int, Option[Window]] = {
    // The date doesn't matter - just used to calculate the largest window
    val date = Date.maxValue
    val windows = dictionary.byConcrete.sources.flatMap {
      case (fid, wins) => wins.virtual.flatMap(_._2.window).sortBy(Window.startingDate(_)(date)).headOption.map(fid ->)
    }.toMap
    dictionary.byFeatureIndex.map {
      case (i, d) => i -> d.fold((fid, _) => windows.get(fid), (_, _) => None)
    }
  }

  def windowTable(dictionary: Dictionary): SnapshotWindowLookup = {
    val lookup = new SnapshotWindowLookup()
    maxConcreteWindows(dictionary).foreach {
      case (i, w) => lookup.putToWindow(i, WindowLookup.toInt(w))
    }
    lookup
  }

  def entityFilter(features: List[FeatureId], entities: List[String]): EntityFilterLookup =
    new EntityFilterLookup(features.map(_.toString).asJava, entities.asJava)

  def sparseMapToArray[A : scala.reflect.ClassTag](map: List[(Int, A)], default: A): Array[A] = {
    val max = map.map(_._1).max
    val array = Array.fill(max + 1)(default)
    map.foreach {
      case (i, a) => array(i) = a
    }
    array
  }
}
