package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._
import com.ambiata.poacher.hdfs._
import FactFormats._

object DenseRowTextStorageV1 {

  type Entity = String
  type Attribute = String
  type StringValue = String

  case class DenseRowTextStorer(path: String, dict: Dictionary, delim: Char = '|', tombstone: String = "NA") {
    lazy val features = indexDictionary(dict, tombstone)
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[String] = {
      val byKey: DList[((Entity, Attribute), Iterable[Fact])] =
        dlist.by(f => (f.entity, f.featureId.toString("."))).groupByKeyWith(Groupings.sortGrouping)

      val row: DList[(Entity, List[StringValue])] = byKey.map({ case ((eid, _), fs) =>
        (eid, makeDense(fs, features, tombstone))
      })
      row.map({ case (eid, vs) => eid + delim + vs.mkString(delim.toString) }).toTextFile(path.toString, overwrite = true)
    }

    def storeMeta: Hdfs[Unit] =
      Hdfs.writeWith(new Path(path, ".dictionary"), os => Streams.write(os, featuresToString(features, delim).mkString("\n")))
  }

  /**
   * Make an Iterable of Facts dense according to a dictionary. 'tombstone' is used as a value for missing facts.
   *
   * Note: It is assumed 'facts' and 'features' are sorted by FeatureId
   */
  def makeDense(facts: Iterable[Fact], features: List[(Int, FeatureId, FeatureMeta)], tombstone: String): List[StringValue] = {
    features.foldLeft((facts, List[StringValue]()))({ case ((fs, acc), (_, fid, _)) =>
      val rest = fs.dropWhile(f => f.featureId.toString(".") < fid.toString("."))
      val value = rest.headOption.collect({
        case f if f.featureId == fid => f.value
      }).getOrElse(TombstoneValue())
      val next = if(value == TombstoneValue() || rest.isEmpty) rest else rest.tail
      (next, Value.toString(value, Some(tombstone)).toList ++ acc)
    })._2.reverse
  }

  def indexDictionary(dict: Dictionary, tombstone: String): List[(Int, FeatureId, FeatureMeta)] =
    dict.meta.toList.filter(f => Encoding.isPrimitive(f._2.encoding)).sortBy(_._1.toString("."))
      .zipWithIndex.map({ case ((f, m), i) => (i, f, m.copy(tombstoneValue = List(tombstone))) })

  def featuresToString(features: List[(Int, FeatureId, FeatureMeta)], delim: Char): List[String] = {
    import com.ambiata.ivory.storage.metadata.DictionaryTextStorage
    features.map({ case (i, f, m) => i.toString + delim + DictionaryTextStorage.delimitedLineWithDelim((f, m), delim.toString) })
  }
}
