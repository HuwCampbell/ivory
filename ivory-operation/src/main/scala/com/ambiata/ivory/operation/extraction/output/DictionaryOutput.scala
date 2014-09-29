package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._

import com.ambiata.mundane.io._
import com.ambiata.mundane.data._
import com.ambiata.poacher.hdfs._

import org.apache.hadoop.fs.Path

object DictionaryOutput {

  /** Turn a dictionary into indexed lines ready to store externally */
  def indexedDictionaryLines(dictionary: Dictionary, tombstone: String, delim: Char): List[String] = {
    import com.ambiata.ivory.storage.metadata.DictionaryTextStorage
    dictionary.byFeatureIndex.toList.sortBy(_._1).map({
      case (i, d) => i.toString + delim + DictionaryTextStorage.delimitedLineWithDelim(d.featureId -> (d match {
        case Concrete(_, m) => m.copy(tombstoneValue = List(tombstone))
        case Virtual(_, vd) =>
          val source = dictionary.byFeatureId.get(vd.source).flatMap {
            case Concrete(_, cd) => Some(cd)
            case Virtual(_, _)   => None
          }.getOrElse(ConcreteDefinition(StringEncoding, None, "", List(tombstone)))
          vd.query.expression match {
            // A short term hack for supporting feature gen based on known functions
            case Count  => ConcreteDefinition(LongEncoding, None, "", List(tombstone))
            case Latest => ConcreteDefinition(source.encoding, None, "", List(tombstone))
          }
      }), delim.toString)
    })
  }

  def writeToHdfs(output: Path, dictionary: Dictionary, missing: String, delimiter: Char): Hdfs[Unit] =
    Hdfs.writeWith(new Path(output, ".dictionary"), os =>
      Streams.write(os, Lists.prepareForFile(indexedDictionaryLines(dictionary, missing, delimiter))))
}
