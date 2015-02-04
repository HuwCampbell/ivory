package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.mr.FactFormats._

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path

/* TODO Remove
 * This is a temp solution to reading v1 and v2 snapshot formats in tests. It should be replaced
 * with notion SequenceUtil when it can handle multiple key/value types in sequence files */
object SnapshotLoader {

  def readV1(path: Path, conf: ScoobiConfiguration): List[Fact] =
    valueFromSequenceFile[Fact](path.toString).run(conf).toList

  def readV2(path: Path, conf: ScoobiConfiguration): List[Fact] =
    fromSequenceFileWithPath[Int, ThriftFact](new Path(path, "*").toString).map[Fact](x => {
      val (path, (d, tfact)) = x
      val namespace: String = new Path(path).getParent.getName
      val date: Date = Date.unsafeFromInt(d)
      FatThriftFact(namespace, date, tfact)
    }).run(conf).toList
}
