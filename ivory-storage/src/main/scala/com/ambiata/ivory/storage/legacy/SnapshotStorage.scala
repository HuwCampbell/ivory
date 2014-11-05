package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.poacher.scoobi._
import com.ambiata.ivory.mr._
import FactFormats._

object SnapshotStorageV1 {
  case class SnapshotLoader(path: Path) {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      valueFromSequenceFile[Fact](path.toString).map(_.right[ParseError])
  }

  def snapshotFromHdfs(path: Path): ScoobiAction[DList[ParseError \/ Fact]] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      SnapshotLoader(path).loadScoobi
    })

}
