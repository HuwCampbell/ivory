package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.mundane.io.{Logger, BytesQuantity}
import com.nicta.scoobi.core.ScoobiConfiguration
import org.apache.hadoop.io.compress.CompressionCodec

case class RecreateConfig(from: HdfsRepository, to: HdfsRepository,
                          sc: ScoobiConfiguration, codec: Option[CompressionCodec],
                          clean: Boolean, dry: Boolean, recreateData: List[RecreateData],
                          maxNumber: Option[Int],
                          reducerSize: BytesQuantity,
                          overwrite: Boolean,
                          logger: Logger) {
  val (hdfsFrom, hdfsTo) = (from, to) match {
    case (f: HdfsRepository, t: HdfsRepository) => (f, t)
    case _ => Crash.error(Crash.ResultTIO ,s"Repository combination '$from' and '$to' not supported!")
  }

  def dryFor(data: RecreateData) =
    dry || !recreateData.contains(data)
}

case class RecreateData(name: String) {
  def plural: String = name+"s"
}
object RecreateData {
  val DICTIONARY = new RecreateData("dictionary") { override def plural = "dictionaries" }
  val STORE      = RecreateData("store")
  val FACTSET    = RecreateData("factset")
  val SNAPSHOT   = RecreateData("snapshot")
  val ALL        = List(DICTIONARY, STORE, FACTSET, SNAPSHOT)

  def parse(s: String) = {
    def parseElement(string: String) =
      if (string == "dictionary")    Some(DICTIONARY)
      else if (string == "store")    Some(STORE)
      else if (string == "factset")  Some(FACTSET)
      else if (string == "snapshot") Some(SNAPSHOT)
      else                           None

    val result = s.toLowerCase.split(",").map(_.trim).map(parseElement).flatten.distinct.toList
    if (result.nonEmpty) result
    else                 ALL
  }
}
