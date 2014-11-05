package com.ambiata.ivory

import com.nicta.scoobi.Scoobi
import com.nicta.scoobi.core.{ScoobiConfiguration, DList}
import org.apache.hadoop.io.compress.CompressionCodec

package object mr {
  implicit class PersistDList[A](list: DList[A]) {
    def persistWithCodec(codec: Option[CompressionCodec])(implicit sc: ScoobiConfiguration): DList[A] = {
      Scoobi.persist(codec.map(list.compressWith(_)).getOrElse(list))
    }
  }
}
