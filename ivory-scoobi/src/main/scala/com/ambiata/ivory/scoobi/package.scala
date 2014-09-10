package com.ambiata.ivory

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.nicta.scoobi.Scoobi
import com.nicta.scoobi.core.{ScoobiConfiguration, DList}
import org.apache.hadoop.io.compress.CompressionCodec

package object scoobi {
  type AmazonEMRClient = AmazonElasticMapReduceClient
  type AmazonS3EMRClient = (AmazonS3Client, AmazonElasticMapReduceClient)


  /**
   * Scoobi syntax
   */
  implicit class PersistDList[A](list: DList[A]) {
    def persistWithCodec(codec: Option[CompressionCodec])(implicit sc: ScoobiConfiguration): DList[A] = {
      Scoobi.persist(codec.map(list.compressWith(_)).getOrElse(list))
    }
  }

}
