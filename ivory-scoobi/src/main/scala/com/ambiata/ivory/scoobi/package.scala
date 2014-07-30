package com.ambiata.ivory

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient

package object scoobi {
  type AmazonEMRClient = AmazonElasticMapReduceClient
  type AmazonS3EMRClient = (AmazonS3Client, AmazonElasticMapReduceClient)
}
