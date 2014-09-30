package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.NotImplemented._
import com.ambiata.ivory.storage.plan._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.hadoop.fs.{Path, Hdfs}

import scalaz._, Scalaz._, effect.IO

object SyncExtract {

   def outputDataSet(input: ShadowOutputDataset, cluster: Cluster, output: OutputDataset): ResultTIO[Unit] =
     output.location match {
       case LocalLocation(p) => SyncHdfs.toLocal(input.path, cluster, p)
       case S3Location(p)    => unImplementedSyncOperation
       case HdfsLocation(_)  => unImplementedSyncOperation
     }

   def toRepository(data:Datasets, cluster: Cluster, repo: Repository): ResultTIO[Unit] = repo match {
     case HdfsRepository(_, _)          => unImplementedSyncOperation
     case S3Repository(bucket, root, _) => unImplementedSyncOperation
     case LocalRepository(root)         => getKeys(data).traverseU(key => SyncHdfs.toLocal(cluster.root </> FileName.unsafe(key.name), cluster, root)).void
   }

 }
