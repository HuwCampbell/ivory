package com.ambiata.ivory.storage.sync

import java.util.UUID

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.ivory.storage.plan._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.hadoop.fs.{Path, Hdfs}

import scalaz._, Scalaz._, effect.IO

object SyncIngest {

   def inputDataSet(input: InputDataset, cluster: Cluster): ResultTIO[ShadowInputDataset] = {
     val outputPath: FilePath = FilePath(s"shadowInputDataset/${UUID.randomUUID()}")
     input.location match {
       case LocalLocation(path) => FilePath(path).toFile.isFile match {
         case true =>  SyncLocal.toHdfs(FilePath(path), outputPath, cluster) >> ResultT.ok(shadowInputDatasetFromPath( outputPath </> FilePath(path).basename, cluster))
         case false => SyncLocal.toHdfs(FilePath(path), outputPath, cluster) >> ResultT.ok(shadowInputDatasetFromPath(outputPath, cluster))
       }
       case S3Location(b,k)     => SyncS3.toHdfs(b, FilePath(k), outputPath, cluster) >> ResultT.ok(shadowInputDatasetFromPath(outputPath, cluster))
       case HdfsLocation(path)  => ResultT.ok(ShadowInputDataset(FilePath(path), cluster.hdfsConfiguration))
     }
   }

   def shadowInputDatasetFromPath(path: FilePath, cluster: Cluster): ShadowInputDataset =
     ShadowInputDataset(cluster.root </> path, cluster.ivory.configuration)

   def toCluster(datasets:Datasets, source: Repository, cluster: Cluster): ResultTIO[ShadowRepository] =
     (source match {
       case S3Repository(bucket, root, _) => getPaths(datasets).traverseU(z => SyncS3.toHdfs(bucket, root, z, cluster)).void
       case LocalRepository(root) => getPaths(datasets).traverseU(z => SyncLocal.toHdfs(root, FilePath(""), cluster)).void
       case HdfsRepository(_, _) => ResultT.unit[IO]
     }).as(source match {
       case HdfsRepository(r, c) => ShadowRepository(r, c)
       case _ => ShadowRepository(cluster.root, cluster.ivory)
     })

 }
