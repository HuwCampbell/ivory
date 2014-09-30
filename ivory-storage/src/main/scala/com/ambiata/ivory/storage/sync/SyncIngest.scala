package com.ambiata.ivory.storage.sync

import java.util.UUID

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.ivory.storage.plan._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.saws.s3.{S3Path, S3}

import scalaz._, Scalaz._, effect.IO

object SyncIngest {

   def inputDataSet(input: InputDataset, cluster: Cluster): ResultTIO[ShadowInputDataset] = {
     val outputPath = DirPath.unsafe(s"shadowInputDataset/${UUID.randomUUID()}")

     input.location match {
       case LocalIvoryLocation(LocalLocation(path, _)) =>
         val out =
           if (path.toFile.isFile) outputPath </> path.basename
           else                    outputPath

         SyncLocal.toHdfs(path, outputPath, cluster) >>
           ResultT.ok(shadowInputDatasetFromPath(out, cluster))

       case S3IvoryLocation(S3Location(path,_ ), _) =>
         SyncS3.toHdfs(path, outputPath, cluster) >>
           ResultT.ok(shadowInputDatasetFromPath(outputPath, cluster))

       case h @ HdfsIvoryLocation(HdfsLocation(_, _), _, _, _) =>
         ResultT.ok(ShadowInputDataset(h))
     }
   }

   def shadowInputDatasetFromPath(path: DirPath, cluster: Cluster): ShadowInputDataset =
     ShadowInputDataset(cluster.root </> path)

   def toCluster(datasets:Datasets, source: Repository, cluster: Cluster): ResultTIO[ShadowRepository] =
     (source.root.location match {
       case S3Location(root, _)    => getKeys(datasets).traverseU(key => SyncS3.toHdfs(root, DirPath.unsafe(key.name), cluster)).void
       case LocalLocation(root, _) => getKeys(datasets).traverseU(key => SyncLocal.toHdfs(root, DirPath.unsafe(key.name), cluster)).void
       case HdfsLocation(_, _)     => ResultT.unit[IO]
     }).as(source match {
       case HdfsRepository(r) => ShadowRepository(r)
       case _                 => ShadowRepository(cluster.root)
     })

 }
