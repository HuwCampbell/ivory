package com.ambiata.ivory.storage.sync

import java.io.File
import java.util.UUID

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.ivory.storage.plan._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{Location => _, HdfsLocation => _, S3Location => _, LocalLocation => _, _}
import com.ambiata.notion.core._
import scalaz._, Scalaz._, effect.IO

object SyncIngest {

   def inputDataSet(input: InputDataset, cluster: Cluster): ResultTIO[ShadowInputDataset] = {
     val outputPath = DirPath.unsafe(s"shadowInputDataset/${UUID.randomUUID()}")

     input.location match {
       case LocalIvoryLocation(l @ LocalLocation(path)) =>
         val out =
           if (new File(path).isFile) outputPath </> FileName.unsafe(new File(path).getName)
           else                       outputPath

         SyncLocal.toHdfs(l.dirPath, outputPath, cluster) >>
           ResultT.ok(shadowInputDatasetFromPath(out, cluster))

       case S3IvoryLocation(S3Location(bucket, key), _) =>
         SyncS3.toHdfs(DirPath.unsafe(key), outputPath, cluster) >>
           ResultT.ok(shadowInputDatasetFromPath(outputPath, cluster))

       case h @ HdfsIvoryLocation(HdfsLocation(_), _, _, _) =>
         ResultT.ok(ShadowInputDataset(h))
     }
   }

   def shadowInputDatasetFromPath(path: DirPath, cluster: Cluster): ShadowInputDataset =
     ShadowInputDataset(cluster.root </> path)

   def toCluster(datasets: Datasets, source: Repository, cluster: Cluster): ResultTIO[ShadowRepository] =
     (source.root.location match {
       case S3Location(bucket, key) => getKeys(datasets).traverseU(key => SyncS3.toHdfs(DirPath.unsafe(key.name), DirPath.unsafe(key.name), cluster)).void
       case l @ LocalLocation(root) => getKeys(datasets).traverseU(key => SyncLocal.toHdfs(l.dirPath, DirPath.unsafe(key.name), cluster)).void
       case HdfsLocation(_)         => ResultT.unit[IO]
     }).as(source match {
       case HdfsRepository(r) => ShadowRepository(r)
       case _                 => ShadowRepository(cluster.root)
     })

 }
