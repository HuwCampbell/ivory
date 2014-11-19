package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.TemporaryLocations._
import com.ambiata.ivory.storage.arbitraries.Arbitraries._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.S3

import org.specs2.{ScalaCheck, Specification}


class SyncExtractSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Sync operations from cluster
============================
 syncing single file from ShadowOutputDataset           $file
 syncing folder from ShadowOutputDataset                $folder
 syncing ShadowOutputDataset handles failure            $handleFailure

 syncing datasets from ShadowRepositroy                 dataset


"""

  def help(location: Location): ShadowOutputDataset = location match {
    case h @ HdfsLocation(_) =>
      ShadowOutputDataset(h)
    case _ =>
      sys.error("Only HdfsLocation is supported by ShadowOutputDataset")
  }

  def file = prop((data: String, location: TemporaryType) => {
    withCluster(cluster =>
      withIvoryLocationFile(TemporaryType.Hdfs)(hdfs =>
        withIvoryLocationFile(location)(output => {
          val dataset = help(hdfs.location)
          for {
            _ <- IvoryLocation.writeUtf8(hdfs, data)
            o = OutputDataset(output)
            _ <- SyncExtract.outputDataset(dataset, cluster, o)
            s <- IvoryLocation.exists(hdfs)
            e <- IvoryLocation.exists(output)
            d <- IvoryLocation.readLines(output)
        } yield (s, e, d.mkString)
        })
      )
    ) must beOkValue((false, true, data))
  }).set(minTestsOk = 10)

  def folder = prop((data: String, location: TemporaryType) => {
    withCluster(cluster =>
      withIvoryLocationDir(TemporaryType.Hdfs)(hdfs => {
        withIvoryLocationDir(location)(output => {
          val dataset = help(hdfs.location)
          for {
            _ <- IvoryLocation.deleteAll(output)
            _ <- IvoryLocation.writeUtf8(hdfs </> FilePath.unsafe("foo"), data)
            _ <- IvoryLocation.writeUtf8(hdfs </> DirPath.unsafe("foos") </> FilePath.unsafe("bar"), data)
            o = OutputDataset(output)
            _ <- SyncExtract.outputDataset(dataset, cluster, o)
            s <- IvoryLocation.exists(hdfs)
            f <- IvoryLocation.exists(output </> FilePath.unsafe("foo"))
            b <- IvoryLocation.exists(output </> DirPath.unsafe("foos") </> FilePath.unsafe("bar"))
            d <- IvoryLocation.readLines(output </> FilePath.unsafe("foo"))
            e <- IvoryLocation.readLines(output </> DirPath.unsafe("foos") </> FilePath.unsafe("bar"))
        } yield (s, f, b, d.mkString, e.mkString)
        })
      })
    ) must beOkValue((false, true, true, data, data))
  }).set(minTestsOk = 10)

  def handleFailure = prop((location: TemporaryType) => {
    withCluster(cluster =>
      withIvoryLocationFile(TemporaryType.Hdfs)(hdfs => {
        withIvoryLocationFile(location)(output => {
          val dataset = help(hdfs.location)
          SyncExtract.outputDataset(dataset, cluster, OutputDataset(output))
        })
      })
    ) must beFail
  }).set(minTestsOk = 10)



  def dataset = ???
}
