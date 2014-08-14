package com.ambiata.ivory.storage.legacy

import org.apache.hadoop.fs.Path

import com.ambiata.poacher.hdfs._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import scalaz.effect.IO

object CreateRepository {

  def onStore(repo: Repository): ResultTIO[Boolean] = repo match {
    case HdfsRepository(root, conf) => onHdfs(root.toHdfs).run(conf.configuration)
    case _                          => ResultT.fail[IO, Boolean]("Can only create HDFS repositories at the moment")
  }

  private def onHdfs(path: Path): Hdfs[Boolean] = {
    val meta = new Path(path, "metadata")
    val dict = new Path(meta, "dictionaries")
    val store = new Path(meta, "stores")
    val factsets = new Path(path, "factsets")
    val errors = new Path(path, "errors")
    val snapshots = new Path(path, "snapshots")
    for {
      e <- Hdfs.exists(path)
      r <- if(e) Hdfs.ok(false) else for {
        _ <- Hdfs.mkdir(dict)
        _ <- Hdfs.mkdir(store)
        _ <- Hdfs.mkdir(factsets)
        _ <- Hdfs.mkdir(errors)
        _ <- Hdfs.mkdir(snapshots)
      } yield true
    } yield r
  }
}
