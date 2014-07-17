package com.ambiata.ivory.storage.metadata

import scalaz.{Value => _, _}, Scalaz._, \&/._, effect.IO
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.saws.s3.S3

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.alien.hdfs.HdfsS3Action._

object FeatureStoreTextStorage {

  def storeFromHdfs(path: Path): Hdfs[FeatureStore] =
    Hdfs.readWith(path, is => fromInputStream(is))

  def storeToHdfs(path: Path, store: FeatureStore): Hdfs[Unit] =
    Hdfs.writeWith(path, os => Streams.write(os, storeAsString(store)))

  def storeFromS3(bucket: String, key: String, tmp: Path): HdfsS3Action[FeatureStore] = for {
    file  <- HdfsS3Action.fromAction(S3.downloadFile(bucket, key, to = tmp.toString))
    store <- HdfsS3Action.fromHdfs(storeFromHdfs(new Path(file.getPath)))
  } yield store

  def storeToS3(bucket: String, key: String, store: FeatureStore, tmp: Path): HdfsS3Action[Unit] = for {
    _ <- HdfsS3Action.fromHdfs(storeToHdfs(tmp, store))
    _ <- HdfsS3.putPaths(bucket, key, tmp, glob = "*")
    _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.delete(tmp, true)))
  } yield ()

  def writeFile(path: String, store: FeatureStore): ResultTIO[Unit] = ResultT.safe({
    Streams.write(new java.io.FileOutputStream(path), storeAsString(store))
  })

  def fromFile(path: String): ResultTIO[FeatureStore] = for {
    raw <- Files.read(path.toFilePath)
    fs  <- ResultT.fromDisjunction[IO, FeatureStore](fromLines(raw.lines.toList).leftMap(err => This(s"Error reading feature store from file '$path': $err")))
  } yield fs

  def fromInputStream(is: java.io.InputStream): ResultTIO[FeatureStore] = for {
    content <- Streams.read(is)
    r <- ResultT.fromDisjunction[IO, FeatureStore](fromLines(content.lines.toList).leftMap(This.apply))
  } yield r

  def fromLines(lines: List[String]): String \/ FeatureStore =
    PrioritizedFactset.fromLines(lines).map(FeatureStore)

  def fromFactsets(sets: List[Factset]): FeatureStore =
    FeatureStore(PrioritizedFactset.fromFactsets(sets))

  def storeAsString(store: FeatureStore): String =
    store.factsets.sortBy(_.priority).map(_.set.name).mkString("\n") + "\n"
}
