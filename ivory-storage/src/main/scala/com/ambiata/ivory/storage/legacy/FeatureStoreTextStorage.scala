package com.ambiata.ivory.storage.legacy

import scalaz.{Value => _, _}, Scalaz._, \&/._, effect.IO
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._

object FeatureStoreTextStorage {

  case class FeatureStoreTextLoader(path: Path) extends IvoryLoader[Hdfs[FeatureStore]] {
    def load: Hdfs[FeatureStore] =
      Hdfs.readWith(path, is => fromInputStream(is))
  }

  case class FeatureStoreTextStorer(path: Path) extends IvoryStorer[FeatureStore, Hdfs[Unit]] {
    def store(store: FeatureStore): Hdfs[Unit] =
      Hdfs.writeWith(path, os => Streams.write(os, storeAsString(store)))
  }

  def storeFromHdfs(path: Path): Hdfs[FeatureStore] =
    FeatureStoreTextLoader(path).load

  def storeToHdfs(path: Path, store: FeatureStore): Hdfs[Unit] =
    FeatureStoreTextStorer(path).store(store)

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
