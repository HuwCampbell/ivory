package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core.{IvorySyntax, Repository, FactsetId, Name}
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.mundane.io.BytesQuantity
import com.ambiata.mundane.io.MemoryConversions._
import org.apache.hadoop.fs.Path
import scalaz._, Scalaz._
import IvorySyntax._

object Namespaces {
  /**
   * @return the list of namespaces for a given factset and their corresponding sizes
   */
  def namespaceSizes(factsetPath: Path): Hdfs[List[(Name, BytesQuantity)]] =
    Hdfs.childrenSizes(factsetPath).map(_.map { case (n, q) => (Name.fromPathName(n), q) })

  def namespaceSizesSingle(factsetPath: Path, namespace: Name): Hdfs[(Name, BytesQuantity)] =
    Hdfs.totalSize(factsetPath).map(namespace ->)

  /* Return the size of specific namespaces/factsets */
  def allNamespaceSizes(repository: Repository, namespaces: List[Name], factsets: List[FactsetId]): Hdfs[List[(Name, BytesQuantity)]] = {
    namespaces.flatMap(ns => factsets.map(ns ->)).traverse {
      case (ns, fsid) => namespaceSizesSingle(repository.toFilePath(Repository.namespace(fsid, ns)).toHdfs, ns)
    }.map(_.foldLeft(Map[Name, BytesQuantity]()) {
      case (k, v) => k + (v._1 -> implicitly[Numeric[BytesQuantity]].mkNumericOps(k.getOrElse(v._1, 0.mb)).+(v._2))
    }.toList)
  }
}

