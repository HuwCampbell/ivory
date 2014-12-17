package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.mundane.io.BytesQuantity
import com.ambiata.mundane.io.MemoryConversions._
import org.apache.hadoop.fs.Path
import scalaz.{Name =>_,_}, Scalaz._


object Namespaces {
  /**
   * @return the list of namespaces for a given factset and their corresponding sizes
   */
  def namespaceSizes(factsetPath: Path): Hdfs[List[(Name, BytesQuantity)]] =
    Hdfs.childrenSizes(factsetPath)
      .flatMap(_.traverseU { case (n, q) => Hdfs.ok(List(n)).filterHidden.map(_.map(_ -> q))}).map(_.flatten)
      .map(_.map { case (n, q) => (Name.fromPathName(n), q) })

  def namespaceSizesSingle(factsetPath: Path, namespace: Name): Hdfs[(Name, BytesQuantity)] =
    Hdfs.totalSize(factsetPath).map(namespace ->)

  /* Return the size of specific namespaces/factsets */
  def allNamespaceSizes(repository: HdfsRepository, namespaces: List[Name], factsets: List[FactsetId]): Hdfs[List[(Name, BytesQuantity)]] = {
    namespaces.flatMap(ns => factsets.map(ns ->)).traverse {
      case (ns, fsid) => namespaceSizesSingle(repository.toIvoryLocation(Repository.namespace(fsid, ns)).toHdfsPath, ns)
    }.map(sum).map(_.toList)
  }

  def sum(sizes: List[(Name, BytesQuantity)]): Map[Name, BytesQuantity] =
    sizes.foldLeft(Map[Name, BytesQuantity]()) {
      case (k, v) => k + (v._1 -> implicitly[Numeric[BytesQuantity]].mkNumericOps(k.getOrElse(v._1, 0.mb)).+(v._2))
   }
}
