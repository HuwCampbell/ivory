package com.ambiata.ivory.storage.fact

import com.ambiata.disorder._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.hdfs.Arbitraries._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.specs2.{ScalaCheck, Specification}
import Namespaces._
import com.ambiata.mundane.testing.RIOMatcher._
import scalaz._, Scalaz._
import MemoryConversions._

case class PathConf(path: Path, conf: Configuration)

class NamespacesSpec extends Specification with ScalaCheck { def is = s2"""

 The Namespaces object helps with finding the sizes of namespaces inside a given directory
   `namespaceSizes` returns the size of each namespace for a given factset                  $e1

   if the path is a directory                                                               $dirOnly

   if singleNamespace is specified then the input path is interpreted
   as the directory for namespace being named <singleNamespace>                             $e2

   `namespaceSizes` returns the size of each namespace for a give factset                   $e3

  All namespaces from input should be present in map                                        $sumNames
  Sum of all the input namespaces should be the same as map                                 $sumTotal
"""

  def prepare[A](hdfs: HdfsTemporary, data: String): RIO[PathConf] = {
    val ns1 = KeyName.unsafe("ns1")
    val ns2 = KeyName.unsafe("ns2")
    for {
      c <- IvoryConfigurationTemporary.random.conf
      p <- hdfs.path.run(c.configuration)
      r <- Repository.fromUri(p.toString, c)
      _ <- List(ns1 / "f1", ns2 / "f2", Key.Root / ".ignore").traverse(r.store.utf8.write(_, data))
    } yield PathConf(p, c.configuration)
  }

  def e1 = prop((hdfs: HdfsTemporary, data: S) => for {
    p <- prepare(hdfs, data.value)
    r <- namespaceSizes(p.path).run(p.conf).map(_.toMap)
  } yield r ==== Map(Namespace("ns1") -> data.value.getBytes("UTF-8").size.bytes, Namespace("ns2") -> data.value.getBytes("UTF-8").size.bytes))

  def dirOnly = prop((hdfs: HdfsTemporary, data: S) => (for {
    c <- IvoryConfigurationTemporary.random.conf
    _ <- (for {
      p <- hdfs.path.map(new Path(_, "child"))
      _ <- Hdfs.write(p, data.value)
      x <- namespaceSizes(p)
    } yield ()).run(c.configuration)
  } yield ()) must beFail)

  def e2 = prop((hdfs: HdfsTemporary, data: S) => for {
    p <- prepare(hdfs, data.value)
    r <- namespaceSizesSingle(new Path(p.path, "ns1"), Namespace("namespace")).run(p.conf)
  } yield r ==== Namespace("namespace") -> data.value.getBytes("UTF-8").size.bytes)

  def e3 = prop((tmp: LocalTemporary, nsIncSet: Set[Namespace], nsExc: Set[Namespace], fsInc: FactsetIds, fsExc: FactsetIds, data: S) => !nsIncSet.exists(nsExc.contains) ==> {
    // Ordering is relevant in the assertion
    val nsInc = nsIncSet.toList
    for {
      c <- IvoryConfigurationTemporary.random.conf
      d <- tmp.directory
      r = HdfsRepository(HdfsLocation(d.path), c)
      n = (nsInc ++ nsExc.toList).flatMap(ns => (fsInc.ids ++ fsExc.ids).map(fs => fs -> ns)).map {
        case (fs, ns) => Repository.namespace(fs, ns)
      }
      _ <- n.traverse(k => Hdfs.mkdir(r.toIvoryLocation(k).toHdfsPath).run(c.configuration))
      _ <- n.map(_ / "f1").traverse(r.store.utf8.write(_, data.value))
      s <- allNamespaceSizes(r, nsInc.toList, fsInc.ids).run(c.configuration)
    } yield s ==== nsInc.map(ns =>
      ns -> (fsInc.ids.length * data.value.getBytes("UTF-8").size).bytes).toList
  }).set(maxSize = 5, minTestsOk = 10)



  def sumNames = prop { (l: List[(Namespace, Long)]) =>
    Namespaces.sum(l.map(x => x._1 -> x._2.bytes)).keySet == l.map(_._1).toSet
  }

  def sumTotal = prop { (l: List[(Namespace, Long)]) =>
    Namespaces.sum(l.map(x => x._1 -> x._2.bytes)).values.map(_.toBytes.value).sum == l.map(_._2).sum
  }

}
