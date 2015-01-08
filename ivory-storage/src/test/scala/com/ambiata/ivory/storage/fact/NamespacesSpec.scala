package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.ScalaCheckManagedProperties
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.specs2.{ScalaCheck, Specification}
import Namespaces._
import com.ambiata.mundane.testing.RIOMatcher._
import scalaz._, Scalaz._
import MemoryConversions._

class NamespacesSpec extends Specification with ScalaCheck with ScalaCheckManagedProperties { def is = s2"""

 The Namespaces object helps with finding the sizes of namespaces inside a given directory
   `namespaceSizes` returns the size of each namespace for a given factset                  $e1

   if singleNamespace is specified then the input path is interpreted
   as the directory for namespace being named <singleNamespace>                             $e2

   `namespaceSizes` returns the size of each namespace for a give factset                   $e3

  All namespaces from input should be present in map                                        $sumNames
  Sum of all the input namespaces should be the same as map                                 $sumTotal
"""

  def e1 = prepare { factsetPath =>
    namespaceSizes(factsetPath).run(new Configuration).map(_.toMap)
  } must beOkValue(Map(Namespace("ns1") -> 4.bytes, Namespace("ns2") -> 4.bytes))

  def e2 = prepare { factsetPath =>
    namespaceSizesSingle(new Path(factsetPath, "ns1"), Namespace("namespace")).run(new Configuration)
  } must beOkValue((Namespace("namespace"), 4.bytes))

  def e3 = managed { temp: TemporaryDirPath => (nsInc: Set[Namespace], nsExc: Set[Namespace], fsInc: FactsetIds, fsExc: FactsetIds) =>
    !nsInc.exists(nsExc.contains) ==> {
      val sc = ScoobiConfiguration()
      val repo = HdfsRepository(HdfsLocation(temp.dir.path), IvoryConfiguration.fromScoobiConfiguration(sc), IvoryFlags.default)
      val namespaces = (nsInc ++ nsExc).toList.flatMap(ns => (fsInc.ids ++ fsExc.ids).map(fs => fs -> ns)).map {
        case (fs, ns) => Repository.namespace(fs, ns)
      }
      val computNamespaces =
        (for {
          _ <- namespaces.traverse(k => Hdfs.mkdir(repo.toIvoryLocation(k).toHdfsPath).run(sc.configuration))
          _ <- namespaces.map(_ / "f1").traverse(createFile(repo))
          sizes <- allNamespaceSizes(repo, nsInc.toList, fsInc.ids).run(sc.configuration)
        } yield sizes).map(_.toSet) must
          beOkValue(nsInc.map(ns => ns -> (fsInc.ids.length * 4).bytes))
    }
  }.set(maxSize = 5, minTestsOk = 5)

  def prepare[A](f: Path => RIO[A]): RIO[A] = TemporaryDirPath.withDirPath { dir =>
    val ns1 = KeyName.unsafe("ns1")
    val ns2 = KeyName.unsafe("ns2")
    for {
      repository <- TemporaryIvoryConfiguration.withConf(conf => Repository.fromUri(dir.path, conf, IvoryFlags.default))
      _          <- List(ns1 / "f1", ns2 / "f2", Key.Root / ".ignore").traverse(createFile(repository))
      result     <- f(new Path(dir.path))
    } yield result
  }

  def createFile(repository: Repository)(key: Key): RIO[Unit] =
    repository.store.utf8.write(key, "test")

  def sumNames = prop { (l: List[(Namespace, Long)]) =>
    Namespaces.sum(l.map(x => x._1 -> x._2.bytes)).keySet == l.map(_._1).toSet
  }

  def sumTotal = prop { (l: List[(Namespace, Long)]) =>
    Namespaces.sum(l.map(x => x._1 -> x._2.bytes)).values.map(_.toBytes.value).sum == l.map(_._2).sum
  }
}
