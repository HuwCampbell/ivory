package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core.Name
import com.ambiata.ivory.storage.ScalaCheckManagedProperties
import com.ambiata.ivory.storage.repository.Repository
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.specs2.{ScalaCheck, Specification}
import Namespaces._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import scalaz.{Name => _, _}, Scalaz._
import MemoryConversions._
import com.ambiata.poacher.hdfs._

class NamespacesSpec extends Specification with ScalaCheck with ScalaCheckManagedProperties { def is = s2"""

 The Namespaces object helps with finding the sizes of namespaces inside a given directory
   `namespaceSizes` returns the size of each namespace for a give factset       $e1
   
   if singleNamespace is specified then the input path is interpreted
   as the directory for namespace being named <singleNamespace>                 $e2

   `namespaceSizes` returns the size of each namespace for a give factset       $e3
"""

  def e1 = prepare { factsetPath =>
    namespaceSizes(factsetPath)
  } must beOkLike((_: List[(Name, BytesQuantity)]) must contain ((Name("ns1"), 4.bytes), (Name("ns2"), 4.bytes)))

  def e2 = prepare { factsetPath =>
    namespaceSizesSingle(new Path(factsetPath, "ns1"), Name("namespace"))
  } must beOkValue((Name("namespace"), 4.bytes))

  def e3 = managed { temp: Temporary => (nsInc: Set[FeatureNamespace], nsExc: Set[FeatureNamespace], fsInc: FactsetIdList, fsExc: FactsetIdList) =>
    !nsInc.exists(nsExc.contains) ==> {
      val sc = ScoobiConfiguration()
      val repo = Repository.fromHdfsPath(temp.file, sc)
      val namespaces = (nsInc ++ nsExc).toList.flatMap(ns => (fsInc.ids ++ fsExc.ids).map(fs => fs -> ns.namespace)).map {
        case (fs, ns) => repo.namespace(fs, ns.name)
      }
      val computNamespaces =
        (for {
          _ <- namespaces.traverse(f => Hdfs.mkdir(f.toHdfs))
          _ <- namespaces.map(_ + "/f1").traverse(createFile)
          sizes <- allNamespaceSizes(repo, nsInc.toList.map(_.namespace), fsInc.ids)
        } yield sizes).run(sc.configuration).map(_.toSet) must
          beOkValue(nsInc.map(ns => ns.namespace -> (fsInc.ids.length * 4).bytes))
    }
  }.set(maxSize = 5, minTestsOk = 5)

  def prepare[A](f: Path => Hdfs[A]): ResultTIO[A] = Temporary.using { dir =>
    val ns1     = dir </> "ns1"
    val ns2     = dir </> "ns2"
    (List(ns1, ns2).traverse(f => Hdfs.mkdir(f.toHdfs)) >>
    List(ns1+"/f1", ns2+"/f2").traverse(createFile) >>
    f(dir.toHdfs)).run(new Configuration)
  }

  def createFile(path: String): Hdfs[Unit] =
    Hdfs.writeWith(new Path(path), out => Streams.write(out, "test"))
}
