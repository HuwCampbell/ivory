package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core.Name
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.specs2.Specification
import Namespaces._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import scalaz.{Name => _, _}, Scalaz._
import MemoryConversions._
import com.ambiata.poacher.hdfs._

class NamespacesSpec extends Specification { def is = s2"""

 The Namespaces object helps with finding the sizes of namespaces inside a given directory
   `namespaceSizes` returns the size of each namespace for a give factset       $e1
   
   if singleNamespace is specified then the input path is interpreted
   as the directory for namespace being named <singleNamespace>                 $e2
"""

  def e1 = prepare { factsetPath =>
    namespaceSizes(factsetPath, singleNamespace = None)
  } must beOkLike((_: List[(Name, BytesQuantity)]) must contain ((Name("ns1"), 4.bytes), (Name("ns2"), 4.bytes)))

  def e2 = prepare { factsetPath =>
    namespaceSizes(new Path(factsetPath, "ns1"), singleNamespace = Some(Name("namespace")))
  } must beOkValue(List((Name("namespace"), 4.bytes)))

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
