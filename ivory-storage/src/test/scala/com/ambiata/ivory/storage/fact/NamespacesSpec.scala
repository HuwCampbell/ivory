package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.alien.hdfs.Hdfs
import com.ambiata.mundane.io.{BytesQuantity, MemoryConversions, Streams}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.specs2.Specification
import Namespaces._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import Streams._
import scalaz._, Scalaz._
import MemoryConversions._

class NamespacesSpec extends Specification { def is = s2"""

 The Namespaces object helps with finding the sizes of namespaces inside a given directory
   `namespaceSizes` returns the size of each namespace for a give factset       $e1
   
   if singleNamespace is specified then the input path is interpreted
   as the directory for namespace being named <singleNamespace>                 $e2
"""

  def e1 = {
    prepare.runNow must beOk

    val factsetPath = new Path(testDir)
    namespaceSizes(factsetPath, singleNamespace = None).runNow must beOkLike((_: List[(String, BytesQuantity)]) must contain (("ns1", 4.bytes), ("ns2", 4.bytes)))
  }

  def e2 =  {
    prepare.runNow must beOk

    val factsetPath = new Path(testDir)
    namespaceSizes(new Path(factsetPath, "ns1"), singleNamespace = Some("namespace")).runNow must beOkValue(List(("namespace", 4.bytes)))
  }

  implicit class runHdfs[A](hdfs: Hdfs[A]) {
    def runNow = hdfs.run(new Configuration)
  }

  def prepare =
    List(testDir, ns1, ns2).traverse(p => Hdfs.mkdir(new Path(p))) >>
    List(ns1+"/f1", ns2+"/f2").traverse(createFile)

  def createFile(path: String): Hdfs[Unit] =
    Hdfs.writeWith(new Path(path), out => Streams.write(out, "test"))

  val testDir = s"target/${getClass.getSimpleName}"
  val ns1     = s"$testDir/ns1"
  val ns2     = s"$testDir/ns2"

}
