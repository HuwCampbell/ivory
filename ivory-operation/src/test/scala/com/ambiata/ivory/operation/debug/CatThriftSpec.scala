package com.ambiata.ivory.operation.debug

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.thrift.ThriftLike
import com.ambiata.ivory.operation.ingestion.thrift._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core.{KeyName, SequenceUtil}
import com.ambiata.poacher.mr.ThriftSerialiser
import org.specs2._
import scalaz.{Value => _, _}, Scalaz._
import scala.collection.JavaConverters._

class CatThriftSpec extends Specification with ScalaCheck { def is = s2"""

  We can cat fact thrift values                           $fact    ${tag("mr")}
  We can cat dense thrift values                          $dense   ${tag("mr")}
  We can cat sparse thrift values                         $sparse  ${tag("mr")}
"""

  def fact = propNoShrink { (facts: List[Fact]) =>
    run(CatThriftFact, facts.map(_.toNamespacedThrift).map(Conversion.fact2thrift)) must beOkValue(facts.map(_.entity))
  }.set(minTestsOk = 3, maxSize = 5)

  def dense = propNoShrink { (vs: List[List[PrimitiveValue]]) =>
    val values = vs.zipWithIndex
    val t = values.map { case (v, e) => new ThriftFactDense(e.toString, v.map(valueToThrift).asJava) }
    run(CatThriftDense, t) must beOkValue(values.map(_._2.toString))
  }.set(minTestsOk = 3, maxSize = 5)

  def sparse = propNoShrink { (vs: List[List[PrimitiveValue]]) =>
    val values = vs.zipWithIndex
    val t = values.map { case (v, e) => new ThriftFactSparse(e.toString, v.map(valueToThrift).zipWithIndex.map(x => x._2.toString -> x._1).toMap.asJava) }
    val expected = values.flatMap(v => v._1.map(_ => v._2.toString))
    run(CatThriftSparse, t) must beOkValue(expected)
  }.set(minTestsOk = 3, maxSize = 5)

  def run[A <: ThriftLike](format: CatThriftFormat, thrifts: List[A]): RIO[List[String]] = {
    RepositoryBuilder.using { repo => for {
      i <- Repository.tmpDir("cat-thrift-spec").map(repo.toIvoryLocation)
      o <- Repository.tmpDir("cat-thrift-spec").map(repo.toIvoryLocation)
      _ <- SequenceUtil.writeHdfsBytes(i.location, repo.configuration, repo.codec) {
        w => RIO.io(thrifts.map(t => w(ThriftSerialiser().toBytes(t)))).void
      }.run(repo.configuration)
      _ <- CatThrift.job(repo.configuration, Nil, i.toHdfsPath, format, o.toHdfsPath, repo.codec)
      r <- IvoryLocation.readLines(o)
    } yield r.map(_.split("\\|")(0))
    }
  }

  def valueToThrift(value: PrimitiveValue): ThriftFactValue =
    Conversion.value2thrift(Value.toThrift(value))
}
