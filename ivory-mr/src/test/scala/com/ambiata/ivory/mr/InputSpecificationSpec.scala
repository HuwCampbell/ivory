package com.ambiata.ivory.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.Mapper

import org.specs2._
import org.scalacheck._, Arbitrary._

class InputSpecificationSpec extends Specification with ScalaCheck { def is = s2"""

  Symmetric thrift conversion:

     ${ prop((specs: List[InputSpecification]) =>
          InputSpecification.fromThrift(new Configuration,
            InputSpecification.toThrift(specs)) ==== specs) }

"""

  implicit def InputSpecificationArbitrary: Arbitrary[InputSpecification] =
    Arbitrary(for {
      f <- Gen.oneOf(classOf[TextInputFormat], classOf[SequenceFileInputFormat[_, _]])
      m <- Gen.oneOf(classOf[MapperX], classOf[MapperY])
      p <- Gen.listOf(path)
    } yield InputSpecification(f, m, p))

  def path: Gen[Path] = for {
      n <- Gen.choose(1, 10)
      s <- Gen.listOfN(n, arbitrary[Int])
  } yield new Path(s"""/${s.mkString("/")}""")

  class MapperX extends CombinableMapper[NullWritable, NullWritable, NullWritable, NullWritable]
  class MapperY extends CombinableMapper[NullWritable, NullWritable, NullWritable, NullWritable]
}
