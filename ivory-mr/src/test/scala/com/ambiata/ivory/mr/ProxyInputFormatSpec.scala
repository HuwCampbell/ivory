package com.ambiata.ivory.mr

import com.ambiata.mundane.io.MemoryConversions._

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.InputSplit

import org.specs2._
import org.scalacheck._, Arbitrary._

class ProxyInputFormatSpec extends Specification with ScalaCheck { def is = s2"""

   When we have small splits, they are combined into a smaller number of chunks.

     ${ prop((small: List[SmallInputSplit], big: List[BigInputSplit]) => (small.size > 2) ==> {
          ProxyInputFormat.chunk((small ::: big).toArray, 32.mb.toBytes.value).size must be_< (small.size + big.size) }) }

   When we have big chunks, they are not combined..

     ${ prop((big: List[BigInputSplit]) => !big.isEmpty ==> {
          ProxyInputFormat.chunk(big.toArray, 32.mb.toBytes.value).size ==== (big.size) }) }

"""
  implicit def SmallInputSplitArbitrary: Arbitrary[SmallInputSplit] =
    Arbitrary(for {
      x <- Gen.choose(1L, 16.mb.toBytes.value)
    } yield new SmallInputSplit(x))


  implicit def BigInputSplitArbitrary: Arbitrary[BigInputSplit] =
    Arbitrary(for {
      x <- Gen.choose(128.mb.toBytes.value, 1024.mb.toBytes.value)
    } yield new BigInputSplit(x))

  class SmallInputSplit(n: Long) extends InputSplit {
    def getLength: Long = n
    def getLocations: Array[String] = Array()
    override def toString: String = s"SmallInputSplit($n)"
  }

  class BigInputSplit(n: Long) extends InputSplit {
    def getLength: Long = n
    def getLocations: Array[String] = Array()
    override def toString: String = s"BigInputSplit($n)"
  }

  def combine(small: Array[SmallInputSplit], big: Array[BigInputSplit]): Array[InputSplit] =
    small.map(x => x: InputSplit) ++ big.map(x => x: InputSplit)
}
