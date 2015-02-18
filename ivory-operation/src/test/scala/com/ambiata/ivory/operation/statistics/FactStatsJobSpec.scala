package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._, Arbitraries._
import com.ambiata.ivory.lookup.{FeatureIdLookup, ThriftNumericalStats, ThriftCategoricalStats, ThriftHistogramEntry}
import com.ambiata.ivory.mr.TestEmitter
//import com.ambiata.ivory.storage.repository.RepositoryBuilder
import FactStatsWritable._

import org.specs2._
import org.specs2.execute.{Result, AsResult}

import com.ambiata.disorder._
//import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr._

import org.apache.hadoop.io._

import scala.collection.JavaConverters._

import scalaz._, Scalaz._, effect.IO

class FactStatsJobSpec extends Specification with ScalaCheck { def is = sequential ^ s2"""

Mapper
------

  Emits initial categorical and numerical stats                             $initStats

  """

  implicit def disjunctionOrdering[A: Order, B: Order]: scala.Ordering[A \/ B] =
    implicitly[Order[A \/ B]].toScalaOrdering

  def initStats = prop((fact: Fact, featureIndex: NaturalInt) => {
    val key = keyString(featureIndex.value, fact.date)

    val expected: List[(String, ThriftNumericalStats \/ ThriftCategoricalStats)] =
      List((key, categoricalStats(fact).right)) ++ doubleValue(fact).map(d => (key, numericalStats(d).left)).toList

    val serialiser = ThriftSerialiser()
    val thriftNumerical = new ThriftNumericalStats
    val thriftCategorical = new ThriftCategoricalStats
    val emitter = newEmitter
    val lookup = new FeatureIdLookup()
    lookup.putToIds(fact.featureId.toString, featureIndex.value)

    FactStatsMapper.emitStats(fact, thriftNumerical, thriftCategorical, emitter, Writables.bytesWritable(4096), Writables.bytesWritable(4096), lookup, serialiser)

    emitter.emitted.toList.sortBy(sortByFunc) ==== expected.sortBy(sortByFunc)
  })

  def sortByFunc(t: (String, ThriftNumericalStats \/ ThriftCategoricalStats)): (String, Option[ThriftNumericalStats], Option[ThriftCategoricalStats]) =
    (t._1, t._2.swap.toOption, t._2.toOption)

  def categoricalStats(fact: Fact): ThriftCategoricalStats =
    new ThriftCategoricalStats(1, List(new ThriftHistogramEntry(FactStatistics.valueToCategory(fact), 1l)).asJava)

  def numericalStats(value: Double): ThriftNumericalStats =
    new ThriftNumericalStats(1, value, value * value)

  def doubleValue(fact: Fact): Option[Double] = PartialFunction.condOpt(fact.value) {
    case IntValue(i)      => implicitly[Numeric[Int]].toDouble(i)
    case LongValue(l)     => implicitly[Numeric[Long]].toDouble(l)
    case DoubleValue(d)   => d
  }

  def keyString(fid: Int, date: Date): String =
    s"${fid}-${date.hyphenated}"

  def newEmitter: TestEmitter[BytesWritable, BytesWritable, (String, ThriftNumericalStats \/ ThriftCategoricalStats)] = {
    val serialiser = ThriftSerialiser()
    TestEmitter((key, value) => {
      val fid: Int = KeyState.getFeatureId(key)
      val date: Date = KeyState.getDate(key)
      val ty: StatsType = KeyState.getType(key)
      val bytes: Array[Byte] = value.copyBytes
      (keyString(fid, date), ty match {
        case Numerical   => serialiser.fromBytesUnsafe(new ThriftNumericalStats, bytes).left
        case Categorical => serialiser.fromBytesUnsafe(new ThriftCategoricalStats, bytes).right
      })
    })
  }
}
