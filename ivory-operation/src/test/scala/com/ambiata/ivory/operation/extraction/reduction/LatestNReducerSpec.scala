package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}
import com.ambiata.ivory.core.thrift.ThriftFactValue
import scalaz.{Value => _, _}
import scalaz.scalacheck.ScalazArbitrary._

class LatestNReducerSpec extends Specification with ScalaCheck { def is = s2"""
  LatestN reducer works with arbitrary facts                 $latestN
  LatestNStruct reducer works with arbitrary facts           $latestNStruct
  LatestNStruct reducer is consistent for consecutive calls  $latestNStructConsecutive
  LatestNStruct reducer is consistent for interleaved calls  $latestNStructInterleaved

"""

  def latestN = prop((n: Int, l: NonEmptyList[Fact]) => {
    val num = Math.abs(n % 7) + 1
    val xs = l.list.filterNot(_.toThrift.getValue.isSetLst).filterNot(_.toThrift.getValue.isSetT)
    val r = new LatestNReducer(num)

    xs.foreach(r.update)

    Value.fromThrift(r.save) ==== ListValue(xs.reverse.take(num).map(_.value).collect { case x: SubValue => x })
  })

  def latestNStruct = prop((n: Int, l: NonEmptyList[Int]) => {
    val num = Math.abs(n % 7) + 1
    val xs = l.list
    ReducerUtil.run(new LatestNStructReducer[Int](0, num), xs) ==== xs.reverse.take(num)
  })

  def latestNStructConsecutive = prop((n: Int, l: NonEmptyList[Int]) =>
    ReducerUtil.consecutive(new LatestNStructReducer[Int](0, Math.abs(n % 7) + 1), l.list))

  def latestNStructInterleaved = prop((n: Int, l: NonEmptyList[Int], x: NonEmptyList[Int]) =>
    ReducerUtil.interleaved(new LatestNStructReducer[Int](0, Math.abs(n % 7) + 1), l.list, x.list))
}
