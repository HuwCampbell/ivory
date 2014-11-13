package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFactValue
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.mr.Counter
import com.ambiata.ivory.operation.extraction.reduction.Reduction
import scalaz.Monoid

case class SquashStats(counts: Map[String, SquashCounts])
case class SquashCounts(countTotal: Long, countSave: Long, profileTotal: Long, profileSave: Long)

object SquashStats {

  def asPsvLines(p: SquashStats): List[String] =
    p.counts.toList.sortBy(_._1.toString).map {
      case (fid, count) => List(fid, count.countTotal, count.countSave, count.profileTotal, count.profileSave).mkString("|")
    }
}

object SquashCounts {

  implicit def SquashCountsMonoid: Monoid[SquashCounts] = new Monoid[SquashCounts] {
    def zero: SquashCounts =
      SquashCounts(0, 0, 0, 0)
    def append(f1: SquashCounts, f2: => SquashCounts): SquashCounts =
      SquashCounts(f1.countTotal + f2.countTotal, f1.countSave + f2.countSave,
        f1.profileTotal + f2.profileTotal, f1.profileSave + f2.profileSave)
  }
}

class SquashProfiler(mod: Int, updates: String => Counter, saves: String => Counter, profile: String => Counter, profileSaves: String => Counter) {

  def wrap(fr: FeatureReduction, reduction: Reduction): Reduction =
    // Completely disable the profiler
    if (mod == 0) reduction
    else new SquashProfileReduction(mod, reduction, updates(fr.getName), saves(fr.getName), profile(fr.getName),
      profileSaves(fr.getName))
}

class SquashProfileReduction(mod: Int, reduction: Reduction, updates: Counter, saves: Counter, profile: Counter,
                             profileSaves: Counter) extends Reduction {

  var count = 0
  var profileOnSave = false

  def clear(): Unit =
    // Just in case it wasn't clear (ha ha) we _don't_ want to reset the count here
    reduction.clear()

  def update(f: Fact): Unit = {
    updates.count(1)
    if (count % mod == 0) {
      profileOnSave = true
      val before = System.nanoTime
      reduction.update(f)
      val after = System.nanoTime
      profile.count((after - before).toInt)
    } else {
      reduction.update(f)
    }
    count += 1
  }

  def skip(f: Fact, reason: String): Unit =
    reduction.skip(f, reason)

  def save: ThriftFactValue = {
    saves.count(1)
    // We could also have a separate sample rate, but this saves having two different values to worry about
    if (profileOnSave) {
      profileOnSave = false
      val before = System.nanoTime
      val result = reduction.save
      val after = System.nanoTime
      profileSaves.count((after - before).toInt)
      result
    } else
      reduction.save
  }
}
