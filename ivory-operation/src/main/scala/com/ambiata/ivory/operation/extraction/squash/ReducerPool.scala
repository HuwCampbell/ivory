package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core.{Date, Expression, Window}
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.operation.extraction.reduction.{DateOffsets, Reduction}
import com.ambiata.ivory.storage.lookup.WindowLookup

trait ReducerPool {

  /**
   * For a range of dates compile the appropriate [[Reduction]]s as defined by the current feature.
   *
   * The returning function takes an offset into the pool.
   */
  def compile(dates: Array[Date]): Int => List[(FeatureReduction, Reduction)]
}

object ReducerPool {

  def createTesting(reductions: List[FeatureReduction], isSet: Boolean): ReducerPool =
    create(reductions, isSet, (_, r) => r)

  def create(reductions: List[FeatureReduction], isSet: Boolean,
             profile: (FeatureReduction, Reduction) => Reduction): ReducerPool = new ReducerPool {
    override def compile(dates: Array[Date]): Int => List[(FeatureReduction, Reduction)] =
      dates.map(date =>
        for {
          fr <- reductions
          // We have to parse it here to work out what the window override might be to create the DateOffsets
          // https://github.com/ambiata/ivory/issues/380
          exp <- Expression.parse(fr.getExpression).toOption
          win  = WindowLookup.fromInt(fr.window)
          // If there is no starting date update the one provided, which will either be a snapshot or chord date
          // This is especially important for chord where it's used to calculate the "latest" date within a window
          srt  = win.map(Window.startingDate(_)(date)).getOrElse(date)
          of   = DateOffsets.calculateLazyCompact(srt, date)
          r   <- Reduction.compile(fr, exp, of, profile(fr, _))
          r2   = if (isSet) new SetReduction(srt, date, r) else new StateReduction(srt, date, r)
        } yield (fr, r2)
      )
  }
}
