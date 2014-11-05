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
  def compile(dates: Array[Date]): Int => List[(FeatureReduction, Date, Reduction)]
}

object ReducerPool {

  def create(reductions: List[FeatureReduction], profile: (FeatureReduction, Reduction) => Reduction): ReducerPool = new ReducerPool {
    override def compile(dates: Array[Date]): Int => List[(FeatureReduction, Date, Reduction)] =
      dates.map(date =>
        for {
          fr <- reductions
          // We have to parse it here to work out what the window override might be to create the DateOffsets
          // https://github.com/ambiata/ivory/issues/380
          exp <- Expression.parse(fr.getExpression).toOption
          win  = WindowLookup.fromInt(fr.window)
          dove = Reduction.getWindowOverride(exp)
          // If no window is specified the only functions we should be applying will deal with a single value,
          // and should _always_ apply; hence the min date
          srt  = (dove orElse win.map(Window.startingDate(_)(date))).getOrElse(Date.minValue)
          of   = DateOffsets.calculateLazyCompact(srt, date)
          r   <- Reduction.compile(fr, exp, of, profile(fr, _))
        } yield (fr, srt, r)
      )
  }
}
