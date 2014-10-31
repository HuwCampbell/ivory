package com.ambiata.ivory.operation.extraction.chord

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWindows
import org.specs2.{ScalaCheck, Specification}

class ChordWindowsSpec extends Specification with ScalaCheck { def is = s2"""

  Can update the chord window                                $updateWindows
"""

  def updateWindows = prop { (ds: List[Date], s: Short) =>
    val i = Math.abs(s)
    val dates = ds.map(_.int).toArray
    val windows = new Array[Int](dates.length)
    ChordWindows.updateWindowsForChords(dates, DateTimeUtil.minusDays(_, i), windows)
    windows ==== ds.map(d => Date.fromLocalDate(d.localDate.minusDays(i)).int).toArray
  }
}
