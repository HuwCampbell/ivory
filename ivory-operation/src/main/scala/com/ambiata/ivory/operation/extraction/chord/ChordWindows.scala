package com.ambiata.ivory.operation.extraction.chord

import com.ambiata.ivory.core._

object ChordWindows {

  /** This mutates `windows` with the new set of dates */
  def updateWindowsForChords(chords: Array[Int], window: Date => Date, windows: Array[Int]): Unit = {
    var i = 0
    while(i < chords.length) {
      windows(i) = window(Date.unsafeFromInt(chords(i))).underlying
      i += 1
    }
  }
}
