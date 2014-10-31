package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class SnapshotWindowsSpec extends Specification with ScalaCheck { def is = s2"""

  Dictionary with at least one virtual definition has at least one window   $someWindow
  Dictionary with two virtual definitions from one source combine           $combinedWindow
  SnapshotWindow byNamespace always contains all concrete namespaces        $byNamespace
  Starting date edge cases                                                  $startingDateEdgeCases
"""

  import SnapshotWindows._

  def someWindow = prop((dict: Dictionary, vdict: VirtualDictionaryWindow, date: Date) => {
    planWindow(dict append vdict.vd.dictionary, date).windows must not(beEmpty)
  })

  def combinedWindow = prop((vdict: VirtualDictionary, vdict2: VirtualDictionaryWindow, date: Date) => {
    planWindow(vdict.dictionary append vdict2.vd.withSource(vdict.vd.source).dictionary, date).windows must haveLength(1)
  })

  def byNamespace = prop((dict: Dictionary, date: Date) => {
    planWindow(dict, date).byNamespace.keySet ==== dict.byConcrete.sources.keySet.map(_.namespace)
  })

  // It's a little hard to write general property tests for this
  def startingDateEdgeCases =
    seqToResult(List(
      (Window(1, Days),   Date(2012, 3, 1),  Date(2012, 2, 29), "day: leap year"),
      (Window(1, Weeks),  Date(2012, 3, 7),  Date(2012, 2, 29), "week: leap year"),
      (Window(1, Months), Date(2011, 3, 30), Date(2011, 2, 28), "month: non leap year"),
      (Window(1, Months), Date(2012, 3, 30), Date(2012, 2, 29), "month: leap year"),
      (Window(1, Years),  Date(2012, 2, 29), Date(2011, 2, 28), "year: from leap year"),
      (Window(1, Years),  Date(2013, 2, 28), Date(2012, 2, 28), "year: to leap year"),
      (Window(1, Years),  Date(2014, 2, 28), Date(2013, 2, 28), "year: not leap year")
    ).map {
      case (window, start, e, reason) => (Window.startingDate(window)(start) ==== e).setMessage(reason)
    })
}
