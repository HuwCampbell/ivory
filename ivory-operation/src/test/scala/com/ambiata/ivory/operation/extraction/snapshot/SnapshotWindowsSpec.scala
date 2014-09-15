package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class SnapshotWindowsSpec extends Specification with ScalaCheck { def is = s2"""

  Dictionary with at least one virtual definition has at least one window   $someWindow
  Dictionary with two virtual definitions from one source combine           $combinedWindow
  SnapshotWindow byNamespace always contains all concrete namespaces        $byNamespace
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
}
