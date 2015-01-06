package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import scalaz._, Scalaz._

class SquashDumpSpec extends Specification with ScalaCheck { def is = s2"""

  Can filter by a concrete feature                                 $filterConcrete
  Can filter by a virtual feature                                  $filterVirtual
  Will filter out everything when not found                        $filterMissing
  Can't lookup a concrete feature from itself                      $lookupConcrete
  Can lookup the concrete feature of a virtual feature             $lookupVirtual
"""

  def filterConcrete = prop((conc: ConcreteGroupFeature, dict: Dictionary) => {
    SquashDump.filterByConcreteOrVirtual(conc.dictionary append dict, Set(conc.fid)) ==== conc.dictionary.right
  })

  def filterVirtual = prop((virt: VirtualDictionary, dict: Dictionary) => {
    SquashDump.filterByConcreteOrVirtual(virt.dictionary append dict, Set(virt.fid)) ==== virt.dictionary.right
  })

  def filterMissing = prop((dict: Dictionary, featureId: FeatureId) => {
    SquashDump.filterByConcreteOrVirtual(dict, Set(featureId)).toEither must beLeft
  })

  def lookupConcrete = prop((virt: VirtualDictionary, dict: Dictionary) => {
    SquashDump.lookupConcreteFromVirtual(virt.dictionary append dict, virt.vd.source) must beNone
  })

  def lookupVirtual = prop((virt: VirtualDictionary, dict: Dictionary) => {
    SquashDump.lookupConcreteFromVirtual(virt.dictionary append dict, virt.fid) must beSome(virt.vd.source)
  })
}
