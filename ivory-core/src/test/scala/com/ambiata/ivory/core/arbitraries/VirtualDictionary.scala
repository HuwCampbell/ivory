package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._
import Arbitraries._


/** Components that make up a dictionary that has _at least_ one virtual definition */
case class VirtualDictionary(cd: ConcreteDefinition, fid: FeatureId, vd: VirtualDefinition) {
  def withExpression(expression: Expression): VirtualDictionary =
    copy(vd = vd.copy(query = vd.query.copy(expression = expression)))

  def withSource(source: FeatureId): VirtualDictionary =
    copy(vd = vd.copy(source = source))

  def dictionary: Dictionary =
    Dictionary(List(cd.toDefinition(vd.source), vd.toDefinition(fid)))
}

object VirtualDictionary {
  implicit def VirtualDictionaryArbitrary: Arbitrary[VirtualDictionary] = Arbitrary(for {
    cd  <- arbitrary[ConcreteDefinition]
    fid <- arbitrary[FeatureId]
    vd  <- GenDictionary.virtual(fid -> cd, 0)
  } yield VirtualDictionary(cd, vd._1, vd._2))
}
