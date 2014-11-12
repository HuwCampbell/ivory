package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._
import org.scalacheck._, Arbitrary._
import Arbitraries._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

/** Helpful wrapper around [[ConcreteGroup]] */
case class ConcreteGroupFeature(fid: FeatureId, cg: ConcreteGroup) {
  def withExpression(expression: Expression): ConcreteGroupFeature =
    copy(cg = cg.copy(virtual = cg.virtual.map(vd => vd._1 -> vd._2.copy(query = vd._2.query.copy(expression = expression)))))

  def dictionary: Dictionary =
    Dictionary(cg.definition.toDefinition(fid) :: cg.virtual.map(vd => vd._2.toDefinition(vd._1)))
}

object ConcreteGroupFeature {
  implicit def ConcreteGroupFeatureArbitrary: Arbitrary[ConcreteGroupFeature] = Arbitrary(for {
    fid <- arbitrary[FeatureId]
    cd  <- arbitrary[ConcreteDefinition]
    vi <- Gen.choose(0, 3)
    vd <- (0 until vi).zipWithIndex.map(_._2).toList.traverse(GenDictionary.virtual(fid -> cd, _))
  } yield ConcreteGroupFeature(fid, ConcreteGroup(cd, vd.toMap.mapValues(_.copy(source = fid)).toList)))
}
