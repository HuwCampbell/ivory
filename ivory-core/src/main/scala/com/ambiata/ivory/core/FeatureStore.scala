package com.ambiata.ivory.core

import scalaz._, Scalaz._, \&/._
import scalaz.effect._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._

/** The feature store is simply an ordered list of path references to fact sets. */
case class FeatureStore(factSets: List[FactSet]) {
  def +++(other: FeatureStore): FeatureStore =
    FeatureStore(FactSets.concat(factSets, other.factSets))

  def ---(other: FeatureStore): FeatureStore =
    FeatureStore(FactSets.diff(factSets, other.factSets))
}
