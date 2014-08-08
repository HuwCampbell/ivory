package com.ambiata.ivory.core

/** The feature store is simply an ordered list of path references to fact sets. */
case class FeatureStore(factsets: List[PrioritizedFactset]) {

  /**
   * add 2 feature stores so that their factsets are prioritized
   */
  def concat(other: FeatureStore): FeatureStore =
    FeatureStore(PrioritizedFactset.concat(factsets, other.factsets))

  /** @return a FeatureStore having only the sets accepted by the predicate */
  def filter(predicate: FactsetId => Boolean): FeatureStore =
    FeatureStore(factsets.filter(set => predicate(set.factsetId)))

  /** @return a FeatureStore that contains everything in this minus what is in other */
  def diff(other: FeatureStore): FeatureStore =
    FeatureStore(PrioritizedFactset.diff(this.factsets, other.factsets))
}
