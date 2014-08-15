package com.ambiata.ivory.core

/** The feature store is a prioritized list of factsets. */
case class FeatureStore(id: FeatureStoreId, factsets: List[Prioritized[Factset]]) {
  def factsetIds: List[Prioritized[FactsetId]] =
    factsets.map(_.map(_.id))

  def filter(f: Factset => Boolean): FeatureStore =
    FeatureStore.fromList(id, factsets.collect({ case Prioritized(_, factset) if f(factset) => factset })).get

  def diff(other: FeatureStore): FeatureStore =
    FeatureStore.fromList(id, factsets.map(_.value).diff(other.factsets.map(_.value))).get
}

object FeatureStore {
  def fromList(id: FeatureStoreId, factsets: List[Factset]): Option[FeatureStore] =
    Prioritized.fromList(factsets).map(FeatureStore(id, _))
}
