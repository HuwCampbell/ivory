package com.ambiata.ivory.core

/** The feature store is a prioritized list of factsets. */
case class FeatureStore(id: FeatureStoreId, factsets: List[Prioritized[Factset]]) {
  def unprioritizedIds: List[FactsetId] =
    factsets.map(_.value.id)

  def factsetIds: List[Prioritized[FactsetId]] =
    factsets.map(_.map(_.id))

  def toDataset: List[Prioritized[Dataset]] =
    factsets.map(_.map(Dataset.factset))

  def filterByPartition(pred: Partition => Boolean): FeatureStore =
    FeatureStore(id, factsets.map(_.map(_.filterByPartition(pred))))

  def filterByDate(pred: Date => Boolean): FeatureStore =
    FeatureStore(id, factsets.map(_.map(_.filterByDate(pred))))

  def filterByFactsetId(pred: FactsetId => Boolean): FeatureStore =
    filter(factset => pred(factset.id))

  // FIX what is this? no .get
  def filter(f: Factset => Boolean): FeatureStore =
    FeatureStore.fromList(id, factsets.collect({ case Prioritized(_, factset) if f(factset) => factset })).get

  // FIX what is this? no .get
  def diff(other: FeatureStore): FeatureStore =
    FeatureStore.fromList(id, factsets.map(_.value).diff(other.factsets.map(_.value))).get

  /**
   * Tests whether _this_ feature store contains only a subset of the factsets
   * as the _other_ specified feature store.
   */
  def subsetOf(other: FeatureStore): Boolean =
    unprioritizedIds.toSet.subsetOf(other.unprioritizedIds.toSet)
}

object FeatureStore {
  def fromList(id: FeatureStoreId, factsets: List[Factset]): Option[FeatureStore] =
    Prioritized.fromList(factsets).map(FeatureStore(id, _))
}
