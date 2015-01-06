package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core._

case class RenamePlan(commit: Commit, datasets: Datasets)

/**
 * This planner is responsible for producing the minimal set of data to read for a
 * rename operation.
 */
object RenamePlan {
  /**
   * Determine the plan datasets for the given commit using an in memory
   * strategy.
   */
  def inmemory(
    commit: Commit
  ): RenamePlan =
    ???
}
