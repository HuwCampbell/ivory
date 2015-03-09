package com.ambiata.ivory.core

/**
 * A single place to keep all currently "in progress" behaviour to allow for greater visibility.
 *
 * Please remove the relevant functions when they are no longer used.
 */
object NotImplemented {

  def unImplementedSyncOperation: Nothing =
    Crash.error(Crash.CodeGeneration, "This is sync operation is not currently implemented")

  def reducerPerformance(name: String): Unit =
    println(s"WARNING: $name has not been optimized")
}
