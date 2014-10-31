package com.ambiata.ivory.core

/**
 * A single place to keep all currently "in progress" behaviour to allow for greater visibility.
 *
 * Please remove the relevant functions when they are no longer used.
 */
object NotImplemented {

  def unImplementedSyncOperation: Nothing =
    Crash.error(Crash.CodeGeneration, "This is sync operation is not currently implemented")

  def chordSquash(): Unit =
    println("WARNING: Chord is running without a squash, virtual features will not be generated!")

  def chordWindow(): Unit =
    println("WARNING: Chord is running with windowing, which isn't fully implemented yet. " +
      "Unless this is a test you have done something VERY wrong!!!")

  def reducerPerformance(name: String): Unit =
    println(s"WARNING: $name has not been optimized")
}
