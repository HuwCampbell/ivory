package com.ambiata.ivory.core

/**
 * A single place to keep all currently "in progress" behaviour to allow for greater visibility.
 *
 * Please remove the relevant functions when they are no longer used.
 */
object NotImplemented {

  def virtualDictionaryFeature: Nothing =
    Crash.error(Crash.CodeGeneration, "Virtual features are not currently supported")

  def unImplementedSyncOperation: Nothing =
    Crash.error(Crash.CodeGeneration, "This is sync operation is not currently implemented")

  def chordSquash(): Unit =
    println("WARNING: Chord is running without a squash, virtual features will not be generated!")
}
