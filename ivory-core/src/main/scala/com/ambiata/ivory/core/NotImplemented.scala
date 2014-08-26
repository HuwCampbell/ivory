package com.ambiata.ivory.core

/**
 * A single place to keep all currently "in progress" behaviour to allow for greater visibility.
 *
 * Please remove the relevant functions when they are no longer used.
 */
object NotImplemented {

  def virtualDictionaryFeature: Nothing =
    sys.error("Virtual features are not currently supported")

  def generate: Nothing =
    sys.error("Generate needs some love")
}
