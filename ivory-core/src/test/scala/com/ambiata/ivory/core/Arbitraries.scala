package com.ambiata.ivory
package core

import arbitraries._

/**
 * All arbitraries
 */
trait Arbitraries extends
       ArbitraryDictionaries
  with ArbitraryEncodings
  with ArbitraryFacts
  with ArbitraryFeatures
  with ArbitraryValues
  with ArbitraryMetadata

object Arbitraries extends Arbitraries
