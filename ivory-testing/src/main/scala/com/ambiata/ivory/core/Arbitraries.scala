package com.ambiata.ivory
package core

/**
 * Re-export the arbitraries from ivory-core
 */
trait Arbitraries extends
       ArbitraryDictionaries
  with ArbitraryEncodings
  with ArbitraryFacts
  with ArbitraryFeatures
  with ArbitraryValues
  with ArbitraryMetadata
  with ArbitraryRepositories

trait ArbitraryDictionaries extends arbitraries.ArbitraryDictionaries
trait ArbitraryEncodings    extends arbitraries.ArbitraryEncodings
trait ArbitraryFacts        extends arbitraries.ArbitraryFacts
trait ArbitraryFeatures     extends arbitraries.ArbitraryFeatures
trait ArbitraryValues       extends arbitraries.ArbitraryValues
trait ArbitraryMetadata     extends arbitraries.ArbitraryMetadata
trait ArbitraryRepositories extends arbitraries.ArbitraryRepositories

object Arbitraries           extends Arbitraries
object ArbitraryDictionaries extends ArbitraryDictionaries
object ArbitraryEncodings    extends ArbitraryEncodings
object ArbitraryFacts        extends ArbitraryFacts
object ArbitraryFeatures     extends ArbitraryFeatures
object ArbitraryValues       extends ArbitraryValues
object ArbitraryMetadata     extends ArbitraryMetadata
object ArbitraryRepositories extends ArbitraryRepositories
