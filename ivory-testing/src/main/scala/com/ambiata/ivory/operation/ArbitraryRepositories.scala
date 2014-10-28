package com.ambiata.ivory.operation

/**
 * Re-export the arbitraries to create repositories from ivory-operation
 */
trait ArbitraryRepositories extends arbitraries.ArbitraryRepositories
object ArbitraryRepositories extends ArbitraryRepositories
