package com.ambiata.ivory.api

/**
 * The ivory "nursery" API forms an exported API for "experimental" or
 * "in-flux" components. The goal of these APIs is to eventually
 * move them to the stable ivory API.
 */
object IvoryNursery {
  /**
   * Repository types
   */
  type Repository = com.ambiata.ivory.core.Repository
  val Repository = com.ambiata.ivory.core.Repository

  type IvoryT[F[+_], +A] = com.ambiata.ivory.storage.control.IvoryT[F, A]
  val IvoryT = com.ambiata.ivory.storage.control.IvoryT
  type IvoryTIO[+A] = com.ambiata.ivory.storage.control.IvoryTIO[A]

  type IvoryRead = com.ambiata.ivory.storage.control.IvoryRead
  val IvoryRead = com.ambiata.ivory.storage.control.IvoryRead
}
