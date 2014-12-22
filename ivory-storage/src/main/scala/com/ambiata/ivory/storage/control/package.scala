package com.ambiata.ivory.storage

import com.ambiata.mundane.control._

package object control {
  type IvoryTIO[A] = IvoryT[RIO, A]
  type RepositoryTIO[A] = RepositoryT[RIO, A]
}
