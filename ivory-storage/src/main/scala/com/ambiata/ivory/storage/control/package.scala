package com.ambiata.ivory.storage

import com.ambiata.mundane.control._

package object control {
  type IvoryTIO[A] = IvoryT[ResultTIO, A]
  type RepositoryTIO[A] = RepositoryT[ResultTIO, A]
}
