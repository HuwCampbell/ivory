package com.ambiata.ivory.storage

import com.ambiata.mundane.control._

package object store {

  type StorePathIO = StorePath[ResultTIO]
  type StorePathResultT[F[+_]] = StorePath[({ type l[a] = ResultT[F, a] })#l]
}
