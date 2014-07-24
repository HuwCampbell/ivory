package com.ambiata.ivory.storage

import com.ambiata.mundane.control._

package object store {

  type ReferenceIO = Reference[ResultTIO]
  type ReferenceResultT[F[+_]] = Reference[({ type l[a] = ResultT[F, a] })#l]
}
