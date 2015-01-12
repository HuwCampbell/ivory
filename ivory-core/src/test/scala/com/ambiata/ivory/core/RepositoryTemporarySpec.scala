package com.ambiata.ivory.core

import com.ambiata.disorder._
import com.ambiata.notion.core._

import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2._

class RepositoryTemporarySpec extends Specification with ScalaCheck { def is = s2"""

 RepositoryTemporary should clean up its own resources
 =====================================================
   repository     $repository
"""

  def repository = prop((tmp: RepositoryTemporary, id: Ident, data: S) => for {
    r <- tmp.repository
    k = Key.unsafe(id.value)
    _ <- r.store.utf8.write(k, data.value)
    b <- r.store.exists(Repository.root / k)
    _ <- RIO.unsafeFlushFinalizers
    a <- r.store.exists(Repository.root / k)
  } yield b -> a ==== true -> false)
}
