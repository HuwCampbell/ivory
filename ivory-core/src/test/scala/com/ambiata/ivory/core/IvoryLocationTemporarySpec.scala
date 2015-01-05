package com.ambiata.ivory.core

import com.ambiata.disorder._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2._
import org.specs2.matcher.Parameters

class IvoryLocationTemporarySpec extends Specification with ScalaCheck { def is = s2"""

 IvoryLocationTemporary should clean up its own resources
 ========================================================
   files                   $file
   directory               $directory
"""
  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 10, workers = 3)

  def file = prop((tmp: IvoryLocationTemporary, data: S) => for {
    f <- tmp.file
    _ <- IvoryLocation.writeUtf8(f, data.value)
    b <- IvoryLocation.exists(f)
    _ <- RIO.unsafeFlushFinalizers
    a <- IvoryLocation.exists(f)
  } yield b -> a ==== true -> false)

  def directory = prop((tmp: IvoryLocationTemporary, id: Ident, data: S) => for {
    f <- tmp.directory
    z = f </> FilePath.unsafe(id.value)
    _ <- IvoryLocation.writeUtf8(z, "asd")
    b <- IvoryLocation.exists(f)
    _ <- RIO.unsafeFlushFinalizers
    a <- IvoryLocation.exists(f)
  } yield b -> a ==== true -> false)

}
