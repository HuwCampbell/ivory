package com.ambiata.ivory.storage.manifest

import com.ambiata.ivory.core.IvoryLocationTemporary._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core._
import com.ambiata.ivory.storage.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._

class ManifestIOSpec extends Specification with ScalaCheck { def is = s2"""

  Symmetric read / write                       $symmetric
  Exists flips from false to true after write  $exists

"""

  def symmetric =
    prop((n: Int, t: TemporaryType) =>
      withIvoryLocationDir(t)(l => {
        val io = ManifestIO[Int](l)
        io.write(n) >> io.read
      }) must beOkValue(n.some))

  def exists =
    prop((n: Int, t: TemporaryType) =>
      withIvoryLocationDir(t)(l => {
        val io = ManifestIO[Int](l)
        for {
          b <- io.exists
          _ <- io.write(n)
          a <- io.exists
        } yield b -> a
      }) must beOkValue(false -> true))
}
