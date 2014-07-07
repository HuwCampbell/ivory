package com.ambiata.ivory.storage.version

import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.mundane.testing.ResultMatcher._
import org.specs2.Specification
import scalaz.{Store => _, _}, Scalaz._

class VersionSpec extends Specification { def is = s2"""

Version
-------

  Read empty                                     $empty
  Read and write                                 $readWrite

"""

  def empty =
    run(path => Version.read(path)).isError

  def readWrite = run { path =>
    Version.write(path, Version("a")) >> Version.read(path)
  } must beOkValue(Version("a"))

  private def run[A](f: StorePathIO => ResultTIO[A]): Result[A] =
    Temporary.using(dir => f(StorePath(PosixStore(dir), FilePath.root))).run.unsafePerformIO()
}
