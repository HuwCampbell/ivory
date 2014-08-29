package com.ambiata.ivory.data

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._

object TemporaryStoreRun {

  def run[A](f: PosixStore => ResultTIO[A]): Result[A] =
    Temporary.using(dir => f(PosixStore(dir))).run.unsafePerformIO()

}
