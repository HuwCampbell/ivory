package com.ambiata.ivory.storage.repository

import scalaz._, effect.IO
import com.ambiata.saws.core._
import com.ambiata.poacher.scoobi._
import com.ambiata.mundane.control._
import com.nicta.scoobi.core.ScoobiConfiguration

/** This is a set of interfaces used to represent rank-n functions to force different computations to RIO[_]  */

trait ScoobiRun {
  def runScoobi[A](action: ScoobiAction[A]): RIO[A]
}

trait S3Run extends ScoobiRun {
  def runScoobi[A](action: ScoobiAction[A]): RIO[A]
  def runS3[A](action: S3Action[A]): RIO[A]
}

object ScoobiRun {
  def apply(c: ScoobiConfiguration): ScoobiRun = new ScoobiRun {
    def runScoobi[A](action: ScoobiAction[A]) = action.run(c)
  }
}

object S3Run {
  def apply(c: ScoobiConfiguration): S3Run = new S3Run {
    def runScoobi[A](action: ScoobiAction[A]) = action.run(c)
    def runS3[A](action: S3Action[A]): RIO[A] = action.eval

  }
}
