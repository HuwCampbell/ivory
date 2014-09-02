package com.ambiata.ivory.storage

import com.ambiata.mundane.io.{FilePath, Temporary}
import org.scalacheck.{Shrink, Prop, Gen, Arbitrary}
import org.specs2.ScalaCheck

/**
 * Create some ScalaCheck properties where some value type can do some resource clean-up when the property
 * has been executed
 */
trait ScalaCheckResourceProperties { this: ScalaCheck =>

  implicit def arbitraryTemporaryDir: Arbitrary[Temporary] =
    Arbitrary[Temporary](genTemporaryDir)

  def genTemporaryDir = Gen.const(Temporary.directory(FilePath(System.getProperty("java.io.tmpdir", "/tmp")), "temporary")
    .run.unsafePerformIO.toOption.get)

  implicit def temporaryDirCleanUp: CleanUp[Temporary] = new CleanUp[Temporary] {
    def clean(dir: Temporary) =
      dir.clean.run.unsafePerformIO
  }

  def property[T, R](result: T => R)(implicit toProp: (=>R) => Prop, a: Arbitrary[T], s: Shrink[T], c: CleanUp[T]): Prop =
    check1 { t: T =>
      try result(t)
      finally CleanUp(t)
    }

  def property[T1, T2, R](result: (T1, T2) => R)(implicit toProp: (=>R) => Prop,
                                                 a1: Arbitrary[T1], s1: Shrink[T1], c1: CleanUp[T1],
                                                 a2: Arbitrary[T2], s2: Shrink[T2], c2: CleanUp[T2]): Prop =
    check2 { (t1: T1, t2: T2) =>
      try result(t1, t2)
      finally {
        CleanUp(t1)
        CleanUp(t2)
      }
    }

  def property[T1, T2, T3, R](result: (T1, T2, T3) => R)(implicit toProp: (=>R) => Prop,
                                                         a1: Arbitrary[T1], s1: Shrink[T1], c1: CleanUp[T1],
                                                         a2: Arbitrary[T2], s2: Shrink[T2], c2: CleanUp[T2],
                                                         a3: Arbitrary[T3], s3: Shrink[T3], c3: CleanUp[T3]): Prop =
    check3 { (t1: T1, t2: T2, t3: T3) =>
      try result(t1, t2, t3)
      finally {
        CleanUp(t1)
        CleanUp(t2)
        CleanUp(t3)
      }
    }

  def property[T1, T2, T3, T4, R](result: (T1, T2, T3, T4) => R)(implicit toProp: (=>R) => Prop,
                                                                 a1: Arbitrary[T1], s1: Shrink[T1], c1: CleanUp[T1],
                                                                 a2: Arbitrary[T2], s2: Shrink[T2], c2: CleanUp[T2],
                                                                 a3: Arbitrary[T3], s3: Shrink[T3], c3: CleanUp[T3],
                                                                 a4: Arbitrary[T4], s4: Shrink[T4], c4: CleanUp[T4]): Prop =
    check4 { (t1: T1, t2: T2, t3: T3, t4: T4) =>
      try result(t1, t2, t3, t4)
      finally {
        CleanUp(t1)
        CleanUp(t2)
        CleanUp(t3)
        CleanUp(t4)
      }
    }

  def property[T1, T2, T3, T4, T5, R](result: (T1, T2, T3, T4, T5) => R)(implicit toProp: (=>R) => Prop,
                                                                         a1: Arbitrary[T1], s1: Shrink[T1], c1: CleanUp[T1],
                                                                         a2: Arbitrary[T2], s2: Shrink[T2], c2: CleanUp[T2],
                                                                         a3: Arbitrary[T3], s3: Shrink[T3], c3: CleanUp[T3],
                                                                         a4: Arbitrary[T4], s4: Shrink[T4], c4: CleanUp[T4],
                                                                         a5: Arbitrary[T5], s5: Shrink[T5], c5: CleanUp[T5]): Prop =
    check5 { (t1: T1, t2: T2, t3: T3, t4: T4, t5: T5) =>
      try result(t1, t2, t3, t4, t5)
      finally {
        CleanUp(t1)
        CleanUp(t2)
        CleanUp(t3)
        CleanUp(t4)
        CleanUp(t5)
      }
    }
}

/**
 * Type class for entities with a clean-up procedure
 */
trait CleanUp[T] {
  def clean(t: T): Unit
}

object CleanUp {
  implicit def anyCleanup[T]: CleanUp[T] = new CleanUp[T] {
    def clean(t: T) {}
  }

  def apply[T : CleanUp](t: T): Unit =
    implicitly[CleanUp[T]].clean(t)
}
