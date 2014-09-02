package com.ambiata.ivory.storage

import com.ambiata.mundane.io.{FilePath, Temporary}
import org.scalacheck.{Shrink, Prop, Gen, Arbitrary}
import org.specs2.ScalaCheck
import scalaz.effect.{IO, Resource}
import scalaz._, Scalaz._

/**
 * Create some ScalaCheck properties where some value type can do some resource clean-up when the property
 * has been executed
 *
 * Each 'managed' function takes a function with 2 parameter sets where the first parameters have Resource instances
 * to clean up resources after the execution
 *
 */
trait ScalaCheckManagedProperties { this: ScalaCheck =>

  def managed[T, R](result: T => R)(implicit toProp: (=>R) => Prop, a: Arbitrary[T], s: Shrink[T], c: Resource[T]): Prop =
    check1 { t: T =>
      try result(t)
      finally Resource[T].close(t).unsafePerformIO
    }

  def managed[T1, T2, R](result: T1 => T2 => R)(implicit toProp: (=>R) => Prop,
                                                 a1: Arbitrary[T1], s1: Shrink[T1], c1: Resource[T1],
                                                 a2: Arbitrary[T2], s2: Shrink[T2]): Prop =
    check2 { (t1: T1, t2: T2) =>
      try result(t1)(t2)
      finally {
        Resource[T1].close(t1).unsafePerformIO
      }
    }

  def managed[T1, T2, T3, R](result: T1 => (T2, T3) => R)(implicit toProp: (=>R) => Prop,
                                                            a1: Arbitrary[T1], s1: Shrink[T1], c1: Resource[T1],
                                                            a2: Arbitrary[T2], s2: Shrink[T2],
                                                            a3: Arbitrary[T3], s3: Shrink[T3]): Prop =
    check3 { (t1: T1, t2: T2, t3: T3) =>
      try result(t1)(t2, t3)
      finally {
        Resource[T1].close(t1).unsafePerformIO
      }
    }

  def managed[T1, T2, T3, T4, R](result: T1 => (T2, T3, T4) => R)(implicit toProp: (=>R) => Prop,
                                                                 a1: Arbitrary[T1], s1: Shrink[T1], c1: Resource[T1],
                                                                 a2: Arbitrary[T2], s2: Shrink[T2],
                                                                 a3: Arbitrary[T3], s3: Shrink[T3],
                                                                 a4: Arbitrary[T4], s4: Shrink[T4]): Prop =
    check4 { (t1: T1, t2: T2, t3: T3, t4: T4) =>
      try result(t1)(t2, t3, t4)
      finally {
        Resource[T1].close(t1).unsafePerformIO
      }
    }

  def managed[T1, T2, T3, T4, T5, R](result: T1 => (T2, T3, T4, T5) => R)(implicit toProp: (=>R) => Prop,
                                                                         a1: Arbitrary[T1], s1: Shrink[T1], c1: Resource[T1],
                                                                         a2: Arbitrary[T2], s2: Shrink[T2],
                                                                         a3: Arbitrary[T3], s3: Shrink[T3],
                                                                         a4: Arbitrary[T4], s4: Shrink[T4],
                                                                         a5: Arbitrary[T5], s5: Shrink[T5]): Prop =
    check5 { (t1: T1, t2: T2, t3: T3, t4: T4, t5: T5) =>
      try result(t1)(t2, t3, t4, t5)
      finally {
        Resource[T1].close(t1).unsafePerformIO
      }
    }

  def managed[T1, T2, T3, R](result: (T1, T2) => T3 => R)(implicit toProp: (=>R) => Prop,
                                                           a1: Arbitrary[T1], s1: Shrink[T1], c1: Resource[T1],
                                                           a2: Arbitrary[T2], s2: Shrink[T2], c2: Resource[T2],
                                                           a3: Arbitrary[T3], s3: Shrink[T3]): Prop =
    check3 { (t1: T1, t2: T2, t3: T3) =>
      try result(t1, t2)(t3)
      finally {
        (Resource[T1].close(t1) ensuring
         Resource[T2].close(t2)).unsafePerformIO
      }
    }

  def managed[T1, T2, T3, T4, R](result: (T1, T2) => (T3, T4) => R)(implicit toProp: (=>R) => Prop,
                                                                   a1: Arbitrary[T1], s1: Shrink[T1], c1: Resource[T1],
                                                                   a2: Arbitrary[T2], s2: Shrink[T2], c2: Resource[T2],
                                                                   a3: Arbitrary[T3], s3: Shrink[T3],
                                                                   a4: Arbitrary[T4], s4: Shrink[T4]): Prop =
    check4 { (t1: T1, t2: T2, t3: T3, t4: T4) =>
      try result(t1, t2)(t3, t4)
      finally {
        (Resource[T1].close(t1) ensuring
         Resource[T2].close(t2)).unsafePerformIO
      }
    }

  def managed[T1, T2, T3, T4, T5, R](result: (T1, T2) => (T3, T4, T5) => R)(implicit toProp: (=>R) => Prop,
                                                                           a1: Arbitrary[T1], s1: Shrink[T1], c1: Resource[T1],
                                                                           a2: Arbitrary[T2], s2: Shrink[T2], c2: Resource[T2],
                                                                           a3: Arbitrary[T3], s3: Shrink[T3],
                                                                           a4: Arbitrary[T4], s4: Shrink[T4],
                                                                           a5: Arbitrary[T5], s5: Shrink[T5]): Prop =
    check5 { (t1: T1, t2: T2, t3: T3, t4: T4, t5: T5) =>
      try result(t1, t2)(t3, t4, t5)
      finally {
        (Resource[T1].close(t1) ensuring
          Resource[T2].close(t2)).unsafePerformIO
      }
    }

  def managed[T1, T2, T3, T4, T5, T6, R](result: (T1, T2) => (T3, T4, T5, T6) => R)(implicit toProp: (=>R) => Prop,
                                                                                 a1: Arbitrary[T1], s1: Shrink[T1], c1: Resource[T1],
                                                                                 a2: Arbitrary[T2], s2: Shrink[T2], c2: Resource[T2],
                                                                                 a3: Arbitrary[T3], s3: Shrink[T3],
                                                                                 a4: Arbitrary[T4], s4: Shrink[T4],
                                                                                 a5: Arbitrary[T5], s5: Shrink[T5],
                                                                                 a6: Arbitrary[T6], s6: Shrink[T6]): Prop =
    check6 { (t1: T1, t2: T2, t3: T3, t4: T4, t5: T5, t6: T6) =>
      try result(t1, t2)(t3, t4, t5, t6)
      finally {
        (Resource[T1].close(t1) ensuring
          Resource[T2].close(t2)).unsafePerformIO
      }
    }

  /**
   * Arbitrary for a temporary directory.
   *
   * The generator returns a new directory each time it is invoked
   */
  implicit def ArbitraryTemporaryDir: Arbitrary[Temporary] =
    Arbitrary[Temporary](genTemporaryDir)

  def genTemporaryDir =
    Gen.wrap(Gen.const(Temporary.directory(FilePath(System.getProperty("java.io.tmpdir", "/tmp")), "temporary")
      .run.unsafePerformIO.toOption.get))

  /**
   * Resource type instance for a Temporary. Should be moved to mundane
   */
  implicit def temporaryDirResource: Resource[Temporary] = new Resource[Temporary] {
    def close(dir: Temporary): IO[Unit] =
      dir.clean.run.void
  }



}
