package com.ambiata.ivory.mr

import org.specs2._
import org.specs2.matcher.ThrownExpectations
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.poacher.hdfs._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._

class CommitterSpec extends Specification with ThrownExpectations { def is = s2"""

Committer
-----------

  Commit multiple dirs (targets don't exist)    $e1
  Commit files fails                            $e2
  target dir exists                             $e3
  Commit nested dirs                            $nestedDir

"""

  def e1 =
    commit((ctx, _) =>
        writeFile(new Path(ctx.output, "path1/f1"), "test1") >>
          writeFile(new Path(ctx.output, "path2/f2"), "test2"),
      (target, p) => if (p == "path1") new Path(target, "p1") else new Path(target, "p2"),
      List("p1/f1" -> "test1", "p2/f2" -> "test2")
    ).toEither must beRight

  def e2 =
    commit((ctx, _) => writeFile(new Path(ctx.output, "f1"), "test1"),
      (target, _) => target, Nil).toEither must beLeft

  def e3 =
    commit((ctx, target) =>
      Hdfs.mkdir(target) >> writeFile(new Path(ctx.output, "path1/f1"), "test1"),
      (target, _) => target, List("f1" -> "test1")
    ).toEither must beRight

  def nestedDir =
    commit((ctx, _) => writeFile(new Path(ctx.output, "path1/f1/f2"), "test1"),
      (target, _) => new Path(target, "p1"), List("p1/f1/f2" -> "test1")
    ).toEither must beRight

  def readFile(path: Path): Hdfs[String] =
    Hdfs.readWith(path, is => Streams.read(is))

  def writeFile(path: Path, content: String): Hdfs[Unit] =
    Hdfs.writeWith(path, os => Streams.write(os, content))

  private def commit(pre: (MrContext, Path) => Hdfs[Unit],
                     mapping: (Path, String) => Path,
                     expected: List[(String, String)]): Result[Unit] =
    Temporary.using { dir =>
      val c = new Configuration
      val ctx = MrContext(ContextId.randomContextId)
      val target = new Path(dir.path)
      (for {
        _ <- pre(ctx, target)
        _ <- Committer.commit(ctx, mapping(target, _), cleanup = true)
        _ <- expected.traverse { case (path, e) => readFile(new Path(target, path)).map(_ must_== e)}
      } yield ()).run(c)
    }.run.unsafePerformIO()
}
