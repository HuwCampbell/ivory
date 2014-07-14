package com.ambiata.ivory.mr

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.conf.Configuration
import java.util.UUID

import com.ambiata.ivory.alien.hdfs._

import scalaz.syntax.bind._

case class ContextId(value: String)

object ContextId {
  def namespacedContextId(namespace: String): ContextId =
    ContextId(s"${namespace}-${UUID.randomUUID.toString}")

  def randomContextId: ContextId =
    ContextId(UUID.randomUUID.toString)
}

/**
 * This is used to handle tmp paths for output and dist cache
 */
case class MrContext(id: ContextId) {
  val tmpBase: Path =
    new Path(s"/tmp/${id.value}")

  val output: Path =
    new Path(tmpBase, "output")

  val distCache: DistCache =
    DistCache(new Path(tmpBase, "dist-cache"), id)

  val thriftCache: ThriftCache =
    ThriftCache(new Path(tmpBase, "dist-cache-thrift"), id)

  def cleanup: Hdfs[Unit] =
    Hdfs.deleteAll(tmpBase)
}

object MrContext {
  /**
   * Create a new MrContext from the namespace and job. This will mutate
   * the job configuration to set the context id so it can be re-created
   * again from the configuration object.
   */
  def newContext(namespace: String, job: Job): MrContext = {
    val id = ContextId.namespacedContextId(namespace)
    job.getConfiguration.set(MrContext.Keys.Id, id.value)
    MrContext(id)
  }

  def fromConfiguration(conf: Configuration): MrContext =
    MrContext(ContextId(conf.get(MrContext.Keys.Id)))

  /**
   * WARNING: This method uses reflection and should _only_ be used in setup methods (unless we find an alternative)
   */
  def getSplitPath(split: InputSplit): Path = {
    // Evil reflection, but unfortunately doing this via a package-protected class would fail in local mode
    (if (split.getClass.getSimpleName == "TaggedInputSplit") {
      Option(split.getClass.getDeclaredMethod("getInputSplit")).flatMap { method =>
        scala.util.Try {
          method.setAccessible(true)
          method.invoke(split).asInstanceOf[InputSplit]
        }.toOption
      }.getOrElse(split)
    } else split).asInstanceOf[FileSplit].getPath
  }

  object Keys {
    val Id = "ivory.ctx.id"
  }
}
