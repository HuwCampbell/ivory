package com.ambiata.ivory.scoobi

import com.ambiata.ivory.core.{HdfsIvoryLocation, IvoryLocation}
import com.ambiata.mundane.control._
import com.ambiata.poacher.scoobi.ScoobiAction
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{BytesWritable, NullWritable, SequenceFile}

import scalaz.effect._

object SequenceUtil {

  // TODO When we upgrade to newer version of scalaz we shouldn't need this anymore...
  implicit def HadoopSequenceFileWriter: Resource[SequenceFile.Writer] = new Resource[SequenceFile.Writer] {
    def close(w: SequenceFile.Writer) = IO(w.close())
  }

  def writeBytes(location: HdfsIvoryLocation, codec: Option[CompressionCodec])(f: (Array[Byte] => Unit) => ResultTIO[Unit]): ScoobiAction[Unit] = for {
    conf <- ScoobiAction.scoobiConfiguration
    _    <- ScoobiAction.fromResultTIO {
      val opts = List(
        SequenceFile.Writer.file(location.toHdfsPath),
        SequenceFile.Writer.keyClass(classOf[NullWritable]),
        SequenceFile.Writer.valueClass(classOf[BytesWritable])
      ) ++ codec.map {
        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, _)
      }
      ResultT.using(ResultT.safe(SequenceFile.createWriter(conf.configuration, opts: _*))) {
        writer => f(bytes => writer.append(NullWritable.get, new BytesWritable(bytes)))
      }
    }
  } yield ()
}
