package com.ambiata.ivory.mr

import com.ambiata.ivory.mr.thrift._
import com.ambiata.notion.distcopy.Partition

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.Mapper

import scala.collection.JavaConverters._

case class InputSpecification(formatClass: Class[_ <: InputFormat[_, _]], mapperClass: Class[_ <: CombinableMapper[_, _, _, _]], paths: List[Path])

object InputSpecification {
  def toThrift(inputs: List[InputSpecification]): InputSpecificationsThrift = {
    val out = new java.util.ArrayList[InputSpecificationThrift](inputs.size)
    inputs.foreach(i => out.add(new InputSpecificationThrift(i.formatClass.getName, i.mapperClass.getName, i.paths.map(_.toString).asJava)))
    new InputSpecificationsThrift(out)
  }

  def fromThrift(configuration: Configuration, inputs: InputSpecificationsThrift): List[InputSpecification] = {
    inputs.inputs.asScala.map(i => {
      val formatClass = configuration.getClassByName(i.format).asInstanceOf[Class[InputFormat[_, _]]]
      val mapperClass = configuration.getClassByName(i.mapper).asInstanceOf[Class[CombinableMapper[_, _, _, _]]]
      val paths = i.paths.asScala.map(new Path(_)).toList
      InputSpecification(formatClass, mapperClass, paths)
    }).toList
  }
}
