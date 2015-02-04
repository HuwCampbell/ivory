package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._

import com.ambiata.mundane.io._
import com.ambiata.mundane.data.{Lists => L}
import com.ambiata.poacher.hdfs._

import org.apache.hadoop.fs.Path

object DictionaryOutput {

  /** Turn a dictionary into indexed lines ready to store externally */
  def indexedDictionaryLines(dictionary: Dictionary, missing: Option[String], delim: Char): List[String] = {
    import com.ambiata.ivory.storage.metadata.DictionaryTextStorage
    dictionary.byFeatureIndex.toList.sortBy(_._1).map({
      case (i, d) => i.toString + delim + DictionaryTextStorage.delimitedLineWithDelim(d.featureId -> (d match {
        case Concrete(_, m) =>
          m.copy(tombstoneValue = missing.toList)
        case Virtual(_, vd) =>
          val (source, mode) = dictionary.byFeatureId.get(vd.source).flatMap {
            case Concrete(_, cd) => Some(cd.encoding -> cd.mode)
            case Virtual(_, _)   => None
          }.getOrElse(StringEncoding.toEncoding -> Mode.State)
          ConcreteDefinition(Expression.expressionEncoding(vd.query.expression, source), mode, None, "", missing.toList)
      }), delim.toString)
    })
  }

  def writeToHdfs(output: Path, dictionary: Dictionary, missing: Option[String], delimiter: Char): Hdfs[Unit] =
    Hdfs.writeWith(new Path(output, ".dictionary"), os =>
      Streams.write(os, L.prepareForFile(indexedDictionaryLines(dictionary, missing, delimiter))))
}
