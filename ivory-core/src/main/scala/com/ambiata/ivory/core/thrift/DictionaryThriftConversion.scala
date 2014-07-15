package com.ambiata.ivory.core.thrift

import com.ambiata.ivory.core._
import scala.collection.JavaConverters._
import scala.util.Try
import scalaz._, Scalaz._, BijectionT._

object DictionaryThriftConversion {

  import ThriftDictionaryEncoding._

  // Not a true bijective due to the struct encoding in thrift
  private val primitiveEncoding = new {
    val encodings = List(
      BooleanEncoding -> BOOLEAN,
      IntEncoding -> INT,
      LongEncoding -> LONG,
      DoubleEncoding -> DOUBLE,
      StringEncoding -> STRING
    )
    val to: PrimitiveEncoding => ThriftDictionaryEncoding =
      enc => encodings.find(_._1 == enc).get._2
    // Technically we can get an error here - consumers need to deal with structs separately/first
    val from: ThriftDictionaryEncoding => PrimitiveEncoding =
      enc => encodings.find(_._2 == enc).get._1
  }

  private val encoding = new {
    val to: Encoding => (ThriftDictionaryEncoding, Option[ThriftDictionaryStruct]) = {
      case enc: PrimitiveEncoding => primitiveEncoding.to(enc) -> None
      case StructEncoding(v) => STRUCT -> Some(new ThriftDictionaryStruct(v.map {
        case (name, StructValue(enc, optional)) => new ThriftDictionaryStructMeta(name, primitiveEncoding.to(enc),
          ThriftDictionaryStructMetaOpts.isOptional(optional))
      }.toList.asJava))
    }
    val from: ThriftDictionaryFeatureMeta => Encoding = meta => meta.getEncoding match {
      case STRUCT => StructEncoding(Option(meta.getValue).flatMap(v => Try(v.getStructValue).toOption).map(_.getValues.asScala).getOrElse(Nil).map {
        meta => meta.getName -> StructValue(primitiveEncoding.from(meta.getEncoding),
          optional = Try(meta.getOpts.getIsOptional).toOption.getOrElse(false))
      }.toMap)
      case enc => primitiveEncoding.from(enc)
    }
  }

  import ThriftDictionaryType._

  private val typeBi = bijection[Id, Id, Type, ThriftDictionaryType]({
    case BinaryType => BINARY
    case CategoricalType => CATEGORICAL
    case ContinuousType => CONTINOUS
    case NumericalType => NUMERICAL
  }, {
    case BINARY => BinaryType
    case CATEGORICAL => CategoricalType
    case CONTINOUS => ContinuousType
    case NUMERICAL => NumericalType
  })

  val dictionary = bijection[Id, Id, Dictionary, ThriftDictionary](
    dict =>
      new ThriftDictionary(
        dict.meta.map {
          case (FeatureId(ns, name), FeatureMeta(enc, ty, desc, tombstoneValue)) =>
            val (encJava, structJava) = encoding.to(enc)
            val meta = new ThriftDictionaryFeatureMeta(encJava, typeBi.to(ty), desc, tombstoneValue.asJava)
            structJava.foreach(s => meta.setValue(ThriftDictionaryFeatureValue.structValue(s)))
            new ThriftDictionaryFeatureId(ns, name) -> meta
        }.asJava),

    dict =>
      Dictionary(dict.meta.asScala.toMap.map {
          case (featureId, meta) => FeatureId(featureId.ns, featureId.name) ->
            FeatureMeta(encoding.from(meta), typeBi.from(meta.`type`), meta.desc, meta.tombstoneValue.asScala.toList)
        })
  )
}
