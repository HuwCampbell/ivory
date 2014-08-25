package com.ambiata.ivory.core.thrift

import com.ambiata.ivory.core._
import scala.collection.JavaConverters._
import scalaz.{Name=>_,_}, Scalaz._, BijectionT._

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

  private val struct = new {
    val to: StructEncoding => ThriftDictionaryStruct = senc => new ThriftDictionaryStruct(senc.values.map {
      case (name, StructEncodedValue(enc, optional)) => new ThriftDictionaryStructMeta(name, primitiveEncoding.to(enc),
        ThriftDictionaryStructMetaOpts.isOptional(optional))
    }.toList.asJava)
    val from: ThriftDictionaryStruct => StructEncoding = enc =>
      StructEncoding(Option(enc.getValues).map(_.asScala).getOrElse(Nil).map {
        meta => meta.getName -> StructEncodedValue(primitiveEncoding.from(meta.getEncoding),
          optional = meta.getOpts.isSetIsOptional && meta.getOpts.getIsOptional)
      }.toMap)
  }

  private val encoding = new {
    val to: Encoding => (ThriftDictionaryEncoding, Option[ThriftDictionaryFeatureValue]) = {
      case enc: PrimitiveEncoding => primitiveEncoding.to(enc) -> None
      case ListEncoding(enc)      =>
        STRUCT -> Some(ThriftDictionaryFeatureValue.listValue(enc match {
          case penc: PrimitiveEncoding => ThriftDictionaryList.encoding(primitiveEncoding.to(penc))
          case senc: StructEncoding    => ThriftDictionaryList.structEncoding(struct.to(senc))
        }))
      case enc: StructEncoding    =>
        STRUCT -> Some(ThriftDictionaryFeatureValue.structValue(struct.to(enc)))
    }
    val from: ThriftDictionaryFeatureMeta => Encoding = meta => meta.getEncoding match {
      case STRUCT if Option(meta.getValue).exists(_.isSetListValue) =>
        ListEncoding(meta.getValue.getListValue match {
          case enc if enc.isSetEncoding       => primitiveEncoding.from(enc.getEncoding)
          case enc if enc.isSetStructEncoding => struct.from(enc.getStructEncoding)
        })
      case STRUCT if Option(meta.getValue).exists(_.isSetStructValue)
                  => struct.from(meta.getValue.getStructValue)
      case STRUCT => StructEncoding(Map()) // Should _never_ happen
      case enc    => primitiveEncoding.from(enc)
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

  def dictionaryToThrift(dictionary: Dictionary): String \/ ThriftDictionary = {
    new ThriftDictionary(
      dictionary.meta.map {
        case (FeatureId(ns, name), FeatureMeta(enc, ty, desc, tombstoneValue)) =>
          val (encJava, structJava) = encoding.to(enc)
          val meta = new ThriftDictionaryFeatureMeta(encJava, desc, tombstoneValue.asJava)
          ty.foreach(t => meta.setType(typeBi.to(t)))
          structJava.foreach(meta.setValue)
          new ThriftDictionaryFeatureId(ns.name, name) -> meta
        case (FeatureId(ns, name), FeatureVirtual(FeatureId(ans, aname))) =>
          NotImplemented.virtualDictionaryFeature
      }.asJava).right
  }

  def dictionaryFromThrift(dictionary: ThriftDictionary): String \/ Dictionary = {
    dictionary.meta.asScala.toList.map { case (featureId, meta) =>
      Name.nameFromStringDisjunction(featureId.ns).map { namespace =>
        FeatureId(namespace, featureId.name) ->
          FeatureMeta(encoding.from(meta), Option(meta.`type`).map(typeBi.from), meta.desc, meta.tombstoneValue.asScala.toList)
      }
    }.sequenceU.map(features => Dictionary(features.toMap)).leftMap("Can't convert the dictionary from Thrift: "+_)
  }

}
