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

  object window {
    def to(window: Window): ThriftDictionaryWindow =
      new ThriftDictionaryWindow(window.length, window.unit match {
        case Days   => ThriftDictionaryWindowUnit.DAYS
        case Weeks  => ThriftDictionaryWindowUnit.WEEKS
        case Months => ThriftDictionaryWindowUnit.MONTHS
        case Years  => ThriftDictionaryWindowUnit.YEARS
      })
    def from(window: ThriftDictionaryWindow): Window =
      Window(window.getLength, window.getUnit match {
        case ThriftDictionaryWindowUnit.DAYS   => Days
        case ThriftDictionaryWindowUnit.WEEKS  => Weeks
        case ThriftDictionaryWindowUnit.MONTHS => Months
        case ThriftDictionaryWindowUnit.YEARS  => Years
      })
  }

  object virtual {
    def to(cd: VirtualDefinition): ThriftDictionaryVirtual = {
      val virt = new ThriftDictionaryVirtual(featureId.to(cd.source))
      cd.window.map(window.to).foreach(virt.setWindow)
      virt
    }
    def from(virt: ThriftDictionaryVirtual): String \/ VirtualDefinition =
      featureId.from(virt.getSourceName).map(VirtualDefinition(_, virt.isSetWindow.option(window.from(virt.getWindow))))
  }

  val concrete = new {
    def to(cd: ConcreteDefinition): ThriftDictionaryFeatureMeta = {
      val (encJava, structJava) = encoding.to(cd.encoding)
      val meta = new ThriftDictionaryFeatureMeta(encJava, cd.desc, cd.tombstoneValue.asJava)
      cd.ty.foreach(t => meta.setType(typeBi.to(t)))
      structJava.foreach(meta.setValue)
      meta
    }
    def from(meta: ThriftDictionaryFeatureMeta): ConcreteDefinition =
      ConcreteDefinition(encoding.from(meta), Option(meta.`type`).map(typeBi.from), meta.desc, meta.tombstoneValue.asScala.toList)
  }

  def dictionaryToThrift(dictionary: Dictionary): ThriftDictionary =
    new ThriftDictionary(Map[ThriftDictionaryFeatureId,ThriftDictionaryFeatureMeta]().asJava).setDict(new ThriftDictionaryV2(
      dictionary.definitions.map({
        case Concrete(fid, d) =>
          featureId.to(fid) -> ThriftDictionaryDefinition.concrete(concrete.to(d))
        case Virtual(fid, d) =>
          featureId.to(fid) -> ThriftDictionaryDefinition.virt(virtual.to(d))
      }).toMap.asJava
    ))

  val featureId = new {
    def to(fid: FeatureId): ThriftDictionaryFeatureId =
      new ThriftDictionaryFeatureId(fid.namespace.name, fid.name)
    def from(featureId: ThriftDictionaryFeatureId): String \/ FeatureId =
      Name.nameFromStringDisjunction(featureId.ns).map { namespace =>
        FeatureId(namespace, featureId.name)
      }
  }

  def dictionaryFromThrift(dictionary: ThriftDictionary): String \/ Dictionary = {
    (if (dictionary.isSetDict) {
      dictionary.dict.meta.asScala.toList.traverseU({ case (tfid, meta) => featureId.from(tfid).flatMap(fid =>
        if      (meta.isSetConcrete) Concrete(fid, concrete.from(meta.getConcrete)).right
        else if (meta.isSetVirt)     virtual.from(meta.getVirt).map(x => Virtual(fid, x))
        else                         "Invalid dictionary definition".left)
      }).map(features => Dictionary(features))
    } else {
      dictionary.meta.asScala.toList.traverseU({ case (tfid, meta) =>
        featureId.from(tfid).map(fid => Concrete(fid, concrete.from(meta)))
      }).map(features => Dictionary(features))
    }).leftMap("Can't convert the dictionary from Thrift: " + _)
  }

}
