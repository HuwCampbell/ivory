package com.ambiata.ivory.core.thrift

import com.ambiata.ivory.core._
import scala.collection.JavaConverters._
import scalaz.{Value => _, _}, Scalaz._, BijectionT._

object DictionaryThriftConversion {

  import ThriftDictionaryEncoding._
  import ThriftDictionaryMode._

  // Not a true bijective due to the struct encoding in thrift
  private val primitiveEncoding = new {
    val encodings = List(
      BooleanEncoding -> BOOLEAN,
      IntEncoding -> INT,
      LongEncoding -> LONG,
      DoubleEncoding -> DOUBLE,
      StringEncoding -> STRING,
      DateEncoding -> DATE
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
    val to: Encoding => (ThriftDictionaryEncoding, Option[ThriftDictionaryFeatureValue]) = _.fold(
      enc => primitiveEncoding.to(enc) -> None,
      enc =>
        STRUCT -> Some(ThriftDictionaryFeatureValue.structValue(struct.to(enc))),
      enc =>
        STRUCT -> Some(ThriftDictionaryFeatureValue.listValue(enc.encoding match {
          case SubPrim(penc) => ThriftDictionaryList.encoding(primitiveEncoding.to(penc))
          case SubStruct(senc) => ThriftDictionaryList.structEncoding(struct.to(senc))
        }))
    )
    val from: ThriftDictionaryFeatureMeta => Encoding = meta => meta.getEncoding match {
      case STRUCT if Option(meta.getValue).exists(_.isSetListValue) =>
        EncodingList(ListEncoding(meta.getValue.getListValue match {
          case enc if enc.isSetEncoding       => SubPrim(primitiveEncoding.from(enc.getEncoding))
          case enc if enc.isSetStructEncoding => SubStruct(struct.from(enc.getStructEncoding))
        }))
      case STRUCT if Option(meta.getValue).exists(_.isSetStructValue)
                  => struct.from(meta.getValue.getStructValue).toEncoding
      case STRUCT => StructEncoding(Map()).toEncoding // Should _never_ happen
      case enc    => primitiveEncoding.from(enc).toEncoding
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

  object modeBi {
    def to(mode: Mode): ThriftDictionaryModeV2 = mode match {
      case Mode.State => ThriftDictionaryModeV2.mode(STATE)
      case Mode.Set => ThriftDictionaryModeV2.mode(SET)
      case Mode.KeyedSet(key) => ThriftDictionaryModeV2.keyedSet(key)
    }

    def from(moveV1: Option[ThriftDictionaryMode], modeV2: Option[ThriftDictionaryModeV2]): Mode = {
      def fromV1(m: ThriftDictionaryMode): Mode =
        m match {
          case STATE => Mode.State
          case SET => Mode.Set
        }
      modeV2
        .map(m => if (m.isSetKeyedSet) Mode.keyedSet(m.getKeyedSet) else fromV1(m.getMode))
        .orElse(moveV1.map(fromV1))
        .getOrElse(Mode.State)
    }
  }

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

  object QueryConversion {
    def to(q: Query): ThriftDictionaryExpression = {
      val e = new ThriftDictionaryExpression(Expression.asString(q.expression))
      q.filter.map(_.render).foreach(e.setFilter)
      e
    }

    def from(expression: ThriftDictionaryExpression): Option[Query] = for {
      exp <- Expression.parse(expression.getExpression).toOption
      fil  = Option(expression.getFilter).map(Filter.apply)
    } yield Query(exp, fil)
  }

  object virtual {
    def to(cd: VirtualDefinition): ThriftDictionaryVirtual = {
      val virt = new ThriftDictionaryVirtual(featureId.to(cd.source))
      cd.window.map(window.to).foreach(virt.setWindow)
      virt.setExpression(QueryConversion.to(cd.query))
      virt
    }
    def from(virt: ThriftDictionaryVirtual): String \/ VirtualDefinition = for {
      source <- featureId.from(virt.getSourceName)
      exp    <- (if (virt.isSetExpression) QueryConversion.from(virt.getExpression) else Some(Query.empty))
        .toRightDisjunction("Invalid expression")
    } yield VirtualDefinition(source, exp, virt.isSetWindow.option(window.from(virt.getWindow)))
  }

  val concrete = new {
    def to(cd: ConcreteDefinition): ThriftDictionaryFeatureMeta = {
      val (encJava, structJava) = encoding.to(cd.encoding)
      val meta = new ThriftDictionaryFeatureMeta(encJava, cd.desc, cd.tombstoneValue.asJava)
      cd.ty.foreach(t => meta.setType(typeBi.to(t)))
      meta.setModeV2(modeBi.to(cd.mode))
      structJava.foreach(meta.setValue)
      meta
    }
    def from(meta: ThriftDictionaryFeatureMeta): ConcreteDefinition =
      ConcreteDefinition(encoding.from(meta), modeBi.from(Option(meta.mode), Option(meta.modeV2)), Option(meta.`type`).map(typeBi.from), meta.desc, meta.tombstoneValue.asScala.toList)
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
      Namespace.nameFromStringDisjunction(featureId.ns).map { namespace =>
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
