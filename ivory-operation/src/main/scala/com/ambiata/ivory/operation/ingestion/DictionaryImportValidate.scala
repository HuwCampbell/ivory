package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import scalaz._, Scalaz._

object DictionaryImportValidate {

  type DictValidation[A] = ValidationNel[DictionaryValidateFailure, A]
  type DictValidationUnit = DictValidation[Unit]
  type StructName = String

  val MAX_FEATURES: Int = Short.MaxValue.toInt

  val OK: DictValidationUnit = Success(())

  def validate(oldDict: Dictionary, newDict: Dictionary): DictValidationUnit = {
    def validateEncoding(e1: Encoding, e2: Encoding, path: ValidationPath): DictValidationUnit = {
      (e1, e2) match {
        case (EncodingStruct(StructEncoding(sm1)), EncodingStruct(StructEncoding(sm2))) =>
          Maps.outerJoin(sm1, sm2).toStream.foldMap {
            case (name, \&/.This(_))                   => MissingStructField(name :: path).failureNel
            case (name, \&/.That(sv2))                 => if (!sv2.optional) NotOptionalStructField(name :: path).failureNel else OK
            case (name, \&/.Both(sv1, sv2))            => () match {
              case _ if sv1 == sv2                     => OK
              case _ if sv1.encoding != sv2.encoding   => EncodingChanged(sv1.encoding.toEncoding, sv2.encoding.toEncoding, name :: path).failureNel
              case _ if !sv2.optional                  => NotOptionalStructField(name :: path).failureNel
              case _                                   => validateEncoding(sv1.encoding.toEncoding, sv2.encoding.toEncoding, name :: path)
            }
          }
        case _ if e1 != e2                             => EncodingChanged(e1, e2, path).failureNel
        case _ => OK
      }
    }
    def validateMeta(id: FeatureId, oldMeta: ConcreteDefinition, newMeta: ConcreteDefinition): DictValidationUnit = {
      validateEncoding(oldMeta.encoding, newMeta.encoding, ValidationPath(id))
    }
    def validateFeature(id: FeatureId, oldFeature: Definition, newFeature: Definition): DictValidationUnit =
      (oldFeature, newFeature) match {
        case (Concrete(_, oldMeta), Concrete(_, newMeta)) => validateMeta(id, oldMeta, newMeta)
        case (Concrete(_, _)      , Virtual(_, _)       ) => RealToVirtualEncoding(ValidationPath(id)).failureNel
        case (Virtual(_, _)       , Concrete(_, _)      ) => OK
        case (Virtual(_, _)       , Virtual(_, _)       ) => OK
      }
    Maps.outerJoin(oldDict.byFeatureId, newDict.byFeatureId).foldMap {
      case (_,   \&/.This(_))                => OK
      case (_,   \&/.That(_))                => OK
      case (fid, \&/.Both(oldMeta, newMeta)) => validateFeature(fid, oldMeta, newMeta)
    }
  }

  /**
   * Make sure a dictionary is self-consistent, such as:
   *
   * - virtual features derived from actual concrete features (for
   *   now, in the future this will be possible).
   * - virtual features with an invalid filter
   */
  def validateSelf(dict: Dictionary): DictValidationUnit =
    if(dict.definitions.length > MAX_FEATURES)
      TooManyFeatures(dict.definitions.length).failureNel.void
    else
      dict.definitions.traverseU {
        case Concrete(fid, cd)    =>
          cd.mode.fold(OK, OK, keys => cd.encoding.fold(
            _ => InvalidEncodingKeyedSet(ValidationPath(fid)).failureNel,
            s => keys.traverseU(key =>
              s.values.get(key).cata(sv =>
                if (sv.optional) OptionalStructValueKeyedSet(key, ValidationPath(fid)).failureNel else OK,
                MissingKey(key, ValidationPath(fid)).failureNel)
            ),
            _ => InvalidEncodingKeyedSet(ValidationPath(fid)).failureNel
          ))
        case Virtual(fid, vd)  => dict.byFeatureId.get(vd.source).cata({
          case Concrete(_, cd) =>
            Expression.validate(vd.query.expression, cd.encoding)
              .leftMap(InvalidExpression(_, ValidationPath(fid))).validation.toValidationNel +++
            vd.query.filter.traverseU(FilterTextV0.encode(_, cd.encoding)
              .leftMap(InvalidFilter(_, ValidationPath(fid))).validation.toValidationNel).void
          case Virtual(_, _)   => InvalidVirtualSource(vd.source, ValidationPath(fid)).failureNel
        }, InvalidVirtualSource(vd.source, ValidationPath(fid)).failureNel)
      }.void

  case class ValidationPath(id: FeatureId, path: List[StructName] = Nil) {
    def ::(name: StructName): ValidationPath = new ValidationPath(id, name :: path)

    override def toString: String =
      s"$id:${path.mkString("/")}"
  }

  sealed trait DictionaryValidateFailure {
    def toString: String
  }
  case class EncodingChanged(e1: Encoding, e2: Encoding, path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Encoding changed from $e1 to $e2 at $path"
  }
  case class MissingStructField(path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Struct field was removed $path"
  }
  case class NotOptionalStructField(path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Struct field $path was made optional"
  }
  case class RealToVirtualEncoding(path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Cannot switch $path from real feature to virtual"
  }
  case class InvalidVirtualSource(source: FeatureId, path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Supplied source '$source' not found at $path or is invalid"
  }
  case class InvalidFilter(error: String, path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Invalid filter at $path: $error"
  }
  case class InvalidExpression(error: String, path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Invalid expression at $path: $error"
  }
  case class TooManyFeatures(features: Int) extends DictionaryValidateFailure {
    override def toString = s"Currently Ivory only supports dictionary sizes up to ${MAX_FEATURES}, but the given dictionary is ${features}." +
                             "If you see this, please open an issue at https://github.com/ambiata/ivory/issues/new"
  }
  case class MissingKey(key: String, path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"The key $key required for keyed_set $path is missing"
  }
  case class InvalidEncodingKeyedSet(path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"The encoding for $path is invalid - only structs are supported for keyed_set"
  }
  case class OptionalStructValueKeyedSet(key: String, path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"The struct field $key for $path is nominated by keyed_set and needs to be mandatory"
  }
}
