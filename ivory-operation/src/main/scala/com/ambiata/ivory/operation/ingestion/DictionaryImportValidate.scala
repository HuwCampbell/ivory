package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import scalaz._, Scalaz._

object DictionaryImportValidate {

  type DictValidation[A] = ValidationNel[DictionaryValidateFailure, A]
  type DictValidationUnit = DictValidation[Unit]
  type StructName = String

  val OK: DictValidationUnit = Success(())

  def validate(oldDict: Dictionary, newDict: Dictionary): DictValidationUnit = {
    def validateEncoding(e1: Encoding, e2: Encoding, path: ValidationPath): DictValidationUnit = {
      (e1, e2) match {
        case (StructEncoding(sm1), StructEncoding(sm2)) =>
          Maps.outerJoin(sm1, sm2).toStream.foldMap {
            case (name, \&/.This(_))                   => MissingStructField(name :: path).failureNel
            case (name, \&/.That(sv2))                 => if (!sv2.optional) NotOptionalStructField(name :: path).failureNel else OK
            case (name, \&/.Both(sv1, sv2))            => () match {
              case _ if sv1 == sv2                     => OK
              case _ if sv1.encoding != sv2.encoding   => EncodingChanged(sv1.encoding, sv2.encoding, name :: path).failureNel
              case _ if !sv2.optional                  => NotOptionalStructField(name :: path).failureNel
              case _                                   => validateEncoding(sv1.encoding, sv2.encoding, name :: path)
            }
          }
        case _ if e1 != e2                             => EncodingChanged(e1, e2, path).failureNel
        case _ => OK
      }
    }
    def validateMeta(id: FeatureId, oldMeta: FeatureMeta, newMeta: FeatureMeta): DictValidationUnit = {
      validateEncoding(oldMeta.encoding, newMeta.encoding, ValidationPath(id))
    }
    def validateFeature(id: FeatureId, oldFeature: Feature, newFeature: Feature): DictValidationUnit =
      (oldFeature, newFeature) match {
        case (oldMeta: FeatureMeta,    newMeta: FeatureMeta   ) => validateMeta(id, oldMeta, newMeta)
        case (_      : FeatureMeta,    _      : FeatureVirtual) => RealToVirtualEncoding(ValidationPath(id)).failureNel
        case (_      : FeatureVirtual, _      : FeatureMeta   ) => OK
        case (_      : FeatureVirtual, _      : FeatureVirtual) => OK
      }
    Maps.outerJoin(oldDict.meta, newDict.meta).foldMap {
      case (_,   \&/.This(_))                => OK
      case (_,   \&/.That(_))                => OK
      case (fid, \&/.Both(oldMeta, newMeta)) => validateFeature(fid, oldMeta, newMeta)
    }
  }

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
}
