package com.ambiata.ivory.core

import scalaz._, Scalaz._

/** The string representation of the filter until such time that we can parse the Dictionary in two passes. */
case class Filter(render: String)

/** The _real_ filter, which can _only_ be safely constructed with knowledge of the concrete encoding. */
sealed trait FilterEncoded {

  def fold[A, B](exp: FilterExpression => B)(values: B => A, struct: (String, B) => A)(op: (FilterOp, List[A]) => A): A = this match {
    case FilterValues(vop) => vop.fold(exp)(values)(op)
    case FilterStruct(vop) => vop.fold(exp)(struct)(op)
  }
}
case class FilterValues(op: FilterValuesOp) extends FilterEncoded
case class FilterStruct(op: FilterStructOp) extends FilterEncoded

case class FilterValuesOp(op: FilterOp, fields: List[FilterExpression], children: List[FilterValuesOp]) {

  def fold[A, B](exp: FilterExpression => B)(field: B => A)(opf: (FilterOp, List[A]) => A): A =
    opf(op, fields.map(field compose exp) ++ children.map(_.fold(exp)(field)(opf)))
}

case class FilterStructOp(op: FilterOp, fields: List[(String, FilterExpression)], children: List[FilterStructOp]) {

  def fold[A, B](exp: FilterExpression => B)(field: (String, B) => A)(opf: (FilterOp, List[A]) => A): A =
    opf(op, fields.map { case (n, fe) => field(n, exp(fe))} ++ children.map(_.fold(exp)(field)(opf)))
}

sealed trait FilterOp {
  def fold[A](and: => A, or: => A):A = this match {
    case FilterOpAnd => and
    case FilterOpOr  => or
  }
}
case object FilterOpAnd extends FilterOp
case object FilterOpOr extends FilterOp

sealed trait FilterExpression
case class FilterEquals(value: PrimitiveValue) extends FilterExpression

/**
 * Current format is _intentionally_ crippled to set expectations that it will change _very_ shortly.
 * This isn't even V1 of the text format; yes it's _that_ bad.
 *
 * {{{
 *  ns1:foo|encoding=int
 *  ns1:bar|source=ns1:foo|expression=count|window=2 months|filter=or,1,2,3
 *  ns2:foo|encoding=(a: string, b: int)
 *  ns2:bar|source=ns2:foo|expression=count|window=2 months|filter=and,a,(or,hello,a,goodbye,b,10)
 * }}}
 */
object FilterTextV0 {

  object FilterOpTextV0 {

    def parse(op: String): String \/ FilterOp = op match {
      case "and" => FilterOpAnd.right
      case "or"  => FilterOpOr.right
      case _     => s"Invalid filter operation '$op'".left
    }

    def asString(op: FilterOp): String =
      op.fold("and", "or")
  }

  object FilterExpressionTextV0 {

    // Among many things, we're assuming that values to compare can never equal a tombstone
    def parse(encoding: PrimitiveEncoding, value: String): String \/ FilterExpression =
       Value.parsePrimitive(encoding, value).disjunction.map(FilterEquals.apply)
 
    /** This won't make any sense until we add more expression types */
    def asString(exp: FilterExpression): String = exp match {
      case FilterEquals(value) => Value.toStringPrimitive(value)
    }
  }

  def encode(filter: Filter, encoding: Encoding): String \/ FilterEncoded = {
    def part(fe: List[FExp]): (List[String], List[FExpNode]) = {
      val (l, r) = fe.collect {
        case FExpS(s) => s.left
        case FExpL(f) => f.right
      }.partition(_.isLeft)
      (l.flatMap(_.swap.toOption), r.flatMap(_.toOption))
    }
    new SimpleExpParser(filter.render).parse.flatMap { fel =>
      encoding match {
        case StructEncoding(values) =>
          def struct(l: FExpNode): String \/ FilterStructOp =
            for {
              op     <- FilterOpTextV0.parse(l.op)
              (tail, exps) = part(l.v)
              fields <- tail.grouped(2).toList.traverseU {
                case List(field, value) =>
                  values.get(field)
                    .toRightDisjunction(s"Invalid filter: struct field $field not found")
                    .flatMap(se => FilterExpressionTextV0.parse(se.encoding, value).map(field ->))
                case _ => "Invalid filter: struct filters require name/value pairs".left
              }
              children <- exps.traverseU(struct)
            } yield FilterStructOp(op, fields, children)

          struct(fel).map(FilterStruct)
        case pe: PrimitiveEncoding =>
          def values(l: FExpNode): String \/ FilterValuesOp =
            for {
              op <- FilterOpTextV0.parse(l.op)
              (tail, exps) = part(l.v)
              fields <- tail.traverseU(FilterExpressionTextV0.parse(pe, _))
              children <- exps.traverseU(values)
            } yield FilterValuesOp(op, fields, children)
          values(fel).map(FilterValues)
        case _: ListEncoding => "Filtering lists is not yet supported".left
      }
    }
  }

  def asString(filter: FilterEncoded): Filter = {
    val str = filter.fold(FilterExpressionTextV0.asString)(
      (exp) => exp,
      (f, exp) => f + "," + exp
    ) {
      (op, values) => "(" + (FilterOpTextV0.asString(op) :: values).mkString(",") + ")"
    }
    // Strip off the top-level ()
    Filter(str.substring(1, str.length - 1))
  }

  import org.parboiled2._, Parser.DeliveryScheme.Either

  trait FExp
  case class FExpS(v: String) extends FExp
  case class FExpL(t: FExpNode) extends FExp
  case class FExpNode(op: String, v: List[FExp])

  class SimpleExpParser(val input: ParserInput) extends Parser {

    private def txt                  = rule(capture(oneOrMore(noneOf(",()"))))
    private def subexp: Rule1[FExp]  = rule(("(" ~ exp ~ ")") ~> FExpL | txt ~> FExpS)
    private def exp: Rule1[FExpNode]    = rule((txt ~ "," ~ oneOrMore(subexp).separatedBy(",")) ~>
      ((s: String, e: Seq[FExp]) => FExpNode(s, e.toList)))

    def parse: String \/ FExpNode       = exp.run().disjunction.leftMap(formatError(_))
  }
}
