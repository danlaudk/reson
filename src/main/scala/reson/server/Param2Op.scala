package reson

import scala.annotation.tailrec

/**
GET /people?select=age,height,weight
GET /people?age=gte.18&student=is.true
GET /people?age=lt.13
GET /people?age=not.eq.2
GET /people?order=age.desc,height.asc
GET /people?order=age
GET /people?order=age.nullsfirst
GET /people?order=age.desc.nullslast

  * if path is null, don't parse params.

  * key is always select, order or a column
  * select and order are handled separately
  * values for column keys are always operators
  * operators can be handled separately.
  */


sealed trait Op

case object NoOp extends Op {
  override def toString = ""
}

trait Negable {
  val not: Boolean
  def isNot = if (not) " NOT " else " "
}

final case class SingleOp(column: String, value: String, key: String) extends Op {
  override def toString = s"""$column $key '$value'"""
}

final case class SingleNegableOp(column: String, not: Boolean, value: String, key: String) extends Op with Negable {
  override def toString = s"""$column $isNot $key '$value'"""
}

class MultiOp(val key: String, values: Seq[String]) extends Op {
  override def toString = s""" $key ${values.map(x => s"'$x'").mkString(",")}"""
}

final case class Order(values: Seq[String]) extends MultiOp("order", values) {

  // order=age.desc,height.asc
  // order=age.nullsfirst
  // order=age.desc.nullslast
  final case class Rule(col: String, desc: Boolean = false, asc: Boolean = false, nullsFirst: Boolean = false, nullsLast: Boolean = false) {
    if (desc && asc || nullsFirst && nullsLast) throw new Exception("invalid ordering rule")

    override def toString =
      s"""$col ${if (desc) "DESC" else ""}${if (asc) "ASC" else ""} """ +
        s"""${if (nullsFirst) "NULLS FIRST" else ""}${if (nullsLast) "NULLS LAST" else ""}"""
  }

  val rules = values.map { rule =>
    val spl = rule.split("\\.")
    spl.toList match {
      case col :: Nil => Rule(col)
      case col :: v => Rule(col, v.contains("desc"), v.contains("asc"), v.contains("nullsfirst"), v.contains("nullslast"))
      case _ => throw new Exception("unparsable ordering rule: " + rule)
    }
  }

  override def toString = "ORDER BY " + rules.mkString(", ")
}

final case class InOp(column:String, value:String, not: Boolean) extends Op with Negable {
  override def toString = s"""`$column`${isNot}IN """ + value.map(v => s"'$v'").mkString("(", ",", ")")
}

final case class Select(value:String) extends Op {
  override def toString = "SELECT " + value
}

// END OF ADT

object Param2Op extends App {

  def parseParam(kv: (String, String)): Op = parseParam(kv._1, kv._2)

  def parseParam(key: String, value: String): Op = {
    def noNeg(n: Boolean) = if (n) throw new Exception("meaningless not")

    @tailrec
    def parse(key: String, values: Seq[String], not: Boolean): Op = {
      (key, values) match {
        case ("select", _) => noNeg(not); Select(value)
        case ("order", _) => noNeg(not); Order(value.split(","))
        case (_, "not" :: v) => parse(key, v, true)
        case (col, "in" :: v) => InOp(col, v.head, not)
        case (col, "eq" :: v) => noNeg(not); SingleOp(col, v.head, "=")
        case (col, "gt" :: v) => noNeg(not); SingleOp(col, v.head, ">")
        case (col, "gte" :: v) => noNeg(not); SingleOp(col, v.head, ">=")
        case (col, "lt" :: v) => noNeg(not); SingleOp(col, v.head, "<")
        case (col, "lte" :: v) => SingleOp(col, v.head, "<=")
        case (col, "like" :: v) => SingleNegableOp(col, not, v.head, "LIKE")
        case (x, y) => throw new Exception(s"nothing matched key=$key, value=$value")
      }
    }
    parse(key, value.split("\\.").toList, false)
  }

  def testPar(str: String) = println(s"$str => " + parseParam(str.split("=")(0), str.split("=")(1)))

  testPar("col1=not.in.2,3")
  testPar("col1=gt.2")
  testPar("col1=gt.2")
  testPar("col1=not.like.xxx*y")
  testPar("order=col1")
  testPar("order=col1.nullsfirst")
  testPar("order=col1.desc.nullsfirst,col2.asc")
  val ex = Map("col1" -> "gt.1", "col2" -> "like.eee")
  val select = parseParam("select", ex.get("select").getOrElse("*"))
  val order = ex.get("order").map(parseParam("order", _))
}