package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeFailMessage, SnowflakePushdownUnsupportedException, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{
  Ascii,
  Attribute,
  Concat,
  ConcatWs,
  Expression,
  Length,
  Like,
  Lower,
  Reverse,
  StringInstr,
  StringLPad,
  StringRPad,
  StringTranslate,
  StringTrim,
  StringTrimLeft,
  StringTrimRight,
  Substring,
  Upper,
  Uuid
}
import org.apache.spark.sql.types.StringType

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object StringStatement {
  // ESCAPE CHARACTER for LIKE is supported from Spark 3.0
  // The default escape character comes from the constructor of Like class.
  private val DEFAULT_LIKE_ESCAPE_CHAR: Char = '\\'

  /** Used mainly by QueryGeneration.convertExpression. This matches
    * a tuple of (Expression, Seq[Attribute]) representing the expression to
    * be matched and the fields that define the valid fields in the current expression
    * scope, respectively.
    *
    * @param expAttr A pair-tuple representing the expression to be matched and the
    *                attribute fields.
    * @return An option containing the translated SQL, if there is a match, or None if there
    *         is no match.
    */
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case _: Ascii | _: Lower | _: Substring | _: StringLPad | _: StringRPad |
          _: StringTranslate | _: StringTrim | _: StringTrimLeft |
          _: StringTrimRight | _: Substring | _: Upper | _: Length =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      // Spark INSTR and Snowflake CHARINDEX are both 1-based, but their args are swapped
      case StringInstr(left, right) =>
        ConstantString("CHARINDEX") +
          blockStatement(convertStatements(fields, Seq(right, left): _*))

      case Concat(children) =>
        val rightSide =
          if (children.length > 2) Concat(children.drop(1)) else children(1)
        ConstantString("CONCAT") + blockStatement(
          convertStatement(children.head, fields) + "," +
            convertStatement(rightSide, fields)
        )

      case ConcatWs(children) =>
        if (children.length >= 2) {
          val separator = children.head
          val snowStm = children.drop(1).foldLeft(convertStatement(separator, fields)) {
            (currentSnowStm, nextExpr) => mkStatement(
              Seq(currentSnowStm, convertStatement(nextExpr, fields)), ","
            )
          }

          // Wrapping around Coalesce to mimic Spark's behavior =>
          //  in case of null, return empty string ('')
          functionStatement(
            "COALESCE",
            Seq(
              functionStatement(
                expr.prettyName.toUpperCase,
                Seq(snowStm),
              ),
              ConstantString("''").toStatement,
            ),
          )
        } else {
          throw new SnowflakePushdownUnsupportedException(
            SnowflakeFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_CONVERSION,
            "Not enough arguments for function [CONCAT_WS(',')], expected 2, got 1",
            "",
            false
          )
        }

      // ESCAPE Char is supported from Spark 3.0
      case Like(left, right, escapeChar) =>
        val escapeClause =
          if (escapeChar == DEFAULT_LIKE_ESCAPE_CHAR) {
            ""
          } else {
            s"ESCAPE '${escapeChar}'"
          }
        convertStatement(left, fields) + "LIKE" + convertStatement(
          right,
          fields
        ) + escapeClause

      // https://docs.snowflake.com/en/sql-reference/functions/reverse
      // Reverse in Snowflake only supports StringType and DateType
      // which Spark doesn't
      case Reverse(child) =>
        child.dataType match {
          case _: StringType =>
            functionStatement(
              expr.prettyName.toUpperCase,
              Seq(convertStatement(child, fields)),
            )
          case _ => null
        }

      case _: Uuid => functionStatement("UUID_STRING", Seq())

      case _ => null
    })
  }
}
