package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{
  ConstantString,
  SnowflakeSQLStatement
}
import org.apache.spark.sql.catalyst.expressions.{
  Ascii,
  Attribute,
  Cast,
  Concat,
  ConcatWs,
  Expression,
  FormatNumber,
  Length,
  Like,
  Literal,
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

      // https://docs.snowflake.com/en/sql-reference/functions/concat_ws
      // Trying to make `concat_ws` work is not possible so adjusting with the
      // Snowflake functions below that do the trick and support the behavior
      // https://docs.snowflake.com/en/sql-reference/functions/array_to_string
      // https://docs.snowflake.com/en/sql-reference/functions/array_construct_compact
      case ConcatWs(children) =>
        if (children.length >= 2) {
          val separator = children.head
          val firstStmt = children(1)
          val snowStm = children.drop(2).foldLeft(convertStatement(firstStmt, fields)) {
            (currentSnowStm, nextExpr) => mkStatement(
              Seq(currentSnowStm, convertStatement(nextExpr, fields)),
              ","
            )
          }

          functionStatement(
            "ARRAY_TO_STRING",
            Seq(
              functionStatement(
                "ARRAY_CONSTRUCT_COMPACT",
                Seq(snowStm),
              ),
              convertStatement(separator, fields),
            )
          )
        } else {
          convertStatement(Literal(""), fields)
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
      // Reverse in Snowflake only supports StringType and DateType.
      // Spark only supports StringType and ArrayType, thus we only
      // implement for StringType
      case Reverse(child) =>
        child.dataType match {
          case _: StringType =>
            functionStatement(
              expr.prettyName.toUpperCase,
              Seq(convertStatement(child, fields)),
            )
          case _ => null
        }

      // https://docs.snowflake.com/en/sql-reference/functions/uuid_string
      case _: Uuid => functionStatement("UUID_STRING", Seq())

      // https://docs.snowflake.com/en/sql-reference/functions/trim
      // https://docs.snowflake.com/en/sql-reference/functions/to_decimal
      // https://docs.snowflake.com/en/sql-reference/functions/to_char
      // https://docs.snowflake.com/en/sql-reference/sql-format-models
      case FormatNumber(number, precision) =>
        val precisionOpt = precision match {
          case p: Literal => Option(p.value).map(_.asInstanceOf[Int])
          case _ => None
        }

        val defaultFormat = "9,999,999,999,999,999,999"

        precisionOpt match {
          case Some(d) =>
            val sqlFormat = if (d > 0) {
              defaultFormat.concat(".").concat(Seq.fill(d)("0").mkString(""))
            } else {
              defaultFormat
            }

            // Wrapping around a trim to remove the leading empty spaces
            // produced by the use of `9` in the default format
            functionStatement(
              "TRIM",
              Seq(
                functionStatement(
                  "TO_VARCHAR",
                  Seq(
                    functionStatement(
                      "TO_NUMERIC",
                      Seq(
                        convertStatement(Cast(number, StringType), fields),
                        ConstantString(s"'TM9'").toStatement,
                        ConstantString("38").toStatement,
                        convertStatement(precision, fields),
                      ),
                    ),
                    ConstantString(s"'$sqlFormat'").toStatement,
                  )
                )
              ),
            )
          case None => null
        }

      case _ => null
    })
  }
}
