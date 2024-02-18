package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake._

import scala.language.postfixOps
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Cast,
  Expression,
  Md5,
  Sha1,
  Sha2,
  XxHash64
}
import org.apache.spark.sql.types.{BinaryType, StringType}

/**
 * Extractor for cryptographic-style expressions.
 */
private[querygeneration] object CryptographicStatement {

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
      // https://docs.snowflake.com/en/sql-reference/functions/md5
      case Md5(child) =>
        val childExpr = child match {
          // Spark always casts child to binary, need to use string for Snowflake otherwise
          // we get: `The following string is not a legal hex-encoded value` error
          case Cast(c, _: BinaryType, tZ, ansiEn) => Cast(c, StringType, tZ, ansiEn)
          case childWithoutCast => Cast(childWithoutCast, StringType)
        }
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatement(childExpr, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/sha1
      case Sha1(child) =>
        val childExpr = child match {
          // Spark always casts child to binary, need to use string for Snowflake otherwise
          // we get: `The following string is not a legal hex-encoded value` error
          case Cast(c, _: BinaryType, tZ, ansiEn) => Cast(c, StringType, tZ, ansiEn)
          case childWithoutCast => Cast(childWithoutCast, StringType)
        }
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatement(childExpr, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/sha2
      case Sha2(left, right) =>
        val leftExpr = left match {
          // Spark always casts child to binary, need to use string for Snowflake otherwise
          // we get: `The following string is not a legal hex-encoded value` error
          case Cast(l, _: BinaryType, tZ, ansiEn) => Cast(l, StringType, tZ, ansiEn)
          case lWithoutCast => Cast(lWithoutCast, StringType)
        }
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(leftExpr, right).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/hash
      case XxHash64(children, _) =>
        functionStatement(
          "HASH",
          Seq(convertStatements(fields, children: _*)),
        )

      case _ => null
    })
  }
}
