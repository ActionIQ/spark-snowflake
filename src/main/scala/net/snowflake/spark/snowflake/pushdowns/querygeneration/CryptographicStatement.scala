package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake._

import scala.language.postfixOps
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Cast,
  Expression,
  Md5,
  Sha1,
  Sha2
}
import org.apache.spark.sql.types.{BinaryType, StringType}

/**
 * Extractor for basic (attributes and literals) expressions.
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
        child match {
          // Spark always casts child to binary, need to use string for Snowflake otherwise
          // we get: `The following string is not a legal hex-encoded value` error
          case Cast(c, _: BinaryType, tZ, ansiEn) =>
            functionStatement(
              "MD5",
              Seq(
                convertStatement(Cast(c, StringType, tZ, ansiEn), fields)
              ),
            )
          case _ => null
        }

      // https://docs.snowflake.com/en/sql-reference/functions/sha1
      case Sha1(child) =>
        child match {
          // Spark always casts child to binary, need to use string for Snowflake otherwise
          // we get: `The following string is not a legal hex-encoded value` error
          case Cast(c, _: BinaryType, tZ, ansiEn) =>
            functionStatement(
              "SHA1",
              Seq(
                convertStatement(Cast(c, StringType, tZ, ansiEn), fields)
              ),
            )
          case _ => null
        }

      // https://docs.snowflake.com/en/sql-reference/functions/sha2
      case Sha2(left, right) =>
        left match {
          // Spark always casts child to binary, need to use string for Snowflake otherwise
          // we get: `The following string is not a legal hex-encoded value` error
          case Cast(l, _: BinaryType, tZ, ansiEn) =>
            functionStatement(
              "SHA2",
              Seq(
                convertStatement(Cast(l, StringType, tZ, ansiEn), fields),
                convertStatement(right, fields),
              ),
            )
          case _ => null
        }

      case _ => null
    })
  }
}
