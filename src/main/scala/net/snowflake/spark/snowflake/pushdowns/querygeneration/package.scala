package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.{ConstantString, EmptySnowflakeSQLStatement, SnowflakeSQLStatement}

import scala.language.postfixOps

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, If, IsNull, Literal, NamedExpression, Or}
import org.apache.spark.sql.types.{MetadataBuilder, NullType}
import org.slf4j.LoggerFactory

/** Package-level static methods and variable constants. These includes helper functions for
  * adding and converting expressions, formatting blocks and identifiers, logging, and
  * formatting SQL.
  */
package object querygeneration {

  private[querygeneration] final val ORIG_NAME = "ORIG_NAME"

  /** This wraps all identifiers with the following symbol. */
  private final val identifier = "\""

  private[querygeneration] final val log = LoggerFactory.getLogger(getClass)

  private[querygeneration] final def blockStatement(
    stmt: SnowflakeSQLStatement
  ): SnowflakeSQLStatement =
    ConstantString("(") + stmt + ")"

  private[querygeneration] final def blockStatement(
    stmt: SnowflakeSQLStatement,
    alias: String
  ): SnowflakeSQLStatement =
    blockStatement(stmt) + "AS" + wrapStatement(alias)

  /** This adds an attribute as part of a SQL expression, searching in the provided
    * fields for a match, so the subquery qualifiers are correct.
    *
    * @param attr The Spark Attribute object to be added.
    * @param fields The Seq[Attribute] containing the valid fields to be used in the expression,
    *               usually derived from the output of a subquery.
    * @return A SnowflakeSQLStatement representing the attribute expression.
    */
  private[querygeneration] final def addAttributeStatement(
    attr: Attribute,
    fields: Seq[Attribute]
  ): SnowflakeSQLStatement =
    fields.find(e => e.exprId == attr.exprId) match {
      case Some(resolved) =>
        qualifiedAttributeStatement(resolved.qualifier, resolved.name)
      case None => qualifiedAttributeStatement(attr.qualifier, attr.name)
    }

  /** Qualifies identifiers with that of the subquery to which it belongs */
  private[querygeneration] final def qualifiedAttribute(
    alias: Seq[String],
    name: String
  ): String = {
    val str =
      if (alias.isEmpty) ""
      else alias.map(wrap).mkString(".") + "."

    if (name.startsWith("\"") && name.endsWith("\"")) str + name
    else str + wrapObjectName(name)
  }

  private[querygeneration] final def qualifiedAttributeStatement(
    alias: Seq[String],
    name: String
  ): SnowflakeSQLStatement =
    ConstantString(qualifiedAttribute(alias, name)) !

  private[querygeneration] final def wrapObjectName(name: String): String =
    globalParameter match {
      case Some(params) =>
        identifier + (if (params.keepOriginalColumnNameCase) name
                      else name.toUpperCase()) + identifier
      case _ => wrap(name)
    }

  private[querygeneration] final def wrap(name: String): String = {
    identifier + name.toUpperCase + identifier
  }

  private[querygeneration] final def wrapStatement(
    name: String
  ): SnowflakeSQLStatement =
    ConstantString(identifier + name.toUpperCase + identifier) !

  /** This performs the conversion from Spark expressions to SQL runnable by Snowflake.
    * We should have as many entries here as possible, or the translation will not be
    * able ot happen.
    *
    * @note (A MatchError may be raised for unsupported Spark expressions).
    */
  private[querygeneration] final def convertStatement(
    expression: Expression,
    fields: Seq[Attribute]
  ): SnowflakeSQLStatement = {
    (expression, fields) match {
      case AggregationStatement(stmt) => stmt
      case BasicStatement(stmt) => stmt
      case BooleanStatement(stmt) => stmt
      case CollectionStatement(stmt) => stmt
      case CryptographicStatement(stmt) => stmt
      case DateStatement(stmt) => stmt
      case MiscStatement(stmt) => stmt
      case NumericStatement(stmt) => stmt
      case StringStatement(stmt) => stmt
      case WindowStatement(stmt) => stmt
      case UnsupportedStatement(stmt) => stmt
      // UnsupportedStatement must be the last CASE
    }
  }

  // Using this Expression to map the Spark-Snowflake function results 1-1
  //  - Spark returns null if any of the inputs is null and a default value if no matches
  //  - Snowflake returns null if any of the inputs is null AND for no matches
  private[querygeneration] final def nullIntolerantExpr(
    inputs: Seq[Expression],
    default: Expression,
  ): Expression = {
    val isNullMappedInputs = inputs.map(IsNull)

    val inputNullCheckExpr = if (isNullMappedInputs.size > 1) {
      val firstInput = isNullMappedInputs.head
      val secondInput = isNullMappedInputs.drop(1).head

      isNullMappedInputs.drop(2).foldLeft(Or(firstInput, secondInput)) {
        (currentExpr, newExpr) => Or(currentExpr, newExpr)
      }
    } else { isNullMappedInputs.head }

    If(inputNullCheckExpr, Literal.default(NullType), default)
  }

  // Same as above, using this Statement to map the Spark-Snowflake function results 1-1
  private[querygeneration] final def nullIntolerantStmt(
    statement: SnowflakeSQLStatement,
    defaultStmt: SnowflakeSQLStatement,
  ): SnowflakeSQLStatement = {
    // Wrapping statement in Coalesce to mimic Spark function's functionality in Snowflake
    functionStatement(
      "COALESCE",
      Seq(statement, defaultStmt),
    )
  }

  private[querygeneration] final def convertStatements(
    fields: Seq[Attribute],
    expressions: Expression*
  ): SnowflakeSQLStatement =
    mkStatement(expressions.map(convertStatement(_, fields)), ",")

  private[querygeneration] def renameColumns(
    origOutput: Seq[NamedExpression],
    alias: String
  ): Seq[NamedExpression] = {

    val col_names = Iterator.from(0).map(n => s"COL_$n")

    origOutput.map { expr =>
      val metadata =
        if (!expr.metadata.contains(ORIG_NAME)) {
          new MetadataBuilder()
            .withMetadata(expr.metadata)
            .putString(ORIG_NAME, expr.name)
            .build
        } else expr.metadata

      val altName = s"""${alias}_${col_names.next()}"""

      expr match {
        case a @ Alias(child: Expression, name: String) =>
          Alias(child, altName)(a.exprId, Seq.empty[String], Some(metadata))
        case _ =>
          Alias(expr, altName)(expr.exprId, Seq.empty[String], Some(metadata))
      }
    }
  }

  private[querygeneration] def optionalExprToFuncArg(
    exprOpt: Option[Expression]
  ): Seq[Expression] = {
    exprOpt.map { expr => Seq(expr) }.getOrElse(Seq.empty)
  }

  private[querygeneration] def functionStatement(
    name: String,
    args: Seq[SnowflakeSQLStatement],
  ): SnowflakeSQLStatement = {
    ConstantString(name) + blockStatement(mkStatement(args, ","))
  }

  final def mkStatement(
    seq: Seq[SnowflakeSQLStatement],
    delimiter: SnowflakeSQLStatement
  ): SnowflakeSQLStatement =
    seq.foldLeft(EmptySnowflakeSQLStatement()) {
      case (left, stmt) =>
        if (left.isEmpty) stmt else left + delimiter + stmt
    }

  final def mkStatement(seq: Seq[SnowflakeSQLStatement],
                        delimiter: String): SnowflakeSQLStatement =
    mkStatement(seq, ConstantString(delimiter) !)
}
