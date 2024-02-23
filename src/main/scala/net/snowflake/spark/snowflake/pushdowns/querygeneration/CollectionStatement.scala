package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{ArrayContains, ArrayDistinct, ArrayExcept, ArrayIntersect, ArrayJoin, ArrayMax, ArrayMin, ArrayPosition, ArrayRemove, ArrayUnion, ArraysOverlap, Attribute, Cast, Concat, CreateArray, CreateNamedStruct, Expression, Flatten, If, IsNull, JsonToStructs, Literal, Or, Size, Slice, SortArray, StructsToJson}
import org.apache.spark.sql.types.{ArrayType, NullType}

import scala.language.postfixOps

/**
 * Extractor for collection-style expressions.
 */
private[querygeneration] object CollectionStatement {

  private def castExpressionToVariant(
    expr: Expression,
    fields: Seq[Attribute],
  ): SnowflakeSQLStatement = convertStatement(expr, fields) + "::VARIANT"

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

      // ARRAY

      // https://docs.snowflake.com/en/sql-reference/functions/array_contains
      case e: ArrayContains =>
        functionStatement(
          expr.prettyName.toUpperCase,
          // arguments are in reverse order in Snowflake so exchanging here
          Seq(
            // value expression must evaluate to variant
            castExpressionToVariant(e.right, fields),
            convertStatement(e.left, fields)
          ),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_distinct
      case e: ArrayDistinct =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatement(e.child, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_except
      case e: ArrayExcept =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(e.left, e.right).map(expr => convertStatement(ArrayDistinct(expr), fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_intersection
      case e: ArrayIntersect =>
        functionStatement(
          "ARRAY_INTERSECTION",
          Seq(e.left, e.right).map(expr => convertStatement(ArrayDistinct(expr), fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_to_string
      case e: ArrayJoin =>
        functionStatement(
          "ARRAY_TO_STRING",
          Seq(e.array, e.delimiter).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_max
      // https://docs.snowflake.com/en/sql-reference/functions/array_min
      case minOrMax @ ( _: ArrayMax | _: ArrayMin) =>
        val minOrMaxStmt = functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatements(fields, minOrMax.children: _*)),
        )

        // Casting to the Array elements type to make sure the returned type is the expected one
        MiscStatement.getCastType(minOrMax.dataType) match {
          case Some(cast) =>
            ConstantString("CAST") +
              blockStatement(minOrMaxStmt + "AS" + cast)
          case _ => null
        }

      // https://docs.snowflake.com/en/sql-reference/functions/array_position
      case e: ArrayPosition =>
        // Wrapping in Coalesce to mimic Spark function's functionality in Snowflake
        functionStatement(
          "COALESCE",
          Seq(
            functionStatement(
              expr.prettyName.toUpperCase,
              // arguments are in reverse order in Snowflake so exchanging here
              Seq(
                // value expression must evaluate to variant
                castExpressionToVariant(e.right, fields),
                convertStatement(e.left, fields)
              ),
            ),
            convertStatement(nullSafeExpr(e.children, Literal(0)), fields),
          ),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_remove
      case e: ArrayRemove =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(e.left, e.right).map(convertStatement(_, fields)),
        )

      case e: ArrayUnion =>
        // we need distinct to map 1-1 with the Spark implementation that returns an
        // array of the elements in the union of array1 and array2 without duplicates
        convertStatement(ArrayDistinct(Concat(Seq(e.left, e.right))), fields)

      // https://docs.snowflake.com/en/sql-reference/functions/arrays_overlap
      case e: ArraysOverlap =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(e.left, e.right).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_cat
      case e: Concat
        if e.children.map(_.dataType.isInstanceOf[ArrayType]).forall(identity) =>
        functionStatement(
          "ARRAY_CAT",
          Seq(convertStatements(fields, e.children: _*)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_construct_compact
      case e: CreateArray if e.checkInputDataTypes().isSuccess =>
        functionStatement(
          "ARRAY_CONSTRUCT",
          Seq(convertStatements(fields, e.children: _*)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_flatten
      case e: Flatten =>
        functionStatement(
          "ARRAY_FLATTEN",
          Seq(convertStatement(e.child, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_size
      case e: Size =>
        e.child.dataType match {
          case _: ArrayType =>
            functionStatement(
              "ARRAY_SIZE",
              Seq(convertStatement(e.child, fields)),
            )
          case _ => null
        }

      // https://docs.snowflake.com/en/sql-reference/functions/array_slice
      case e: Slice =>
        functionStatement(
          "ARRAY_SLICE",
          Seq(e.x, e.start, e.length).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_sort
      case e: SortArray =>
        // setting `nulls_first` to always TRUE to mimic Spark's behavior
        val nullsFirst = Literal(true)
        functionStatement(
          "ARRAY_SORT",
          Seq(e.base, e.ascendingOrder, nullsFirst).map(convertStatement(_, fields)),
        )

      // JSON

      // https://docs.snowflake.com/en/sql-reference/functions/parse_json
      case e: JsonToStructs =>
        functionStatement(
          "PARSE_JSON",
          Seq(e.child).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/to_json
      case e: StructsToJson =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(e.child).map(convertStatement(_, fields)),
        )

      // STRUCT

      // https://docs.snowflake.com/en/sql-reference/functions/object_construct_keep_null
      case e: CreateNamedStruct =>
        functionStatement(
          "OBJECT_CONSTRUCT_KEEP_NULL",
          Seq(convertStatements(fields, e.children: _*)),
        )

      case _ => null
    })
  }
}