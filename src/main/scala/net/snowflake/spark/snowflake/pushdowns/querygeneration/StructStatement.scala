package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake._

import scala.language.postfixOps
import org.apache.spark.sql.catalyst.expressions.{
  ArrayContains,
  ArrayDistinct,
  ArrayExcept,
  ArrayIntersect,
  ArrayMax,
  ArrayMin,
  ArrayPosition,
  ArrayRemove,
  ArrayUnion,
  ArraysOverlap,
  Attribute,
  CreateArray,
  CreateNamedStruct,
  Expression,
  Flatten,
  JsonToStructs,
  Literal,
  Size,
  Slice,
  SortArray,
  StructsToJson
}
import org.apache.spark.sql.types.{ArrayType, MapType}

/**
 * Extractor for basic (attributes and literals) expressions.
 */
private[querygeneration] object StructStatement {

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
      case ArrayContains(left, right) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          // arguments are in reverse order in Snowflake so exchanging here
          Seq(right, left).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_distinct
      case ArrayDistinct(child) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatement(child, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_except
      case ArrayExcept(left, right) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(left, right).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_intersection
      case ArrayIntersect(left, right) =>
        functionStatement(
          "ARRAY_INTERSECTION",
          Seq(left, right).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_max
      case ArrayMax(child) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatement(child, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_min
      case ArrayMin(child) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatement(child, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_position
      case ArrayPosition(left, right) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          // arguments are in reverse order in Snowflake so exchanging here
          Seq(right, left).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_remove
      case ArrayRemove(left, right) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(left, right).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_cat
      case ArrayUnion(left, right) =>
        // we need distinct to map 1-1 with the Spark implementation that returns an
        // array of the elements in the union of array1 and array2 without duplicates
        // Note: no good way to do this with Spark Expressions, so wrapping with functionStatement
        functionStatement(
          "ARRAY_DISTINCT",
          Seq(
            functionStatement(
              "ARRAY_CAT",
              Seq(left, right).map(convertStatement(_, fields)),
            ),
          ),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/arrays_overlap
      case ArraysOverlap(left, right) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(left, right).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_construct
      case CreateArray(children, _) =>
        functionStatement(
          "ARRAY_CONSTRUCT",
          Seq(convertStatements(fields, children: _*)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_flatten
      case Flatten(child) =>
        functionStatement(
          "ARRAY_FLATTEN",
          Seq(convertStatement(child, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_size
      // https://docs.snowflake.com/en/sql-reference/functions/map_size
      case Size(child, _) =>
        val functionName = child.dataType match {
          case _: ArrayType => "ARRAY_SIZE"
          case _: MapType => "MAP_SIZE"
          case _ => null
        }

        functionStatement(
          functionName,
          Seq(convertStatement(child, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_slice
      case Slice(x, start, length) =>
        functionStatement(
          "ARRAY_SLICE",
          Seq(x, start, length).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/array_sort
      case SortArray(base, ascendingOrder) =>
        // setting `nulls_first` to always TRUE to mimic Spark's behavior
        val nullsFirst = Literal(true)
        functionStatement(
          "ARRAY_SORT",
          Seq(base, ascendingOrder, nullsFirst).map(convertStatement(_, fields)),
        )

      // JSON

      // https://docs.snowflake.com/en/sql-reference/functions/parse_json
      case JsonToStructs(_, _, child, _) =>
        functionStatement(
          "PARSE_JSON",
          Seq(child).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/to_json
      case StructsToJson(_, child, _) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(child).map(convertStatement(_, fields)),
        )

      // STRUCT

      // https://docs.snowflake.com/en/sql-reference/functions/object_agg
      case CreateNamedStruct(children) =>
        functionStatement(
          "OBJECT_AGG",
          Seq(convertStatements(fields, children: _*)),
        )

      case _ => null
    })
  }
}