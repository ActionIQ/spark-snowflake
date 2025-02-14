package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{
  ConstantString,
  EmptySnowflakeSQLStatement,
  SnowflakeFailMessage,
  SnowflakePushdownUnsupportedException,
  SnowflakeSQLStatement
}

import scala.language.postfixOps

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/**
  * Extractor for aggregate-style expressions.
  */
private[querygeneration] object AggregationStatement {
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    expr match {
      case _: AggregateExpression =>
        // Take only the first child, as all of the functions below have only one.
        expr.children.headOption.flatMap(agg_fun => {
          Option(agg_fun match {
            case _: Average | _: Corr | _: CovPopulation | _: CovSample | _: Count |
                 _: Max | _: Min | _: Sum | _: StddevPop | _: StddevSamp |
                 _: VariancePop | _: VarianceSamp =>
              val distinct: SnowflakeSQLStatement =
                if (expr.sql contains "(DISTINCT ") ConstantString("DISTINCT") !
                else EmptySnowflakeSQLStatement()

              ConstantString(agg_fun.prettyName.toUpperCase) +
                blockStatement(
                  distinct + convertStatements(fields, agg_fun.children: _*)
                )
            case _: HyperLogLogPlusPlus =>
              // NOTE: We are not passing through the other parameters in Spark's HLL
              // like mutableAggBufferOffset and inputAggBufferOffset
              ConstantString("HLL") +
                blockStatement(convertStatements(fields, agg_fun.children: _*))
            case _: CollectList =>
              // https://docs.snowflake.com/en/sql-reference/functions/array_agg
              functionStatement(
                "ARRAY_AGG",
                Seq(convertStatements(fields, agg_fun.children: _*)),
              )
            case _: CollectSet =>
              // scalastyle:off line.size.limit
              // https://docs.snowflake.com/en/sql-reference/functions/array_agg
              // https://community.snowflake.com/s/question/0D50Z00009bTcpkSAC/equivalent-of-collectset-in-hive
              // scalastyle:on line.size.limit
              functionStatement(
                "ARRAY_AGG",
                Seq(ConstantString("DISTINCT") + convertStatements(fields, agg_fun.children: _*)),
              )
            case _ =>
              // This exception is not a real issue. It will be caught in
              // QueryBuilder.treeRoot and a telemetry message will be sent if
              // there are any snowflake tables in the query.
              throw new SnowflakePushdownUnsupportedException(
                SnowflakeFailMessage.FAIL_PUSHDOWN_AGGREGATE_EXPRESSION,
                s"${agg_fun.prettyName} @ AggregationStatement",
                agg_fun.sql,
                false
              )
          })
        })
      case _ => None
    }
  }
}
