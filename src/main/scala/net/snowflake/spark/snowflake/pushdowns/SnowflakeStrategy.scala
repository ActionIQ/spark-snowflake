package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.{SnowflakeConnectorFeatureNotSupportException, SnowflakeRelation}
import net.snowflake.spark.snowflake.pushdowns.querygeneration.QueryBuilder
import org.apache.spark.{DataSourceTelemetryHelpers, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** Clean up the plan, then try to generate a query from it for Snowflake.
  * The Try-Catch is unnecessary and may obfuscate underlying problems,
  * but in beta mode we'll do it for safety and let Spark use other strategies
  * in case of unexpected failure.
  */
class SnowflakeStrategy(sparkContext: SparkContext)
  extends Strategy
    with DataSourceTelemetryHelpers {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      buildQueryRDD(plan.transform({
        case Project(Nil, child) => child
        case SubqueryAlias(_, child) => child
      })).getOrElse(Nil)
    } catch {
      case ue: SnowflakeConnectorFeatureNotSupportException =>
        log.warn(s"Snowflake doesn't support this feature:\n${ue.getMessage}")
        throw ue
      case e: Exception =>
        if (foundSnowflakeRelation(plan)) {
          log.warn(logEventNameTagger(s"PushDown failed:\n${e.getMessage}"))
        }
        Nil
    }
  }

  /** Attempts to get a SparkPlan from the provided LogicalPlan.
    *
    * @param plan The LogicalPlan provided by Spark.
    * @return An Option of Seq[SnowflakePlan] that contains the PhysicalPlan if
    *         query generation was successful, None if not.
    */
  private def buildQueryRDD(plan: LogicalPlan): Option[Seq[SnowflakePlan]] =
    QueryBuilder.getRDDFromPlan(plan).map {
      case (output: Seq[Attribute], rdd: RDD[InternalRow], sql: String) =>
        Seq(SnowflakePlan(output, rdd, sql))
    }.orElse {
      // Set `dataSourceTelemetry.pushDownStrategyFailed` to `true` for when QueryBuilder fails
      // ONLY when Cloud tables are involved in a query plan otherwise it's false signal
      if (foundSnowflakeRelation(plan)) {
        sparkContext.dataSourceTelemetry.pushDownStrategyFailed.set(true)
      }
      None
    }

  private def foundSnowflakeRelation(plan: LogicalPlan): Boolean = {
    plan.collectFirst {
      case LogicalRelation(r, _, _, _) if r.isInstanceOf[SnowflakeRelation] => true
    }.getOrElse(false)
  }
}
