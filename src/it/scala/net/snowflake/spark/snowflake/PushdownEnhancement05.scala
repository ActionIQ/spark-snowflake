/*
 * Copyright 2015-2019 Snowflake Computing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.util.TimeZone

// scalastyle:off println
class PushdownEnhancement05 extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_table_basic: String = s"test_basic_$randomSuffix"
  private val test_table_number = s"test_table_number_$randomSuffix"
  private val test_table_date = s"test_table_date_$randomSuffix"
  private val test_table_rank = s"test_table_rank_$randomSuffix"
  private val test_table_string = s"test_table_string_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_basic")
      jdbcUpdate(s"drop table if exists $test_table_number")
      jdbcUpdate(s"drop table if exists $test_table_date")
      jdbcUpdate(s"drop table if exists $test_table_rank")
      jdbcUpdate(s"drop table if exists $test_table_string")
    } finally {
      TestHook.disableTestHook()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
      super.afterAll()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // There is bug for Date.equals() to compare Date with different timezone,
    // so set up the timezone to work around it.
    val gmtTimezone = TimeZone.getTimeZone("GMT")
    TimeZone.setDefault(gmtTimezone)

    connectorOptionsNoTable.foreach(tup => {
      thisConnectorOptionsNoTable += tup
    })
  }

  // Collection-Style

  test("AIQ test pushdown array") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello2', 'test2', 3, 4),
         |(2, 'hello3', 'test3', 5, 6),
         |(2, 'hello4', 'test4', 7, 8),
         |(3, 'hello5', NULL, 9, NULL),
         |(3, NULL, 'test6', NULL, 12),
         |(3, NULL, NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDFStr = tmpDF.selectExpr("array(s1, s2)")
    val expectedResultStr = Seq(
      Array("hello1", "test1"),
      Array("hello2", "test2"),
      Array("hello3", "test3"),
      Array("hello4", "test4"),
      Array("hello5"),
      Array("test6"),
      Array[String](),
    ).map(Row(_))

    testPushdown(
      s"""
         |SELECT (
         |  ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S1" , "SUBQUERY_0"."S2" )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFStr,
      expectedResultStr,
    )

    val resultDFInt = tmpDF.selectExpr("array(i1, i2)")
    val expectedResultInt = Seq(
      Seq(1, 2),
      Seq(3, 4),
      Seq(5, 6),
      Seq(7, 8),
      Seq(9),
      Seq(12),
      Seq[Int](),
    ).map { r => Row(r.map(BigDecimal(_)).toArray) }

    testPushdown(
      s"""
         |SELECT (
         |  ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."I1" , "SUBQUERY_0"."I2" )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFInt,
      expectedResultInt,
    )
  }

  //  test("AIQ test pushdown size") {
  //    jdbcUpdate(s"create or replace table $test_table_basic " +
  //      s"(s1 string, s2 bigint)")
  //    jdbcUpdate(s"insert into $test_table_basic values " +
  //      s"""
  //         |('hello this is a test1', 1),
  //         |('hello this is a test2', 1),
  //         |('hello this is a test', 2),
  //         |('hello this is a test', 3),
  //         |(NULL, NULL)
  //         |""".stripMargin.linesIterator.mkString(" ").trim
  //    )
  //
  //    val tmpDF = sparkSession.read
  //      .format(SNOWFLAKE_SOURCE_NAME)
  //      .options(thisConnectorOptionsNoTable)
  //      .option("dbtable", test_table_basic)
  //      .load()
  //
  //    val resultDFStr = tmpDF.selectExpr("collect_set(s1)")
  //    val resultDFInt = tmpDF.selectExpr("collect_set(s2)")
  //
  //    // Cannot test the expected result cause the order of
  //    // the items returned in the array is non-deterministic
  //    testPushdownSql(
  //      s"""
  //         |SELECT (
  //         |  ARRAY_UNIQUE_AGG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" )
  //         |) AS "SUBQUERY_2_COL_0"
  //         |FROM (
  //         |  SELECT ( "SUBQUERY_0"."S1" ) AS "SUBQUERY_1_COL_0"
  //         |  FROM (
  //         |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
  //         |  ) AS "SUBQUERY_0"
  //         |) AS "SUBQUERY_1" LIMIT 1
  //         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
  //      resultDFStr
  //    )
  //    assert(
  //      resultDFStr.collect().head.get(0).asInstanceOf[Seq[String]].sorted ==
  //        Seq("hello this is a test", "hello this is a test1", "hello this is a test2").sorted
  //    )
  //
  //    testPushdownSql(
  //      s"""
  //         |SELECT (
  //         |  ARRAY_UNIQUE_AGG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" )
  //         |) AS "SUBQUERY_2_COL_0"
  //         |FROM (
  //         |  SELECT ( "SUBQUERY_0"."S2" ) AS "SUBQUERY_1_COL_0"
  //         |  FROM (
  //         |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
  //         |  ) AS "SUBQUERY_0"
  //         |) AS "SUBQUERY_1" LIMIT 1
  //         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
  //      resultDFInt
  //    )
  //    assert(
  //      resultDFInt
  //        .collect()
  //        .head
  //        .get(0)
  //        .asInstanceOf[Seq[java.math.BigDecimal]]
  //        .map(_.intValue)
  //        .sorted == Seq(1, 2, 3).sorted
  //    )
  //  }
}