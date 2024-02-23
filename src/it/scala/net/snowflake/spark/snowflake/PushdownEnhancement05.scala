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
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.snowflake.SFQueryTest

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

  test("AIQ test pushdown array_contains") {
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

    val resultDF = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_set(s1) as s1_agg"),
        expr("collect_set(s2) as s2_agg"),
        expr("collect_set(i1) as i1_agg"),
        expr("collect_set(i2) as i2_agg"),
      )
      .select(
        col("id"),
        expr("array_contains(s1_agg, 'hello1')").alias("arr_contains_s1"),
        expr("array_contains(s2_agg, 'test3')").alias("arr_contains_s2"),
        expr("array_contains(i1_agg, 9)").alias("arr_contains_i1"),
        expr("array_contains(i2_agg, 12)").alias("arr_contains_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), true, false, false, false),
      Row(BigDecimal(2), false, true, false, false),
      Row(BigDecimal(3), false, false, true, true),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_CONTAINS (
         |      'hello1' ::VARIANT ,
         |      ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S1" )
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_CONTAINS (
         |      'test3' ::VARIANT ,
         |      ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S2" )
         |    )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    ARRAY_CONTAINS (
         |      9 ::VARIANT ,
         |      ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I1" )
         |    )
         |  ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    ARRAY_CONTAINS (
         |      12 ::VARIANT ,
         |      ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I2" )
         |    )
         |  ) AS "SUBQUERY_1_COL_4"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown array_distinct") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello1', 'test1', 1, 2),
         |(2, 'hello2', 'test2', 3, 4),
         |(2, 'hello2', 'test2', 3, 4),
         |(3, 'hello3', NULL, 5, NULL),
         |(3, NULL, 'test3', NULL, 6),
         |(3, NULL, NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDF = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_list(s1) as s1_agg"),
        expr("collect_list(s2) as s2_agg"),
        expr("collect_list(i1) as i1_agg"),
        expr("collect_list(i2) as i2_agg"),
      )
      .select(
        col("id"),
        expr("array_distinct(s1_agg)").alias("arr_distinct_s1"),
        expr("array_distinct(s2_agg)").alias("arr_distinct_s2"),
        expr("array_distinct(i1_agg)").alias("arr_distinct_i1"),
        expr("array_distinct(i2_agg)").alias("arr_distinct_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), Array("hello1"), Array("test1"), Array(1), Array(2)),
      Row(BigDecimal(2), Array("hello2"), Array("test2"), Array(3), Array(4)),
      Row(BigDecimal(3), Array("hello3"), Array("test3"), Array(5), Array(6)),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) )
         |  ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) )
         |  ) AS "SUBQUERY_1_COL_4"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown array_except") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'hello1', 1, 2),
         |(1, 'hello2', NULL, 2, NULL),
         |(1, 'hello3', NULL, 2, NULL),
         |(2, 'hello4', 'hello4', 4, 3),
         |(2, 'hello5', NULL, 5, NULL),
         |(2, NULL, NULL, NULL, 6),
         |(2, NULL, NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDF = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_list(s1) as s1_agg"),
        expr("collect_list(s2) as s2_agg"),
        expr("collect_list(i1) as i1_agg"),
        expr("collect_list(i2) as i2_agg"),
      )
      .select(
        col("id"),
        expr("array_except(s1_agg, s2_agg)").alias("arr_except_s1_s2"),
        expr("array_except(i1_agg, i2_agg)").alias("arr_except_i1_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), Array("hello2", "hello3"), Array(1)),
      Row(BigDecimal(2), Array("hello5"), Array(4, 5)),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_EXCEPT (
         |      ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) ) ,
         |      ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) )
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_EXCEPT (
         |      ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) ) ,
         |      ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) )
         |    )
         |  ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown array_intersect") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'hello1', 1, 2),
         |(1, 'hello2', NULL, 2, NULL),
         |(1, 'hello3', NULL, 2, NULL),
         |(2, 'hello4', 'hello4', 4, 3),
         |(2, 'hello5', NULL, 5, NULL),
         |(2, NULL, NULL, NULL, 6),
         |(2, NULL, NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDF = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_list(s1) as s1_agg"),
        expr("collect_list(s2) as s2_agg"),
        expr("collect_list(i1) as i1_agg"),
        expr("collect_list(i2) as i2_agg"),
      )
      .select(
        col("id"),
        expr("array_intersect(s1_agg, s2_agg)").alias("arr_except_s1_s2"),
        expr("array_intersect(i1_agg, i2_agg)").alias("arr_except_i1_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), Array("hello1"), Array(2)),
      Row(BigDecimal(2), Array("hello4"), Array()),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_INTERSECTION (
         |      ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) ) ,
         |      ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) )
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_INTERSECTION (
         |      ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) ) ,
         |      ARRAY_DISTINCT ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) )
         |    )
         |  ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_basic  ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown array_join") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello2', NULL, 2, NULL),
         |(1, 'hello3', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', NULL, 5, NULL),
         |(2, NULL, NULL, NULL, 6),
         |(2, NULL, NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDF = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_list(s1) as s1_agg"),
        expr("collect_list(s2) as s2_agg"),
        expr("collect_list(i1) as i1_agg"),
        expr("collect_list(i2) as i2_agg"),
      )
      .select(
        col("id"),
        expr("array_join(s1_agg, ' | ')").alias("arr_join_s1"),
        expr("array_join(s2_agg, ' | ')").alias("arr_join_s2"),
        expr("array_join(i1_agg, ' | ')").alias("arr_join_i1"),
        expr("array_join(i2_agg, ' | ')").alias("arr_join_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), "hello1 | hello2 | hello3", "test1", "1 | 2 | 2", "2"),
      Row(BigDecimal(2), "hello4 | hello5", "test4", "4 | 5", "3 | 6"),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  ( ARRAY_TO_STRING ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) , ' | ' ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( ARRAY_TO_STRING ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) , ' | ' ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( ARRAY_TO_STRING ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) , ' | ' ) ) AS "SUBQUERY_1_COL_3" ,
         |  ( ARRAY_TO_STRING ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) , ' | ' ) ) AS "SUBQUERY_1_COL_4"
         |FROM (
         |  SELECT * FROM ( $test_table_basic  ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown array_max") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(1, 'hello3', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', NULL, 5, NULL),
         |(2, NULL, NULL, NULL, 6),
         |(2, NULL, NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDF = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_list(s1) as s1_agg"),
        expr("collect_list(s2) as s2_agg"),
        expr("collect_list(i1) as i1_agg"),
        expr("collect_list(i2) as i2_agg"),
      )
      .select(
        col("id"),
        expr("array_max(s1_agg)").alias("arr_max_s1"),
        expr("array_max(s2_agg)").alias("arr_max_s2"),
        expr("array_max(i1_agg)").alias("arr_max_i1"),
        expr("array_max(i2_agg)").alias("arr_max_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), "hello3", "test1", 2, null),
      Row(BigDecimal(2), "hello5", "test4", 5, 6),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    CAST ( ARRAY_MAX ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) ) AS VARCHAR )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    CAST ( ARRAY_MAX ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) ) AS VARCHAR )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    CAST ( ARRAY_MAX ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) ) AS DECIMAL(38, 0) )
         |  ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    CAST ( ARRAY_MAX ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) ) AS DECIMAL(38, 0) )
         |  ) AS "SUBQUERY_1_COL_4"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown array_min") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(1, 'hello3', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', NULL, 5, NULL),
         |(2, NULL, NULL, NULL, 6),
         |(2, NULL, NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDF = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_list(s1) as s1_agg"),
        expr("collect_list(s2) as s2_agg"),
        expr("collect_list(i1) as i1_agg"),
        expr("collect_list(i2) as i2_agg"),
      )
      .select(
        col("id"),
        expr("array_min(s1_agg)").alias("arr_min_s1"),
        expr("array_min(s2_agg)").alias("arr_min_s2"),
        expr("array_min(i1_agg)").alias("arr_min_i1"),
        expr("array_min(i2_agg)").alias("arr_min_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), "hello1", "test1", 1, null),
      Row(BigDecimal(2), "hello4", "test4", 4, 3),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    CAST ( ARRAY_MIN ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) ) AS VARCHAR )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    CAST ( ARRAY_MIN ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) ) AS VARCHAR )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    CAST ( ARRAY_MIN ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) ) AS DECIMAL(38, 0) )
         |  ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    CAST ( ARRAY_MIN ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) ) AS DECIMAL(38, 0) )
         |  ) AS "SUBQUERY_1_COL_4"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

//  test("AIQ test pushdown named_struct") {
//    jdbcUpdate(s"create or replace table $test_table_basic " +
//      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
//    jdbcUpdate(s"insert into $test_table_basic values " +
//      s"""
//         |(1, 'hello1', 'test1', 1, 2),
//         |(1, 'hello2', 'test2', 3, 4),
//         |(2, 'hello3', 'test3', 5, 6),
//         |(2, 'hello4', 'test4', 7, 8),
//         |(3, 'hello5', NULL, 9, NULL),
//         |(3, NULL, 'test6', NULL, 12),
//         |(3, NULL, NULL, NULL, NULL)
//         |""".stripMargin.linesIterator.mkString(" ").trim
//    )
//
//    val tmpDF = sparkSession.read
//      .format(SNOWFLAKE_SOURCE_NAME)
//      .options(thisConnectorOptionsNoTable)
//      .option("dbtable", test_table_basic)
//      .load()
//
//    val resultDF = tmpDF
//      .select(
//        col("id"),
//        expr("named_struct('a', s1, 'b', s2, 'c', i1, 'd', i2)").alias("struct_col"),
//      )
//      .groupBy(col("id"), col("struct_col")).
//      .select(col("id"), expr("cast(struct_col_agg as string)"))
//
//    val expectedResult = Seq(
//      """{"a":"hello1","b":"test1","c":1,"d":2}""",
//      """{"a":"hello2","b":"test2","c":3,"d":4}""",
//      """{"a":"hello3","b":"test3","c":5,"d":6}""",
//      """{"a":"hello4","b":"test4","c":7,"d":8}""",
//      """{"a":"hello5","b":null,"c":9,"d":null}""",
//      """{"a":null,"b":"test6","c":null,"d":12}""",
//      """{"a":null,"b":null,"c":null,"d":null}""",
//    ).map(Row(_))
//
//    testPushdown(
//      s"""
//         |SELECT (
//         |  CAST (
//         |    OBJECT_CONSTRUCT_KEEP_NULL (
//         |      'a' , "SUBQUERY_0"."S1" ,
//         |      'b' , "SUBQUERY_0"."S2" ,
//         |      'c' , "SUBQUERY_0"."I1" ,
//         |      'd' , "SUBQUERY_0"."I2"
//         |    ) AS VARCHAR
//         |  )
//         |) AS "SUBQUERY_1_COL_0"
//         |FROM (
//         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
//         |) AS "SUBQUERY_0"
//         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
//      resultDF,
//      expectedResult,
//    )
//  }
//
//  test("AIQ test pushdown array") {
//    jdbcUpdate(s"create or replace table $test_table_basic " +
//      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
//    jdbcUpdate(s"insert into $test_table_basic values " +
//      s"""
//         |(1, 'hello1', 'test1', 1, 2),
//         |(1, 'hello2', 'test2', 3, 4),
//         |(2, 'hello3', 'test3', 5, 6),
//         |(2, 'hello4', 'test4', 7, 8),
//         |(3, 'hello5', NULL, 9, NULL),
//         |(3, NULL, 'test6', NULL, 12),
//         |(3, NULL, NULL, NULL, NULL)
//         |""".stripMargin.linesIterator.mkString(" ").trim
//    )
//
//    val tmpDF = sparkSession.read
//      .format(SNOWFLAKE_SOURCE_NAME)
//      .options(thisConnectorOptionsNoTable)
//      .option("dbtable", test_table_basic)
//      .load()
//
//    val resultDFStr = tmpDF.selectExpr("array(s1, s2)")
//    val expectedResultStr = Seq(
//      Array("hello1", "test1"),
//      Array("hello2", "test2"),
//      Array("hello3", "test3"),
//      Array("hello4", "test4"),
//      Array("hello5"),
//      Array("test6"),
//      Array[String](),
//    ).map(Row(_))
//
//    testPushdownSql(
//      s"""
//         |SELECT (
//         |  ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S1" , "SUBQUERY_0"."S2" )
//         |) AS "SUBQUERY_1_COL_0"
//         |FROM (
//         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
//         |) AS "SUBQUERY_0"
//         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
//      resultDFStr,
//    )
//    SFQueryTest.checkAnswer(resultDFStr, expectedResultStr)
//
//    val resultDFInt = tmpDF.selectExpr("array(i1, i2)")
//    val expectedResultInt = Seq(
//      Seq(1, 2),
//      Seq(3, 4),
//      Seq(5, 6),
//      Seq(7, 8),
//      Seq(9),
//      Seq(12),
//      Seq[Int](),
//    ).map { r => Row(r.map(BigDecimal(_)).toArray) }
//
//    testPushdownSql(
//      s"""
//         |SELECT (
//         |  ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."I1" , "SUBQUERY_0"."I2" )
//         |) AS "SUBQUERY_1_COL_0"
//         |FROM (
//         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
//         |) AS "SUBQUERY_0"
//         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
//      resultDFInt,
//    )
//    SFQueryTest.checkAnswer(resultDFInt, expectedResultInt)
//  }

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