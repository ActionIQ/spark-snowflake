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

import java.util.TimeZone

// scalastyle:off println
class PushdownEnhancementAiqArrays extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_table_basic: String = s"test_basic_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_basic")
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

  test("AIQ test pushdown array_position") {
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
        expr("sort_array(collect_list(s1)) as s1_agg"),
        expr("sort_array(collect_list(s2)) as s2_agg"),
        expr("sort_array(collect_list(i1)) as i1_agg"),
        expr("sort_array(collect_list(i2)) as i2_agg"),
      )
      .select(
        col("id"),
        expr("array_position(s1_agg, 'hello2')").alias("arr_position_s1"),
        expr("array_position(s2_agg, 'test4')").alias("arr_position_s2"),
        expr("array_position(i1_agg, 2)").alias("arr_position_i1"),
        expr("array_position(i2_agg, 6)").alias("arr_position_i2"),
        expr("array_position(i2_agg, NULL)").alias("arr_position_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), 2, 0, 2, 0, null),
      Row(BigDecimal(2), 0, 1, 0, 2, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    COALESCE (
         |      (
         |        ARRAY_POSITION (
         |          'hello2' ::VARIANT ,
         |          ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) , true , true )
         |        ) + 1
         |      ) ,
         |      IFF (
         |        ( ( ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) , true , true ) IS NULL ) OR
         |        ( 'hello2' IS NULL ) ) ,
         |        NULL ,
         |        0
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    COALESCE (
         |      (
         |        ARRAY_POSITION (
         |          'test4' ::VARIANT ,
         |          ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) , true , true )
         |        ) + 1
         |      ) ,
         |      IFF (
         |        ( ( ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) , true , true ) IS NULL ) OR
         |        ( 'test4' IS NULL ) ) ,
         |        NULL ,
         |        0
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    COALESCE (
         |      (
         |        ARRAY_POSITION (
         |          2 ::VARIANT ,
         |          ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) , true , true )
         |        ) + 1
         |      ) ,
         |      IFF (
         |        ( ( ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) , true , true ) IS NULL ) OR
         |        ( 2 IS NULL ) ) ,
         |        NULL ,
         |        0
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    COALESCE (
         |      (
         |        ARRAY_POSITION (
         |          6 ::VARIANT ,
         |          ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) , true , true )
         |        ) + 1
         |      ) ,
         |      IFF (
         |        ( ( ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) , true , true ) IS NULL ) OR
         |        ( 6 IS NULL ) ) ,
         |        NULL ,
         |        0
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_4" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_5"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown array_remove") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 7),
         |(1, 'hello2', 'test2', 2, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', 'test5', 5, NULL),
         |(2, 'hello6', NULL, NULL, 6),
         |(2, NULL, 'test7', NULL, NULL)
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
        expr("sort_array(array_remove(s1_agg, 'hello2'))").alias("arr_remove_s1"),
        expr("sort_array(array_remove(s2_agg, 'test4'))").alias("arr_remove_s2"),
        expr("sort_array(array_remove(i1_agg, 2))").alias("arr_remove_i1"),
        expr("sort_array(array_remove(i2_agg, 6))").alias("arr_remove_i2"),
        expr("sort_array(array_remove(i2_agg, NULL))").alias("arr_remove_i2"),
      )
    val expectedResult = Seq(
      Row(
        BigDecimal(1),
        Array("hello1"),
        Array("test1", "test2"),
        Array(1),
        Array(7),
        null,
      ),
      Row(
        BigDecimal(2),
        Array("hello4", "hello5", "hello6"),
        Array("test5", "test7"),
        Array(4, 5),
        Array(3),
        null,
      ),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_REMOVE ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) , 'hello2' ::VARIANT ) ,
         |      true ,
         |      true
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_REMOVE ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) , 'test4' ::VARIANT ) ,
         |      true ,
         |      true
         |    )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_REMOVE ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) , 2 ::VARIANT ) ,
         |      true ,
         |      true
         |    )
         |  ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_REMOVE ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) , 6 ::VARIANT ) ,
         |      true ,
         |      true
         |    )
         |  ) AS "SUBQUERY_1_COL_4" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_5"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown array_union") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 7),
         |(1, 'hello2', 'test2', 2, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', 'test5', 5, NULL),
         |(2, 'hello6', NULL, NULL, 6),
         |(2, NULL, 'test7', NULL, NULL)
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
        expr("sort_array(array_union(s1_agg, s2_agg))").alias("arr_union_s1_s2"),
        expr("sort_array(array_union(i1_agg, i2_agg))").alias("arr_union_i1_i2"),
      )
    val expectedResult = Seq(
      Row(
        BigDecimal(1),
        Array("hello1", "hello2", "test1", "test2"),
        Array(1, 2, 7),
      ),
      Row(
        BigDecimal(2),
        Array("hello4", "hello5", "hello6", "test4", "test5", "test7"),
        Array(3, 4, 5, 6),
      ),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_DISTINCT (
         |        ARRAY_CAT (
         |          ARRAY_AGG ( "SUBQUERY_0"."S1" ) ,
         |          ARRAY_AGG ( "SUBQUERY_0"."S2" )
         |        )
         |      ) , true , true
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_DISTINCT (
         |        ARRAY_CAT (
         |          ARRAY_AGG ( "SUBQUERY_0"."I1" ) ,
         |          ARRAY_AGG ( "SUBQUERY_0"."I2" )
         |        )
         |      ) , true , true
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

  test("AIQ test pushdown concat (arrays)") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello2', 'test2', 2, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', 'test5', 5, NULL),
         |(2, 'hello6', NULL, NULL, 6),
         |(2, NULL, 'test7', NULL, NULL)
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
        expr("sort_array(concat(s1_agg, s2_agg))").alias("concat_s1_s2"),
        expr("sort_array(concat(i1_agg, i2_agg))").alias("concat_i1_i2"),
      )
    val expectedResult = Seq(
      Row(
        BigDecimal(1),
        Array("hello1", "hello2", "hello2", "test1", "test2"),
        Array(1, 2, 2, 2),
      ),
      Row(
        BigDecimal(2),
        Array("hello4", "hello5", "hello6", "test4", "test5", "test7"),
        Array(3, 4, 5, 6),
      ),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_CAT (
         |        ARRAY_AGG ( "SUBQUERY_0"."S1" ) ,
         |        ARRAY_AGG ( "SUBQUERY_0"."S2" )
         |      ) , true , true
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_CAT (
         |        ARRAY_AGG ( "SUBQUERY_0"."I1" ) ,
         |        ARRAY_AGG ( "SUBQUERY_0"."I2" )
         |      ) , true , true
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

  test("AIQ test pushdown flatten") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id1 bigint, id2 bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(0, 1, 'hello1', 'test1', 1, 2),
         |(0, 1, 'hello2', 'test2', 2, NULL),
         |(0, 1, 'hello2', NULL, 2, NULL),
         |(0, 2, 'hello4', 'test4', 4, 3),
         |(0, 2, 'hello5', 'test5', 5, NULL),
         |(0, 2, 'hello6', NULL, NULL, 6),
         |(0, 2, NULL, 'test7', NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDF = tmpDF
      .groupBy("id1", "id2")
      .agg(
        expr("collect_list(s1) as s1_agg"),
        expr("collect_list(s2) as s2_agg"),
        expr("collect_list(i1) as i1_agg"),
        expr("collect_list(i2) as i2_agg"),
      )
      .selectExpr("*")
      .groupBy("id1")
      .agg(
        expr("collect_list(s1_agg) as s1_overall_agg"),
        expr("collect_list(s2_agg) as s2_overall_agg"),
        expr("collect_list(i1_agg) as i1_overall_agg"),
        expr("collect_list(i2_agg) as i2_overall_agg"),
      )
      .select(
        col("id1"),
        expr("sort_array(flatten(s1_overall_agg))").alias("flatten_s1"),
        expr("sort_array(flatten(s2_overall_agg))").alias("flatten_s2"),
        expr("sort_array(flatten(i1_overall_agg))").alias("flatten_i1"),
        expr("sort_array(flatten(i2_overall_agg))").alias("flatten_i2"),
      )
    val expectedResult = Seq(
      Row(
        BigDecimal(0),
        Array("hello1", "hello2", "hello2", "hello4", "hello5", "hello6"),
        Array("test1", "test2", "test4", "test5", "test7"),
        Array(1, 2, 2, 4, 5),
        Array(2, 3, 6),
      ),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_FLATTEN (
         |        ARRAY_AGG ( "SUBQUERY_1"."SUBQUERY_1_COL_1" )
         |      ) , true , true
         |    )
         |  ) AS "SUBQUERY_2_COL_1" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_FLATTEN (
         |        ARRAY_AGG ( "SUBQUERY_1"."SUBQUERY_1_COL_2" )
         |      ) , true , true
         |    )
         |  ) AS "SUBQUERY_2_COL_2" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_FLATTEN (
         |        ARRAY_AGG ( "SUBQUERY_1"."SUBQUERY_1_COL_3" )
         |      ) , true , true
         |    )
         |  ) AS "SUBQUERY_2_COL_3" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_FLATTEN (
         |        ARRAY_AGG ( "SUBQUERY_1"."SUBQUERY_1_COL_4" )
         |      ) , true , true
         |    )
         |  ) AS "SUBQUERY_2_COL_4"
         |FROM (
         |  SELECT
         |    ( "SUBQUERY_0"."ID1" ) AS "SUBQUERY_1_COL_0" ,
         |    ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) ) AS "SUBQUERY_1_COL_1" ,
         |    ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) ) AS "SUBQUERY_1_COL_2" ,
         |    ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) ) AS "SUBQUERY_1_COL_3" ,
         |    ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) ) AS "SUBQUERY_1_COL_4"
         |  FROM (
         |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |  GROUP BY "SUBQUERY_0"."ID1" , "SUBQUERY_0"."ID2"
         |) AS "SUBQUERY_1"
         |GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown size") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello2', 'test2', 2, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', 'test5', 5, NULL),
         |(2, 'hello6', NULL, NULL, 6),
         |(2, NULL, 'test7', NULL, NULL)
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
        expr("size(s1_agg)").alias("size_s1"),
        expr("size(s2_agg)").alias("size_s2"),
        expr("size(i1_agg)").alias("size_i1"),
        expr("size(i2_agg)").alias("size_i2"),
      )
    val expectedResult = Seq(
      Row(BigDecimal(1), 3, 2, 3, 1),
      Row(BigDecimal(2), 3, 3, 2, 2),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  ( ARRAY_SIZE ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( ARRAY_SIZE ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( ARRAY_SIZE ( ARRAY_AGG ( "SUBQUERY_0"."I1" ) ) ) AS "SUBQUERY_1_COL_3" ,
         |  ( ARRAY_SIZE ( ARRAY_AGG ( "SUBQUERY_0"."I2" ) ) ) AS "SUBQUERY_1_COL_4"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown sort_array") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello2', 'test2', 2, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', 'test5', 5, NULL),
         |(2, 'hello6', NULL, NULL, 6),
         |(2, NULL, 'test7', NULL, NULL)
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
        expr("sort_array(array_distinct(concat(s1_agg, s2_agg)))").alias("sort_array_s1_s2"),
        expr("sort_array(array_distinct(concat(i1_agg, i2_agg)))").alias("sort_array_i1_i2"),
      )
    val expectedResult = Seq(
      Row(
        BigDecimal(1),
        Array("hello1", "hello2", "test1", "test2"),
        Array(1, 2),
      ),
      Row(
        BigDecimal(2),
        Array("hello4", "hello5", "hello6", "test4", "test5", "test7"),
        Array(3, 4, 5, 6),
      ),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_DISTINCT (
         |        ARRAY_CAT (
         |          ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S1" ) ,
         |          ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S2" )
         |        )
         |      ) , true , true
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_SORT (
         |      ARRAY_DISTINCT (
         |        ARRAY_CAT (
         |          ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I1" ) ,
         |          ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I2" )
         |        )
         |      ) , true , true
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

  test("AIQ test pushdown named_struct") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello2', 'test2', 2, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', 'test5', 5, NULL),
         |(2, 'hello6', NULL, NULL, 6),
         |(2, NULL, 'test7', NULL, NULL)
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
        expr("sort_array(collect_set(s1)) as s1_agg"),
        expr("sort_array(collect_set(s2)) as s2_agg"),
        expr("sort_array(collect_set(i1)) as i1_agg"),
        expr("sort_array(collect_set(i2)) as i2_agg"),
      )
      .select(
        col("id"),
        expr(
          "named_struct('a', s1_agg, 'b', s2_agg, 'c', i1_agg, 'd', i2_agg)"
        ).alias("named_struct_col"),
      )
    val expectedResult = Seq(
      Row(
        BigDecimal(1),
        Row(Array("hello1", "hello2"), Array("test1", "test2"), Array(1, 2), Array(2)),
      ),
      Row(
        BigDecimal(2),
        Row(
          Array("hello4", "hello5", "hello6"),
          Array("test4", "test5", "test7"),
          Array(4, 5),
          Array(3, 6),
        ),
      ),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    OBJECT_CONSTRUCT_KEEP_NULL (
         |      'a' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S1" ) , true , true ) ,
         |      'b' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S2" ) , true , true ) ,
         |      'c' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I1" ) , true , true ) ,
         |      'd' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I2" ) , true , true )
         |    )
         |  ) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown to_json") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello2', 'test2', 2, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', 'test5', 5, NULL),
         |(2, 'hello6', NULL, NULL, 6),
         |(2, NULL, 'test7', NULL, NULL)
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
        expr("sort_array(collect_set(s1)) as s1_agg"),
        expr("sort_array(collect_set(s2)) as s2_agg"),
        expr("sort_array(collect_set(i1)) as i1_agg"),
        expr("sort_array(collect_set(i2)) as i2_agg"),
      )
      .select(
        col("id"),
        expr(
          "to_json(named_struct('a', s1_agg, 'b', s2_agg, 'c', i1_agg, 'd', i2_agg))"
        ).alias("to_json_col"),
      )
    val expectedResult = Seq(
      Row(
        BigDecimal(1),
        """{"a":["hello1","hello2"],"b":["test1","test2"],"c":[1,2],"d":[2]}""",
      ),
      Row(
        BigDecimal(2),
        """{"a":["hello4","hello5","hello6"],"b":["test4","test5","test7"],"c":[4,5],"d":[3,6]}""",
      ),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    TO_JSON (
         |      OBJECT_CONSTRUCT_KEEP_NULL (
         |        'a' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S1" ) , true , true ) ,
         |        'b' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S2" ) , true , true ) ,
         |        'c' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I1" ) , true , true ) ,
         |        'd' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I2" ) , true , true )
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown from_json") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 string, i1 bigint, i2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, 'hello1', 'test1', 1, 2),
         |(1, 'hello2', 'test2', 2, NULL),
         |(1, 'hello2', NULL, 2, NULL),
         |(2, 'hello4', 'test4', 4, 3),
         |(2, 'hello5', 'test5', 5, NULL),
         |(2, 'hello6', NULL, NULL, 6),
         |(2, NULL, 'test7', NULL, NULL)
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
        expr("sort_array(collect_set(s1)) as s1_agg"),
        expr("sort_array(collect_set(s2)) as s2_agg"),
        expr("sort_array(collect_set(i1)) as i1_agg"),
        expr("sort_array(collect_set(i2)) as i2_agg"),
      )
      .select(
        col("id"),
        expr(
          s"""from_json(
             |  to_json(named_struct('a', s1_agg, 'b', s2_agg, 'c', i1_agg, 'd', i2_agg)),
             |  'a ARRAY<STRING>, b ARRAY<STRING>, c ARRAY<BIGINT>, d ARRAY<BIGINT>'
             |)
             |""".stripMargin.linesIterator.mkString("").trim
        ).alias("from_json_col"),
      )
    val expectedResult = Seq(
      Row(
        BigDecimal(1),
        Row(Array("hello1", "hello2"), Array("test1", "test2"), Array(1, 2), Array(2)),
      ),
      Row(
        BigDecimal(2),
        Row(
          Array("hello4", "hello5", "hello6"),
          Array("test4", "test5", "test7"),
          Array(4, 5),
          Array(3, 6),
        ),
      ),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    PARSE_JSON (
         |      TO_JSON (
         |        OBJECT_CONSTRUCT_KEEP_NULL (
         |          'a' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S1" ) , true , true ) ,
         |          'b' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S2" ) , true , true ) ,
         |          'c' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I1" ) , true , true ) ,
         |          'd' , ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."I2" ) , true , true )
         |        )
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }
}