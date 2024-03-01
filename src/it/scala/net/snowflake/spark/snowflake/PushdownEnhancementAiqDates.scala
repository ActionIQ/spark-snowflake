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
class PushdownEnhancementAiqDates extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_table_date = s"test_table_date_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_date")
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

  // Date-Style

  test("AIQ test pushdown datediff") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d1 date, d2 date)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2020-07-29', '2020-07-28'),
         |('2020-07-31', '2020-07-29'),
         |('2020-07-28', '2020-07-29'),
         |('2020-07-29', '2020-07-31'),
         |(NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("datediff(d1, d2)")
    val expectedResult = Seq(Row(1L), Row(2L), Row(-1L), Row(-2L), Row(null))
    testPushdown(
      s"""
         |SELECT (
         |  DATEDIFF ( 'DAY' , "SUBQUERY_0"."D2" , "SUBQUERY_0"."D1" )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown hour") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("hour(d)", "hour(t)", "hour(s)")
    val expectedResult = Seq(
      Row(0, 13, 13),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( HOUR ( CAST ( "SUBQUERY_0"."D" AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( HOUR ( "SUBQUERY_0"."T" ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( HOUR ( CAST ( "SUBQUERY_0"."S" AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown minute") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("minute(d)", "minute(t)", "minute(s)")
    val expectedResult = Seq(
      Row(0, 10, 10),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( MINUTE ( CAST ( "SUBQUERY_0"."D" AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( MINUTE ( "SUBQUERY_0"."T" ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( MINUTE ( CAST ( "SUBQUERY_0"."S" AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown second") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("second(d)", "second(t)", "second(s)")
    val expectedResult = Seq(
      Row(0, 15, 15),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( SECOND ( CAST ( "SUBQUERY_0"."D" AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( SECOND ( "SUBQUERY_0"."T" ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( SECOND ( CAST ( "SUBQUERY_0"."S" AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown dayofmonth") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("dayofmonth(d)", "dayofmonth(t)", "dayofmonth(s)")
    val expectedResult = Seq(
      Row(8, 8, 8),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( DAYOFMONTH ( "SUBQUERY_0"."D" ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( DAYOFMONTH ( CAST ( "SUBQUERY_0"."T" AS DATE ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( DAYOFMONTH ( CAST ( "SUBQUERY_0"."S" AS DATE ) ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown dayofyear") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("dayofyear(d)", "dayofyear(t)", "dayofyear(s)")
    val expectedResult = Seq(
      Row(98, 98, 98),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( DAYOFYEAR ( "SUBQUERY_0"."D" ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( DAYOFYEAR ( CAST ( "SUBQUERY_0"."T" AS DATE ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( DAYOFYEAR ( CAST ( "SUBQUERY_0"."S" AS DATE ) ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown weekofyear") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("weekofyear(d)", "weekofyear(t)", "weekofyear(s)")
    val expectedResult = Seq(
      Row(15, 15, 15),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( WEEKOFYEAR ( "SUBQUERY_0"."D" ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( WEEKOFYEAR ( CAST ( "SUBQUERY_0"."T" AS DATE ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( WEEKOFYEAR ( CAST ( "SUBQUERY_0"."S" AS DATE ) ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown last_day") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "cast(last_day(d) as string)",
      "cast(last_day(t) as string)",
      "cast(last_day(s) as string)"
    )
    val expectedResult = Seq(
      Row("2015-04-30", "2015-04-30", "2015-04-30"),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( CAST ( LAST_DAY ( "SUBQUERY_0"."D" , 'MONTH' ) AS VARCHAR ) ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    CAST (
         |      LAST_DAY ( CAST ( "SUBQUERY_0"."T" AS DATE ) , 'MONTH' ) AS VARCHAR
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    CAST (
         |      LAST_DAY ( CAST ( "SUBQUERY_0"."S" AS DATE ) , 'MONTH' ) AS VARCHAR
         |    )
         |  ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown months_between") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d1 date, d2 date)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('1997-02-28', '1996-10-30'),
         |('2020-02-01', '2020-01-01'),
         |(NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("round(months_between(d1, d2), 6)")
    val expectedResult = Seq(
      Row(3.935484),
      Row(1.000000),
      Row(null),
    )
    testPushdown(
      s"""
         |SELECT (
         |  ROUND (
         |    MONTHS_BETWEEN (
         |      CAST ( "SUBQUERY_0"."D1" AS TIMESTAMP ) ,
         |      CAST ( "SUBQUERY_0"."D2" AS TIMESTAMP )
         |    ) ,
         |    6
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown next_day") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(dt date, d string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-01-14', 'TU'),
         |('2015-01-14', 'WE'),
         |('2015-01-14', 'FR'),
         |(NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("cast(next_day(dt, d) as string)")
    val expectedResult = Seq(
      Row("2015-01-20"),
      Row("2015-01-21"),
      Row("2015-01-16"),
      Row(null),
    )
    testPushdown(
      s"""
         |SELECT (
         |  CAST (
         |    NEXT_DAY ( "SUBQUERY_0"."DT" , "SUBQUERY_0"."D" ) AS VARCHAR
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown dayofweek") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("dayofweek(d)", "dayofweek(t)", "dayofweek(s)")
    val expectedResult = Seq(
      Row(4, 4, 4),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( ( DAYOFWEEK ( "SUBQUERY_0"."D" ) + 1 ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( ( DAYOFWEEK ( CAST ( "SUBQUERY_0"."T" AS DATE ) ) + 1 ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( ( DAYOFWEEK ( CAST ( "SUBQUERY_0"."S" AS DATE ) ) + 1 ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown weekday") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2015-04-08 13:10:15', '2015-04-08 13:10:15', '2015-04-08 13:10:15'),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("weekday(d)", "weekday(t)", "weekday(s)")
    val expectedResult = Seq(
      Row(2, 2, 2),
      Row(null, null, null),
    )
    testPushdown(
      s"""
         |SELECT
         |  ( ( DAYOFWEEKISO ( "SUBQUERY_0"."D" ) - 1 ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( ( DAYOFWEEKISO ( CAST ( "SUBQUERY_0"."T" AS DATE ) ) - 1 ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( ( DAYOFWEEKISO ( CAST ( "SUBQUERY_0"."S" AS DATE ) ) - 1 ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown make_date") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(y bigint, m bigint, d bigint)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |(2015, 4, 8),
         |(2015, 04, 08),
         |(NULL, NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("cast(make_date(y, m, d) as string)")
    val expectedResult = Seq(
      Row("2015-04-08"),
      Row("2015-04-08"),
      Row(null),
    )
    testPushdown(
      s"""
         |SELECT (
         |  CAST (
         |    DATE_FROM_PARTS (
         |      CAST ( "SUBQUERY_0"."Y" AS NUMBER ) ,
         |      CAST ( "SUBQUERY_0"."M" AS NUMBER ) ,
         |      CAST ( "SUBQUERY_0"."D" AS NUMBER )
         |    ) AS VARCHAR
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown make_timestamp") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(y bigint, m bigint, d bigint, h bigint, mi bigint, s decimal(16, 6))")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |(2015, 4, 8, 1, 1, 1.123),
         |(2015, 04, 08, 13, 10, 15.456),
         |(NULL, NULL, NULL, NULL, NULL, NULL)
         |""".stripMargin
    )

    def expectedPushdownSql(timeZone: Boolean = false): String = {
      val partialFunc =
        s"""
           |CAST (
           |  TIMESTAMP_NTZ_FROM_PARTS (
           |    CAST ( "SUBQUERY_0"."Y" AS NUMBER ) ,
           |    CAST ( "SUBQUERY_0"."M" AS NUMBER ) ,
           |    CAST ( "SUBQUERY_0"."D" AS NUMBER ) ,
           |    CAST ( "SUBQUERY_0"."H" AS NUMBER ) ,
           |    CAST ( "SUBQUERY_0"."MI" AS NUMBER ) ,
           |    CAST ( "SUBQUERY_0"."S" AS NUMBER ) ,
           |    CAST ( ( ( "SUBQUERY_0"."S" % 1 ) * 1000000000 ) AS NUMBER )
           |    ) AS VARCHAR
           |)
           |"""

      val fullFunc = if (timeZone) {
        s"""
           |CAST (
           |  CONVERT_TIMEZONE (
           |    'America/New_York' ,
           |    'UTC' ,
           |    $partialFunc
           |  ) AS VARCHAR
           |)
           |"""
      } else { s"$partialFunc" }

      s"""
         |SELECT (
         |  $fullFunc
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim
    }

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("timestamp_tz_output_format", "YYYY-MM-DD HH24:MI:SS.FF3")
      .option("timestamp_ntz_output_format", "YYYY-MM-DD HH24:MI:SS.FF3" )
      .option("dbtable", test_table_date)
      .load()

    val resultDFNTZ = tmpDF.selectExpr("cast(make_timestamp(y, m, d, h, mi, s) as string)")
    val expectedResultNTZ = Seq(
      Row("2015-04-08 01:01:01.123"),
      Row("2015-04-08 13:10:15.456"),
      Row(null),
    )
    testPushdown(expectedPushdownSql(), resultDFNTZ, expectedResultNTZ)

    val resultDFTZ = tmpDF.selectExpr(
      "cast(make_timestamp(y, m, d, h, mi, s, 'America/New_York') as string)"
    )
    val expectedResultTZ = Seq(
      Row("2015-04-08 05:01:01.123"),
      Row("2015-04-08 17:10:15.456"),
      Row(null),
    )
    testPushdown(expectedPushdownSql(true), resultDFTZ, expectedResultTZ)

    // Test for when the sec argument equals to 60 (the seconds field
    // is set to 0 and 1 minute is added to the final timestamp)

    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(y bigint, m bigint, d bigint, h bigint, mi bigint, s decimal(16, 6))")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"(2015, 04, 08, 13, 10, 60)"
    )

    val tmpDFFmt0 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("timestamp_tz_output_format", "YYYY-MM-DD HH24:MI:SS")
      .option("timestamp_ntz_output_format", "YYYY-MM-DD HH24:MI:SS" )
      .option("dbtable", test_table_date)
      .load()

    val resultDFFmt0NTZ = tmpDFFmt0.selectExpr("cast(make_timestamp(y, m, d, h, mi, s) as string)")
    val expectedResultFmt0NTZ = Seq(Row("2015-04-08 13:11:00"))
    testPushdown(expectedPushdownSql(), resultDFFmt0NTZ, expectedResultFmt0NTZ)

    val resultDFFmt0TZ = tmpDFFmt0.selectExpr(
      "cast(make_timestamp(y, m, d, h, mi, s, 'America/New_York') as string)"
    )
    val expectedResultFmt0TZ = Seq(Row("2015-04-08 17:11:00"))
    testPushdown(expectedPushdownSql(true), resultDFFmt0TZ, expectedResultFmt0TZ)
  }

  test("AIQ test pushdown extract") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d date, t timestamp)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2019-08-12', '2019-08-12 01:00:00.123456'),
         |(NULL, NULL)
         |""".stripMargin
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "extract(year from d)",
      "extract(year from t)",
      "extract(month from d)",
      "extract(month from t)",
      "extract(day from d)",
      "extract(day from t)",
    )
    val expectedResult = Seq(
      Row(2019, 2019, 8, 8, 12, 12),
      Row(null, null, null, null, null, null),
    )
    // Extract SQL function rewrites the queries to equivalent
    // `DatePart` functions hence the produced PushDown SQL below
    testPushdown(
      s"""
         |SELECT
         |  ( YEAR ( "SUBQUERY_0"."D" ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( YEAR ( CAST ( "SUBQUERY_0"."T" AS DATE ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( MONTH ( "SUBQUERY_0"."D" ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( MONTH ( CAST ( "SUBQUERY_0"."T" AS DATE ) ) ) AS "SUBQUERY_1_COL_3" ,
         |  ( DAYOFMONTH ( "SUBQUERY_0"."D" ) ) AS "SUBQUERY_1_COL_4" ,
         |  ( DAYOFMONTH ( CAST ( "SUBQUERY_0"."T" AS DATE ) ) ) AS "SUBQUERY_1_COL_5"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown day_start") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(ts bigint, tz string, pd int)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"(1460080000000, 'America/New_York', 2)"
    )
    jdbcUpdate(s"insert into $test_table_date values " +
      s"(1460080000000, 'Asia/Tokyo', -1)"
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("aiq_day_start(ts, tz, pd)")
    val expectedResult = Seq(
      Row(1459954800000L),
      Row(1460174400000L),
    )

    testPushdown(
      s"""
         |SELECT (
         |  DATE_PART (
         |    'EPOCH_MILLISECOND' ,
         |    DATE_TRUNC (
         |      'DAY' ,
         |      DATEADD (
         |        day ,
         |        CAST ( "SUBQUERY_0"."PD" AS NUMBER ) ,
         |        CONVERT_TIMEZONE (
         |          "SUBQUERY_0"."TZ" ,
         |          CAST ( CAST ( "SUBQUERY_0"."TS" AS NUMBER ) AS VARCHAR )
         |        )
         |      )
         |    )
         |  )
         |) AS "SUBQUERY_1_COL_0" FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown string_to_date") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(dt string, fmt string, tz string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2019-09-01 14:50', 'yyyy-MM-dd HH:mm', 'America/New_York'),
         |('2019-09-01 02:50 PM', 'yyyy-MM-dd hh:mm a', 'America/New_York'),
         |('2019-09-01 PM 02:50', 'yyyy-MM-dd a hh:mm', 'America/New_York')
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("aiq_string_to_date(dt, fmt, tz)")
    val expectedResult = Seq(
      Row(1567363800000L),
      Row(1567363800000L),
      Row(1567363800000L),
    )
    checkAnswer(resultDF, expectedResult)

    jdbcUpdate(s"create or replace table $test_table_date " +
      "(dt string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      "('2019-09-01 14:50:52')")

    val pushDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val pushResultDF = pushDf.selectExpr(
      "aiq_string_to_date(dt, 'yyyy-MM-dd HH:mm:ss', 'America/New_York')"
    )

    testPushdown(
      s"""
         |SELECT (
         |  DATE_PART (
         |    'EPOCH_MILLISECOND' ,
         |    CONVERT_TIMEZONE (
         |      'America/New_York' ,
         |      'UTC' ,
         |      CAST ( TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."DT" , 'yyyy-MM-dd HH24:mi:SS' ) AS VARCHAR )
         |    )
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM ( SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      pushResultDF,
      Seq(Row(1567363852000L))
    )
  }

  test("AIQ test pushdown date_to_string") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(ts bigint, fmt string, tz string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |(1567363852000, 'MM', 'America/New_York'),
         |(1567363852000, 'yyyy-MM-dd', 'America/New_York'),
         |(1567363852000, 'yyyy-MM-dd HH:mm', 'America/New_York'),
         |(1567363852000, 'yyyy-MM-dd hh:mm a', 'America/New_York'),
         |(1567363852000, 'yyyy-MM-dd a hh:mm', 'America/New_York'),
         |(1567363852000, 'yyyy-MM-dd a hh:mm:mm:ss a', 'America/New_York'),
         |(1567363852000, 'yyyy-MM-dd HH:mm:ss', 'America/New_York'),
         |(1567363852000, 'yyyy-MM-dd hh:mm:ss', 'America/New_York'),
         |(1567363852000, 'yyyy-MM-dd hh:mm:mm:ss', 'America/New_York')
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("aiq_date_to_string(ts, fmt, tz)")
    val expectedResult = Seq(
      Row("09"),
      Row("2019-09-01"),
      Row("2019-09-01 14:50"),
      Row("2019-09-01 02:50 PM"),
      Row("2019-09-01 PM 02:50"),
      Row("2019-09-01 PM 02:50:50:52 PM"),
      Row("2019-09-01 14:50:52"),
      Row("2019-09-01 02:50:52"),
      Row("2019-09-01 02:50:50:52"),
    )
    checkAnswer(resultDF, expectedResult)

    jdbcUpdate(s"create or replace table $test_table_date " +
      "(ts bigint)")
    jdbcUpdate(s"insert into $test_table_date values " +
      "(1567363852000)")

    val pushDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val pushResultDF = pushDf.select(
      aiq_date_to_string(col("ts"), "MM", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd HH:mm", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd hh:mm a", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd a hh:mm", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd a hh:mm:mm:ss a", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd hh:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd hh:mm:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd M HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd MM HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd aMa HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd MMM HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd aMMMa HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd MMMM HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd MMMMM HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd MMMMMM HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd E HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd EE HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd EEE HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd EEEE HH:mm:ss", "America/New_York"),
      aiq_date_to_string(col("ts"), "yyyy-MM-dd EEEEEEEE HH:mm:ss", "America/New_York"),
    )
    val extraFormattingExpectedResults = Seq(
      "2019-09-01 09 14:50:52",
      "2019-09-01 09 14:50:52",
      "2019-09-01 PM09PM 14:50:52",
      "2019-09-01 Sep 14:50:52",
      "2019-09-01 PMSepPM 14:50:52",
      "2019-09-01 September 14:50:52",
      "2019-09-01 SeptemberM 14:50:52",
      "2019-09-01 September 14:50:52",
      "2019-09-01 Sun 14:50:52",
      "2019-09-01 Sun 14:50:52",
      "2019-09-01 Sun 14:50:52",
      "2019-09-01 Sunday 14:50:52",
      "2019-09-01 Sunday 14:50:52",
    )
    val pushExpectedResult = Seq(
      Row(expectedResult.map(_.getString(0)) ++ extraFormattingExpectedResults: _*)
    )
    checkAnswer(pushResultDF, pushExpectedResult)

    val finalPushResultDF = pushDf.select(
      aiq_date_to_string(col("ts"), "yyyy-MM-dd HH:mm:ss", "America/New_York")
    )
    testPushdown(
      s"""
         |SELECT (
         |  TO_CHAR (
         |    CONVERT_TIMEZONE (
         |      'America/New_York' ,
         |      CAST ( CAST ( "SUBQUERY_0"."TS" AS NUMBER ) AS VARCHAR )
         |    ),
         |    'yyyy-MM-dd HH24:mi:SS'
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      finalPushResultDF,
      Seq(Row("2019-09-01 14:50:52"))
    )
  }

  test("AIQ test pushdown day_diff") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(startMs bigint, endMs bigint, tz string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |(1693609200000, 1693616400000, 'UTC'),
         |(1693609200000, 1693616400000, 'EST'),
         |(1693609200000, NULL, 'UTC'),
         |(NULL, 1693616400000, 'UTC'),
         |(1693609200000, 1693616400000, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("aiq_day_diff(startMs, endMs, tz)")
    val expectedResult = Seq(
      Row(1L),
      Row(0L),
      Row(null),
      Row(null),
      Row(null),
    )

    testPushdown(
      s"""
         |SELECT (
         |  DATEDIFF (
         |    'DAY' ,
         |    CONVERT_TIMEZONE (
         |      "SUBQUERY_0"."TZ" ,
         |      CAST ( CAST ( "SUBQUERY_0"."STARTMS" AS NUMBER ) AS VARCHAR )
         |    ) ,
         |    CONVERT_TIMEZONE (
         |      "SUBQUERY_0"."TZ" ,
         |      CAST ( CAST ( "SUBQUERY_0"."ENDMS" AS NUMBER ) AS VARCHAR )
         |    )
         |  )
         |) AS "SUBQUERY_1_COL_0" FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown week_diff") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(startMs bigint, endMs bigint, tz string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |(1551880107963, 1553890107963, 'UTC'),
         |(1551880107963, 1553890107963, 'Asia/Ulan_Bator'),
         |(NULL, 1553890107963, 'UTC'),
         |(1551880107963, NULL, 'UTC'),
         |(1551880107963, 1553890107963, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("aiq_week_diff(startMs, endMs, 'sunday', tz)")
    val expectedResult = Seq(
      Row(3L),
      Row(3L),
      Row(null),
      Row(null),
      Row(null),
    )

    testPushdown(
      s"""
         |SELECT (
         |  (
         |    FLOOR (
         |      (
         |        (
         |          DATEDIFF (
         |            'DAY' ,
         |            CONVERT_TIMEZONE ( "SUBQUERY_0"."TZ" , CAST ( 0 AS VARCHAR ) ) ,
         |            CONVERT_TIMEZONE (
         |              "SUBQUERY_0"."TZ" ,
         |              CAST ( CAST ( "SUBQUERY_0"."ENDMS" AS NUMBER ) AS VARCHAR )
         |            )
         |          ) + 4
         |        ) / 7
         |      )
         |    )
         |    -
         |    FLOOR (
         |      (
         |        (
         |          DATEDIFF (
         |            'DAY' ,
         |            CONVERT_TIMEZONE ( "SUBQUERY_0"."TZ" , CAST ( 0 AS VARCHAR ) ) ,
         |            CONVERT_TIMEZONE (
         |              "SUBQUERY_0"."TZ" ,
         |              CAST ( CAST ( "SUBQUERY_0"."STARTMS" AS NUMBER ) AS VARCHAR ) )
         |          ) + 4
         |        ) / 7
         |      )
         |    )
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )

    assert(
      tmpDF.limit(1).selectExpr(
        "aiq_week_diff(startMs, endMs, 'SUNDAY', tz)"
      ).collect().head == Row(3L)
    )

    assert(
      tmpDF.limit(1).selectExpr(
        "aiq_week_diff(startMs, endMs, 'SUN', tz)"
      ).collect().head == Row(3L)
    )

    assert(
      tmpDF.limit(1).selectExpr(
        "aiq_week_diff(1567363852000, 1567450252000, 'monday', tz)"
      ).collect().head == Row(1L)
    )

    assert(
      tmpDF.limit(1).selectExpr(
        "aiq_week_diff(1567363852000, 1567450252000, NULL, tz)"
      ).collect().head == Row(null)
    )
  }

  test("AIQ test pushdown day_of_the_week") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(ts bigint, tz string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |(1553890107963, 'UTC'),
         |(1553890107963, 'Asia/Ulan_Bator'),
         |(1553890107963, 'Pacific/Gambier'),
         |(1553890107963, NULL),
         |(NULL, 'UTC')
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("aiq_day_of_the_week(ts, tz)")
    val expectedResult = Seq(
      Row("friday"),
      Row("saturday"),
      Row("friday"),
      Row(null),
      Row(null),
    )

    testPushdown(
      s"""
         |SELECT (
         |  DECODE (
         |    (
         |      DAYOFWEEKISO (
         |        CONVERT_TIMEZONE (
         |          "SUBQUERY_0"."TZ" ,
         |          CAST ( CAST ( "SUBQUERY_0"."TS" AS NUMBER ) AS VARCHAR )
         |        )
         |      ) - 1
         |    ),
         |    0 , 'monday' ,
         |    1 , 'tuesday' ,
         |    2 , 'wednesday' ,
         |    3 , 'thursday' ,
         |    4 , 'friday' ,
         |    5 , 'saturday' ,
         |    6 , 'sunday' ,
         |    NULL
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown aiq_from_unixtime") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(ts bigint)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"(0), (NULL)"
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "aiq_from_unixtime(ts, 'yyyy-MM-dd HH:mm:ss', 'UTC')"
    )
    val expectedResult = Seq(Row("1970-01-01 00:00:00"), Row(null))
    testPushdown(
      s"""
         |SELECT
         |  (
         |    TO_CHAR (
         |      CONVERT_TIMEZONE (
         |        'UTC' ,
         |        CAST ( CAST ( "SUBQUERY_0"."TS" AS NUMBER ) AS VARCHAR )
         |      ) ,
         |      'yyyy-MM-dd HH24:mi:SS'
         |    )
         |  ) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown to_timestamp") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(s1 string, s2 string, s3 string, d date)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2019-08-12', '2019-08-12 01:00:00', '2019/08/12', '2019-08-12'),
         |(NULL, NULL, NULL, NULL)
         |"""
        .stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "to_timestamp(s1, 'yyyy-MM-dd')",
      "to_timestamp(s2, 'yyyy-MM-dd hh:mm:ss')",
      "to_timestamp(s3, 'yyyy/MM/dd')",
      "to_timestamp(d)",
    )
    val expectedResult = Seq(
      Row(
        Timestamp.valueOf("2019-08-12 00:00:00"),
        Timestamp.valueOf("2019-08-12 01:00:00"),
        Timestamp.valueOf("2019-08-12 00:00:00"),
        Timestamp.valueOf("2019-08-12 00:00:00"),
      ),
      Row(null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S1" , 'yyyy-MM-dd' ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S2" , 'yyyy-MM-dd HH12:mi:SS' ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S3" , 'yyyy/MM/dd' ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( CAST ( "SUBQUERY_0"."D" AS TIMESTAMP ) ) AS "SUBQUERY_1_COL_3"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown to_date") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(t timestamp, s1 string, s2 string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2019-08-12 01:00:00', '2019/08/12', '2019-08-12'),
         |(NULL, NULL, NULL)
         |"""
        .stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "cast(to_date(t) as string)",
      "cast(to_date(s1, 'yyyy/MM/dd') as string)",
      "cast(to_date(s2, 'yyyy-MM-dd') as string)",
    )
    val expectedResult = Seq(
      Row("2019-08-12", "2019-08-12", "2019-08-12"),
      Row(null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( CAST ( CAST ( "SUBQUERY_0"."T" AS DATE ) AS VARCHAR ) ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    CAST ( CAST ( TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S1" , 'yyyy/MM/dd' ) AS DATE ) AS VARCHAR )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    CAST ( CAST ( TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S2" , 'yyyy-MM-dd' ) AS DATE ) AS VARCHAR )
         |  ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown from_unixtime") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(ts bigint)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"(1553890107), (NULL)"
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "from_unixtime(ts)",
      "from_unixtime(ts, 'yyyy/MM/dd')",
      "from_unixtime(ts, 'yyyy-MM-dd')",
    )
    val expectedResult = Seq(
      Row("2019-03-29 20:08:27", "2019/03/29", "2019-03-29"),
      Row(null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  (
         |    TO_CHAR (
         |      CONVERT_TIMEZONE (
         |        'UTC' ,
         |        CAST ( CAST ( ( CAST ( "SUBQUERY_0"."TS" AS NUMBER ) * 1000 ) AS NUMBER ) AS VARCHAR )
         |      ) ,
         |      'yyyy-MM-dd HH24:mi:SS'
         |    )
         |  ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    TO_CHAR (
         |      CONVERT_TIMEZONE (
         |        'UTC' ,
         |        CAST ( CAST ( ( CAST ( "SUBQUERY_0"."TS" AS NUMBER ) * 1000 ) AS NUMBER ) AS VARCHAR )
         |      ) ,
         |      'yyyy/MM/dd'
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    TO_CHAR (
         |      CONVERT_TIMEZONE (
         |        'UTC' ,
         |        CAST ( CAST ( ( CAST ( "SUBQUERY_0"."TS" AS NUMBER ) * 1000 ) AS NUMBER ) AS VARCHAR )
         |      ) ,
         |      'yyyy-MM-dd'
         |    )
         |  ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown from_utc_timestamp") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(t timestamp, d date, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2019-08-12 01:00:00', '2019-08-12', '2019-08-12'),
         |(NULL, NULL, NULL)
         |"""
        .stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "from_utc_timestamp(t, 'America/New_York')",
      "from_utc_timestamp(d, 'Asia/Seoul')",
      "from_utc_timestamp(s, 'America/New_York')",
    )
    val expectedResult = Seq(
      Row(
        Timestamp.valueOf("2019-08-11 21:00:00"),
        Timestamp.valueOf("2019-08-12 09:00:00"),
        Timestamp.valueOf("2019-08-11 20:00:00"),
      ),
      Row(null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  (
         |    CONVERT_TIMEZONE (
         |      'UTC' ,
         |      'America/New_York' ,
         |      CAST ( "SUBQUERY_0"."T" AS VARCHAR )
         |    )
         |  ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    CONVERT_TIMEZONE (
         |      'UTC' ,
         |      'Asia/Seoul' ,
         |      CAST ( CAST ( "SUBQUERY_0"."D" AS TIMESTAMP ) AS VARCHAR )
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    CONVERT_TIMEZONE (
         |      'UTC' ,
         |      'America/New_York' ,
         |      CAST ( CAST ( "SUBQUERY_0"."S" AS TIMESTAMP ) AS VARCHAR )
         |    )
         |  ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown to_unix_timestamp") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(t timestamp, d date, s1 string, s2 string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2019-08-12 01:00:00', '2019-08-12', '2019-08-12', '2019/08/12'),
         |(NULL, NULL, NULL, NULL)
         |"""
        .stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "to_unix_timestamp(t, 'yyyy-MM-dd hh:mm:ss')",
      "to_unix_timestamp(t)",
      "to_unix_timestamp(d, 'yyyy-MM-dd')",
      "to_unix_timestamp(d)",
      "to_unix_timestamp(s1, 'yyyy-MM-dd')",
      "to_unix_timestamp(s2, 'yyyy/MM/dd')",
    )
    val expectedResult = Seq(
      Row(
        1565571600L,
        1565571600L,
        1565568000L,
        1565568000L,
        1565568000L,
        1565568000L,
      ),
      Row(null, null, null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."T" ) ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."T" ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."D" ) ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."D" ) ) ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S1" , 'yyyy-MM-dd' ) )
         |  ) AS "SUBQUERY_1_COL_4" ,
         |  (
         |    DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S2" , 'yyyy/MM/dd' ) )
         |  ) AS "SUBQUERY_1_COL_5"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown to_utc_timestamp") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(t timestamp, s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2019-08-12 01:00:00', '2019-08-12'),
         |(NULL, NULL)
         |"""
        .stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "to_utc_timestamp(t, 'America/New_York')",
      "to_utc_timestamp(t, 'Asia/Seoul')",
      "to_utc_timestamp(s, 'America/New_York')",
      "to_utc_timestamp(s, 'Asia/Seoul')",
    )
    val expectedResult = Seq(
      Row(
        Timestamp.valueOf("2019-08-12 05:00:00"),
        Timestamp.valueOf("2019-08-11 16:00:00"),
        Timestamp.valueOf("2019-08-12 04:00:00"),
        Timestamp.valueOf("2019-08-11 15:00:00"),
      ),
      Row(null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  (
         |    CONVERT_TIMEZONE ( 'America/New_York' , 'UTC' , CAST ( "SUBQUERY_0"."T" AS VARCHAR ) )
         |  ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    CONVERT_TIMEZONE ( 'Asia/Seoul' , 'UTC' , CAST ( "SUBQUERY_0"."T" AS VARCHAR ) )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    CONVERT_TIMEZONE (
         |      'America/New_York' ,
         |      'UTC' ,
         |      CAST ( CAST ( "SUBQUERY_0"."S" AS TIMESTAMP ) AS VARCHAR )
         |    )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    CONVERT_TIMEZONE (
         |      'Asia/Seoul' ,
         |      'UTC' ,
         |      CAST ( CAST ( "SUBQUERY_0"."S" AS TIMESTAMP ) AS VARCHAR )
         |    )
         |  ) AS "SUBQUERY_1_COL_3"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown unix_timestamp") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(t timestamp, d date, s1 string, s2 string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"""
         |('2019-08-12 01:00:00', '2019-08-12', '2019-08-12', '2019/08/12'),
         |(NULL, NULL, NULL, NULL)
         |"""
        .stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr(
      "unix_timestamp(t, 'yyyy-MM-dd')",
      "unix_timestamp(d, 'yyyy-MM-dd')",
      "unix_timestamp(s1, 'yyyy-MM-dd')",
      "unix_timestamp(s2, 'yyyy/MM/dd')",
    )
    val expectedResult = Seq(
      Row(1565571600L, 1565568000L, 1565568000L, 1565568000L),
      Row(null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."T" ) ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."D" ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S1" , 'yyyy-MM-dd' ) )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    DATE_PART ( 'EPOCH_SECOND' , TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."S2" , 'yyyy/MM/dd' ) )
         |  ) AS "SUBQUERY_1_COL_3"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown unix_seconds") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(t timestamp)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"('1970-01-01 00:00:01'), (NULL)"
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("unix_seconds(t)")
    val expectedResult = Seq(Row(1L), Row(null))
    testPushdown(
      s"""
         |SELECT
         |  (
         |    DATE_PART ( 'EPOCH_SECOND' , "SUBQUERY_0"."T" )
         |  ) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown unix_millis") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(t timestamp)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"('1970-01-01 00:00:01'), (NULL)"
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("unix_millis(t)")
    val expectedResult = Seq(Row(1000L), Row(null))
    testPushdown(
      s"""
         |SELECT
         |  (
         |    DATE_PART ( 'EPOCH_MILLISECOND' , "SUBQUERY_0"."T" )
         |  ) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }
}