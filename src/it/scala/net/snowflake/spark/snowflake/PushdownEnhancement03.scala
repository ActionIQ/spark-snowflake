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

import java.util.TimeZone
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.snowflake.SFQueryTest

// scalastyle:off println
class PushdownEnhancement03 extends IntegrationSuiteBase {
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

  private def testString(id: Option[Int] = None): String =
    "hello this is a test" + id.map(s => s.toString).getOrElse("")

  // Date-Style

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

  // Aggregate-style

  test("AIQ test pushdown approx_count_dist") {
    jdbcUpdate(s"create or replace table $test_table_date (s string, i int)")
    (0 until 100).foreach { i =>
      if (i % 5 == 0) {
        jdbcUpdate(s"insert into $test_table_date values ('hello $i', $i)")
      }
      jdbcUpdate(s"insert into $test_table_date values ('hello $i', ${i.max(30)})")
    }

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    {
      val pushDf = tmpDF.selectExpr("approx_count_distinct(s)")
      testPushdownSql(
        s"""
           |SELECT ( HLL ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
           |FROM (
           |  SELECT ( "SUBQUERY_0"."S" ) AS "SUBQUERY_1_COL_0"
           |  FROM (
           |    SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |  ) AS "SUBQUERY_1" LIMIT 1
           |""".stripMargin,
        pushDf,
      )
      val approxCount = pushDf.collect().head.getLong(0)
      // approx_count_distinct is not accurate, so we just check the range
      assert(approxCount > 90 && approxCount < 130)
    }

    {
      val pushDf = tmpDF.selectExpr("approx_count_distinct(i)")
      testPushdownSql(
        s"""
           |SELECT ( HLL ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
           |FROM (
           |  SELECT ( "SUBQUERY_0"."I" ) AS "SUBQUERY_1_COL_0"
           |  FROM (
           |    SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |  ) AS "SUBQUERY_1" LIMIT 1
           |""".stripMargin,
        pushDf,
      )
      val approxCount = pushDf.collect().head.getLong(0)
      // approx_count_distinct is not accurate, so we just check the range
      assert(approxCount > 50 && approxCount < 90)
    }
  }

  test("AIQ test pushdown collect_list") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, '${testString(Some(1))}', 1),
         |(1, '${testString(Some(2))}', 1),
         |(1, '${testString()}', 2),
         |(2, '${testString()}', 3),
         |(NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDFStr = tmpDF.selectExpr("sort_array(collect_list(s1))")
    val expectedResultStr = Seq(
      Row(
        Array(
          testString(),
          testString(),
          testString(Some(1)),
          testString(Some(2))
        )
      )
    )
    testPushdownSql(
      s"""
         |SELECT (
         |  ARRAY_SORT (
         |    ARRAY_AGG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ,
         |    true ,
         |    true
         |  )
         |) AS "SUBQUERY_2_COL_0"
         |FROM (
         |  SELECT ( "SUBQUERY_0"."S1" ) AS "SUBQUERY_1_COL_0"
         |  FROM (
         |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |) AS "SUBQUERY_1" LIMIT 1
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFStr,
    )
    SFQueryTest.checkAnswer(resultDFStr, expectedResultStr)

    val resultDFInt = tmpDF.selectExpr("sort_array(collect_list(s2))")
    val expectedResultInt = Seq(Row(Array(1, 1, 2, 3).map(BigDecimal(_))))
    testPushdownSql(
      s"""
         |SELECT (
         |  ARRAY_SORT (
         |    ARRAY_AGG ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ,
         |    true ,
         |    true
         |  )
         |) AS "SUBQUERY_2_COL_0"
         |FROM (
         |  SELECT ( "SUBQUERY_0"."S2" ) AS "SUBQUERY_1_COL_0"
         |  FROM (
         |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |) AS "SUBQUERY_1" LIMIT 1
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFInt,
    )
    SFQueryTest.checkAnswer(resultDFInt, expectedResultInt)

    val resultDFGroupBy = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_list(s1) as s1_agg"),
        expr("collect_list(s2) as s2_agg"),
      )
      .select(
        col("id"),
        expr("sort_array(s1_agg)"),
        expr("sort_array(s2_agg)"),
      )
    val expectedResultGroupBy = Seq(
      Row(
        BigDecimal(1),
        Array(testString(), testString(Some(1)), testString(Some(2))),
        Array(1, 1, 2).map(BigDecimal(_))
      ),
      Row(BigDecimal(2), Array(testString()), Array(BigDecimal(3))),
      Row(null, Array(), Array()),
    )
    testPushdownSql(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  ( ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."S1" ) , true , true ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( ARRAY_SORT ( ARRAY_AGG ( "SUBQUERY_0"."S2" ) , true , true ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFGroupBy,
    )
    SFQueryTest.checkAnswer(resultDFGroupBy, expectedResultGroupBy)
  }

  test("AIQ test pushdown collect_set") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(id bigint, s1 string, s2 bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"""
         |(1, '${testString(Some(1))}', 1),
         |(1, '${testString(Some(2))}', 1),
         |(1, '${testString()}', 2),
         |(2, '${testString()}', 3),
         |(NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDFStr = tmpDF.selectExpr("sort_array(collect_set(s1))")
    val expectedResultStr = Seq(
      Row(
        Array(
          testString(),
          testString(Some(1)),
          testString(Some(2))
        )
      )
    )
    testPushdownSql(
      s"""
         |SELECT (
         |  ARRAY_SORT (
         |    ARRAY_AGG ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ,
         |    true ,
         |    true
         |  )
         |) AS "SUBQUERY_2_COL_0"
         |FROM (
         |  SELECT ( "SUBQUERY_0"."S1" ) AS "SUBQUERY_1_COL_0"
         |  FROM (
         |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |) AS "SUBQUERY_1" LIMIT 1
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFStr,
    )
    SFQueryTest.checkAnswer(resultDFStr, expectedResultStr)

    val resultDFInt = tmpDF.selectExpr("sort_array(collect_set(s2))")
    val expectedResultInt = Seq(Row(Array(1, 2, 3).map(BigDecimal(_))))
    testPushdownSql(
      s"""
         |SELECT (
         |  ARRAY_SORT (
         |    ARRAY_AGG ( DISTINCT "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ,
         |    true ,
         |    true
         |  )
         |) AS "SUBQUERY_2_COL_0"
         |FROM (
         |  SELECT ( "SUBQUERY_0"."S2" ) AS "SUBQUERY_1_COL_0"
         |  FROM (
         |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |) AS "SUBQUERY_1" LIMIT 1
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFInt,
    )
    SFQueryTest.checkAnswer(resultDFInt, expectedResultInt)

    val resultDFGroupBy = tmpDF
      .groupBy("id")
      .agg(
        expr("collect_set(s1) as s1_agg"),
        expr("collect_set(s2) as s2_agg"),
      )
      .select(
        col("id"),
        expr("sort_array(s1_agg)"),
        expr("sort_array(s2_agg)"),
      )
    val expectedResultGroupBy = Seq(
      Row(
        BigDecimal(1),
        Array(testString(), testString(Some(1)), testString(Some(2))),
        Array(1, 2).map(BigDecimal(_))
      ),
      Row(BigDecimal(2), Array(testString()), Array(BigDecimal(3))),
      Row(null, Array(), Array()),
    )
    testPushdownSql(
      s"""
         |SELECT
         |  ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |  ( ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S1" ) , true , true ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( ARRAY_SORT ( ARRAY_AGG ( DISTINCT "SUBQUERY_0"."S2" ) , true , true ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |GROUP BY "SUBQUERY_0"."ID"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFGroupBy,
    )
    SFQueryTest.checkAnswer(resultDFGroupBy, expectedResultGroupBy)
  }

  // Cryptographic-Style

  test("AIQ test pushdown md5") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake'),
         |('spark'),
         |(''),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr("md5(s)", "md5(concat(s,s))")
    val expectedResult = Seq(
      Row("08c8c3a0b5d92627f24fed878afd8325", "0ff79810b26fec3f6830fcd74bf840dc"),
      Row("98f11b7a7880169c3bd62a5a507b3965", "b62d4c654f6a2247f76d99535140e61a"),
      Row("d41d8cd98f00b204e9800998ecf8427e", "d41d8cd98f00b204e9800998ecf8427e"),
      Row(null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |(
         |  MD5 (
         |    CAST ( "SUBQUERY_0"."S" AS VARCHAR )
         |  )
         |) AS "SUBQUERY_1_COL_0" ,
         |(
         |  MD5 (
         |    CAST ( CONCAT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) AS VARCHAR )
         |  )
         |) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown sha1") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake'),
         |('spark'),
         |(''),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr("sha1(s)", "sha1(concat(s,s))")
    val expectedResult = Seq(
      Row("b0c5516f28a7c36bd0233dfd6f3bab3c2a0c4010", "70008ae99efe220e76aa8134535d61c07c0f3638"),
      Row("7187dadeaa9825054bf26bb1a84055243400af16", "81a2a0cf10e8b2766928528011aa9827083f97c6"),
      Row("da39a3ee5e6b4b0d3255bfef95601890afd80709", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
      Row(null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |(
         |  SHA1 (
         |    CAST ( "SUBQUERY_0"."S" AS VARCHAR )
         |  )
         |) AS "SUBQUERY_1_COL_0" ,
         |(
         |  SHA1 (
         |    CAST ( CONCAT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) AS VARCHAR )
         |  )
         |) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown sha2") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake'),
         |('spark'),
         |(''),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr("sha2(s, 224)", "sha2(concat(s,s), 224)")
    val expectedResult = Seq(
      Row(
        "7d75f89fcf30830b83dcd5fbc274f3aea01bc320ce6d8d13ca598c72",
        "fbc7a8b80d3d8dde4ac3acc3bd2afbda6e70390389f1c587d0e0dffe",
      ),
      Row(
        "fdc47fb2d90b5ede526639c9419d46502e333f45d52e02e5c4b76229",
        "29eae7e99d4ac1420e434878a1306524cf10e818bef2c8507d866a19",
      ),
      Row(
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
      ),
      Row(null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |(
         |  SHA2 (
         |    CAST ( "SUBQUERY_0"."S" AS VARCHAR ) ,
         |    224
         |  )
         |) AS "SUBQUERY_1_COL_0" ,
         |(
         |  SHA2 (
         |    CAST ( CONCAT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) AS VARCHAR ) ,
         |    224
         |  )
         |) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown xxhash64") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(id bigint, s1 string, s2 string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |(1, 'snowflake', 'snowflake'),
         |(2, 'spark', 'spark'),
         |(2, '', ''),
         |(3, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr("xxhash64(s1, s2)", "xxhash64(concat(s1,s2), s2)")
    // Hash function between Snowflake and Spark operates the same but does not return
    // the same results for testing the output (Spark uses a Seed) so skipping here
    testPushdownSql(
      s"""
         |SELECT (
         |  HASH ( "SUBQUERY_0"."S1" , "SUBQUERY_0"."S2" ) ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    HASH (
         |      CONCAT ( "SUBQUERY_0"."S1" , "SUBQUERY_0"."S2" ) , "SUBQUERY_0"."S2"
         |    )
         |  ) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
    )

    val resultDFGroupBy = tmpDF
      .select(
        col("id"),
        expr("xxhash64(s1, s2) as hash_1"),
        expr("xxhash64(concat(s1,s2), s2) as hash_2"),
      )
      .groupBy("id")
      .agg("hash_1" -> "max", "hash_2" -> "max")
      .selectExpr("*")
    // Hash function between Snowflake and Spark operates the same but does not return
    // the same results for testing the output (Spark uses a Seed) so skipping here
    testPushdownSql(
      s"""
         |SELECT
         |  ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) AS "SUBQUERY_2_COL_0" ,
         |  ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_1" ) ) AS "SUBQUERY_2_COL_1" ,
         |  ( MAX ( "SUBQUERY_1"."SUBQUERY_1_COL_2" ) ) AS "SUBQUERY_2_COL_2"
         |FROM (
         |  SELECT
         |    ( "SUBQUERY_0"."ID" ) AS "SUBQUERY_1_COL_0" ,
         |    ( HASH ( "SUBQUERY_0"."S1" , "SUBQUERY_0"."S2" ) ) AS "SUBQUERY_1_COL_1" ,
         |    (
         |      HASH (
         |        CONCAT ( "SUBQUERY_0"."S1" , "SUBQUERY_0"."S2" ) ,
         |        "SUBQUERY_0"."S2"
         |      )
         |    ) AS "SUBQUERY_1_COL_2"
         |  FROM (
         |    SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |) AS "SUBQUERY_1"
         |GROUP BY "SUBQUERY_1"."SUBQUERY_1_COL_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFGroupBy,
    )
  }

  // Collection-Style

  //  test("AIQ test pushdown array") {
  //    jdbcUpdate(s"create or replace table $test_table_basic " +
  //      s"(s1 string, s2 string, i1 bigint, i2 bigint)")
  //    jdbcUpdate(s"insert into $test_table_basic values " +
  //      s"""
  //         |('hello1', 'test1', 1, 2),
  //         |('hello2', 'test2', 3, 4),
  //         |('hello3', 'test3', 5, 6),
  //         |('hello4', 'test4', 7, 8),
  //         |('hello5', NULL, 9, NULL),
  //         |(NULL, 'test6', NULL, 12),
  //         |(NULL, NULL, NULL, NULL)
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
  //      ("hello1", "test1"),
  //      ("hello2", "test2"),
  //      ("hello3", "test3"),
  //      ("hello4", "test4"),
  //      ("hello5", undefined),
  //      (undefined, "test6"),
  //      (undefined, undefined),
  //    ).map{ case (v1, v2) => Row(Seq(v1, v2).toArray) }
  //
  //    val resultDFInt = tmpDF.selectExpr("array(i1, i2)")
  //    val expectedResultInt = Seq(
  //      (1, 2),
  //      (3, 4),
  //      (5, 6),
  //      (7, 8),
  //      (9, null),
  //      (null, 12),
  //      (null, null),
  //    ).map{ case (v1, v2) => Row(Seq(v1, v2).toArray) }
  //
  //    testPushdown(
  //      s"""
  //         |SELECT (
  //         |  ARRAY_CONSTRUCT ( "SUBQUERY_0"."S1" , "SUBQUERY_0"."S2" )
  //         |) AS "SUBQUERY_1_COL_0"
  //         |FROM (
  //         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
  //         |) AS "SUBQUERY_0"
  //         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
  //      resultDFStr,
  //      expectedResultStr
  //    )
  //
  //    testPushdown(
  //      s"""
  //         |SELECT (
  //         |  ARRAY_CONSTRUCT ( "SUBQUERY_0"."I1" , "SUBQUERY_0"."I2" )
  //         |) AS "SUBQUERY_1_COL_0"
  //         |FROM (
  //         |  SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS"
  //         |) AS "SUBQUERY_0"
  //         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
  //      resultDFInt,
  //      expectedResultInt
  //    )
  //  }
  //
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

  // String-Style

  test("AIQ test pushdown instr") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"('hello world')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr(
      "instr(s, 'l')",
      "instr(s, 'hell')",
      "instr(s, ' w')",
      "instr(s, ' ')",
      "instr(s, ' d ')",
      "instr(s, 'NOPE')",
      "instr(s, NULL)",
      "instr(NULL, 'foo')",
    )
    val expectedResult = Seq(Row(
      3,
      1,
      6,
      6,
      0,
      0,
      null,
      null,
    ))
    checkAnswer(resultDF, expectedResult)

    val pushDf = tmpDF.selectExpr("instr(s, 'hell')")
    testPushdown(
      s"""
         |SELECT ( CHARINDEX ( 'hell' , "SUBQUERY_0"."S" )
         |  ) AS "SUBQUERY_1_COL_0" FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |""".stripMargin,
      pushDf,
      Seq(Row(1))
    )
  }

  test("AIQ test pushdown reverse") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake'),
         |('spark'),
         |(''),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr("reverse(s)", "reverse(concat(s,s))")
    val expectedResult = Seq(
      Row("ekalfwons", "ekalfwonsekalfwons"),
      Row("kraps", "krapskraps"),
      Row("", ""),
      Row(null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( REVERSE ( "SUBQUERY_0"."S" ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( REVERSE ( CONCAT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) ) ) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown concat_ws") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake'),
         |('spark'),
         |(''),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr(
      "concat_ws('|')",
      "concat_ws('|', s)",
      "concat_ws('|', s, s)",
      "concat_ws('/', s, s, s)",
      "concat_ws(NULL, s, s, s)",
      "concat_ws('/', s, s, s, 'a')",
      "reverse(concat_ws('/', s, s, s))"
    )
    val expectedResult = Seq(
      Row(
        "",
        "snowflake",
        "snowflake|snowflake",
        "snowflake/snowflake/snowflake",
        null,
        "snowflake/snowflake/snowflake/a",
        "ekalfwons/ekalfwons/ekalfwons"
      ),
      Row(
        "",
        "spark",
        "spark|spark",
        "spark/spark/spark",
        null,
        "spark/spark/spark/a",
        "kraps/kraps/kraps"
      ),
      Row("", "", "|", "//", null, "///a", "//"),
      Row("", "", "", "", null, "a", ""),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( '' ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    ARRAY_TO_STRING (
         |      ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S" ) ,
         |      '|'
         |    )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  (
         |    ARRAY_TO_STRING (
         |      ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) ,
         |      '|'
         |    )
         |  ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    ARRAY_TO_STRING (
         |      ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) ,
         |      '/'
         |    )
         |  ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    ARRAY_TO_STRING (
         |      ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) ,
         |      NULL
         |    )
         |  ) AS "SUBQUERY_1_COL_4" ,
         |  (
         |    ARRAY_TO_STRING (
         |      ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" , "SUBQUERY_0"."S" , 'a' ) ,
         |      '/'
         |    )
         |  ) AS "SUBQUERY_1_COL_5" ,
         |  (
         |    REVERSE (
         |      ARRAY_TO_STRING (
         |        ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) ,
         |        '/'
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_6"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown generate_uuid") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"('produced id')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr("concat_ws(' : ', s, uuid())")

    testPushdownSql(
      s"""
         |SELECT (
         |  ARRAY_TO_STRING (
         |    ARRAY_CONSTRUCT_COMPACT ( "SUBQUERY_0"."S" , UUID_STRING ( ) ) ,
         |    ' : '
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
    )
  }

  test("AIQ test pushdown rand") {
    jdbcUpdate(s"create or replace table $test_table_number " +
      s"(i decimal(38, 10))")
    jdbcUpdate(s"insert into $test_table_number values" +
      s"""
         |(1.0),
         |(10.0),
         |(100.0),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_number)
      .load()

    val resultDFSelect = tmpDF.selectExpr("round(i * rand(0), 8)")
    val expectedResultSelect = Seq(
      Row(0.68002735),
      Row(1.35410152),
      Row(8.82875424),
      Row(null),
    )

    testPushdown(
      s"""
         |SELECT (
         |  ROUND (
         |    ( CAST ( "SUBQUERY_0"."I" AS DOUBLE ) * UNIFORM ( 0::float , 1::float , RANDOM ( 0 ) ) ::double ) ,
         |    8
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_number ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFSelect,
      expectedResultSelect,
      // Cannot test the returned values against Spark because Spark and
      // Snowflake return different results for the same seed which is expected
      testPushdownOff = false,
    )

    val resultDFWhere = tmpDF.where("i > rand(0)")
    testPushdownSql(
      s"""
         |SELECT * FROM
         |(
         |  SELECT * FROM ( $test_table_number ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |  WHERE (
         |    ( "SUBQUERY_0"."I" IS NOT NULL ) AND
         |    ( CAST ( "SUBQUERY_0"."I" AS DOUBLE ) > UNIFORM ( 0::float , 1::float , RANDOM ( 0 ) ) ::double
         |  )
         |)
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFWhere,
    )

    assert(resultDFWhere.collect().length == 3)
  }

  test("AIQ test pushdown log") {
    jdbcUpdate(s"create or replace table $test_table_number " +
      s"(i number)")
    jdbcUpdate(s"insert into $test_table_number values" +
      s"""
         |(2.0),
         |(3.0),
         |(4.0),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_number)
      .load()

    val resultDFStaticBase = tmpDF.selectExpr("round(log(2, i), 2)")
    val expectedResultStaticBase = Seq(
      Row(1.0),
      Row(1.58),
      Row(2.0),
      Row(null),
    )

    testPushdown(
      s"""
         |SELECT (
         |  ROUND ( LOG ( 2.0 , ( CAST ( "SUBQUERY_0"."I" AS DOUBLE ) ) ) , 2 )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_number ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFStaticBase,
      expectedResultStaticBase,
    )

    val resultDFDynamicBase = tmpDF.selectExpr("log(i, i)")
    val expectedResultDynamicBase = Seq(
      Row(1.0),
      Row(1.0),
      Row(1.0),
      Row(null),
    )

    testPushdown(
      s"""
         |SELECT (
         |  LOG (
         |    CAST ( "SUBQUERY_0"."I" AS DOUBLE ) , ( CAST ( "SUBQUERY_0"."I" AS DOUBLE ) )
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM (
         |  SELECT * FROM ( $test_table_number ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDFDynamicBase,
      expectedResultDynamicBase,
    )
  }

  test("AIQ test pushdown format_number") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(i float)")
    jdbcUpdate(s"insert into $test_table_string values" +
      s"""
         |(5.0000),
         |(3.00000000),
         |(4444444.0),
         |(4444444.12342134),
         |(44444444444.12342134),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr(
      "format_number(i, 4)",
      "format_number(i, 2)",
      "format_number(i, 0)",
    )
    val expectedResult = Seq(
      Row("5.0000", "5.00", "5"),
      Row("3.0000", "3.00", "3"),
      Row("4,444,444.0000", "4,444,444.00", "4,444,444"),
      Row("4,444,444.1234", "4,444,444.12", "4,444,444"),
      Row("44,444,444,444.1234", "44,444,444,444.12", "44,444,444,444"),
      Row(null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |(
         |  TRIM (
         |    TO_VARCHAR (
         |      TO_NUMERIC ( CAST ( "SUBQUERY_0"."I" AS VARCHAR ) , 'TM9' , 38 , 4 ) ,
         |      '9,999,999,999,999,999,999.0000'
         |    )
         |  )
         |) AS "SUBQUERY_1_COL_0" ,
         |(
         |  TRIM (
         |    TO_VARCHAR (
         |      TO_NUMERIC ( CAST ( "SUBQUERY_0"."I" AS VARCHAR ) , 'TM9' , 38 , 2 ) ,
         |      '9,999,999,999,999,999,999.00'
         |    )
         |  )
         |) AS "SUBQUERY_1_COL_1" ,
         |(
         |  TRIM (
         |    TO_VARCHAR (
         |      TO_NUMERIC ( CAST ( "SUBQUERY_0"."I" AS VARCHAR ) , 'TM9' , 38 , 0 ) ,
         |      '9,999,999,999,999,999,999'
         |    )
         |  )
         |) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }
}