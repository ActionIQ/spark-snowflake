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

import java.sql._
import java.util.TimeZone

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.reflect.internal.util.TableDef

// scalastyle:off println
class PushdownEnhancement02 extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_table_basic: String = s"test_basic_$randomSuffix"
  private val test_table_number = s"test_table_number_$randomSuffix"
  private val test_table_date = s"test_table_date_$randomSuffix"
  private val test_table_rank = s"test_table_rank_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_basic")
      jdbcUpdate(s"drop table if exists $test_table_number")
      jdbcUpdate(s"drop table if exists $test_table_date")
      jdbcUpdate(s"drop table if exists $test_table_rank")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
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

  test("test pushdown basic: OR, BIT operation") {
    jdbcUpdate(s"create or replace table $test_table_basic(name String, value1 Integer, value2 Integer)")
    jdbcUpdate(s"insert into $test_table_basic values ('Ray', 1, 9), ('Ray', 2, 8), ('Ray', 3, 7), ('Emily', 4, 6), ('Emily', 5, 5)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()
      // Spark doesn't support these bit operation on Decimal, so we convert them.
      .withColumn("value1", col("value1").cast(IntegerType))
      .withColumn("value2", col("value2").cast(IntegerType))

    tmpDF.printSchema()
    tmpDF.createOrReplaceTempView("test_table_basic")

    val resultDF =
      sparkSession
        .sql(s"select name, value1 as v1, value2 as v2," +
          s"(value1 & value2) as bitand, (value1 | value2) as bitor," +
          s"(value1 ^ value2) as xor , ( ~value1 ) as bitnot" +
          " from test_table_basic where name = 'Ray' or name = 'Emily'")

    resultDF.show(10, false)

    val expectedResult = Seq(
      Row("Ray"  , 1, 9, 1,  9, 8, -2),
      Row("Ray"  , 2, 8, 0, 10, 10, -3),
      Row("Ray"  , 3, 7, 3,  7, 4, -4),
      Row("Emily", 4, 6, 4,  6, 2, -5),
      Row("Emily", 5, 5, 5,  5, 0, -6)
    )

    testPushdown(
      s"""SELECT ( "SUBQUERY_1"."NAME" ) AS "SUBQUERY_2_COL_0" ,
         |( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ) AS "SUBQUERY_2_COL_1" ,
         |( CAST ( "SUBQUERY_1"."VALUE2" AS NUMBER ) ) AS "SUBQUERY_2_COL_2" ,
         |( BITAND ( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ,
         |CAST ( "SUBQUERY_1"."VALUE2" AS NUMBER ) ) ) AS "SUBQUERY_2_COL_3" ,
         |( BITOR ( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ,
         |CAST ( "SUBQUERY_1"."VALUE2" AS NUMBER ) ) ) AS "SUBQUERY_2_COL_4" ,
         |( BITXOR ( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ,
         |CAST ( "SUBQUERY_1"."VALUE2" AS NUMBER ) ) ) AS "SUBQUERY_2_COL_5" ,
         |( BITNOT ( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ) )
         |AS "SUBQUERY_2_COL_6" FROM ( SELECT * FROM (
         |SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."NAME" = 'Ray' ) OR
         |( "SUBQUERY_0"."NAME" = 'Emily' ) ) ) AS "SUBQUERY_1"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("test pushdown boolean functions: NOT/Contains/EndsWith/StartsWith") {
    jdbcUpdate(s"create or replace table $test_table_basic(name String, value1 Integer, value2 Integer)")
    jdbcUpdate(s"insert into $test_table_basic values ('Ray', 1, 9), ('Ray', 2, 8), ('Ray', 3, 7), ('Emily', 4, 6), ('Emily', 5, 5)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    tmpDF.printSchema()
    tmpDF.createOrReplaceTempView("test_table_basic")

    val resultDF =
      sparkSession
        .sql(s"select * " +
          " from test_table_basic where name != 'Ray'" +
          " OR (name like '%ay')" +
          " OR (name like 'Emi%')" +
          " OR (name like '%i%')" +
          " OR (not (value1 >= 5))" +
          " OR (not (value1 <= 6))" +
          " OR (not (value2 >  7))" +
          " OR (not (value2 <  8))" )

    resultDF.show(10, false)

    val expectedResult = Seq(
      Row("Ray"  , 1, 9),
      Row("Ray"  , 2, 8),
      Row("Ray"  , 3, 7),
      Row("Emily", 4, 6),
      Row("Emily", 5, 5)
    )

    testPushdown(
      s"""SELECT * FROM ( SELECT * FROM ( $test_table_basic )
         | AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( ( (
         |    "SUBQUERY_0"."NAME" != 'Ray' )
         | OR "SUBQUERY_0"."NAME" LIKE '%ay' )
         | OR ( "SUBQUERY_0"."NAME" LIKE 'Emi%'
         | OR "SUBQUERY_0"."NAME" LIKE '%i%' ) )
         | OR ( ( ( "SUBQUERY_0"."VALUE1" < 5 )
         | OR ( "SUBQUERY_0"."VALUE1" > 6 ) )
         | OR ( ( "SUBQUERY_0"."VALUE2" <= 7 )
         | OR ( "SUBQUERY_0"."VALUE2" >= 8 ) ) ) )
         |""".stripMargin,
      resultDF,
      expectedResult, false, true
    )
  }

  // Random() is new for spark 3.0
  ignore("test pushdown number functions: PI() and Round()/Random") {
    // Don't run test with use_copy_unload because COPY UNLOAD converts
    // PI value 3.141592653589793 to 3.141592654
    if (!params.useCopyUnload) {
      jdbcUpdate(s"create or replace table $test_table_number " +
        s"(d1 decimal(38, 10), f1 float)")
      jdbcUpdate(s"insert into $test_table_number values " +
        s"(-1.9, -1.9),  (-1.1, -1.1), (0, 0), (1.1, 1.1), (1.9, 1.9)")

      val tmpDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_number)
        .load()

      tmpDF.createOrReplaceTempView("test_table_number")

      val resultDF =
        sparkSession
          .sql(s"select round(d1), round(f1), PI()" +
            " from test_table_number")

      resultDF.printSchema()
      resultDF.show(10, false)

      val PI = 3.141592653589793
      val expectedResult = Seq(
        Row(BigDecimal(-2), (-2).toDouble, PI),
        Row(BigDecimal(-1), (-1).toDouble, PI),
        Row(BigDecimal(0), (0).toDouble, PI),
        Row(BigDecimal(1), (1).toDouble, PI),
        Row(BigDecimal(2), (2).toDouble, PI)
      )

      testPushdown(
        s"""SELECT ( ROUND ( "SUBQUERY_0"."D1" , 0 ) ) AS "SUBQUERY_1_COL_0",
           |( ROUND ( "SUBQUERY_0"."F1" , 0 ) ) AS "SUBQUERY_1_COL_1",
           |( 3.141592653589793 ) AS "SUBQUERY_1_COL_2" FROM
           |( SELECT * FROM ( $test_table_number ) AS
           |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |""".stripMargin,
        resultDF,
        expectedResult
      )

      // Can't assert the returned value for random(). So just run it.
      sparkSession
        .sql(s"select d1, random(100), random() from test_table_number")
        .show()
    }
  }

  test("AIQ test pushdown day_start") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(ts bigint, tz string, pd int)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"(1460080000000, 'America/New_York', 2)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.selectExpr("aiq_day_start(ts, tz, pd)")
    val expectedResult = Seq(Row(1460174400000L))

    testPushdown(
      s"""
         |SELECT (
         |  DATE_PART (
         |    epoch_millisecond ,
         |    DATE_TRUNC (
         |      'day' ,
         |      DATEADD (
         |        'day' ,
         |        CAST ( "SUBQUERY_0"."PD" AS NUMBER ) ,
         |        CONVERT_TIMEZONE (
         |          "SUBQUERY_0"."TZ" ,
         |          CAST ( "SUBQUERY_0"."TS" AS NUMBER ) ::varchar ) ) ) )
         |  ) AS "SUBQUERY_1_COL_0" FROM (
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("AIQ test pushdown instr") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"('hello world')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
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
         |  SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |  ) AS "SUBQUERY_0"
         |""".stripMargin,
      pushDf,
      Seq(Row(1))
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
         |    epoch_millisecond ,
         |    CONVERT_TIMEZONE (
         |      'America/New_York' ,
         |      'UTC' ,
         |      TO_TIMESTAMP_NTZ ( "SUBQUERY_0"."DT" , 'yyyy-MM-dd HH24:mi:SS' )
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
      aiq_date_to_string(col("ts"), "yyyy-MM-dd MMM HH:mm:ss", "America/New_York"),
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
      "2019-09-01 Sep 14:50:52",
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
         |    TO_TIMESTAMP (
         |      CONVERT_TIMEZONE ( 'America/New_York' , CAST ( "SUBQUERY_0"."TS" AS NUMBER ) ::varchar )
         |    ) ,
         |    'yyyy-MM-dd HH24:mi:SS'
         |  )
         |) AS "SUBQUERY_1_COL_0"
         |FROM ( SELECT * FROM ( $test_table_date ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      finalPushResultDF,
      Seq(Row("2019-09-01 14:50:52"))
    )
  }

  test("test pushdown functions date_add/date_sub") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(d1 date)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"('2020-07-28')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.select(
      col("d1"),
      date_add(col("d1"),4).as("date_add"),
      date_sub(col("d1"),4).as("date_sub")
    )

    val expectedResult = Seq(
      Row(
        Date.valueOf("2020-07-28"),
        Date.valueOf("2020-08-01"),
        Date.valueOf("2020-07-24"))
    )

    testPushdown(
      s"""SELECT (
         |  "SUBQUERY_0"."D1" ) AS "SUBQUERY_1_COL_0" ,
         |  ( DATEADD(day, 4 , "SUBQUERY_0"."D1" ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( DATEADD(day, (0 - ( 4 )), "SUBQUERY_0"."D1" ) ) AS "SUBQUERY_1_COL_2"
         |FROM (
         |  SELECT * FROM (
         |    $test_table_date
         |  ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("test pushdown functions date_add/date_sub on Feb last day in leap and non-leap year") {
    jdbcUpdate(s"create or replace table $test_table_date " +
      s"(leap_02_29 date, leap_03_01 date, non_leap_02_28 date, non_leap_03_01 date)")
    jdbcUpdate(s"insert into $test_table_date values " +
      s"('2016-02-29', '2016-03-01', '2015-02-28', '2015-03-01')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_date)
      .load()

    val resultDF = tmpDF.select(
      col("leap_02_29"),
      date_add(col("leap_02_29"),1),  // Add +1
      date_sub(col("leap_02_29"),-1), // Sub -1
      date_add(col("leap_03_01"),-1), // Add -1
      date_sub(col("leap_03_01"),1),  // Sub +1
      date_add(col("non_leap_02_28"),1),  // Add +1
      date_sub(col("non_leap_02_28"),-1), // Sub -1
      date_add(col("non_leap_03_01"),-1), // Add -1
      date_sub(col("non_leap_03_01"),1)   // Sub +1
    )

    val expectedResult = Seq(
      Row(
        Date.valueOf("2016-02-29"),
        Date.valueOf("2016-03-01"),
        Date.valueOf("2016-03-01"),
        Date.valueOf("2016-02-29"),
        Date.valueOf("2016-02-29"),
        Date.valueOf("2015-03-01"),
        Date.valueOf("2015-03-01"),
        Date.valueOf("2015-02-28"),
        Date.valueOf("2015-02-28"))
    )

    testPushdown(
      s"""SELECT
         |  ( "SUBQUERY_0"."LEAP_02_29" ) AS "SUBQUERY_1_COL_0" ,
         |  ( DATEADD ( day, 1 , "SUBQUERY_0"."LEAP_02_29" ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( DATEADD ( day, (0 - ( -1 ) ), "SUBQUERY_0"."LEAP_02_29" ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( DATEADD ( day, -1 , "SUBQUERY_0"."LEAP_03_01" ) ) AS "SUBQUERY_1_COL_3" ,
         |  ( DATEADD ( day, (0 - ( 1 ) ), "SUBQUERY_0"."LEAP_03_01" ) ) AS "SUBQUERY_1_COL_4" ,
         |  ( DATEADD ( day, 1 , "SUBQUERY_0"."NON_LEAP_02_28" ) ) AS "SUBQUERY_1_COL_5" ,
         |  ( DATEADD ( day, (0 - ( -1 ) ), "SUBQUERY_0"."NON_LEAP_02_28" ) ) AS "SUBQUERY_1_COL_6" ,
         |  ( DATEADD ( day, -1 , "SUBQUERY_0"."NON_LEAP_03_01" ) ) AS "SUBQUERY_1_COL_7" ,
         |  ( DATEADD ( day, (0 - ( 1 ) ), "SUBQUERY_0"."NON_LEAP_03_01" ) ) AS "SUBQUERY_1_COL_8"
         |FROM (
         |  SELECT * FROM (
         |    $test_table_date
         |  ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("test pushdown WindowExpression: Rank without PARTITION BY") {
    jdbcUpdate(s"create or replace table $test_table_rank" +
      s"(state String, bushels_produced Integer)")
    jdbcUpdate(s"insert into $test_table_rank values" +
      s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
      s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_rank)
      .load()

    tmpDF.printSchema()
    tmpDF.createOrReplaceTempView("test_table_rank")

    val resultDF =
      sparkSession
        .sql(s"select state, bushels_produced," +
          " rank() over (order by bushels_produced desc) as total_rank" +
          " from test_table_rank")

    resultDF.show(10, false)

    val expectedResult = Seq(
      Row("Iowa", 130, 1),
      Row("Iowa", 120, 2),
      Row("Iowa", 120, 2),
      Row("Kansas", 100, 4),
      Row("Kansas", 100, 4),
      Row("Kansas", 90, 6)
    )

    testPushdown(
      s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
         |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
         |( RANK ()  OVER ( ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
         |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
         |    AS "SUBQUERY_1_COL_2"
         |FROM ( SELECT * FROM ( $test_table_rank )
         |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("test pushdown WindowExpression: Rank with PARTITION BY") {
    jdbcUpdate(s"create or replace table $test_table_rank" +
      s"(state String, bushels_produced Integer)")
    jdbcUpdate(s"insert into $test_table_rank values" +
      s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
      s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_rank)
      .load()

    tmpDF.printSchema()
    tmpDF.createOrReplaceTempView("test_table_rank")

    val resultDF =
      sparkSession
        .sql(s"select state, bushels_produced," +
          " rank() over (partition by state " +
          "   order by bushels_produced desc) as group_rank" +
          " from test_table_rank")

    resultDF.show(10, false)

    val expectedResult = Seq(
      Row("Iowa", 130, 1),
      Row("Iowa", 120, 2),
      Row("Iowa", 120, 2),
      Row("Kansas", 100, 1),
      Row("Kansas", 100, 1),
      Row("Kansas", 90, 3)
    )

    testPushdown(
      s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
         |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
         |( RANK ()  OVER ( PARTITION BY "SUBQUERY_0"."STATE"
         |  ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
         |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
         |    AS "SUBQUERY_1_COL_2"
         |FROM ( SELECT * FROM ( $test_table_rank )
         |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("test pushdown WindowExpression: DenseRank without PARTITION BY") {
    jdbcUpdate(s"create or replace table $test_table_rank" +
      s"(state String, bushels_produced Integer)")
    jdbcUpdate(s"insert into $test_table_rank values" +
      s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
      s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_rank)
      .load()

    tmpDF.printSchema()
    tmpDF.createOrReplaceTempView("test_table_rank")

    val resultDF =
      sparkSession
        .sql(s"select state, bushels_produced," +
          " dense_rank() over (order by bushels_produced desc) as total_rank" +
          " from test_table_rank")

    resultDF.show(10, false)

    val expectedResult = Seq(
      Row("Iowa", 130, 1),
      Row("Iowa", 120, 2),
      Row("Iowa", 120, 2),
      Row("Kansas", 100, 3),
      Row("Kansas", 100, 3),
      Row("Kansas", 90, 4)
    )

    testPushdown(
      s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
         |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
         |( DENSE_RANK ()  OVER ( ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
         |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
         |    AS "SUBQUERY_1_COL_2"
         |FROM ( SELECT * FROM ( $test_table_rank )
         |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("test pushdown WindowExpression: DenseRank with PARTITION BY") {
    jdbcUpdate(s"create or replace table $test_table_rank" +
      s"(state String, bushels_produced Integer)")
    jdbcUpdate(s"insert into $test_table_rank values" +
      s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
      s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_rank)
      .load()

    tmpDF.printSchema()
    tmpDF.createOrReplaceTempView("test_table_rank")

    val resultDF =
      sparkSession
        .sql(s"select state, bushels_produced," +
          " dense_rank() over (partition by state " +
          "   order by bushels_produced desc) as group_rank" +
          " from test_table_rank")

    resultDF.show(10, false)

    val expectedResult = Seq(
      Row("Iowa", 130, 1),
      Row("Iowa", 120, 2),
      Row("Iowa", 120, 2),
      Row("Kansas", 100, 1),
      Row("Kansas", 100, 1),
      Row("Kansas", 90, 2)
    )

    testPushdown(
      s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
         |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
         |( DENSE_RANK ()  OVER ( PARTITION BY "SUBQUERY_0"."STATE"
         |  ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
         |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
         |    AS "SUBQUERY_1_COL_2"
         |FROM ( SELECT * FROM ( $test_table_rank )
         |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }
}
// scalastyle:on println

