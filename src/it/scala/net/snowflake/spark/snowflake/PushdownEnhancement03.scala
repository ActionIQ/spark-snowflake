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

import java.sql.Timestamp

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

  // Aggregate-style

  test("AIQ test pushdown approx_count_dist") {
    jdbcUpdate(s"create or replace table $test_table_basic (s string, i int)")
    (0 until 100).foreach { i =>
      if (i % 5 == 0) {
        jdbcUpdate(s"insert into $test_table_basic values ('hello $i', $i)")
      }
      jdbcUpdate(s"insert into $test_table_basic values ('hello $i', ${i.max(30)})")
    }

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    {
      val pushDf = tmpDF.selectExpr("approx_count_distinct(s)")
      testPushdownSql(
        s"""
           |SELECT ( HLL ( "SUBQUERY_1"."SUBQUERY_1_COL_0" ) ) AS "SUBQUERY_2_COL_0"
           |FROM (
           |  SELECT ( "SUBQUERY_0"."S" ) AS "SUBQUERY_1_COL_0"
           |  FROM (
           |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
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
           |    SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
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
    testPushdown(
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
      expectedResultStr,
    )

    val resultDFInt = tmpDF.selectExpr("sort_array(collect_list(s2))")
    val expectedResultInt = Seq(Row(Array(1, 1, 2, 3).map(BigDecimal(_))))
    testPushdown(
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
      expectedResultInt,
    )

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
    testPushdown(
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
      expectedResultGroupBy,
    )
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
    testPushdown(
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
      expectedResultStr,
    )

    val resultDFInt = tmpDF.selectExpr("sort_array(collect_set(s2))")
    val expectedResultInt = Seq(Row(Array(1, 2, 3).map(BigDecimal(_))))
    testPushdown(
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
      expectedResultInt,
    )

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
    testPushdown(
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
      expectedResultGroupBy,
    )
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

  // Misc-Style

  test("AIQ test pushdown decode") {
    jdbcUpdate(s"create or replace table $test_table_basic " +
      s"(i bigint)")
    jdbcUpdate(s"insert into $test_table_basic values " +
      s"(1), (2), (3), (NULL)"
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    val resultDF = tmpDF.selectExpr(
      "decode(i, 1, 'snowflake', 2, 'spark', 'n/a')",
      "decode(i, 1, 'snowflake', 2, 'spark')",
    )
    val expectedResult = Seq(
      ("snowflake", "snowflake"),
      ("spark", "spark"),
      ("n/a", null),
      ("n/a", null)
    ).map{ case (col1, col2) => Row(col1, col2) }

    // Decode get rewritten during runtime hence
    // the resulting PushDown SQL below
    testPushdown(
      s"""
         |SELECT
         |  (
         |    CASE WHEN EQUAL_NULL ( "SUBQUERY_0"."I" , 1 ) THEN 'snowflake'
         |         WHEN EQUAL_NULL ( "SUBQUERY_0"."I" , 2 ) THEN 'spark'
         |    ELSE 'n/a' END
         |  ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    CASE WHEN EQUAL_NULL ( "SUBQUERY_0"."I" , 1 ) THEN 'snowflake'
         |         WHEN EQUAL_NULL ( "SUBQUERY_0"."I" , 2 ) THEN 'spark' END
         |  ) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_basic  ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  // Numeric-Style

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
      Seq(Row(1)),
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
      expectedResult,
    )
  }

  test("AIQ test pushdown regexp_extract") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake-snowflake'),
         |('spark-spark'),
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
      "regexp_extract(s, '(\\\\w+)-(\\\\w+)', 1)",
      "regexp_extract(s, NULL, 1)",
      "regexp_extract(s, '(\\\\w+)-(\\\\w+)', NULL)",
      "regexp_extract(concat(s,s), '(\\\\w+)-(\\\\w+)', 1)",
    )
    val expectedResult = Seq(
      Row("snowflake", null, null, "snowflake"),
      Row("spark", null, null, "spark"),
      Row("", null, null, ""),
      Row(null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  (
         |    COALESCE (
         |      REGEXP_SUBSTR (
         |        "SUBQUERY_0"."S" ,
         |        '(\\\\w+)-(\\\\w+)' ,
         |        1 ,
         |        1 ,
         |        'c' ,
         |        1
         |      ) ,
         |      IFF (
         |        (
         |          ( ( "SUBQUERY_0"."S" IS NULL ) OR ( '(\\\\w+)-(\\\\w+)' IS NULL ) ) OR
         |          ( 1 IS NULL )
         |        ) ,
         |        NULL ,
         |        ''
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_0" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_1" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    COALESCE (
         |      REGEXP_SUBSTR (
         |        CONCAT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) ,
         |        '(\\\\w+)-(\\\\w+)' ,
         |        1 ,
         |        1 ,
         |        'c' ,
         |        1
         |      ) ,
         |      IFF (
         |        (
         |          (
         |            (
         |              CONCAT ( "SUBQUERY_0"."S" , "SUBQUERY_0"."S" ) IS NULL ) OR
         |              ( '(\\\\w+)-(\\\\w+)' IS NULL
         |            )
         |          ) OR ( 1 IS NULL )
         |        ) ,
         |        NULL ,
         |        ''
         |      )
         |    )
         |  ) AS "SUBQUERY_1_COL_3"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown regexp_extract_all") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake-snowflake'),
         |('spark-spark'),
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
      "regexp_extract_all(s, '(\\\\w+)-(\\\\w+)', 1)",
      "regexp_extract_all(s, NULL, 1)",
      "regexp_extract_all(s, '(\\\\w+)-(\\\\w+)', NULL)",
      "regexp_extract_all(concat(concat(s, ' '),s), '(\\\\w+)-(\\\\w+)', 1)",
    )
    val expectedResult = Seq(
      Row(Array("snowflake"), null, null, Array("snowflake", "snowflake")),
      Row(Array("spark"), null, null, Array("spark", "spark")),
      Row(Array(), null, null, Array()),
      Row(null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  (
         |    REGEXP_SUBSTR_ALL (
         |      "SUBQUERY_0"."S" ,
         |      '(\\\\w+)-(\\\\w+)' ,
         |      1 ,
         |      1 ,
         |      'c' ,
         |      1
         |    )
         |  ) AS "SUBQUERY_1_COL_0" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_1" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_2" ,
         |  (
         |    REGEXP_SUBSTR_ALL (
         |      CONCAT (
         |        "SUBQUERY_0"."S" ,
         |        CONCAT ( '' , "SUBQUERY_0"."S" )
         |      ) ,
         |      '(\\\\w+)-(\\\\w+)' ,
         |      1 ,
         |      1 ,
         |      'c' ,
         |      1
         |    )
         |  ) AS "SUBQUERY_1_COL_3"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown regexp_replace") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake-snowflake'),
         |('spark-spark'),
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
      "regexp_replace(s, '(\\\\w+)', 'snow')",
      "regexp_replace(s, '(\\\\w+)', 'sp')",
      "regexp_replace(s, NULL, '')",
      "regexp_replace(s, '(\\\\w+)', NULL)",
      "regexp_replace(concat(concat(s, ' '),s), '(\\\\w+)', 'snow')",
    )
    val expectedResult = Seq(
      Row("snow-snow", "sp-sp", null, null, "snow-snow snow-snow"),
      Row("snow-snow", "sp-sp", null, null, "snow-snow snow-snow"),
      Row("", "", null, null, " "),
      Row(null, null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  (
         |    REGEXP_REPLACE ( "SUBQUERY_0"."S" , '(\\\\w+)' , 'snow' , 1 )
         |  ) AS "SUBQUERY_1_COL_0" ,
         |  (
         |    REGEXP_REPLACE ( "SUBQUERY_0"."S" , '(\\\\w+)' , 'sp' , 1 )
         |  ) AS "SUBQUERY_1_COL_1" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_2" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_3" ,
         |  (
         |    REGEXP_REPLACE (
         |      CONCAT ( "SUBQUERY_0"."S" , CONCAT ( ' ' , "SUBQUERY_0"."S" ) ) ,
         |      '(\\\\w+)' ,
         |      'snow' ,
         |      1
         |    )
         |  ) AS "SUBQUERY_1_COL_4"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
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

  test("AIQ test pushdown regexp_like") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake-snowflake'),
         |('spark-spark'),
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
      "regexp_like(s, '(\\\\w+)-(\\\\w+)')",
      "regexp_like(s, NULL)",
    )
    val expectedResult = Seq(
      Row(true, null),
      Row(true, null),
      Row(false, null),
      Row(null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( REGEXP_LIKE ( "SUBQUERY_0"."S" , '(\\\\w+)-(\\\\w+)' ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( NULL ) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown replace") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('snowflake-snowflake'),
         |('spark-spark'),
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
      "replace(s, 'snowflake', 'snow')",
      "replace(s, 'spark', 'sp')",
      "replace(s, 'snowflake', '')",
      "replace(s, 'spark')",
    )
    val expectedResult = Seq(
      Row("snow-snow", "snowflake-snowflake", "-", "snowflake-snowflake"),
      Row("spark-spark", "sp-sp", "spark-spark", "-"),
      Row("", "", "", ""),
      Row(null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( REPLACE ( "SUBQUERY_0"."S" , 'snowflake' , 'snow' ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( REPLACE ( "SUBQUERY_0"."S" , 'spark' , 'sp' ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( REPLACE ( "SUBQUERY_0"."S" , 'snowflake' , '' ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( REPLACE ( "SUBQUERY_0"."S" , 'spark' , '' ) ) AS "SUBQUERY_1_COL_3"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown btrim") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('    SparkSQL   '),
         |('SSparkSQLS'),
         |(''),
         |(NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr("btrim(s)", "btrim(s, 'SL')")
    val expectedResult = Seq(
      Row("SparkSQL", "    SparkSQL   "),
      Row("SSparkSQLS", "parkSQ"),
      Row("", ""),
      Row(null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( TRIM ( "SUBQUERY_0"."S" ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( TRIM ( "SUBQUERY_0"."S" , 'SL' ) ) AS "SUBQUERY_1_COL_1"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
    )
  }

  test("AIQ test pushdown to_number") {
    jdbcUpdate(s"create or replace table $test_table_string " +
      s"(s1 string, s2 string, s3 string, s4 string)")
    jdbcUpdate(s"insert into $test_table_string values " +
      s"""
         |('454', '454.00', '12,454', '12,454.8-'),
         |(NULL, NULL, NULL, NULL)
         |""".stripMargin.linesIterator.mkString(" ").trim
    )

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_string)
      .load()

    val resultDF = tmpDF.selectExpr(
      "to_number(s1, '999')",
      "to_number(s2, '000.00')",
      "to_number(s3, '99,999')",
      "to_number(s4, '99,999.9S')",
    )
    val expectedResult = Seq(
      Row(454, BigDecimal(454.00).setScale(2), 12454, -12454.8),
      Row(null, null, null, null),
    )

    testPushdown(
      s"""
         |SELECT
         |  ( TO_NUMBER ( "SUBQUERY_0"."S1" , '999' , 3 , 0 ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( TO_NUMBER ( "SUBQUERY_0"."S2" , '000.00' , 5 , 2 ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( TO_NUMBER ( "SUBQUERY_0"."S3" , '99,999' , 5 , 0 ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( TO_NUMBER ( "SUBQUERY_0"."S4" , '99,999.9S' , 6 , 1 ) ) AS "SUBQUERY_1_COL_3"
         |FROM (
         |  SELECT * FROM ( $test_table_string ) AS "SF_CONNECTOR_QUERY_ALIAS"
         |) AS "SUBQUERY_0"
         |""".stripMargin.linesIterator.map(_.trim).mkString(" ").trim,
      resultDF,
      expectedResult,
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