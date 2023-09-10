package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{AddMonths, AiqDateToString, AiqDayStart, AiqStringToDate, Attribute, DateAdd, DateSub, Expression, Month, Quarter, TruncDate, TruncTimestamp, Year}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object DateStatement {
  // DateAdd's pretty name in Spark is "date_add",
  // the counterpart's name in SF is "DATEADD".
  // And the syntax is some different.
  val SNOWFLAKE_DATEADD = "DATEADD"

  /**
    * Function to convert a regular date format to a Snowflake date format
    *   - Note: This solution has the limitation that `mm` indicates only minutes.
    *     This is true for regular date formats but in Snowflake `MM` (month)
    *     and `mm` are the same. Since our system supports the former this is an
    *     acceptable limitation (for now).
    */
  private def sparkDateFmtToSnowflakeDateFmt(format: String): String = {
    format
      .replaceAll("HH", "HH24") // Snowflake Two digits for hour (00 through 23)
      .replaceAll("hh", "HH12") // Snowflake Two digits for hour (01 through 12)
      .replaceAll("a", "AM")    // Snowflake Ante meridiem (am) / post meridiem (pm)
      .replaceAll("mm", "mi")   // Snowflake Two digits for minute (00 through 59)
      .replaceAll("ss", "SS")   // Snowflake Two digits for second (00 through 59)
  }

  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case DateAdd(startDate, days) =>
        ConstantString(SNOWFLAKE_DATEADD) +
          blockStatement(
            ConstantString("day,") +
              convertStatement(days, fields) + "," +
              convertStatement(startDate, fields)
          )

      // Snowflake has no direct DateSub function,
      // it is pushdown by DATEADD with negative days
      case DateSub(startDate, days) =>
        ConstantString(SNOWFLAKE_DATEADD) +
          blockStatement(
            ConstantString("day, (0 - (") +
              convertStatement(days, fields) + ") )," +
              convertStatement(startDate, fields)
          )

      // AddMonths can't be pushdown to snowflake because their functionality is different.
      // For Snowflake and Spark 2.3/2.4, AddMonths() will preserve the end-of-month information.
      // But, Spark 3.0, it doesn't. For example,
      // On spark 2.3/2.4, "2015-02-28" +1 month -> "2015-03-31"
      // On spark 3.0,     "2015-02-28" +1 month -> "2015-03-28"
      case AddMonths(_, _) => null

      case _: Month | _: Quarter | _: Year |
           _: TruncDate | _: TruncTimestamp =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      /*
      --- spark.sql(
      ---   "select aiq_day_start(1460080000000, 'America/New_York', 2)"
      --- ).as[Long].collect.head == 1460174400000L
      select DATE_PART(
        epoch_millisecond,
        DATE_TRUNC(
          'day',
          DATEADD(
            day,
            2,
            CONVERT_TIMEZONE(
              'America/New_York',
              1460080000000::varchar
            )
          )
        )
      )
      -- 1460174400000
      */
      case AiqDayStart(timestampLong, timezoneStr, plusDaysInt) =>
        functionStatement(
          "DATE_PART",
          Seq(
            ConstantString("epoch_millisecond").toStatement,
            functionStatement(
              "DATE_TRUNC",
              Seq(
                ConstantString("'day'").toStatement,
                functionStatement(
                  "DATEADD",
                  Seq(
                    ConstantString("'day'").toStatement,
                    convertStatement(plusDaysInt, fields),
                    functionStatement(
                      "CONVERT_TIMEZONE",
                      Seq(
                        convertStatement(timezoneStr, fields),
                        convertStatement(timestampLong, fields) + ConstantString("::varchar"),
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        )

      /*
      --- spark.sql(
      ---   "select aiq_string_to_date('2019-09-01 14:50:52', 'yyyy-MM-dd HH:mm:ss', 'America/New_York')"
      --- ).as[Long].collect.head == 1567363852000L
      select DATE_PART(
        epoch_millisecond,
        CONVERT_TIMEZONE(
          'America/New_York',
          'UTC',
          TO_TIMESTAMP_NTZ(
            '2019-09-01 14:50:52',
            'yyyy-MM-dd HH:mm:ss'
          )
        )
      )
      -- 1567363852000
      */
      case AiqStringToDate(dateStr, formatStr, timezoneStr) if formatStr.foldable =>
        val format = sparkDateFmtToSnowflakeDateFmt(formatStr.eval().toString)
        functionStatement(
          "DATE_PART",
          Seq(
            ConstantString("epoch_millisecond").toStatement,
            functionStatement(
              "CONVERT_TIMEZONE",
              Seq(
                convertStatement(timezoneStr, fields), // time zone of the input timestamp
                ConstantString("'UTC'").toStatement, // time zone to be converted
                functionStatement(
                  "TO_TIMESTAMP_NTZ", // timestamp with no time zone
                  Seq(
                    convertStatement(dateStr, fields),
                    ConstantString(s"'$format'").toStatement,
                  )
                ),
              ),
            ),
          )
        )

      /*
      --- spark.sql(
      ---   """select aiq_date_to_string(1567363852000, "yyyy-MM-dd HH:mm", 'America/New_York')"""
      --- ).as[String].collect.head == "2019-09-01 14:50"
      select TO_CHAR(
        TO_TIMESTAMP(
          CONVERT_TIMEZONE(
            'America/New_York',
            1567363852000::varchar
          )
        ),
        'yyyy-mm-dd HH:mm'
      )
      -- 2019-09-01 14:50
      */
      case AiqDateToString(timestampLong, formatStr, timezoneStr) if formatStr.foldable =>
        val format = sparkDateFmtToSnowflakeDateFmt(formatStr.eval().toString)
        functionStatement(
          "TO_CHAR",
          Seq(
            functionStatement(
              "TO_TIMESTAMP",
              Seq(
                functionStatement(
                  "CONVERT_TIMEZONE",
                  Seq(
                    convertStatement(timezoneStr, fields),
                    convertStatement(timestampLong, fields) + ConstantString("::varchar"),
                  ),
                )
              )
            ),
            ConstantString(s"'$format'").toStatement,
          )
        )

      case _ => null
    })
  }
}
