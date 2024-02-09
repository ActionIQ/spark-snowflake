package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{
  ConstantString,
  SnowflakeSQLStatement
}
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  AddMonths,
  AiqDateToString,
  AiqDayDiff,
  AiqDayStart,
  AiqStringToDate,
  AiqWeekDiff,
  Attribute,
  DateAdd,
  DateSub,
  Divide,
  Expression,
  Floor,
  Literal,
  Month,
  Quarter,
  Subtract,
  TruncDate,
  TruncTimestamp,
  Year
}

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
      // Snowflake Two digits for hour (00 through 23)
      .replaceAll("HH", "HH24")
      // Snowflake Two digits for hour (01 through 12)
      .replaceAll("hh", "HH12")
      // Snowflake Two digits for minute (00 through 59)
      .replaceAll("mm", "mi")
      // Snowflake Two digits for second (00 through 59)
      .replaceAll("ss", "SS")
      // Snowflake Two-digit month => M -> MM
      .replaceAll("(?<=[^M])M(?=[^M])", "MM")
      // Snowflake Abbreviated month name => MMM -> MON
      .replaceAll("(?<=[^M])M{3}(?=[^M])", "MON")
      // Snowflake Abbreviated day of week
      .replaceAll("E{1,3}", "DY")
      // Snowflake Ante meridiem (am) / post meridiem (pm)
      .replaceAll("a", "AM")
  }

  // This is an AIQ map to the offset of the epoch date to the imposed "first day of the week."
  // (Default Sunday). The Epoch date is a thursday (offset = 0).
  // scalastyle:off line.size.limit
  // https://github.com/ActionIQ/aiq/blob/master/libs/flame_utils/src/main/scala/co/actioniq/flame/SqlFunctions.scala#L356-L366
  // scalastyle:on line.size.limit
  private def getAiqDayOfWeekFromString(dayStr: String): Int = {
    val dowString = dayStr.toUpperCase()
    dowString match {
      case "SU" | "SUN" | "SUNDAY" => 4
      case "MO" | "MON" | "MONDAY" => 3
      case "TU" | "TUE" | "TUESDAY" => 2
      case "WE" | "WED" | "WEDNESDAY" => 1
      case "TH" | "THU" | "THURSDAY" => 0
      case "FR" | "FRI" | "FRIDAY" => 6
      case "SA" | "SAT" | "SATURDAY" => 5
      case _ =>
        throw new IllegalArgumentException(s"""Illegal input for day of week: $dayStr""")
    }
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

      /*
      --- 2023-09-01 to 2023-09-02
      --- spark.sql(
      ---   """select aiq_day_diff(1693609200000, 1693616400000, 'UTC')"""
      --- ).as[Long].collect.head == 1
      select DATEDIFF(
        'day',
        TO_TIMESTAMP(
          CONVERT_TIMEZONE(
            'UTC',
            1693609200000::varchar
          )
        ),
        TO_TIMESTAMP(
          CONVERT_TIMEZONE(
            'UTC',
            1693616400000::varchar
          )
        )
      )
      -- 1
      */
      case AiqDayDiff(startTimestampLong, endTimestampLong, timezoneStr) =>
        val startTimestampStm = functionStatement(
          "TO_TIMESTAMP",
          Seq(
            functionStatement(
              "CONVERT_TIMEZONE",
              Seq(
                convertStatement(timezoneStr, fields),
                convertStatement(startTimestampLong, fields) + ConstantString("::varchar"),
              ),
            )
          )
        )
        val endTimestampStm = functionStatement(
          "TO_TIMESTAMP",
          Seq(
            functionStatement(
              "CONVERT_TIMEZONE",
              Seq(
                convertStatement(timezoneStr, fields),
                convertStatement(endTimestampLong, fields) + ConstantString("::varchar"),
              ),
            )
          )
        )

        functionStatement(
          "DATEDIFF",
          Seq(
            ConstantString("'day'").toStatement,
            startTimestampStm,
            endTimestampStm,
          )
        )

      /*
     --- 2023-09-01 to 2023-09-02
     --- spark.sql(
     ---   """select aiq_week_diff(1551880107963, 1553890107963, 'sunday', 'UTC')"""
     --- ).as[Long].collect.head == 3
     select (
       (
         FLOOR (
           (
             (
               DATEDIFF (
                 'day' ,
                 TO_TIMESTAMP(CONVERT_TIMEZONE ('UTC', 0::varchar)),
                 TO_TIMESTAMP(CONVERT_TIMEZONE ('UTC', 1551880107963::varchar))
               ) + 4
             ) / 7
           )
         )
         -
         FLOOR (
           (
             (
               DATEDIFF (
                 'day' ,
                 TO_TIMESTAMP (CONVERT_TIMEZONE('UTC' , 0::varchar)),
                 TO_TIMESTAMP (CONVERT_TIMEZONE('UTC' , 1553890107963::varchar))
               ) + 4
             ) / 7
           )
         )
       )
     -- 3
     */
      case AiqWeekDiff(startTimestampLong, endTimestampLong, startDayStr, timezoneStr)
        if startDayStr.foldable =>

        val startDayInt = getAiqDayOfWeekFromString(startDayStr.eval().toString)
        val Seq(daysSinceEndExpr, daysSinceStartExpr) =
          Seq(endTimestampLong, startTimestampLong).map { expr =>
            // Wrapping in `FLOOR` because Casting to Int in Snowflake is
            // creating data issues. Example:
            // - Java/Spark: (18140 + 3) / 7 = 2591 (Original value: 2591.8571428571427)
            // - Snowflake: SELECT ((18140 + 3) / 7) ::int = 2592 (Original value: 2591.857143)
            Floor(
              Divide(
                Add(AiqDayDiff(Literal(0L), expr, timezoneStr), Literal(startDayInt)),
                Literal(7)
              )
            )
          }

        convertStatement(Subtract(daysSinceEndExpr, daysSinceStartExpr), fields)

      case _ => null
    })
  }
}
