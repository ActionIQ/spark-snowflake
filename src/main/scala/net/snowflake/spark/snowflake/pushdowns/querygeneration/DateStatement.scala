package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Add, AddMonths, AiqDateToString, AiqDayDiff, AiqDayOfTheWeek, AiqDayStart, AiqStringToDate, AiqWeekDiff, Attribute, Cast, ConvertTimezone, CurrentTimeZone, DateAdd, DateDiff, DateSub, DayOfMonth, DayOfWeek, DayOfYear, Decode, Divide, Expression, Extract, Floor, FromUTCTimestamp, FromUnixTime, Hour, LastDay, Literal, MakeDate, MakeTimestamp, Minute, Month, MonthsBetween, Multiply, NextDay, ParseToDate, ParseToTimestamp, Quarter, Remainder, Second, Subtract, ToUTCTimestamp, ToUnixTimestamp, TruncDate, TruncTimestamp, UnixMillis, UnixSeconds, UnixTimestamp, WeekDay, WeekOfYear, Year}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, NullType, StringType, TimestampType}

/**
 * Extractor for date-style expressions.
 */
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

  private def formatToFunctionArg(format: Option[Expression]): Seq[SnowflakeSQLStatement] = {
    require(
      format.forall(_.foldable),
      "`formatToFunctionArg` should ONLY be called on a foldable `format` Expression"
    )

    optionalExprToFuncArg(format).map { formatStr =>
      val fmt = sparkDateFmtToSnowflakeDateFmt(formatStr.eval().toString)
      ConstantString(s"'$fmt'").toStatement
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

      // https://docs.snowflake.com/en/sql-reference/functions/datediff
      case DateDiff(endDate, startDate) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          // Using `DAY` because the Spark equivalent returns the number of days
          Seq(ConstantString("'DAY'").toStatement) ++
          // arguments are in reverse order in Snowflake so exchanging the sign here
          Seq(startDate, endDate).map(convertStatement(_, fields)),
        )

      // AddMonths can't be pushdown to snowflake because their functionality is different.
      // For Snowflake and Spark 2.3/2.4, AddMonths() will preserve the end-of-month information.
      // But, Spark 3.0, it doesn't. For example,
      // On spark 2.3/2.4, "2015-02-28" +1 month -> "2015-03-31"
      // On spark 3.0,     "2015-02-28" +1 month -> "2015-03-28"
      case AddMonths(_, _) => null

      case _: Month | _: Quarter | _: Year |
           _: TruncDate | _: TruncTimestamp |
           // https://docs.snowflake.com/en/sql-reference/functions/hour-minute-second
           // In Snowflake, DateType does NOT save the time part which means that the
           // following functions will return 0
           _: Hour | _: Minute | _: Second |
           // https://docs.snowflake.com/en/sql-reference/functions/year
           _: DayOfMonth | _: DayOfYear | _: WeekOfYear =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      // https://docs.snowflake.com/en/sql-reference/functions/last_day
      case LastDay(startDate) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(
            convertStatement(startDate, fields),
            // Using `MONTH` because the Spark equivalent returns
            // the last day of the month which the date belongs to
            ConstantString("'MONTH'").toStatement,
          ),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/months_between
      case MonthsBetween(date1, date2, _, _) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(date1, date2).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/next_day
      case NextDay(startDate, dayOfWeek, _) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(startDate, dayOfWeek).map(convertStatement(_, fields)),
        )

      // We implement `DayOfWeek` as `DAYOFWEEK` (day of the week as an integer value
      // in the range 0-6, where 0 represents Sunday) +1 since the Spark equivalent
      // returns the day of the week with 1 = Sunday, 2 = Monday, ..., 7 = Saturday
      case DayOfWeek(child) =>
        blockStatement(
          functionStatement(
            "DAYOFWEEK",
            Seq(convertStatement(child, fields)),
          ) + ConstantString("+ 1")
        )

      // We implement `WeekDay` as `DAYOFWEEK_ISO` (day of the week as an integer value
      // in the range 1-7, where 1 represents Monday) -1 since the Spark equivalent
      // returns the day of the week with 0 = Monday, 1 = Tuesday, ..., 6 = Sunday
      case WeekDay(child) =>
        blockStatement(
          functionStatement(
            "DAYOFWEEKISO",
            Seq(convertStatement(child, fields)),
          ) + ConstantString("- 1")
        )

      // https://docs.snowflake.com/en/sql-reference/functions/date_from_parts
      case MakeDate(year, month, day, _) =>
        functionStatement(
          "DATE_FROM_PARTS",
          Seq(year, month, day).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/timestamp_from_parts
      case MakeTimestamp(year, month, day, hour, min, sec, timezoneOpt, _, _, _) =>
        timezoneOpt.map { timezone =>
          val dateExpr = ConvertTimezone(
            timezone,
            Literal("UTC"),
            MakeTimestamp(year, month, day, hour, min, sec, None),
          )

          convertStatement(dateExpr, fields)
        }.getOrElse {
          // Snowflake requires an extra argument for nanoseconds which Spark includes
          // in the Seconds Expression. Splitting it up here and passing it as it is
          // suppose to be in Snowflake
          val (secExpr, nanoExpr) = (
            Cast(sec, IntegerType),
            Cast(Multiply(Remainder(sec, Literal(1)), Literal(1000000000)), IntegerType)
          )

          functionStatement(
            "TIMESTAMP_NTZ_FROM_PARTS",
            (Seq(year, month, day, hour, min, secExpr, nanoExpr) ++
              optionalExprToFuncArg(timezoneOpt)).map(convertStatement(_, fields)),
          )
        }

      // https://docs.snowflake.com/en/sql-reference/functions/extract
      case Extract(field, source, _) if field.foldable =>
        val fieldStr = field.eval().toString
        val sourceStmt = convertStatement(source, fields)
        functionStatement(
          "EXTRACT",
          Seq(ConstantString(fieldStr) + "FROM" + sourceStmt)
        )

      /*
      --- spark.sql(
      ---   "select aiq_day_start(1460080000000, 'America/New_York', 2)"
      --- ).as[Long].collect.head == 1460174400000L
      SELECT DATE_PART(
        'EPOCH_MILLISECOND',
        DATE_TRUNC(
          'DAY',
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
        val dateExpr = UnixMillis(
          TruncTimestamp(
            Literal("DAY"),
            DateAdd(
              ConvertTimezone(CurrentTimeZone(), timezoneStr, timestampLong),
              plusDaysInt,
            ),
          )
        )

        convertStatement(dateExpr, fields)

      /*
      --- spark.sql(
      ---   "select aiq_string_to_date('2019-09-01 14:50:52', 'yyyy-MM-dd HH:mm:ss', 'America/New_York')"
      --- ).as[Long].collect.head == 1567363852000L
      SELECT DATE_PART(
        'EPOCH_MILLISECOND',
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
        val dateExpr = UnixMillis(
            ConvertTimezone(
            timezoneStr,
            Literal("UTC"),
            ParseToTimestamp(dateStr, Some(formatStr), TimestampType),
          )
        )

        convertStatement(dateExpr, fields)

      /*
      --- spark.sql(
      ---   """select aiq_date_to_string(1567363852000, "yyyy-MM-dd HH:mm", 'America/New_York')"""
      --- ).as[String].collect.head == "2019-09-01 14:50"
      SELECT TO_CHAR(
        CONVERT_TIMEZONE(
          'America/New_York',
          CAST(1567363852000 AS VARCHAR)
        ),
        'yyyy-mm-dd HH:mm'
      )
      -- 2019-09-01 14:50
      */
      case AiqDateToString(timestampLong, formatStr, timezoneStr) if formatStr.foldable =>
        val format = sparkDateFmtToSnowflakeDateFmt(formatStr.eval().toString)
        val dateExpr = ConvertTimezone(CurrentTimeZone(), timezoneStr, timestampLong)

        functionStatement(
          "TO_CHAR",
          Seq(
            convertStatement(dateExpr, fields),
            ConstantString(s"'$format'").toStatement,
          ),
        )

      /*
      --- 2023-09-01 to 2023-09-02
      --- spark.sql(
      ---   """select aiq_day_diff(1693609200000, 1693616400000, 'UTC')"""
      --- ).as[Long].collect.head == 1
      SELECT DATEDIFF(
        'DAY',
        CONVERT_TIMEZONE(
          'UTC',
          CAST(1693609200000 AS VARCHAR)
        ),
        CONVERT_TIMEZONE(
          'UTC',
          CAST(1693616400000 AS VARCHAR)
        )
      )
      -- 1
      */
      case AiqDayDiff(startTimestampLong, endTimestampLong, timezoneStr) =>
        val startDateExpr = ConvertTimezone(CurrentTimeZone(), timezoneStr, startTimestampLong)
        val endDateExpr = ConvertTimezone(CurrentTimeZone(), timezoneStr, endTimestampLong)

        convertStatement(DateDiff(endDateExpr, startDateExpr), fields)

      /*
     --- 2019-03-06 to 2019-03-29
     --- spark.sql(
     ---   """select aiq_week_diff(1551880107963, 1553890107963, 'sunday', 'UTC')"""
     --- ).as[Long].collect.head == 3
     SELECT (
       (
         FLOOR(
           (
             (
               DATEDIFF(
                 'DAY' ,
                 CONVERT_TIMEZONE('UTC', CAST(0 AS VARCHAR)),
                 CONVERT_TIMEZONE('UTC', CAST(1551880107963 AS VARCHAR))
               ) + 4
             ) / 7
           )
         )
         -
         FLOOR(
           (
             (
               DATEDIFF(
                 'DAY' ,
                 CONVERT_TIMEZONE('UTC', CAST(0 AS VARCHAR)),
                 CONVERT_TIMEZONE('UTC', CAST(1553890107963 AS VARCHAR))
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
                Literal(7),
              )
            )
          }

        convertStatement(Subtract(daysSinceEndExpr, daysSinceStartExpr), fields)

      /*
      --- 2019-03-29
      --- spark.sql(
      ---   """select aiq_day_of_the_week(1553890107963, 'UTC')"""
      --- ).as[String].collect.head == friday
      SELECT DECODE(
        (
          EXTRACT(
            'DAYOFWEEK_ISO' ,
            CONVERT_TIMEZONE('UTC', CAST(1553890107963 AS VARCHAR))
          ) - 1
        ) ,
        0 , 'monday' ,
        1 , 'tuesday' ,
        2 , 'wednesday' ,
        3 , 'thursday' ,
        4 , 'friday' ,
        5 , 'saturday' ,
        6 , 'sunday' ,
        NULL
      )
      -- friday
      */
      // scalastyle:off line.size.limit
      // https://docs.snowflake.com/sql-reference/date-time-examples#retrieving-dates-and-days-of-the-week
      // scalastyle:on line.size.limit
      case AiqDayOfTheWeek(epochTimestamp, timezoneStr) =>
        val dateExpr = Decode(
          Seq(
            WeekDay(ConvertTimezone(CurrentTimeZone(), timezoneStr, epochTimestamp)),
            Literal(0), Literal("monday"),
            Literal(1), Literal("tuesday"),
            Literal(2), Literal("wednesday"),
            Literal(3), Literal("thursday"),
            Literal(4), Literal("friday"),
            Literal(5), Literal("saturday"),
            Literal(6), Literal("sunday"),
          ),
          Literal.default(NullType),
        )

        convertStatement(dateExpr, fields)

      case ConvertTimezone(sourceTz, targetTz, sourceTs) =>
        // time zone of the input timestamp
        val sourceArg = sourceTz match {
          // For the 2-argument version the return value is always of type TIMESTAMP_TZ which means
          // that we don't necessarily need to wrap `ConvertTimezone` around `ParseToTimestamp`
          case _: CurrentTimeZone => Seq.empty
          // For the 3-argument version the return value is always of type TIMESTAMP_NTZ which means
          // that we may have to wrap `ConvertTimezone` around `ParseToTimestamp` or something else
          case _ => Seq(convertStatement(sourceTz, fields))
        }

        functionStatement(
          expr.prettyName.toUpperCase,
          sourceArg ++ Seq(
            convertStatement(targetTz, fields), // time zone to be converted
            convertStatement(Cast(sourceTs, StringType), fields),
          ),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/to_timestamp
      case ParseToTimestamp(left, formatStrOpt, _, timezoneStrOpt)
        if formatStrOpt.forall(_.foldable) =>

        // This is a workaround for now since TimestampNTZType is private case class in Spark 3.3
        // Spark 3.4 makes it a public class at which point we can change this logic to use
        // TimestampType and TimestampNTZType to decide
        val functionName = timezoneStrOpt.map ( _ =>
          "TO_TIMESTAMP"
        ).getOrElse(
          "TO_TIMESTAMP_NTZ" // timestamp with no time zone
        )

        functionStatement(
          functionName,
          Seq(convertStatement(left, fields)) ++ formatToFunctionArg(formatStrOpt),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/to_date
      case ParseToDate(left, formatStrOpt, _) if formatStrOpt.forall(_.foldable) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatement(left, fields)) ++ formatToFunctionArg(formatStrOpt),
        )

      // scalastyle:off line.size.limit
      // https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/1.12.0/api/snowflake.snowpark.functions.from_unixtime
      // scalastyle:on line.size.limit
      case FromUnixTime(sec, formatStr, _) =>
        val dateExpr = ParseToTimestamp(
          Cast(Multiply(sec, Literal(1000L)), LongType), // msExpr
          Some(formatStr),
          TimestampType
        )

        convertStatement(dateExpr, fields)

      case FromUTCTimestamp(left, right) =>
        convertStatement(ConvertTimezone(Literal("UTC"), right, left), fields)

      case ToUnixTimestamp(timeExp, formatStr, _, _) =>
        val dateExpr = UnixSeconds(
          ParseToTimestamp(timeExp, Some(formatStr), TimestampType)
        )

        convertStatement(dateExpr, fields)

      case ToUTCTimestamp(left, right) =>
        val dateExpr = ParseToTimestamp(
          ConvertTimezone(right, Literal("UTC"), left),
          Some(Literal(TimestampFormatter.defaultPattern)),
          TimestampType,
        )

        convertStatement(dateExpr, fields)

      case UnixTimestamp(timeExp, formatStr, _, _) =>
        val dateExpr = UnixSeconds(
          ParseToTimestamp(
            timeExp,
            Some(formatStr),
            TimestampType,
          ),
        )

        convertStatement(dateExpr, fields)

      case UnixSeconds(child) =>
        functionStatement(
          "DATE_PART",
          Seq(
            ConstantString("'EPOCH_SECOND'").toStatement,
            convertStatement(child, fields),
          )
        )

      case UnixMillis(child) =>
        functionStatement(
          "DATE_PART",
          Seq(
            ConstantString("'EPOCH_MILLISECOND'").toStatement,
            convertStatement(child, fields),
          )
        )

      case _ => null
    })
  }
}
