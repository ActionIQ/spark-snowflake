package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Add, AddMonths, AiqDateToString, AiqDayDiff, AiqDayOfTheWeek, AiqDayStart, AiqFromUnixTime, AiqStringToDate, AiqWeekDiff, Attribute, Cast, ConvertTimezone, CurrentTimeZone, DateAdd, DateDiff, DateSub, DayOfMonth, DayOfWeek, DayOfYear, Decode, Divide, Expression, Extract, Floor, FromUTCTimestamp, FromUnixTime, GetTimestamp, Hour, LastDay, Literal, MakeDate, MakeTimestamp, Minute, Month, MonthsBetween, Multiply, NextDay, ParseToDate, ParseToTimestamp, Quarter, Remainder, Second, Subtract, ToUTCTimestamp, ToUnixTimestamp, TruncDate, TruncTimestamp, UnixMillis, UnixSeconds, UnixTimestamp, WeekDay, WeekOfYear, Year}
import org.apache.spark.sql.types._

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
      case e: DateDiff =>
        functionStatement(
          expr.prettyName.toUpperCase,
          // Using `DAY` because the Spark equivalent returns the number of days
          Seq(ConstantString("'DAY'").toStatement) ++
          // arguments are in reverse order in Snowflake so exchanging the sign here
          Seq(e.startDate, e.endDate).map(convertStatement(_, fields)),
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
      case e: LastDay =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(
            convertStatement(e.startDate, fields),
            // Using `MONTH` because the Spark equivalent returns
            // the last day of the month which the date belongs to
            ConstantString("'MONTH'").toStatement,
          ),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/months_between
      case e: MonthsBetween =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(e.date1, e.date2).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/next_day
      case e: NextDay =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(e.startDate, e.dayOfWeek).map(convertStatement(_, fields)),
        )

      // We implement `DayOfWeek` as `DAYOFWEEK` (day of the week as an integer value
      // in the range 0-6, where 0 represents Sunday) +1 since the Spark equivalent
      // returns the day of the week with 1 = Sunday, 2 = Monday, ..., 7 = Saturday
      case e: DayOfWeek =>
        blockStatement(
          functionStatement(
            "DAYOFWEEK",
            Seq(convertStatement(e.child, fields)),
          ) + ConstantString("+ 1")
        )

      // We implement `WeekDay` as `DAYOFWEEK_ISO` (day of the week as an integer value
      // in the range 1-7, where 1 represents Monday) -1 since the Spark equivalent
      // returns the day of the week with 0 = Monday, 1 = Tuesday, ..., 6 = Sunday
      case e: WeekDay =>
        blockStatement(
          functionStatement(
            "DAYOFWEEKISO",
            Seq(convertStatement(e.child, fields)),
          ) + ConstantString("- 1")
        )

      // https://docs.snowflake.com/en/sql-reference/functions/date_from_parts
      case e: MakeDate =>
        functionStatement(
          "DATE_FROM_PARTS",
          Seq(e.year, e.month, e.day).map(convertStatement(_, fields)),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/timestamp_from_parts
      case e: MakeTimestamp =>
        e.timezone.map { timezone =>
          val dateExpr = ConvertTimezone(
            timezone,
            Literal("UTC"),
            MakeTimestamp(e.year, e.month, e.day, e.hour, e.min, e.sec, None),
          )

          convertStatement(dateExpr, fields)
        }.getOrElse {
          // Snowflake requires an extra argument for nanoseconds which Spark includes
          // in the Seconds Expression. Splitting it up here and passing it as it is
          // suppose to be in Snowflake
          val (secExpr, nanoExpr) = (
            Cast(e.sec, IntegerType),
            Cast(Multiply(Remainder(e.sec, Literal(1)), Literal(1000000000)), IntegerType)
          )

          functionStatement(
            "TIMESTAMP_NTZ_FROM_PARTS",
            (Seq(e.year, e.month, e.day, e.hour, e.min, secExpr, nanoExpr) ++
              optionalExprToFuncArg(e.timezone)).map(convertStatement(_, fields)),
          )
        }

      // https://docs.snowflake.com/en/sql-reference/functions/extract
      case e: Extract if e.field.foldable =>
        val fieldStr = e.field.eval().toString
        val sourceStmt = convertStatement(e.source, fields)
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
      case e: AiqDayStart =>
        val dateExpr = UnixMillis(
          TruncTimestamp(
            Literal("DAY"),
            DateAdd(
              ConvertTimezone(CurrentTimeZone(), e.timezone, e.timestamp),
              e.plusDays,
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
      case e: AiqStringToDate if e.format.foldable =>
        val dateExpr = UnixMillis(
            ConvertTimezone(
            e.timeZone,
            Literal("UTC"),
            ParseToTimestamp(e.dateStr, Some(e.format), TimestampType),
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
      case e: AiqDateToString if e.dateFormat.foldable =>
        val format = sparkDateFmtToSnowflakeDateFmt(e.dateFormat.eval().toString)
        val dateExpr = ConvertTimezone(CurrentTimeZone(), e.timezoneId, e.timestamp)

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
      case e: AiqDayDiff =>
        val startDateExpr = ConvertTimezone(CurrentTimeZone(), e.timezoneId, e.startTs)
        val endDateExpr = ConvertTimezone(CurrentTimeZone(), e.timezoneId, e.endTs)

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
      case e: AiqWeekDiff if e.startDay.foldable =>

        val startDayInt = getAiqDayOfWeekFromString(e.startDay.eval().toString)
        val Seq(daysSinceEndExpr, daysSinceStartExpr) =
          Seq(e.endTs, e.startTs).map { expr =>
            // Wrapping in `FLOOR` because Casting to Int in Snowflake is
            // creating data issues. Example:
            // - Java/Spark: (18140 + 3) / 7 = 2591 (Original value: 2591.8571428571427)
            // - Snowflake: SELECT ((18140 + 3) / 7) ::int = 2592 (Original value: 2591.857143)
            Floor(
              Divide(
                Add(AiqDayDiff(Literal(0L), expr, e.timezoneId), Literal(startDayInt)),
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
      case e: AiqDayOfTheWeek =>
        val dateExpr = Decode(
          Seq(
            WeekDay(ConvertTimezone(CurrentTimeZone(), e.timezoneId, e.epochTimestamp)),
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

      /*
      --- spark.sql(
      ---   """select aiq_from_unixtime(0, 'yyyy-MM-dd HH:mm:ss', 'UTC')"""
      --- ).as[String].collect.head == 1970-01-01 00:00:00
      SELECT (
        TO_CHAR(
          CONVERT_TIMEZONE('UTC', CAST(CAST(0 AS NUMBER) AS VARCHAR)),
          'yyyy-MM-dd HH24:mi:SS'
        )
      )
      -- 1970-01-01 00:00:00
      */
      case e: AiqFromUnixTime if e.format.foldable =>
        val dateExpr = ConvertTimezone(CurrentTimeZone(), e.timeZone, e.sec)

        functionStatement(
          "TO_CHAR",
          Seq(convertStatement(dateExpr, fields)) ++ formatToFunctionArg(Some(e.format)),
        )

      case e: ConvertTimezone =>
        // time zone of the input timestamp
        val sourceArg = e.sourceTz match {
          // For the 2-argument version the return value is always of type TIMESTAMP_TZ which means
          // that we don't necessarily need to wrap `ConvertTimezone` around `ParseToTimestamp`
          case _: CurrentTimeZone => Seq.empty
          // For the 3-argument version the return value is always of type TIMESTAMP_NTZ which means
          // that we may have to wrap `ConvertTimezone` around `ParseToTimestamp` or something else
          case _ => Seq(convertStatement(e.sourceTz, fields))
        }

        functionStatement(
          expr.prettyName.toUpperCase,
          sourceArg ++ Seq(
            convertStatement(e.targetTz, fields), // time zone to be converted
            convertStatement(Cast(e.sourceTs, StringType), fields),
          ),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/to_timestamp
      case e: ParseToTimestamp if e.format.forall(_.foldable) =>
        val fmtExpr = if (e.left.dataType.isInstanceOf[StringType]) e.format else None

        functionStatement(
          "TO_TIMESTAMP_NTZ",
          Seq(convertStatement(e.left, fields)) ++ formatToFunctionArg(fmtExpr),
        )

      // https://docs.snowflake.com/en/sql-reference/functions/to_timestamp
      case e: GetTimestamp if e.right.foldable =>
        convertStatement(ParseToTimestamp(e.left, Some(e.right), e.dataType, e.timeZoneId), fields)

      // https://docs.snowflake.com/en/sql-reference/functions/to_date
      case e: ParseToDate if e.format.forall(_.foldable) =>
        functionStatement(
          expr.prettyName.toUpperCase,
          Seq(convertStatement(e.left, fields)) ++ formatToFunctionArg(e.format),
        )

      // scalastyle:off line.size.limit
      // https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/1.12.0/api/snowflake.snowpark.functions.from_unixtime
      // scalastyle:on line.size.limit
      case e: FromUnixTime if e.format.foldable =>
        val dateExpr = ConvertTimezone(
          CurrentTimeZone(),
          Literal("UTC"),
          Cast(Multiply(e.sec, Literal(1000L)), LongType), // msExpr
        )

        functionStatement(
          "TO_CHAR",
          Seq(convertStatement(dateExpr, fields)) ++ formatToFunctionArg(Some(e.format)),
        )

      case e: FromUTCTimestamp =>
        convertStatement(ConvertTimezone(Literal("UTC"), e.right, e.left), fields)

      case e: ToUnixTimestamp =>
        val dateExpr = UnixSeconds(
          ParseToTimestamp(e.timeExp, Some(e.format), TimestampType)
        )

        convertStatement(dateExpr, fields)

      case e: ToUTCTimestamp =>
        convertStatement(ConvertTimezone(e.right, Literal("UTC"), e.left), fields)

      case e: UnixTimestamp =>
        val dateExpr = UnixSeconds(
          ParseToTimestamp(e.timeExp, Some(e.format), TimestampType)
        )

        convertStatement(dateExpr, fields)

      case e: UnixSeconds =>
        functionStatement(
          "DATE_PART",
          Seq(
            ConstantString("'EPOCH_SECOND'").toStatement,
            convertStatement(e.child, fields),
          )
        )

      case e: UnixMillis =>
        functionStatement(
          "DATE_PART",
          Seq(
            ConstantString("'EPOCH_MILLISECOND'").toStatement,
            convertStatement(e.child, fields),
          )
        )

      case _ => null
    })
  }
}
