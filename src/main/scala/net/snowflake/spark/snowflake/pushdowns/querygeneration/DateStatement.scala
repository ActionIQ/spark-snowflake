package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{AddMonths, AiqDayStart, AiqDateToString, Attribute, DateAdd, DateSub, Expression, Month, Quarter, TruncDate, TruncTimestamp, Year}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object DateStatement {
  // DateAdd's pretty name in Spark is "date_add",
  // the counterpart's name in SF is "DATEADD".
  // And the syntax is some different.
  val SNOWFLAKE_DATEADD = "DATEADD"

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
      ---   "select aiq_date_to_string(1567363852000, "yyyy-MM-dd HH:mm", 'America/New_York')"
      --- ).as[String].collect.head == "2019-09-01 14:50"
      select TO_CHAR(
        TO_TIMESTAMP(
          CONVERT_TIMEZONE(
            'America/New_York',
            1567363852000::varchar
          )
        ),
        REGEXP_REPLACE(
          REPLACE(
            REPLACE(
              'yyyy-mm-dd HH:mm', 'HH', 'HH24'
            ),
            'hh', 'HH12'
          ),
          'mm', 'mi', 1, 2
        )
      )
      -- 2019-09-01 14:50
      */
      case AiqDateToString(timestampLong, formatStr, timezoneStr) =>
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
            functionStatement(
              "REGEXP_REPLACE",
              Seq(
                functionStatement(
                  "REPLACE",
                  Seq(
                    functionStatement(
                      "REPLACE",
                      Seq(
                        convertStatement(formatStr, fields),
                        ConstantString("'HH'").toStatement,
                        ConstantString("'HH24'").toStatement,
                      )
                    ),
                    ConstantString("'hh'").toStatement,
                    ConstantString("'HH12'").toStatement,
                  ),
                ),
                ConstantString("'mm'").toStatement,
                ConstantString("'mi'").toStatement,
                ConstantString("'1'").toStatement,
                ConstantString("'2'").toStatement,
              )
            ),
          )
        )

      case _ => null
    })
  }
}
