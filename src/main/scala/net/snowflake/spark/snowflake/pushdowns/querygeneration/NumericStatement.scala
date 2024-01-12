package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, LongVariable, SnowflakeSQLStatement}

import scala.language.postfixOps
import org.apache.spark.sql.catalyst.expressions.{
  Abs,
  Acos,
  Asin,
  Atan,
  Attribute,
  Ceil,
  CheckOverflow,
  Cos,
  Cosh,
  Exp,
  Expression,
  Floor,
  Greatest,
  Least,
  Log,
  Logarithm,
  Pi,
  Pow,
  PromotePrecision,
  Rand,
  Round,
  Sin,
  Sinh,
  Sqrt,
  Tan,
  Tanh,
  UnaryMinus
}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object NumericStatement {

  /** Used mainly by QueryGeneration.convertExpression. This matches
    * a tuple of (Expression, Seq[Attribute]) representing the expression to
    * be matched and the fields that define the valid fields in the current expression
    * scope, respectively.
    *
    * @param expAttr A pair-tuple representing the expression to be matched and the
    *                attribute fields.
    * @return An option containing the translated SQL, if there is a match, or None if there
    *         is no match.
    */
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case _: Abs | _: Acos | _: Cos | _: Tan | _: Tanh | _: Cosh | _: Atan |
           _: Floor | _: Sin | _: Log | _: Asin | _: Sqrt | _: Ceil | _: Sqrt |
           _: Sinh | _: Greatest | _: Least | _: Exp =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case Logarithm(base, expr) =>
        functionStatement(
          "LOG",
          Seq(
            convertStatement(base, fields),
            blockStatement(convertStatement(expr, fields)),
          ),
        )

      // From spark 3.1, UnaryMinus() has 2 parameters.
      case UnaryMinus(child, _) =>
        ConstantString("-") +
          blockStatement(convertStatement(child, fields))

      case Pow(left, right) =>
        ConstantString("POWER") +
          blockStatement(
            convertStatement(left, fields) + "," + convertStatement(
              right,
              fields
            )
          )

      case PromotePrecision(child) => convertStatement(child, fields)

      case CheckOverflow(child, t, _) =>
        MiscStatement.getCastType(t) match {
          case Some(cast) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
          case _ => convertStatement(child, fields)
        }

      // Spark has resolved PI() as 3.141592653589793
      // Suppose connector can't see Pi().
      case Pi() => ConstantString("PI()") !

      case Rand(seed, _) =>
        seed match {
          case _: Expression =>
            // https://docs.snowflake.com/en/sql-reference/functions/uniform
            // Spark follows the following Snowflake equivalent:
            //  uniform(0::float, 1::float, random(seed))
            functionStatement(
              "UNIFORM",
              Seq(
                ConstantString("0::float").toStatement,
                ConstantString("1::float").toStatement,
                functionStatement(
                  "RANDOM",
                  Seq(convertStatement(seed, fields)),
                ),
              ),
            )
          // default to original implementation if new one isn't applicable
          case _ =>
            // From spark 3.1, Rand() has 2 parameters.
            ConstantString("RANDOM") + blockStatement(
              LongVariable(Option(seed).map(_.asInstanceOf[Long])) !
            )
        }

      case Round(child, scale) =>
        ConstantString("ROUND") + blockStatement(
          convertStatements(fields, child, scale)
        )

      case _ => null
    })
  }
}
