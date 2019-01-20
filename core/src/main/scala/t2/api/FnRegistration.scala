package nimble.api

import nimble.internal.SparkUDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression


object FnRegistration {
  def register(sparkSession: SparkSession, fn: Fn): Unit = {
    sparkSession
      .sessionState
      .functionRegistry
      .registerFunction(
        FunctionIdentifier(fn.name),
        (children: Seq[Expression]) => SparkUDF(fn, children))
  }
}