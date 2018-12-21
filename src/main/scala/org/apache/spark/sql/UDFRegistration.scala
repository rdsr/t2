package org.apache.spark.sql

import nimble.api.UDF
import nimble.internal.SparkUDF
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression

object UDFRegistration {
  def register(fn: UDF, sparkSession: SparkSession): Unit = {
    sparkSession
      .sessionState
      .functionRegistry
      .registerFunction(FunctionIdentifier(fn.name), (children: Seq[Expression]) => SparkUDF(fn, children))
  }
}
