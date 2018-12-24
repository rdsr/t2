package org.apache.spark.sql

import nimble.api.Fn
import nimble.internal.SparkUDF
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression

object FnRegistration {
  def register(fn: Fn, sparkSession: SparkSession): Unit = {
    sparkSession
      .sessionState
      .functionRegistry
      .registerFunction(
        FunctionIdentifier(fn.name),
        (children: Seq[Expression]) => SparkUDF(fn, children))
  }
}
