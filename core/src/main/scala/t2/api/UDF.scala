package nimble.api

import nimble.internal.SparkUDF
import org.apache.spark.sql.Column

case class UDF(fn: Fn) {
  def apply(exprs: Column*): Column = new Column(SparkUDF(fn, exprs.map(_.expr)))
}
