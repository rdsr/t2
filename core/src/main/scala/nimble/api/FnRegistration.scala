package nimble.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{FnRegistration => Helper}


object FnRegistration {
  def register(fn: Fn, sparkSession: SparkSession): Unit = {
    Helper.register(fn, sparkSession)
  }
}
