package nimble.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{UDFRegistration => Helper}


object UDFRegistration {
  def register(fn: UDF, sparkSession: SparkSession): Unit = {
    Helper.register(fn, sparkSession)
  }
}
