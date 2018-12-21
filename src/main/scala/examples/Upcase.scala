package examples

import java.util

import nimble.api.UDF1
import org.apache.spark.sql.types.{DataType, StringType}

class Upcase {
  val fn1: UDF1[String, String] = new UDF1[String, String] {

    override def apply(v1: String): String = v1.toUpperCase()

    override def dataType(args: util.List[DataType]): DataType = StringType

    override def name: String = "Upcase"
  }

  val c: UDF1[String, String] = (a: String) => a
}
