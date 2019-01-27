package examples

import java.util

import t2.api.Fn1
import org.apache.spark.sql.types.{DataType, StringType}

class Upcase {
  val fn1 = new Fn1[String, String] {
    override def call(v1: String): String = v1.toUpperCase()
    override def returnType(args: util.List[DataType]): DataType = StringType
    override def name: String = "Upcase"
  }
}
