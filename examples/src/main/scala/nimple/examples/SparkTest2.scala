package nimple.examples

import java.util

import nimble.api.{Fn1, FnRegistration, GenericRecord, UDF}
import org.apache.spark
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, MapType}

object SparkTest2 {
  case class SAB(a: String, b: String, c: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val df = List(Map("a" -> Row("a", "b", "c"))).toDF("c")

    val p = new Fn1[java.util.Map[String, GenericRecord], GenericRecord] {

      override def call(t1: util.Map[String, GenericRecord]): GenericRecord = t1.get("a")

      override def name(): String = "p"

      override def returnType(inputs: java.util.List[DataType]): DataType = {
        inputs.get(0).asInstanceOf[MapType].valueType
      }
    }

    spark.udf.register("p", (a: Map[String, Row]) => a("a"))
    //FnRegistration.register(spark, p)
    df.createOrReplaceTempView("x")
    benchmark(spark, 1000)
  }

  def benchmark(spark: SparkSession, count: Int): Unit = {
    var i = 0
    val s = System.currentTimeMillis()
    while (i < count) {
      spark.sql("select p(c) from x").collect()
      i += 1
    }
    val e = System.currentTimeMillis()

    println(s"${e - s} ms")
  }
}
