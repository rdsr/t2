package nimple.examples

import java.util.{List => JList}

import nimble.api.{Fn1, UDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, DataTypes}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val dataset = Seq((0, "hello"), (1, "world")).toDF("id", "text")

    val upper = new Fn1[String, String] {
      override def call(v1: String): String = v1.toUpperCase

      override def name: String = "upper"

      override def returnType(args: JList[DataType]): DataType = DataTypes.StringType
    }
    val c = UDF(upper)

    val add = new Fn1[Int, Int] {
      override def call(v1: Int): Int = {
        v1 + 2
      }

      override def name: String = "add"

      override def returnType(args: JList[DataType]): DataType = DataTypes.IntegerType
    }

    //FnRegistration.register(spark, upper)
    val df = dataset.withColumn("x", c('text))
    df.show()
    //dataset.createOrReplaceTempView("x")
    //spark.sql("select upper(text), add(id) from x").show()
  }
}
