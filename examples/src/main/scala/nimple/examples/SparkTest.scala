package nimple.examples

import java.util

import nimble.api.{Fn1, FnRegistration}
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
      override def apply(v1: String): String = v1.toUpperCase
      override def name: String = "upper"
      override def returnType(args: util.List[DataType]): DataType = DataTypes.StringType
    }

    val add = new Fn1[Int, Int] {
      override def apply(v1: Int): Int = v1 + 2
      override def name: String = "add"
      override def returnType(args: util.List[DataType]): DataType = DataTypes.IntegerType
    }
    FnRegistration.register(upper, spark)
    FnRegistration.register(add, spark)
    dataset.createOrReplaceTempView("x")
    spark.sql("select upper(text), add(id) from x").show()
  }
}
