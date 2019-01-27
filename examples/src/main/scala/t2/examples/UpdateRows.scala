package t2.examples

import t2.api.{Fn1, FnRegistration, GenericRecord}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object UpdateRows {

  case class SAB(a: Int, b: Int, c: Int)
  case class TAB(t: SAB)

  def oldApi(spark: SparkSession): Unit = {
    //spark.udf.register("ur", (r: Row) => Row(r.getAs[Int](0) + 1, r.get(1), r.get(2)))
    spark.udf.register("ur", (r: SAB) => r.copy(a = r.a + 1))
  }

  def newApi(spark: SparkSession): Unit = {
    val ur = new Fn1[GenericRecord, GenericRecord] {

      override def name(): String = "ur"

      override def returnType(inputs: java.util.List[DataType]): DataType = inputs.get(0)

      override def call(t1: GenericRecord): GenericRecord = {
        t1.put(0, t1.get[Int](0) + 1)
        t1
      }
    }

    FnRegistration.register(spark, ur)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq.fill(100)(TAB(SAB(1, 2, 3))).toDF("r")
    df.show(1)
    df.createOrReplaceTempView("v")
    oldApi(spark)
    benchmark(spark, 1000)

    newApi(spark)
    benchmark(spark, 1000)

    oldApi(spark)
    benchmark(spark, 1000)

    newApi(spark)
    benchmark(spark, 1000)

    oldApi(spark)
    benchmark(spark, 1000)

    newApi(spark)
    benchmark(spark, 1000)

    oldApi(spark)
    benchmark(spark, 1000)

    newApi(spark)
    benchmark(spark, 1000)
  }

  def benchmark(spark: SparkSession, count: Int): Unit = {
    var i = 0
    val s = System.currentTimeMillis()
    while (i < count) {
      spark.sql("select ur(r) from v").collect()
      i += 1
    }
    val e = System.currentTimeMillis()

    println(s"${e - s} ms")
  }
}
