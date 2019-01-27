package t2.examples

import examples.{Add, MapValuesFunction}
import t2.api.FnRegistration
import org.apache.spark.sql.SparkSession

object SparkTest {
  def benchmark(count: Long)(f: => Unit): Unit = {
    var i = 0
    while (i < count) {
      val s = System.currentTimeMillis()
      f
      val e = System.currentTimeMillis()
      println(s"${e - s} ms")
      i += 1
    }
  }

  case class MyRow(a: Int, b: Int, c: Int)

  def testMapValues(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq.fill(10)(Map("a" -> MyRow(1, 2, 3))).toDF("c")
    FnRegistration.register(spark, new MapValuesFunction[Any, Any]())
    df.createOrReplaceTempView("v")
    spark.sql("select map_values(c) from v").show()
  }

  def testAdd(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = Seq.fill(10000000)(10).toDF("c")

    FnRegistration.register(spark, new Add())
    df.createOrReplaceTempView("v")
    benchmark(5) {
      spark.sql("select add(c,c) from v").collect()
    }
  }

  def sparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()
  }

  def testArrayFill(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    FnRegistration.register(spark, new ArrayFillFn[Int])
    benchmark(5) {
      spark.sql("select array_fill(1, 200000000L)").first
    }
  }

  def main(args: Array[String]): Unit = {
    sparkSession()
    //testAdd()
    testArrayFill()
    //testMapValues(sparkSession())
  }
}
