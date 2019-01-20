package nimple.examples

import java.util

import nimble.api.{Fn1, FnRegistration, GenericRecord}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

object SelectRow {

  case class SAB(a: Int, b: Int, c: Int)

  val s = StructType(
    StructField("a", IntegerType, true) ::
      StructField("a", IntegerType, true) ::
      StructField("a", IntegerType, true) :: Nil
  )

  val udf = new UDF1[Map[String, Row], Row] {
    override def call(r: Map[String, Row]): Row = {
      val v = r("a")
      val i = (v.getAs[Int](0) + 1).asInstanceOf[AnyRef]
      val j = v.get(1).asInstanceOf[AnyRef]
      val k = v.get(2).asInstanceOf[AnyRef]
      RowFactory.create(i, j, k)
    }
  }

  def oldApi(spark: SparkSession): Unit = {
    spark.udf.register("selectRow", udf, s)
  }

  def newApi(spark: SparkSession): Unit = {
    val selectRow = new Fn1[java.util.Map[String, GenericRecord], GenericRecord] {
      override def call(t1: util.Map[String, GenericRecord]): GenericRecord = {
        val r = t1.get("a")
        r.put(0, r.get[Int](0) + 1)
        r
      }

      override def name(): String = "selectRow"

      override def returnType(inputs: java.util.List[DataType]): DataType = {
        inputs.get(0).asInstanceOf[MapType].valueType
      }
    }
    FnRegistration.register(spark, selectRow)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq.fill(1000000)(Map("a" -> SAB(1, 2, 3))).toDF("c")
    df.createOrReplaceTempView("v")
    oldApi(spark)
    benchmark(spark, 5)


    //newApi(spark)
    //benchmark(spark, 5)

    /*
    oldApi(spark)
    benchmark(spark, 1)

    newApi(spark)
    benchmark(spark, 1)

    oldApi(spark)
    benchmark(spark, 1000)

    newApi(spark)
    benchmark(spark, 1000)

    oldApi(spark)
    benchmark(spark, 1000)

    newApi(spark)
    benchmark(spark, 1000)
    */
  }

  def benchmark(spark: SparkSession, count: Int): Unit = {
    var i = 0

    while (i < count) {
      val s = System.currentTimeMillis()
      spark.sql("select selectRow(c) from v").collect()
      i += 1
      val e = System.currentTimeMillis()
      println(s"${e - s} ms")
    }
  }
}
