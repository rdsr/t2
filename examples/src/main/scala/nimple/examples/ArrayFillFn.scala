package nimple.examples

import java.util

import nimble.api.{Fn2, FnRegistration, SparkDataTypes}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataType}


class ArrayFillFn extends Fn2[Any, Long, util.List[Any]] {
  private var _outputType: ArrayType = _

  override def call(elem: Any, size: Long): util.List[Any] = {
    var i = 0
    val r = SparkDataTypes.emptyList[Any](_outputType)
    while (i < size) {
      r.add(elem)
      i += 1
    }
    r
  }

  override def name(): String = "array_fill"

  override def returnType(inputs: util.List[DataType]): DataType = {
    _outputType = ArrayType(inputs.get(0))
    _outputType
  }
}


object x {
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

  private val sparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("x-udfs")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    FnRegistration.register(sparkSession, new ArrayFillFn)

    import sparkSession.sql
    benchmark(5) {
      sql("SELECT array_fill(1, 5000000L)").show()
    }
  }

}