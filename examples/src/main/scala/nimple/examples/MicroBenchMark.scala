package nimple.examples

import java.util

import nimble.api.Fn1
import nimble.internal.SparkUDF
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class Floor(child: Expression) extends UnaryExpression with Predicate with CodegenFallback {
  override def foldable = child.foldable

  override def nullable = child.nullable

  override def toString = s"Floor $child"

  override def eval(input: InternalRow): Any = {
    child.eval(input) match {
      case null => null
      case ts: Int => ts - ts % 300
    }
  }
}

object T {
  def benchmark(count: Long, expr: Expression): Unit = {
    var i = 0
    val row = new GenericInternalRow(Array[Any](123, 21, 42))
    val s = System.currentTimeMillis()
    while (i < count) {
      expr.eval(row)
      i += 1
    }
    val e = System.currentTimeMillis()

    println(s"${expr.getClass.getSimpleName}  -- ${e - s} ms")
  }

  def main(args: Array[String]) {
    def func(ts: Int) = ts - ts % 300

    class X extends Fn1[Int, Int] {
      override def call(t1: Int): Int = t1 - t1 % 300
      override def name(): String = "XXXX"
      override def returnType(inputs: util.List[DataType]): DataType = inputs.get(0)
    }

    val udf0 = ScalaUDF(func _, IntegerType, BoundReference(0, IntegerType, true) :: Nil)
    val udf1 = Floor(BoundReference(0, IntegerType, true))
    val udf2 = SparkUDF(new X(), BoundReference(0, IntegerType, true) :: Nil)


    benchmark(10000000, udf0)
    benchmark(10000000, udf0)
    benchmark(10000000, udf0)
    benchmark(10000000, udf0)
    benchmark(10000000, udf0)

    benchmark(10000000, udf2)
    benchmark(10000000, udf2)
    benchmark(10000000, udf2)
    benchmark(10000000, udf2)
    benchmark(10000000, udf2)
  }
}