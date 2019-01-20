package nimble.examples

import java.util

import nimble.api.Fn2
import org.apache.spark.sql.types.{DataType, DataTypes}

class AddFn extends Fn2[Int, Int, Int] {
  override def name(): String = "scala_add"

  override def returnType(inputs: util.List[DataType]): DataType = DataTypes.IntegerType

  override def call(t1: Int, t2: Int): Int = t1 + t2
}
