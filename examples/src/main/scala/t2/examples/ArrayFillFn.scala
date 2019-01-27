package t2.examples

import java.util

import t2.api.{DataTypeFactory, Fn2}
import org.apache.spark.sql.types.{ArrayType, DataType}


class ArrayFillFn[T] extends Fn2[T, Long, util.List[T]] {
  private var _outputType: ArrayType = _

  override def call(elem: T, size: Long): util.List[T] = {
    var i = 0
    val r = DataTypeFactory.list[T](_outputType)
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