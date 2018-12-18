package nimble.internal.data

import java.util

import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.ArrayType

class SparkList[T](private val _listType: ArrayType, private val _data: ArrayData)
  extends util.AbstractList[T] with SparkData {

  override def get(index: Int): Any = ???
  override def size(): Int = ???
  override def add(e: Any): Boolean = {
  }
  override def set(index: Int, element: T): T = {
  }
  override def underlyingData: Any = {
    ArrayData.toArrayData()
  }
}
