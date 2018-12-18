package nimble.internal.data

import java.util
import java.util.Map

import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.DataType

class SparkMap[K, V](private val _mapType: DataType, private val _data: MapData)
  extends util.AbstractMap[K, V]
    with SparkData {

  override def put(key: K, value: V): V = ???

  override def get(key: Any): V = ???

  override def entrySet(): util.Set[Map.Entry[K, V]] = ???

  override def underlyingData: Any = ???
}
