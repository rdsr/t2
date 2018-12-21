package nimble.api
import java.util

import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.types._

object DataFactory {
  def emptyList[T](listType: ArrayType): util.List[T] =
    new SparkList[T](listType)

  def emptyMap[K,V](mapType: MapType): util.Map[K, V] =
    new SparkMap[K,V](mapType)

  def emptyRecord(structType: StructType): GenericRecord =
    new SparkRecord(structType)
}
