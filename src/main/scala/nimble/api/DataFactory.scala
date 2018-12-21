package nimble.api
import java.util

import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.types._

object DataFactory {
  def emptyList[T](elemType: DataType): util.List[T] =
    new SparkList[T](elemType)

  def emptyMap[K,V](keyType: DataType, valueType: DataType): util.Map[K, V] =
    new SparkMap[K,V](keyType, valueType)

  def emptyRecord(structType: StructType): GenericRecord =
    new SparkRecord(structType)
}
