package nimble.api
import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.types._

object SparkDataTypes {
  def emptyList[T](listType: ArrayType): GenericList[T] =
    new SparkList[T](listType)

  def emptyMap[K,V](mapType: MapType): GenericMap[K, V] =
    new SparkMap[K,V](mapType)

  def emptyRecord(recordType: StructType): GenericRecord =
    new SparkRecord(recordType)
}
