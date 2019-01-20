package nimble.api
import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.types._

object DataTypeFactory {
  def list[T](listType: ArrayType): GenericList[T] =
    new SparkList[T](listType)

  def map[K,V](mapType: MapType): GenericMap[K, V] =
    new SparkMap[K,V](mapType)

  def record(recordType: StructType): GenericRecord =
    new SparkRecord(recordType)
}
