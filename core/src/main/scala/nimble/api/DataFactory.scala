package nimble.api
import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.types._

object DataFactory {
  def emptyList[T](listType: ArrayType): JList[T] =
    new SparkList[T](listType)

  def emptyMap[K,V](mapType: MapType): JMap[K, V] =
    new SparkMap[K,V](mapType)

  def emptyRecord(recordType: StructType): GenericRecord =
    new SparkRecord(recordType)
}
