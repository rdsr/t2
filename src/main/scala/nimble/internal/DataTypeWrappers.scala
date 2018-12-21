package nimble.internal

import nimble.internal.api.SparkData
import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._

object DataTypeWrappers {
  def wrapFn[T, R](dataType: DataType): Any => Any = {
    dataType match {
      case arrayType: ArrayType => {
        e => new SparkList(arrayType, e.asInstanceOf[ArrayData])
      }
      case mapType: MapType => {
        e => new SparkMap(mapType.keyType, mapType.valueType, e.asInstanceOf[MapData])
      }
      case structType: StructType => {
        e => new SparkRecord(structType, e.asInstanceOf[InternalRow])
      }
      // Use Catalyst converters for primitives
      case _ => CatalystTypeConverters.createToScalaConverter(dataType)
    }
  }

  def unwrapFn(dataType: DataType): Any => Any = {
    val unwrap = { e: Any => e.asInstanceOf[SparkData].underlyingData }
    dataType match {
      case _: ArrayType => unwrap
      case _: MapType => unwrap
      case _: StructType => unwrap
      case _ => CatalystTypeConverters.createToCatalystConverter(dataType)
    }
  }
}
