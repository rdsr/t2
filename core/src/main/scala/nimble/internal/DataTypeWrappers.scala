package nimble.internal

import nimble.internal.api.SparkDataType
import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._

object DataTypeWrappers {
  def wrapFn(dataType: DataType): Any => Any = {
    dataType match {
      case listType: ArrayType => {
        e => new SparkList(listType, e.asInstanceOf[ArrayData])
      }
      case mapType: MapType => { e =>
        new SparkMap(mapType, e.asInstanceOf[MapData])
      }
      case recordType: StructType => {
        e => new SparkRecord(recordType, e.asInstanceOf[InternalRow])
      }
      // Use Catalyst converters for primitives
      case _ => CatalystTypeConverters.createToScalaConverter(dataType)
    }
  }

  def unwrapFn(dataType: DataType): Any => Any = {
    dataType match {
      case
        _: ArrayType |
        _: MapType |
        _: StructType => {
        e: Any => e.asInstanceOf[SparkDataType].underlyingType
      }
      // Use Catalyst converters for primitives
      case _ => CatalystTypeConverters.createToCatalystConverter(dataType)
    }
  }
}
