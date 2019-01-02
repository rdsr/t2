package nimble

import nimble.api.{GenericRecord, Schema}
import nimble.internal.api.SparkDataTypes
import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.testng.Assert

object Verifier {
  def verifyPrimitive(data: Any, dataType: DataType): Unit = {
    dataType match {
      case BooleanType => verifyPrimitive[Boolean](data)
      case ByteType => verifyPrimitive[Byte](data)
      case ShortType => verifyPrimitive[Short](data)
      case IntegerType => verifyPrimitive[Integer](data)
      case LongType => verifyPrimitive[Long](data)
      case FloatType => verifyPrimitive[Float](data)
      case DoubleType => verifyPrimitive[Double](data)
      case dt => throw new IllegalStateException(s"Cannot handle primitive datatype $dt")
    }
  }

  def verifyPrimitive[T](data: Any)(implicit tag: reflect.ClassTag[T]): Unit = {
    val c = tag.runtimeClass
    if (!c.isAssignableFrom(data.getClass)) {
      Assert.fail(s"$data does not belong to type $c")
    }
  }
}

object SparkVerifier {
  def verifyArray(data: SparkList[Any], listType: ArrayType): Unit = {
    val elementType = listType.elementType
    data.toArray.foreach(e => verify(e, elementType))
  }

  def verifyMap(data: SparkMap[Any, Any], mapType: MapType): Unit = {
    val keyType = mapType.keyType
    val valueType = mapType.valueType
    data.keySet().toArray.foreach(k => verify(k, keyType))
    data.values().toArray.foreach(v => verify(v, valueType))
  }

  def verifyRecord(data: GenericRecord, recordType: StructType): Unit = {
    recordType.foreach(f => verify(data.get(f.name), f.dataType))
  }

  def verify(data: Any, dataType: DataType): Unit = {
    dataType match {
      case listType: ArrayType => verifyArray(data.asInstanceOf[SparkList[Any]], listType)
      case mapType: MapType => verifyMap(data.asInstanceOf[SparkMap[Any, Any]], mapType)
      case recordType: StructType => verifyRecord(data.asInstanceOf[SparkRecord], recordType)
      case StringType => data.isInstanceOf[String]
      case DateType => data.isInstanceOf[Int]
      case TimestampType => data.isInstanceOf[Long]
      case _: DecimalType => data.isInstanceOf[Decimal]
      case _ => Verifier.verifyPrimitive(data, dataType)
    }
  }
}

object CatalystVerifier {
  def verifyArray(data: ArrayData, listType: ArrayType): Unit = {
    val elementType = listType.elementType
    data.foreach(elementType, (_, e) => verify(e, elementType))
  }

  def verifyMap(data: MapData, mapType: MapType): Unit = {
    val keyType = mapType.keyType
    val valueType = mapType.valueType
    data.foreach(keyType, valueType, (k, v) => {
      verify(k, keyType)
      verify(v, valueType)
    })
  }

  def verifyRecord(data: InternalRow, recordType: StructType): Unit = {
    recordType.fields.zipWithIndex.foreach { case (f, i) => verify(data.get(i, f.dataType), f.dataType)}
  }

  def verify(data: Any, dataType: DataType): Unit = {
    dataType match {
      case listType: ArrayType => verifyArray(data.asInstanceOf[ArrayData], listType)
      case mapType: MapType => verifyMap(data.asInstanceOf[MapData], mapType)
      case recordType: StructType => verifyRecord(data.asInstanceOf[InternalRow], recordType)
      case StringType => data.isInstanceOf[UTF8String]
      case DateType => data.isInstanceOf[Int]
      case TimestampType => data.isInstanceOf[Long]
      case _: DecimalType => data.isInstanceOf[Decimal]
      case _ => Verifier.verifyPrimitive(data, dataType)
    }
  }
}
