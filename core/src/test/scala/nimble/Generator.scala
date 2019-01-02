package nimble

import java.sql.Timestamp
import java.util.Date

import nimble.api.SparkDataTypes
import nimble.internal.api.SparkDataTypes
import org.apache.spark.sql.types._

object Generator {
  private val _rand = scala.util.Random
  private val _maxSize = 10

  def genList(listType: ArrayType): Any = {
    val r = SparkDataTypes.emptyList[Any](listType)
    val size = _rand.nextInt(_maxSize)
    (0 to size) foreach {
      _ => r.add(genData(listType))
    }
    r
  }

  def genMap(mapType: MapType): Any = {
    val r = SparkDataTypes.emptyMap[Any, Any](mapType)
    val size = _rand.nextInt(_maxSize)
    val keyType = mapType.keyType
    val valueType = mapType.valueType
    (0 to size) foreach {
      _ => r.put(genData(keyType), genData(valueType))
    }
    r
  }

  def genRecord(recordType: StructType): Any = {
    val r = SparkDataTypes.emptyRecord(recordType)
    recordType.fields.zipWithIndex.foreach { case (f, i) => r.put(i, genData(f.dataType)) }
    r
  }

  def genData(dataType: DataType): Any = {
    dataType match {
      case listType: ArrayType => genList(listType)
      case mapType: MapType => genMap(mapType)
      case recordType: StructType => genRecord(recordType)
      case StringType => _rand.nextString(_maxSize)
      case DateType => new java.sql.Date(new Date().getTime)
      case TimestampType => new Timestamp(new Date().getTime)
      case dt: DecimalType => java.math.BigDecimal.ONE
      case BooleanType => _rand.nextBoolean
      case ByteType => {
        val a = new Array[Byte](1)
        _rand.nextBytes(a)
        a(0)
      }
      case ShortType => _rand.nextInt.toShort
      case IntegerType => _rand.nextInt
      case LongType => _rand.nextLong
      case FloatType => _rand.nextFloat
      case DoubleType => _rand.nextDouble
      case dt => throw new IllegalStateException(s"Unknown datatype $dt")
    }
  }
}
