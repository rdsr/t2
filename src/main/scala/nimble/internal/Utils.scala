package nimble.internal

import nimble.internal.data.{SparkList, SparkMap, SparkRecord}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object Utils {
  def wrapFn[T,R](dataType: DataType): Any => Any = {
    if (isPrimitive(dataType))
      identity[Any]
    else
      dataType match {
        case arrayType: ArrayType => { e => new SparkList(arrayType, e.asInstanceOf[ArrayData]) }
        case mapType: MapType => { e => new SparkMap(mapType, mapType, e) }
        case structType: StructType => { e => new SparkRecord(structType, e) }
        case stringType: StringType => StringConverter(stringType).toScala
        case DateType => DateConverter
        case TimestampType => new TimestampConverter
        case dt: DecimalType => DecimalConverter(dt).toScala
        case BooleanType => BooleanConverter
        case _ => throw new UnsupportedOperationException(s"$dataType not supported")
      }
  }

  def unwrap(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType))
      identity
    else {
      case _: ArrayType | StructType | MapType => { e: Any => e.asInstanceOf[PlatformData].underlyingData }
      case stringType: StringType => StringConverter(stringType).toCatalyst
      case DateType => DateConverter
      case TimestampType => new TimestampConverter
      case decimalType: DecimalType => DecimalConverter(decimalType).toScala
      case BooleanType => BooleanConverter
      case _ => throw new UnsupportedOperationException(s"$dataType not supported")
    }
  }

  private case class DecimalConverter(dataType: DecimalType) extends Converter[Any, Any] {
    override def toCatalyst(scalaValue: Any): Any = {
      val decimal = Decimal(scalaValue.asInstanceOf[JavaBigDecimal])
      decimal.toPrecision(dataType.precision, dataType.scale)
    }

    override def toScala(catalystValue: Any): Any = {
      if (catalystValue == null) null
      else catalystValue.asInstanceOf[Decimal].toJavaBigDecimal
    }
  }

  private case class StringConverter(stringType: StringType) extends Converter[Any, Any] {
    override def toCatalyst(v: Any): Any = UTF8String.fromString(v.toString)

    override def toScala(v: Any): Any = if (v == null) null else v.toString
  }

  def unWrapFn(dataType: DataType): Any => Any {

  }

  def isPrimitive(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case _ => false
    }
  }
}
