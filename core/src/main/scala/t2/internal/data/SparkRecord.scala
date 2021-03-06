package t2.internal.data

import t2.api.GenericRecord
import t2.internal.DataTypeWrappers
import t2.internal.api.SparkDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

class SparkRecord(private val _recordType: StructType,
                  private val _data: InternalRow = null)
  extends GenericRecord with SparkDataType {

  // _data and _mutableBuffer cannot both be null
  // if _mutableBuffer is non-null, that is the source of truth
  private var _mutableBuffer = if (_data == null) createMutableStruct() else null

  private val _wrapFns = _recordType.fields.map(f => DataTypeWrappers.wrapFn(f.dataType))
  private val _unwrapFns = _recordType.fields.map(f => DataTypeWrappers.unwrapFn(f.dataType))
  private val _updateFns = _recordType.fields.map(f => updateFn(f.dataType))

  /** Set the value of a field given its name. */
  override def put[V](key: String, v: V): Unit = {
    val i = _recordType.fieldIndex(key)
    put(i, v)
  }

  /** Return the value of a field given its name. */
  override def get[V](key: String): V = {
    val i = _recordType.fieldIndex(key)
    get(i)
  }

  override def put[V](i: Int, v: V): Unit = {
    _updateFns(i)(i, v)
  }

  override def get[V](i: Int): V = {
    val f = _recordType.fields(i)
    val r =
      if (_mutableBuffer == null)
        _data.get(i, f.dataType)
      else
        _wrapFns(i)(_mutableBuffer(i))
    r.asInstanceOf[V]
  }

  override def underlyingType: InternalRow = {
    if (_mutableBuffer == null) _data
    else InternalRow.fromSeq(_mutableBuffer)
  }

  override def schema: DataType = _recordType

  private def createMutableStruct() = {
    if (_data != null) ArrayBuffer[Any](_data.toSeq(_recordType))
    else ArrayBuffer.fill[Any](_recordType.size)(null)
  }

  private def updateFn(dataType: DataType): (Int, Any) => Unit = {
    val bufferUpdate = (i: Int, v: Any) => {
      if (_mutableBuffer == null) {
        _mutableBuffer = createMutableStruct()
      }
      _mutableBuffer(i) = _unwrapFns(i)(v)
    }
    if (_data == null) {
      bufferUpdate
    }
    else {
      dataType match {
        case BooleanType => (i, v) => _data.setBoolean(i, v.asInstanceOf[Boolean])
        case ByteType => (i, v) => _data.setByte(i, v.asInstanceOf[Byte])
        case ShortType => (i, v) => _data.setShort(i, v.asInstanceOf[Short])
        case IntegerType => (i, v) => _data.setInt(i, v.asInstanceOf[Integer])
        case LongType => (i, v) => _data.setLong(i, v.asInstanceOf[Long])
        case FloatType => (i, v) => _data.setFloat(i, v.asInstanceOf[Float])
        case DoubleType => (i, v) => _data.setDouble(i, v.asInstanceOf[Double])
        case _ => bufferUpdate
      }
    }
  }
}
