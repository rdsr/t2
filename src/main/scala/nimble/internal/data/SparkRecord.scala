package nimble.internal.data

import nimble.api.GenericRecord
import nimble.internal.DataTypeWrappers
import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

case class SparkRecord(private val _structType: StructType, private val _data: InternalRow = null)
  extends GenericRecord
    with SparkData {

  private var _mutableBuffer = if (_data == null) createMutableStruct() else null

  private val _wrapFns = _structType.fields.map(f => DataTypeWrappers.wrapFn(f.dataType))
  private val _unwrapFns = _structType.fields.map(f => DataTypeWrappers.unwrapFn(f.dataType))
  private val _updateFns = _structType.fields.map(f => updateFn(f.dataType))

  /** Set the value of a field given its name. */
  override def put[V](key: String, v: V): Unit = {
    val i = _structType.fieldIndex(key)
    put(i, v)
  }

  /** Return the value of a field given its name. */
  override def get[V](key: String): V = {
    val i = _structType.fieldIndex(key)
    get(i)
  }

  override def put[V](i: Int, v: V): Unit = {
    _updateFns(i)(i, v)
  }

  override def get[V](i: Int): V = {
    val f = _structType.fields(i)
    val r =
      if (_mutableBuffer == null)
        _data.get(i, f.dataType)
      else
        _wrapFns(i)(_mutableBuffer(i))
    r.asInstanceOf[V]
  }

  override def underlyingData: Any = {
    if (_mutableBuffer == null) _data
    else InternalRow.fromSeq(_mutableBuffer)
  }

  private def createMutableStruct() = {
    if (_data != null) ArrayBuffer[Any](_data.toSeq(_structType))
    else ArrayBuffer.fill[Any](_structType.size)(null)
  }

  private def updateFn(dataType: DataType): (Int, Any) => Unit = {
    val defaultUpdate = (i: Int, v: Any) => {
      if (_mutableBuffer == null)
        _mutableBuffer = createMutableStruct()
      _mutableBuffer(i) = _unwrapFns(i)(v)
    }
    if (_data == null)
      defaultUpdate
    else {
      dataType match {
        case BooleanType
             | ByteType
             | ShortType
             | IntegerType
             | LongType
             | FloatType
             | DoubleType
        => (i, v) => _data.update(i, v)
        case _ => defaultUpdate
      }
    }
  }
}
