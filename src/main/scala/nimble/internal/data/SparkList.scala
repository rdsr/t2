package nimble.internal.data


import nimble.internal.DataTypeWrappers
import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

class SparkList[T](private val listType: ArrayType, private val _data: ArrayData = null)
  extends nimble.api.JList[T] with SparkData {

  private val _elementType = listType.elementType

  private var _mutableBuffer: ArrayBuffer[Any] = if (_data == null) createMutableArray() else null

  private val _wrap: Any => Any = DataTypeWrappers.wrapFn(_elementType)
  private val _unwrap: Any => Any = DataTypeWrappers.unwrapFn(_elementType)
  private val _update: (Int, Any) => Any = updateFn(_elementType)

  override def get(index: Int): T = {
    val e =
      if (_mutableBuffer == null) _data.get(index, _elementType)
      else _mutableBuffer(index)
    _wrap(e).asInstanceOf[T]
  }

  override def size(): Int = {
    if (_mutableBuffer == null) _data.numElements()
    else _mutableBuffer.size
  }

  override def add(e: T): Boolean = {
    if (_mutableBuffer == null)
      _mutableBuffer = createMutableArray()
    _mutableBuffer.append(_unwrap(e))
    true
  }

  override def add(index: Int, e: T): Unit = {
    if (_mutableBuffer == null)
      _mutableBuffer = createMutableArray()
    _mutableBuffer.insert(index, _unwrap(e))
  }

  override def set(index: Int, e: T): T = {
    _update(index, e)
    null.asInstanceOf[T]
  }

  override def remove(index: Int): T = {
    if (_mutableBuffer == null)
      _mutableBuffer = createMutableArray()
    val e = _mutableBuffer.remove(index)
    _wrap(e).asInstanceOf[T]
  }

  override def underlyingData: ArrayData = {
    if (_mutableBuffer == null) _data
    else ArrayData.toArrayData(_mutableBuffer)
  }

  override def schema: DataType = DataTypes.createArrayType(_elementType)

  private def createMutableArray(): ArrayBuffer[Any] = {
    if (_data == null) {
      new ArrayBuffer[Any]()
    } else {
      val arrayBuffer = new ArrayBuffer[Any](_data.numElements())
      _data.foreach(_elementType, (_, e) => arrayBuffer.append(e))
      arrayBuffer
    }
  }

  private def updateFn(dataType: DataType): (Int, Any) => Any = {
    dataType match {
      case BooleanType
           | ByteType
           | ShortType
           | IntegerType
           | LongType
           | FloatType
           | DoubleType
      => (i, v) => _data.update(i, v)
      case _ => (i, v) => {
        if (_mutableBuffer == null)
          _mutableBuffer = createMutableArray()
        _mutableBuffer(i) = _wrap(v)
      }
    }
  }
}
