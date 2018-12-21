package nimble.internal.data

import java.util

import nimble.internal.Utils
import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.ArrayType

import scala.collection.mutable.ArrayBuffer

class SparkList[T](private val _listType: ArrayType, private val _data: ArrayData = null)
  extends util.AbstractList[T] with SparkData {

  private val _elementType = _listType.elementType
  private var _mutableBuffer: ArrayBuffer[Any] = if (_data == null) createMutableArray() else null

  private val _wrapFn = Utils.wrapFn(_elementType)
  private val _unWrapFn = Utils.unWrapFn(_elementType)

  override def get(index: Int): Any = {
    val e = if (_mutableBuffer == null) _data(index)
    else _mutableBuffer(index)
    _wrapFn(e)
  }

  override def size(): Int = {
    if (_mutableBuffer == null) _data.numElements()
    else _mutableBuffer.size
  }

  override def add(e: T): Boolean = {
    if (_mutableBuffer == null)
      _mutableBuffer = createMutableArray()
    _mutableBuffer.append(_unWrapFn(e))
    true
  }

  override def add(index: Int, e: T): Unit = {
    if (_mutableBuffer == null)
      _mutableBuffer = createMutableArray()
    _mutableBuffer(index) = _unWrapFn(e)
  }

  override def set(index: Int, e: T): T = {
    if (_mutableBuffer == null)
      _mutableBuffer = createMutableArray()
    val pe = _mutableBuffer(index)
    _mutableBuffer(index) = _unWrapFn(e)
    _wrapFn(pe).asInstanceOf[T]
  }

  override def underlyingData: Any = {
    if (_mutableBuffer == null) _data
    else ArrayData.toArrayData(_mutableBuffer)
  }

  private def createMutableArray(): ArrayBuffer[Any] = {
    var arrayBuffer: ArrayBuffer[Any] = null
    if (_data == null) {
      arrayBuffer = new ArrayBuffer[Any]()
    } else {
      arrayBuffer = new ArrayBuffer[Any](_data.numElements())
      _data.foreach(_elementType, (_, e) => arrayBuffer.append(e))
    }
    arrayBuffer
  }
}
