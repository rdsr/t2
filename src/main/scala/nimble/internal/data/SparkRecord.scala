package nimble.internal.data

import nimble.api.GenericRecord
import nimble.internal.Utils
import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

case class SparkRecord(private val _structType: StructType, private val _data: InternalRow)
  extends GenericRecord
    with SparkData {

  private var _mutableBuffer = if (_data == null) createMutableStruct() else null

  private val _wrapFns = _structType.fields.map(f => Utils.wrapFn(f.dataType))
  private val _unWrapFns = _structType.fields.map(f => Utils.unWrapFn(f.dataType))
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
        _unWrapFns(i)(_mutableBuffer(i))
    r.asInstanceOf[V]
  }

  override def underlyingData: Any = {
    if (_mutableBuffer == null) _data
    else InternalRow.fromSeq(_mutableBuffer)
  }

  override def schema: DataType = _structType

  private def createMutableStruct() = {
    if (_data != null) ArrayBuffer[Any](_data.toSeq(_structType))
    else new ArrayBuffer[Any](_structType.size)
  }

  private def updateFn(dataType: DataType): (Int, Any) => Unit = {
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
          _mutableBuffer = createMutableStruct()
        _mutableBuffer(i) = _wrapFns(i)(v)
      }
    }
  }
}
