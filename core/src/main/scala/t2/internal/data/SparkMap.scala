package t2.internal.data

import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry

import t2.api.GenericMap
import t2.internal.DataTypeWrappers
import t2.internal.api.SparkDataType
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.types.{DataType, MapType}

import scala.collection.mutable.Map

class SparkMap[K, V](private val _mapType: MapType,
                     private val _data: MapData = null) extends GenericMap[K, V] with SparkDataType {

  private val _keyType = _mapType.keyType
  private val _valueType = _mapType.valueType

  private var _mutableMap = if (_data == null) createMutableMap() else null

  private val _wrapKey = DataTypeWrappers.wrapFn(_keyType)
  private val _wrapVal = DataTypeWrappers.wrapFn(_valueType)
  private val _unwrapKey = DataTypeWrappers.unwrapFn(_keyType)
  private val _unwrapVal = DataTypeWrappers.unwrapFn(_valueType)


  override def put(key: K, value: V): V = {
    if (_mutableMap == null) _mutableMap = createMutableMap

    val previousValue = _mutableMap.put(_unwrapKey(key), _unwrapVal(value))
    previousValue
      .map(_unwrapVal)
      .orNull
      .asInstanceOf[V]
  }

  override def get(key: Any): V = {
    if (_mutableMap == null) _mutableMap = createMutableMap

    _mutableMap.get(_unwrapKey(key))
      .map(_wrapVal)
      .orNull
      .asInstanceOf[V]
  }

  override def entrySet(): util.Set[Entry[K, V]] = {
    val r = new util.HashSet[Entry[K, V]]()
    val fn: (Any, Any) => Unit = (k, v) => {
      r.add(
        new SimpleEntry(
          _wrapKey(k).asInstanceOf[K],
          _wrapVal(v).asInstanceOf[V]
        ))
    }
    if (_mutableMap == null)
      _data.foreach(_keyType, _valueType, fn)
    else
      _mutableMap.foreach {
        case (k, v) => fn(k, v)
      }
    r
  }

  override def schema: DataType = _mapType

  override def underlyingType: MapData = {
    if (_mutableMap == null) _data
    else ArrayBasedMapData(_mutableMap)
  }

  private def createMutableMap(): Map[Any, Any] = {
    val m = Map.empty[Any, Any]
    if (_data != null) {
      _data.foreach(_keyType, _valueType, (k, v) => m.put(k, v))
    }
    m
  }
}
