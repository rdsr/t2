package nimble.internal.data

import java.util
import java.util.AbstractMap.SimpleEntry

import nimble.api.JMap
import nimble.internal.DataTypeWrappers
import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.types.{DataType, MapType}

import scala.collection.mutable.Map

class SparkMap[K, V](private val _mapType: MapType,
                     private val _data: MapData = null) extends JMap[K, V] with SparkData {

  private val _keyType =  _mapType.keyType
  private val _valueType = _mapType.valueType

  private var _mutableMap: Map[Any, Any] = if (_data == null) createMutableMap() else null

  private val _wrapKey = DataTypeWrappers.wrapFn(_keyType)
  private val _wrapVal = DataTypeWrappers.wrapFn(_valueType)
  private val _unwrapKey = DataTypeWrappers.unwrapFn(_keyType)
  private val _unwrapVal = DataTypeWrappers.unwrapFn(_valueType)


  override def put(key: K, value: V): V = {
    if (_mutableMap == null)
      _mutableMap = createMutableMap()
    val previousValue = _mutableMap.put(_unwrapKey(key), _unwrapVal(value))
    previousValue
      .map(_unwrapVal)
      .orNull
      .asInstanceOf[V]
  }

  override def get(key: Any): V = {
    if (_mutableMap == null)
      _mutableMap = createMutableMap()
    _mutableMap.get(_wrapKey(key))
      .map(_wrapVal)
      .orNull
      .asInstanceOf[V]
  }

  override def entrySet(): util.Set[util.Map.Entry[K, V]] = {
    val r = new util.HashSet[util.Map.Entry[K, V]]()
    _mutableMap.foreach(
      e => r.add(
        new SimpleEntry(
          e._1.asInstanceOf[K],
          e._2.asInstanceOf[V])))
    r
  }

  override def schema: DataType = _mapType

  override def underlyingData: MapData = ArrayBasedMapData(_mutableMap)

  private def createMutableMap(): Map[Any, Any] = {
    val m = Map.empty[Any, Any]
    if (_data != null) {
      _data.foreach(_keyType, _valueType, (k, v) => m.put(k, v))
    }
    m
  }
}
