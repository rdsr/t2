package nimble.internal.data

import java.util
import java.util.AbstractMap.SimpleEntry

import nimble.internal.Utils
import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.types.MapType

import scala.collection.mutable.Map

class SparkMap[K, V](private val _mapType: MapType, private val _data: MapData)
  extends util.AbstractMap[K, V]
    with SparkData {

  private val _keyType = _mapType.keyType
  private val _valueType = _mapType.valueType
  private var _mutableMap: Map[Any, Any] = if (_data == null) createMutableMap() else null

  private val _wrapKeyFn = Utils.wrapFn(_keyType)
  private val _wrapValFn = Utils.wrapFn(_valueType)
  private val _unWrapKeyFn = Utils.unWrapFn(_keyType)
  private val _unWrapValFn = Utils.unWrapFn(_valueType)


  override def put(key: K, value: V): V = {
    if (_mutableMap == null)
      _mutableMap = createMutableMap()
    val previousValue = _mutableMap.put(_unWrapKeyFn(key), _unWrapValFn(value))
    previousValue
      .map(_unWrapValFn)
      .orNull
      .asInstanceOf[V]
  }

  override def get(key: Any): V = {
    if (_mutableMap == null)
      _mutableMap = createMutableMap()
    _mutableMap.get(_wrapKeyFn(key))
      .map(_wrapValFn)
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

  override def underlyingData: Any =
    ArrayBasedMapData(_mutableMap, identity, identity)

  private def createMutableMap(): Map[Any, Any] = {
    val m = Map.empty[Any, Any]
    if (_data != null) {
      _data.foreach(_keyType, _valueType, (k, v) => m.put(k, v))
    }
    m
  }
}
