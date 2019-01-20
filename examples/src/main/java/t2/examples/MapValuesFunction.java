package t2.examples;

import t2.api.DataTypeFactory;
import t2.api.Fn1;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;

import java.util.List;
import java.util.Map;

public class MapValuesFunction<K, V> implements Fn1<Map<K, V>, List<V>> {
  private ArrayType _outputType;

  @Override
  public String name() {
    return "map_values";
  }

  @Override
  public List<V> call(Map<K, V> m) {
    final List<V> r = DataTypeFactory.list(_outputType);
    r.addAll(m.values());
    return r;
  }

  @Override
  public DataType returnType(List<DataType> inputs) {
    final MapType mapType = (MapType) inputs.get(0);
    _outputType = DataTypes.createArrayType(mapType.valueType());
    return _outputType;
  }
}
