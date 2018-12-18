import nimble.api.UDF;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;

import java.util.List;
import java.util.Map;

public class UDFExample<K,V> implements UDF1<Map<K, V>, V>, UDF {
  @Override
  public boolean nullable() {
  }

  @Override
  public boolean foldable() {
  }

  @Override
  public boolean deterministic() {
  }

  @Override
  public void dataType(List<DataType> args) {
  }

  @Override
  public V call(Map<K, V> kvMap) {
    return null;
  }
}