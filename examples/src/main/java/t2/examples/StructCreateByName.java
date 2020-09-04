package t2.examples;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import t2.api.Fn4;
import t2.api.GenericRecord;

public class StructCreateByName<V1, V2> implements Fn4<String, V1, String, V2, GenericRecord> {
  private StructType _returnType;

  public String name() {
    return "create_struct_by_name";
  }

  public GenericRecord call(String k1, V1 v1, String k2, V2 v2) {

    final GenericRecord r = ...
    final List<Integer> f = r.get("A");

    r.put(k1, v1);
    r.put(k2, v2);
    return r;
  }

  public DataType returnType(List<DataType> inputs) {
    _returnType = DataTypes.createStructType(
        ImmutableList.of(
            DataTypes.createStructField("f1", inputs.get(0), true),
            DataTypes.createStructField("f2", inputs.get(1), true)));
    return _returnType;
  }
}
