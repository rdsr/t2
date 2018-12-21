package examples;

import nimble.api.GenericRecord;
import nimble.api.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

public class StructCreateByNameFn implements UDF4<String, Object, String, Object, GenericRecord> {
  @Override
  public DataType dataType(List<DataType> args) {
    return DataTypes.IntegerType;
  }

  @Override
  public GenericRecord apply(String v1, Object v2, String v3, Object v4) {
    GenericRecord r = null;
    r.put(v1, v2);
    r.put(v3, v4);
    return r;
  }
}
