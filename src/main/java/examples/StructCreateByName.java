/*
package examples;

import com.google.common.collect.ImmutableList;
import nimble.api.DataFactory;
import nimble.api.GenericRecord;
import nimble.api.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class StructCreateByName implements UDF4<String, Object, String, Object, GenericRecord> {
  private StructType _returnType;

  @Override
  public String name() {
    return "Create struct by Name";
  }

  @Override
  public DataType returnType(List<DataType> inputs) {
    _returnType = DataTypes.createStructType(
        ImmutableList.of(
            DataTypes.createStructField("f1", inputs.get(0), true),
            DataTypes.createStructField("f2", inputs.get(1), true)));
    return _returnType;
  }

  @Override
  public GenericRecord apply(String v1, Object v2, String v3, Object v4) {
    GenericRecord r = DataFactory.emptyRecord(_returnType);
    r.put(v1, v2);
    r.put(v3, v4);
    return r;
  }
}
*/