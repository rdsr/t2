package examples;

import nimble.api.GenericRecord;
import nimble.api.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import scala.Function1;
import scala.Tuple4;

import java.util.List;

public class StructCreateByNameFn implements UDF4<String, Object, String, Object, GenericRecord> {
  @Override
  public boolean nullable() {
    return super.nullable();
  }

  @Override
  public boolean foldable() {
    return super.foldable();
  }

  @Override
  public boolean deterministic() {
    return super.deterministic();
  }

  @Override
  public DataType dataType(List<DataType> args) {
    return DataTypes.IntegerType
  }


  @Override
  public GenericRecord apply(String v1, Object v2, String v3, Object v4) {
    GenericRecord r = null;
    r.put(v1, v2);
    r.put(v3, v4);
    return r;
  }
}
