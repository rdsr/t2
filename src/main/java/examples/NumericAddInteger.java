package examples;

import nimble.api.UDF2;
import org.apache.spark.sql.types.DataType;

import java.util.List;

public class NumericAddInteger implements UDF2<Integer, Integer, Integer> {
  @Override
  public boolean nullable() {
    return false;
  }

  @Override
  public boolean foldable() {
    return false;
  }

  @Override
  public boolean deterministic() {
    return true;
  }

  @Override
  public DataType dataType(List<DataType> args) {
    return null;
  }

  @Override
  public Integer apply(Integer v1, Integer v2) {
    return v1 + v2;
  }
}
