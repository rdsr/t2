package examples;

import nimble.api.Fn2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

public class NumericAddInteger implements Fn2<Integer, Integer, Integer> {
  @Override
  public String name() {
    return "Numeric Add";
  }

  @Override
  public Integer call(Integer v1, Integer v2) {
    return v1 + v2;
  }

  @Override
  public DataType returnType(List<DataType> ignore) {
    return DataTypes.IntegerType;
  }
}

