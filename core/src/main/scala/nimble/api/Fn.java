package nimble.api;

import org.apache.spark.sql.types.DataType;

import java.io.Serializable;
import java.util.List;

/**
 *  We cannot directly use Scala trait with default methods in Java classes
 *  See: https://alvinalexander.com/scala/how-to-wrap-scala-traits-used-accessed-java-classes-methods
 */
public interface Fn extends Serializable {
  String name();

  DataType returnType(List<DataType> inputs);

  default boolean nullable() {
    return true;
  }

  default boolean deterministic() {
    return true;
  }
}
