package t2.api;

import org.apache.spark.sql.types.DataType;

import java.io.Serializable;
import java.util.List;

/**
 * Java classes cannot directly extend Scala trait with default methods
 * See: https://alvinalexander.com/scala/how-to-wrap-scala-traits-used-accessed-java-classes-methods
 */
public interface Fn extends Serializable {
  String name();

  default String description() {
    return "Placeholder description for " + name();
  }

  DataType returnType(List<DataType> inputs);

  default boolean nullable() {
    return true;
  }

  default boolean deterministic() {
    return true;
  }
}
