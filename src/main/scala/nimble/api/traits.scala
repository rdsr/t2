package nimble.api

import java.util

import org.apache.spark.sql.types.DataType

trait Schema {
  def schema: DataType
}

trait IndexedRecord extends Schema {
  def put[V](i: Int, v: V): Unit
  def get[V](i: Int): V
}

trait GenericRecord extends IndexedRecord {
  /** Set the value of a field given its name. */
  def put[V](key: String, v: V): Unit

  /** Return the value of a field given its name. */
  def get[V](key: String): V
}

trait UDF {
  def nullable: Boolean = true
  def foldable: Boolean = true
  def deterministic: Boolean = true

  def dataType(args: util.List[DataType]): DataType
}
