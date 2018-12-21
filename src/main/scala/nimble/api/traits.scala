package nimble.api

import java.util

import org.apache.spark.sql.types.DataType

trait IndexedRecord {
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
  def name: String
  def nullable: Boolean = true
  def deterministic: Boolean = true

  def dataType(args: util.List[DataType]): DataType
}

trait UDF1[-T,+F] extends (T => F) with UDF
trait UDF2[-T1, -T2, +F] extends ((T1, T2) => F) with UDF
trait UDF3[-T1, -T2, -T3, +F] extends ((T1, T2, T3) => F) with UDF
trait UDF4[-T1, -T2, -T3, -T4, +F] extends ((T1, T2, T3, T4) => F) with UDF
trait UDF5[-T1, -T2, -T3, -T4, -T5, +F] extends ((T1, T2, T3, T4, T5) => F) with UDF
